use bytes::BytesMut;
use http;
use monoio::{
    buf::VecBuf,
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
    start,
};
use std::{io::IoSlice, ops::Range};

use super::{apply_mask, FrameConfig, FrameReadState, FrameWriteState};
use crate::{
    codec::Split,
    errors::WsError,
    frame::{ctor_header, header_len, OpCode, OwnedFrame, SimplifiedHeader},
    protocol::standard_handshake_resp_check,
};

type IOResult<T> = std::io::Result<T>;

impl FrameReadState {
    #[inline]
    async fn monoio_poll<S: AsyncReadRent + Unpin>(&mut self, stream: &mut S) -> IOResult<usize> {
        let [start_idx, mut finish_idx, buf_size] =
            self.buf.prepare_idx_and_size(self.config.resize_size);
        let buf = BytesMut::with_capacity(buf_size);
        let (result, read_buf) = stream.read(buf).await;
        let count = result?;
        if finish_idx > count {
            finish_idx = start_idx + count;
        }
        self.buf.buf[start_idx..finish_idx].copy_from_slice(&read_buf);
        self.buf.produce(count);
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "read eof",
            ));
        }
        Ok(count)
    }

    #[inline]
    async fn monoio_poll_one_frame<S: AsyncReadRent + Unpin>(
        &mut self,
        stream: &mut S,
        size: usize,
    ) -> IOResult<()> {
        let read_len = self.buf.ava_data().len();
        if read_len < size {
            let [start_idx, end_idx, buf_len] = self.buf.prepare_idx_and_size(size - read_len);
            let buf = BytesMut::with_capacity(buf_len);
            let (result, read_buf) = stream.read_exact(buf).await;
            let _read_bytes = result?;
            self.buf.buf[start_idx..end_idx].copy_from_slice(&read_buf);
            self.buf.produce(size - read_len);
        }
        Ok(())
    }

    #[inline]
    async fn monoio_read_one_frame<S: AsyncReadRent + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<(SimplifiedHeader, Range<usize>), WsError> {
        while !self.is_header_ok() {
            self.monoio_poll(stream).await?;
        }
        let (header_len, payload_len, total_len) = self.parse_frame_header()?;
        self.monoio_poll_one_frame(stream, total_len).await?;
        Ok(self.consume_frame(header_len, payload_len, total_len))
    }

    /// **NOTE** masked frame has already been unmasked
    pub async fn monoio_receive<S: AsyncReadRent + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<(SimplifiedHeader, &[u8]), WsError> {
        if self.config.merge_frame {
            loop {
                let (mut header, range) = self.monoio_read_one_frame(stream).await?;
                if let Some(merged) = self
                    .check_frame(header, range.clone())
                    .and_then(|_| self.merge_frame(header, range.clone()))?
                {
                    if merged {
                        header.code = self.fragmented_type;
                        break Ok((header, &self.fragmented_data));
                    } else {
                        break Ok((header, &self.buf.buf[range]));
                    }
                }
            }
        } else {
            let (header, range) = self.monoio_read_one_frame(stream).await?;
            self.check_frame(header, range.clone())?;
            Ok((header, &self.buf.buf[range]))
        }
    }
}

impl FrameWriteState {
    /// send immutable payload
    ///
    /// if need to mask, copy data to inner buffer and then apply mask
    ///
    /// will auto fragment if auto_fragment_size > 0
    pub async fn monoio_send<S: AsyncWriteRent + Unpin>(
        &mut self,
        stream: &mut S,
        opcode: OpCode,
        payload: &[u8],
    ) -> IOResult<()> {
        if payload.is_empty() {
            let mask = if self.config.mask_send_frame {
                Some(rand::random())
            } else {
                None
            };
            let header = ctor_header(
                &mut self.header_buf,
                true,
                false,
                false,
                false,
                mask,
                opcode,
                0,
            );
            // let write_result = stream.write_all(header.to_vec()).await;
            let write_result = stream.write_all(header.to_vec()).await;
            let _written_bytes = write_result.0?;
            return Ok(());
        }
        if self.config.auto_fragment_size > 0 && self.config.auto_fragment_size < payload.len() {
            let chunk_size = self.config.auto_fragment_size;
            let parts: Vec<&[u8]> = payload.chunks(chunk_size).collect();
            let total = parts.len();
            let single_header_len = header_len(self.config.mask_send_frame, chunk_size as u64);
            let mut header_buf_len = single_header_len * parts.len();
            let left = payload.len() % chunk_size;
            if left != 0 {
                header_buf_len += header_len(self.config.mask_send_frame, left as u64);
            }
            let total_bytes = payload.len() + header_buf_len;
            if self.config.mask_send_frame {
                if self.buf.len() < total_bytes {
                    self.buf.resize(total_bytes, 0);
                }
                parts.iter().enumerate().for_each(|(idx, chunk)| {
                    let fin = idx + 1 == total;
                    let s_idx = idx * single_header_len;
                    let mask = rand::random();
                    let header_len = ctor_header(
                        &mut self.buf[s_idx..],
                        fin,
                        false,
                        false,
                        false,
                        mask,
                        opcode,
                        chunk.len() as u64,
                    )
                    .len();
                    let slice = &mut self.buf[(s_idx + header_len)..];
                    slice.copy_from_slice(chunk);
                    apply_mask(slice, mask);
                });
                let write_result = stream.write_all(self.buf[..total_bytes].to_vec()).await;
                let _written_bytes = write_result.0?;
            } else {
                if self.buf.len() < header_buf_len {
                    self.buf.resize(header_buf_len, 0);
                }
                let mut slices = Vec::with_capacity(total * 2);
                parts.iter().enumerate().for_each(|(idx, chunk)| {
                    let fin = idx + 1 == total;
                    let s_idx = idx * chunk_size;
                    ctor_header(
                        &mut self.buf[s_idx..],
                        fin,
                        false,
                        false,
                        false,
                        None,
                        opcode,
                        chunk.len() as u64,
                    );
                });
                parts.iter().enumerate().for_each(|(idx, chunk)| {
                    let fin = idx + 1 == total;
                    if fin {
                        slices.push(IoSlice::new(&self.buf[(idx * single_header_len)..]).to_vec())
                    } else {
                        slices.push(
                            IoSlice::new(
                                &self.buf[(idx * single_header_len)..(idx + 1) * single_header_len],
                            )
                            .to_vec(),
                        )
                    }
                    slices.push(IoSlice::new(chunk).to_vec());
                });

                let (result, _) = stream
                    .write_vectored_all(VecBuf::from(slices.to_owned()))
                    .await;
                let num = result?;
                let remain = total_bytes - num;
                if remain > 0 {
                    if let Some(buf) = slices.last() {
                        let write_result =
                            stream.write_all(buf[(buf.len() - remain)..].to_vec()).await;
                        let _written_bytes = write_result.0?;
                    }
                }
            }
        } else if self.config.mask_send_frame {
            let total_bytes = header_len(true, payload.len() as u64) + payload.len();
            let mask: [u8; 4] = rand::random();
            let header = ctor_header(
                &mut self.header_buf,
                true,
                false,
                false,
                false,
                mask,
                opcode,
                payload.len() as u64,
            );
            if self.buf.len() < payload.len() {
                self.buf.resize(payload.len(), 0)
            }
            self.buf[..(payload.len())].copy_from_slice(payload);
            apply_mask(&mut self.buf[..(payload.len())], mask);
            let (result, _) = stream
                .write_vectored_all(VecBuf::from(
                    [header.to_vec(), self.buf[..(payload.len())].to_vec()].to_vec(),
                ))
                .await;
            let num = result?;
            let remain = total_bytes - num;
            if remain > 0 {
                let (result, _) = stream
                    .write_all(self.buf[(payload.len() - remain)..(payload.len())].to_vec())
                    .await;
                let _written_bytes = result?;
            }
        } else {
            let total_bytes = header_len(false, payload.len() as u64) + payload.len();
            let header = ctor_header(
                &mut self.header_buf,
                true,
                false,
                false,
                false,
                None,
                opcode,
                payload.len() as u64,
            );
            // if self.buf.len() < payload.len() {
            //     self.buf.resize(payload.len(), 0)
            // }
            let (result, _) = stream
                .write_vectored_all(VecBuf::from([header.to_vec(), payload.to_vec()].to_vec()))
                .await;
            let num = result?;
            let remain = total_bytes - num;
            if remain > 0 {
                let (result, _) = stream
                    .write_all(payload[(payload.len() - remain)..].to_vec())
                    .await;
                let _written_bytes = result?;
            }
        };

        if self.config.renew_buf_on_write {
            self.buf = BytesMut::new()
        }
        Ok(())
    }

    pub(crate) async fn monoio_send_owned_frame<S: AsyncWriteRent + Unpin>(
        &mut self,
        stream: &mut S,
        frame: OwnedFrame,
    ) -> IOResult<()> {
        let (result, _) = stream.write_all(frame.header().0.to_vec()).await;
        let _written_bytes = result?;
        let (result, _) = stream.write_all(frame.payload().to_vec()).await;
        let _written_bytes = result?;
        Ok(())
    }
}

/// recv part of websocket stream
pub struct MonoioFrameRecv<S: AsyncReadRent> {
    stream: S,
    read_state: FrameReadState,
}

impl<S: AsyncReadRent + Unpin> MonoioFrameRecv<S> {
    /// construct method
    pub fn new(stream: S, read_state: FrameReadState) -> Self {
        Self { stream, read_state }
    }

    /// receive a frame
    pub async fn receive(&mut self) -> Result<(SimplifiedHeader, &[u8]), WsError> {
        self.read_state.monoio_receive(&mut self.stream).await
    }
}

/// send part of websocket frame
pub struct MonoioFrameSend<S: AsyncWriteRent> {
    stream: S,
    write_state: FrameWriteState,
}

impl<S: AsyncWriteRent + Unpin> MonoioFrameSend<S> {
    /// construct method
    pub fn new(stream: S, write_state: FrameWriteState) -> Self {
        Self {
            stream,
            write_state,
        }
    }

    /// send immutable payload
    ///
    /// will auto fragment if auto_fragment_size > 0
    pub async fn send(&mut self, opcode: OpCode, payload: &[u8]) -> Result<(), WsError> {
        self.write_state
            .monoio_send(&mut self.stream, opcode, payload)
            .await
            .map_err(WsError::IOError)
    }

    /// send a read frame, **this method will not check validation of frame and do not fragment**
    pub async fn send_owned_frame(&mut self, frame: OwnedFrame) -> Result<(), WsError> {
        self.write_state
            .monoio_send_owned_frame(&mut self.stream, frame)
            .await
            .map_err(WsError::IOError)
    }

    /// flush to ensure all data are send
    pub async fn flush(&mut self) -> Result<(), WsError> {
        self.stream.flush().await.map_err(WsError::IOError)
    }
}

/// recv/send websocket frame
pub struct MonoioFrameCodec<S: AsyncReadRentExt + AsyncWriteRentExt> {
    /// underlying transport stream
    pub stream: S,
    /// read state
    pub read_state: FrameReadState,
    /// write state
    pub write_state: FrameWriteState,
}

impl<S: AsyncReadRent + AsyncWriteRent + Unpin> MonoioFrameCodec<S> {
    /// construct method
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            read_state: FrameReadState::default(),
            write_state: FrameWriteState::default(),
        }
    }

    /// construct with stream and config
    pub fn new_with(stream: S, config: FrameConfig) -> Self {
        Self {
            stream,
            read_state: FrameReadState::with_config(config.clone()),
            write_state: FrameWriteState::with_config(config),
        }
    }

    /// get mutable underlying stream
    pub fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// used for server side to construct a new server
    pub fn factory(_req: http::Request<()>, stream: S) -> Result<Self, WsError> {
        let config = FrameConfig {
            mask_send_frame: false,
            ..Default::default()
        };
        Ok(Self::new_with(stream, config))
    }

    /// used to client side to construct a new client
    pub fn check_fn(key: String, resp: http::Response<()>, stream: S) -> Result<Self, WsError> {
        standard_handshake_resp_check(key.as_bytes(), &resp)?;
        Ok(Self::new_with(stream, FrameConfig::default()))
    }

    /// receive a frame
    pub async fn receive(&mut self) -> Result<(SimplifiedHeader, &[u8]), WsError> {
        self.read_state.monoio_receive(&mut self.stream).await
    }

    /// send payload
    ///
    /// will auto fragment if auto_fragment_size > 0
    pub async fn send(&mut self, opcode: OpCode, payload: &[u8]) -> Result<(), WsError> {
        self.write_state
            .monoio_send(&mut self.stream, opcode, payload)
            .await
            .map_err(WsError::IOError)
    }

    /// send a read frame, **this method will not check validation of frame and do not fragment**
    pub async fn send_owned_frame(&mut self, frame: OwnedFrame) -> Result<(), WsError> {
        self.write_state
            .monoio_send_owned_frame(&mut self.stream, frame)
            .await
            .map_err(WsError::IOError)
    }

    /// flush to ensure all data are send
    pub async fn flush(&mut self) -> Result<(), WsError> {
        self.stream.flush().await.map_err(WsError::IOError)
    }
}

impl<R, W, S> MonoioFrameCodec<S>
where
    R: AsyncReadRent + Unpin,
    W: AsyncWriteRent + Unpin,
    S: AsyncReadRent + AsyncWriteRent + Unpin + Split<R = R, W = W>,
{
    /// split codec to recv and send parts
    pub fn split(self) -> (MonoioFrameRecv<R>, MonoioFrameSend<W>) {
        let MonoioFrameCodec {
            stream,
            read_state,
            write_state,
        } = self;
        let (read, write) = stream.split();
        (
            MonoioFrameRecv::new(read, read_state),
            MonoioFrameSend::new(write, write_state),
        )
    }
}
