use crate::errors::{ProtocolError, WsError};
use crate::frame::{get_bit, HeaderView, OpCode, SimplifiedHeader};
use crate::protocol::{cal_accept_key, standard_handshake_req_check};
use bytes::BytesMut;
use http;
use std::fmt::Debug;
use std::ops::Range;

#[cfg(feature = "sync")]
mod blocking;

#[cfg(feature = "sync")]
pub use blocking::*;

#[cfg(feature = "async")]
mod non_blocking;

#[cfg(feature = "async")]
pub use non_blocking::*;

#[cfg(feature = "async_monoio")]
mod non_blocking_monoio;

#[cfg(feature = "async_monoio")]
pub use non_blocking_monoio::*;

/// text frame utf-8 checking policy
#[derive(Debug, Clone)]
pub enum ValidateUtf8Policy {
    /// no not validate utf
    Off,
    /// fail if fragment frame payload is not valid utf8
    FastFail,
    /// check utf8 after merged
    On,
}

#[allow(missing_docs)]
impl ValidateUtf8Policy {
    pub fn should_check(&self) -> bool {
        !matches!(self, Self::Off)
    }

    pub fn is_fast_fail(&self) -> bool {
        matches!(self, Self::FastFail)
    }
}

/// frame send/recv config
#[derive(Debug, Clone)]
pub struct FrameConfig {
    /// check rsv1 bits
    pub check_rsv: bool,
    /// auto mask send frame payload, for client, it must be true
    pub mask_send_frame: bool,
    /// allocate new buf for every frame
    pub renew_buf_on_write: bool,
    /// auto unmask a masked frame payload
    pub auto_unmask: bool,
    /// limit max payload size
    pub max_frame_payload_size: usize,
    /// auto split size, if set 0, do not split frame
    pub auto_fragment_size: usize,
    /// auto merge fragmented frames into one frame
    pub merge_frame: bool,
    /// utf8 check policy
    pub validate_utf8: ValidateUtf8Policy,
    /// resize size of read buf, default 4K
    pub resize_size: usize,
    /// if available len < resize, resize read buf, default 1K
    pub resize_thresh: usize,
}

impl Default for FrameConfig {
    fn default() -> Self {
        Self {
            check_rsv: true,
            mask_send_frame: true,
            renew_buf_on_write: false,
            auto_unmask: true,
            max_frame_payload_size: 0,
            auto_fragment_size: 0,
            merge_frame: true,
            validate_utf8: ValidateUtf8Policy::FastFail,
            resize_size: 4096,
            resize_thresh: 1024,
        }
    }
}

/// apply websocket mask to buf by given key
#[inline]
pub fn apply_mask(buf: &mut [u8], mask: [u8; 4]) {
    apply_mask_array_chunk(buf, mask)
}

#[inline]
fn apply_mask_array_chunk(buf: &mut [u8], mask: [u8; 4]) {
    let mask32 = u32::from_ne_bytes(mask);
    let mut iter = buf.chunks_exact_mut(4);
    while let Some(chunk) = iter.next() {
        let val: &mut u32 = unsafe { std::mem::transmute(chunk.as_mut_ptr().cast::<u32>()) };
        *val ^= mask32;
    }
    for (i, byte) in iter.into_remainder().iter_mut().enumerate() {
        *byte ^= mask[i & 3];
    }
}

/// websocket frame reader
pub struct FrameReadState {
    fragmented: bool,
    config: FrameConfig,
    fragmented_data: Vec<u8>,
    fragmented_type: OpCode,
    buf: FrameBuffer,
}

impl Default for FrameReadState {
    fn default() -> Self {
        Self {
            fragmented: false,
            config: Default::default(),
            fragmented_data: vec![],
            fragmented_type: OpCode::default(),
            buf: FrameBuffer::new(),
        }
    }
}

impl FrameReadState {
    /// construct with config
    pub fn with_config(config: FrameConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    /// check if data in buffer is enough to parse frame header
    pub fn is_header_ok(&self) -> bool {
        let ava_data = self.buf.ava_data();
        if ava_data.len() < 2 {
            false
        } else {
            let len = ava_data[1] & 0b01111111;
            let mask = get_bit(&ava_data, 1, 0);
            let mut min_len = match len {
                0..=125 => 2,
                126 => 4,
                127 => 10,
                _ => unreachable!(),
            };
            if mask {
                min_len += 4;
            }
            ava_data.len() >= min_len
        }
    }

    /// return current frame header bits of buffer
    #[inline]
    pub fn get_leading_bits(&self) -> u8 {
        self.buf.ava_data()[0] >> 4
    }

    /// try to parse frame header in buffer, return (header_len, payload_len, header_len + payload_len)
    #[inline]
    pub fn parse_frame_header(&mut self) -> Result<(usize, usize, usize), WsError> {
        let ava_data = self.buf.ava_data();
        let leading_bits = self.get_leading_bits();
        let max_payload_size = self.config.max_frame_payload_size;
        let check_rsv = self.config.check_rsv;

        fn parse_payload_len(source: &[u8]) -> Result<(usize, usize), ProtocolError> {
            match source[1] {
                len @ (0..=125 | 128..=253) => Ok((1, (len & 127) as usize)),
                126 | 254 => {
                    if source.len() < 4 {
                        return Err(ProtocolError::InsufficientLen(source.len()));
                    }
                    Ok((
                        1 + 2,
                        u16::from_be_bytes((&source[2..4]).try_into().unwrap()) as usize,
                    ))
                }
                127 | 255 => {
                    if source.len() < 10 {
                        return Err(ProtocolError::InsufficientLen(source.len()));
                    }
                    Ok((
                        1 + 8,
                        usize::from_be_bytes((&source[2..(8 + 2)]).try_into().unwrap()),
                    ))
                }
            }
        }

        if check_rsv && !(leading_bits == 0b00001000 || leading_bits == 0b00000000) {
            return Err(WsError::ProtocolError {
                close_code: 1008,
                error: ProtocolError::InvalidLeadingBits(leading_bits),
            });
        }
        let (len_occ_bytes, payload_len) =
            parse_payload_len(ava_data).map_err(|e| WsError::ProtocolError {
                close_code: 1008,
                error: e,
            })?;

        if max_payload_size > 0 && payload_len > max_payload_size {
            return Err(WsError::ProtocolError {
                close_code: 1008,
                error: ProtocolError::PayloadTooLarge(max_payload_size),
            });
        }
        let mask = get_bit(ava_data, 1, 0);
        let header_len = 1 + len_occ_bytes + if mask { 4 } else { 0 };
        Ok((header_len, payload_len, header_len + payload_len))
    }

    /// get a frame and reset state
    #[inline]
    pub fn consume_frame(
        &mut self,
        header_len: usize,
        payload_len: usize,
        total_len: usize,
    ) -> (SimplifiedHeader, Range<usize>) {
        let buf = &mut self.buf;
        let auto_unmask = self.config.auto_unmask;

        let ava_data = buf.ava_mut_data();
        let (header_data, remain) = ava_data.split_at_mut(header_len);
        let header = HeaderView(header_data);
        let payload = remain.split_at_mut(payload_len).0;
        if auto_unmask {
            if let Some(mask) = header.masking_key() {
                apply_mask(payload, mask)
            }
        }
        let header: SimplifiedHeader = header.into();
        let s_idx = buf.consume_idx + header_len;
        let e_idx = s_idx + payload_len;
        buf.consume(total_len);
        (header, s_idx..e_idx)
    }

    fn check_frame(
        &mut self,
        header: SimplifiedHeader,
        range: Range<usize>,
    ) -> Result<(), WsError> {
        let fragmented = &mut self.fragmented;
        let utf8_policy = &self.config.validate_utf8;
        let payload = &self.buf.buf[range];
        match header.code {
            OpCode::Continue => {
                if !*fragmented {
                    return Err(WsError::ProtocolError {
                        close_code: 1002,
                        error: ProtocolError::MissInitialFragmentedFrame,
                    });
                }
                if header.fin {
                    *fragmented = false;
                }
                Ok(())
            }
            OpCode::Binary => {
                if *fragmented {
                    return Err(WsError::ProtocolError {
                        close_code: 1002,
                        error: ProtocolError::NotContinueFrameAfterFragmented,
                    });
                }
                *fragmented = !header.fin;
                Ok(())
            }
            OpCode::Text => {
                if *fragmented {
                    return Err(WsError::ProtocolError {
                        close_code: 1002,
                        error: ProtocolError::NotContinueFrameAfterFragmented,
                    });
                }
                if !header.fin {
                    *fragmented = true;
                    if header.code == OpCode::Text
                        && utf8_policy.is_fast_fail()
                        && simdutf8::basic::from_utf8(payload).is_err()
                    {
                        return Err(WsError::ProtocolError {
                            close_code: 1007,
                            error: ProtocolError::InvalidUtf8,
                        });
                    }

                    Ok(())
                } else {
                    if header.code == OpCode::Text
                        && utf8_policy.should_check()
                        && simdutf8::basic::from_utf8(payload).is_err()
                    {
                        return Err(WsError::ProtocolError {
                            close_code: 1007,
                            error: ProtocolError::InvalidUtf8,
                        });
                    }
                    Ok(())
                }
            }
            OpCode::Close | OpCode::Ping | OpCode::Pong => {
                if !header.fin {
                    return Err(WsError::ProtocolError {
                        close_code: 1002,
                        error: ProtocolError::FragmentedControlFrame,
                    });
                }
                let payload_len = payload.len();
                if payload.len() > 125 {
                    let error = ProtocolError::ControlFrameTooBig(payload_len);
                    return Err(WsError::ProtocolError {
                        close_code: 1002,
                        error,
                    });
                }
                if header.code == OpCode::Close {
                    if payload_len == 1 {
                        let error = ProtocolError::InvalidCloseFramePayload;
                        return Err(WsError::ProtocolError {
                            close_code: 1002,
                            error,
                        });
                    }
                    if payload_len >= 2 {
                        // check close code
                        let mut code_byte = [0u8; 2];
                        code_byte.copy_from_slice(&payload[..2]);
                        let code = u16::from_be_bytes(code_byte);
                        if code < 1000
                            || (1004..=1006).contains(&code)
                            || (1015..=2999).contains(&code)
                            || code >= 5000
                        {
                            let error = ProtocolError::InvalidCloseCode(code);
                            return Err(WsError::ProtocolError {
                                close_code: 1002,
                                error,
                            });
                        }

                        // utf-8 validation
                        if String::from_utf8(payload[2..].to_vec()).is_err() {
                            let error = ProtocolError::InvalidUtf8;
                            return Err(WsError::ProtocolError {
                                close_code: 1007,
                                error,
                            });
                        }
                    }
                }
                Ok(())
            }
            _ => Err(WsError::UnsupportedFrame(header.code)),
        }
    }

    /// This method is technically private, but custom parsers are allowed to use it.
    #[doc(hidden)]
    #[inline]
    pub fn merge_frame(
        &mut self,
        header: SimplifiedHeader,
        range: Range<usize>,
    ) -> Result<Option<bool>, WsError> {
        let fragmented = &mut self.fragmented;
        let fragmented_data = &mut self.fragmented_data;
        let fragmented_type = &mut self.fragmented_type;
        let payload = &self.buf.buf[range];
        match header.code {
            OpCode::Continue => {
                fragmented_data.extend_from_slice(payload);
                if header.fin {
                    *fragmented = false;
                    Ok(Some(true))
                } else {
                    Ok(None)
                }
            }
            OpCode::Text | OpCode::Binary => {
                *fragmented_type = header.code;
                if !header.fin {
                    *fragmented = true;
                    *fragmented_type = header.code;
                    fragmented_data.clear();
                    fragmented_data.extend_from_slice(payload);
                    Ok(None)
                } else {
                    Ok(Some(false))
                }
            }
            OpCode::Close | OpCode::Ping | OpCode::Pong => Ok(Some(false)),
            _ => unreachable!(),
        }
    }
}

pub(crate) struct FrameBuffer {
    pub(crate) buf: Vec<u8>,
    tmp: Vec<u8>,
    produce_idx: usize,
    consume_idx: usize,
}

impl FrameBuffer {
    pub(crate) fn new() -> Self {
        Self {
            buf: vec![0; 8192],
            tmp: vec![0; 8192],
            produce_idx: 0,
            consume_idx: 0,
        }
    }

    pub(crate) fn prepare_idx_and_size(&mut self, payload_size: usize) -> [usize; 3] {
        let remain = self.buf.len() - self.produce_idx;
        if remain >= payload_size {
            [
                self.produce_idx,
                self.produce_idx + payload_size,
                self.buf[self.produce_idx..(self.produce_idx + payload_size)].len(),
            ]
        } else {
            if self.produce_idx == self.consume_idx {
                if payload_size > self.buf.len() {
                    self.buf.resize(payload_size, 0);
                }
                self.consume_idx = 0;
                self.produce_idx = 0;
                [0, payload_size, self.buf[0..payload_size].len()]
            } else {
                self.tmp.resize(self.produce_idx - self.consume_idx, 0);
                self.tmp
                    .copy_from_slice(&self.buf[self.consume_idx..self.produce_idx]);
                if payload_size + self.tmp.len() > self.buf.len() {
                    self.buf.resize(payload_size + self.tmp.len(), 0);
                }
                self.buf[..(self.tmp.len())].copy_from_slice(&self.tmp);
                self.consume_idx = 0;
                self.produce_idx = self.tmp.len();
                [
                    self.produce_idx,
                    self.produce_idx + payload_size,
                    self.buf[self.produce_idx..(self.produce_idx + payload_size)].len(),
                ]
            }
        }
    }

    pub(crate) fn prepare(&mut self, payload_size: usize) -> &mut [u8] {
        let remain = self.buf.len() - self.produce_idx;
        if remain >= payload_size {
            &mut self.buf[self.produce_idx..(self.produce_idx + payload_size)]
        } else {
            if self.produce_idx == self.consume_idx {
                if payload_size > self.buf.len() {
                    self.buf.resize(payload_size, 0);
                }
                self.consume_idx = 0;
                self.produce_idx = 0;
                &mut self.buf[0..payload_size]
            } else {
                self.tmp.resize(self.produce_idx - self.consume_idx, 0);
                self.tmp
                    .copy_from_slice(&self.buf[self.consume_idx..self.produce_idx]);
                if payload_size + self.tmp.len() > self.buf.len() {
                    self.buf.resize(payload_size + self.tmp.len(), 0);
                }
                self.buf[..(self.tmp.len())].copy_from_slice(&self.tmp);
                self.consume_idx = 0;
                self.produce_idx = self.tmp.len();
                &mut self.buf[self.produce_idx..(self.produce_idx + payload_size)]
            }
        }
    }

    pub(crate) fn ava_data(&self) -> &[u8] {
        &self.buf[self.consume_idx..self.produce_idx]
    }

    pub(crate) fn ava_mut_data(&mut self) -> &mut [u8] {
        &mut self.buf[self.consume_idx..self.produce_idx]
    }

    pub(crate) fn produce(&mut self, num: usize) {
        self.produce_idx += num;
    }

    pub(crate) fn consume(&mut self, num: usize) {
        self.consume_idx += num;
    }
}

/// websocket writing state
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct FrameWriteState {
    config: FrameConfig,
    header_buf: [u8; 14],
    buf: BytesMut,
}

impl FrameWriteState {
    /// construct with config
    pub fn with_config(config: FrameConfig) -> Self {
        Self {
            config,
            header_buf: [0; 14],
            buf: BytesMut::new(),
        }
    }
}

/// do standard handshake check and return response
pub fn default_handshake_handler(
    req: http::Request<()>,
) -> Result<(http::Request<()>, http::Response<String>), (http::Response<String>, WsError)> {
    match standard_handshake_req_check(&req) {
        Ok(_) => {
            let key = req.headers().get("sec-websocket-key").unwrap();
            let resp = http::Response::builder()
                .version(http::Version::HTTP_11)
                .status(http::StatusCode::SWITCHING_PROTOCOLS)
                .header("Upgrade", "WebSocket")
                .header("Connection", "Upgrade")
                .header("Sec-WebSocket-Accept", cal_accept_key(key.as_bytes()))
                .body(String::new())
                .unwrap();
            Ok((req, resp))
        }
        Err(e) => {
            let resp = http::Response::builder()
                .version(http::Version::HTTP_11)
                .status(http::StatusCode::BAD_REQUEST)
                .header("Content-Type", "text/html")
                .body(e.to_string())
                .unwrap();
            Err((resp, e))
        }
    }
}
