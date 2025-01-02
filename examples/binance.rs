use clap::Parser;
use monoio_compat::StreamWrapper;
use rustls::crypto::CryptoProvider;
use tracing::Level;
use tracing_subscriber::util::SubscriberInitExt;
use ws_tool::{
    codec::{AsyncStringCodec, MonoioStringCodec},
    connector::{async_monoio_tcp_connect, async_wrap_monoio_rustls, get_host},
    ClientBuilder,
};

// /// websocket client connect to binance futures websocket
// #[derive(Parser)]
// struct Args {
//     /// channel name, such as btcusdt@depth20
//     channels: Vec<String>,
//
//     /// http proxy port
//     #[arg(long, default_value = "1088")]
//     hp_port: u16,
//
//     /// http proxy host
//     #[arg(long)]
//     hp_host: Option<String>,
//
//     ///http proxy auth fmt -> username:password
//     #[arg(long)]
//     hp_auth: Option<String>,
//     /// socks5 proxy port
//     #[arg(long, default_value = "1087")]
//     sp_port: u16,
//
//     /// socks5 proxy host
//     #[arg(long)]
//     sp_host: Option<String>,
//
//     /// socks5 proxy auth fmt -> username:password
//     #[arg(long)]
//     sp_auth: Option<String>,
// }

#[monoio::main]
async fn main() -> Result<(), ()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::ERROR)
        .finish()
        .try_init()
        .expect("failed to init log");
    // let args = Args::parse();
    // let channels = args.channels.join("/");
    let uri: http::Uri = format!("wss://fstream.binance.com/stream?streams=btcusdt@bookTicker")
        .parse()
        .unwrap();
    let builder = ClientBuilder::new();
    // let stream = if let Some(host) = args.hp_host {
    //     let auth = args
    //         .hp_auth
    //         .map(|auth| {
    //             let (user, passwd) = auth.split_once(':').expect("invalid auth format");
    //             hproxy::AuthCredential::Basic {
    //                 user: user.trim().into(),
    //                 passwd: passwd.trim().into(),
    //             }
    //         })
    //         .unwrap_or(hproxy::AuthCredential::None);
    //     let config = hproxy::ProxyConfig {
    //         host,
    //         port: args.hp_port,
    //         auth,
    //         keep_alive: true,
    //     };
    //     hproxy::async_create_conn(&config, "fstream.binance.com")
    //         .await
    //         .unwrap()
    // } else if let Some(host) = args.sp_host {
    //     let auth = args
    //         .sp_auth
    //         .map(|auth| {
    //             let (user, passwd) = auth.split_once(':').expect("invalid auth format");
    //             sproxy::AuthCredential::Basic {
    //                 user: user.trim().into(),
    //                 passwd: passwd.trim().into(),
    //             }
    //         })
    //         .unwrap_or(sproxy::AuthCredential::None);
    //     let config = sproxy::ProxyConfig {
    //         host,
    //         port: args.sp_port,
    //         auth,
    //     };
    //     let (stream, _, _) = sproxy::async_create_conn(&config, "fstream.binance.com".into(), 443)
    //         .await
    //         .unwrap();
    //     stream
    // } else {
    let stream = async_monoio_tcp_connect(&uri).await.unwrap();
    // };
    let stream = async_wrap_monoio_rustls(stream, get_host(&uri).unwrap().to_string())
        .await
        .unwrap();

    let mut client = builder
        .async_with_monoio_stream(uri, stream, MonoioStringCodec::check_fn)
        .await
        .unwrap();

    loop {
        let message = client.receive().await;
        match message {
            Ok(message) => println!("{:?}", message),
            Err(why) => {
                println!("error {why}");
                break;
            }
        }
    }

    Ok(())
}
