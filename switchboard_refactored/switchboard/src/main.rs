//! Switchboard — zero-copy async message router.

use switchboard::{connection::Connection, router::Router};

use std::net::SocketAddr;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

// `Connection` and `Router` are re-exported from the library crate

#[derive(Debug)]
struct Config {
    bind: SocketAddr,
    client: Option<ClientMode>,
}

#[derive(Debug)]
enum ClientMode {
    Subscribe { topic: String },
    Publish    { topic: String, message: String },
}

fn parse_args() -> Result<Config> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut port    = 7777u16;
    let mut host    = "0.0.0.0".to_string();
    let mut client  = None;
    let mut topic   = String::new();
    let mut message = String::new();
    let mut mode    = String::new();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--port"    => { port    = args[i+1].parse().context("invalid port")?; i += 2; }
            "--host"    => { host    = args[i+1].clone(); i += 2; }
            "--client"  => { mode    = args[i+1].clone(); i += 2; }
            "--topic"   => { topic   = args[i+1].clone(); i += 2; }
            "--message" => { message = args[i+1].clone(); i += 2; }
            _           => { i += 1; }
        }
    }

    if !mode.is_empty() {
        client = Some(match mode.as_str() {
            "subscribe" => ClientMode::Subscribe { topic },
            "publish"   => ClientMode::Publish { topic, message },
            other       => anyhow::bail!("unknown client mode: {}", other),
        });
    }

    Ok(Config {
        bind: format!("{}:{}", host, port).parse()?,
        client,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("switchboard=info".parse()?))
        .init();

    let config = parse_args()?;

    match config.client {
        Some(mode) => run_client(config.bind, mode).await,
        None       => run_server(config.bind).await,
    }
}

async fn run_server(bind: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding to {}", bind))?;

    let router = Router::new();
    info!(addr = %bind, "switchboard listening");

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("accepting TCP connection")?;

        stream.set_nodelay(true)?;

        let router = router.clone();
        tokio::spawn(async move {
            let conn = Connection::new(stream, peer, router);
            if let Err(e) = conn.run().await {
                error!(peer = %peer, error = %e, "connection error");
            }
        });
    }
}

async fn run_client(server: SocketAddr, mode: ClientMode) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use switchboard::protocol::{encode_publish, encode_subscribe, Frame};
    use bytes::BytesMut;

    let mut stream = tokio::net::TcpStream::connect(server)
        .await
        .with_context(|| format!("connecting to {}", server))?;
    stream.set_nodelay(true)?;

    match mode {
        ClientMode::Subscribe { topic } => {
            info!(topic = %topic, "subscribing");
            let frame = encode_subscribe(&topic);
            stream.write_all(&frame).await?;

            info!("waiting for messages (Ctrl-C to quit)…");

            let mut read_buf = BytesMut::with_capacity(4096);
            loop {
                let mut header = [0u8; 4];
                match stream.read_exact(&mut header).await {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        info!("server closed connection");
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }

                let length = u32::from_be_bytes(header) as usize;
                read_buf.resize(length, 0);
                stream.read_exact(&mut read_buf[..length]).await?;

                let raw = read_buf.split_to(length).freeze();
                match Frame::parse(raw) {
                    Ok(Frame::Publish { topic, payload }) => {
                        let topic_str   = String::from_utf8_lossy(&topic);
                        let payload_str = String::from_utf8_lossy(&payload);
                        println!("[{}] {}", topic_str, payload_str);
                    }
                    Ok(other) => {
                        info!(?other, "unexpected frame type from server");
                    }
                    Err(e) => {
                        error!(error = %e, "failed to parse server frame");
                    }
                }
            }
        }

        ClientMode::Publish { topic, message } => {
            info!(topic = %topic, message = %message, "publishing");
            let frame = encode_publish(&topic, message.as_bytes());
            stream.write_all(&frame).await?;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            info!("published");
        }
    }

    Ok(())
}
