use anyhow::{Context, Result};
use std::{env, net::SocketAddr, time::Instant};
use tokio::{io::AsyncWriteExt, net::TcpStream, task};

struct Config {
    server: SocketAddr,
    topics: usize,
    messages: usize,
    parallel: usize,
    payload_size: usize,
}

fn parse_args() -> Result<Config> {
    let mut server = "127.0.0.1:7777".to_string();
    let mut topics = 1000;
    let mut messages = 100_000;
    let mut parallel = 4;
    let mut payload_size = 64;

    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--server" => {
                i += 1;
                server = args[i].clone();
            }
            "--topics" => {
                i += 1;
                topics = args[i].parse().context("invalid topics")?;
            }
            "--messages" => {
                i += 1;
                messages = args[i].parse().context("invalid messages")?;
            }
            "--parallel" => {
                i += 1;
                parallel = args[i].parse().context("invalid parallel count")?;
            }
            "--payload-size" => {
                i += 1;
                payload_size = args[i].parse().context("invalid payload size")?;
            }
            other => anyhow::bail!("unknown argument: {}", other),
        }
        i += 1;
    }

    Ok(Config {
        server: server.parse().context("invalid server address")?,
        topics,
        messages,
        parallel,
        payload_size,
    })
}

fn encode_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let frame_len = 1 + 2 + topic_bytes.len() + payload.len();
    let mut buffer = Vec::with_capacity(4 + frame_len);
    buffer.extend_from_slice(&(frame_len as u32).to_be_bytes());
    buffer.push(0x02);
    buffer.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
    buffer.extend_from_slice(topic_bytes);
    buffer.extend_from_slice(payload);
    buffer
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_args()?;
    let payload = vec![b'x'; config.payload_size];
    let start = Instant::now();

    let mut handles = Vec::with_capacity(config.parallel);
    let messages_per_worker = config.messages / config.parallel;
    let remainder = config.messages % config.parallel;

    for worker_id in 0..config.parallel {
        let server = config.server;
        let topics = config.topics;
        let payload = payload.clone();
        let count = messages_per_worker + if worker_id < remainder { 1 } else { 0 };

        handles.push(task::spawn(async move {
            let mut stream = TcpStream::connect(server)
                .await
                .context("connecting to server")?;
            stream.set_nodelay(true)?;

            for i in 0..count {
                let topic = format!("topic{}", i % topics);
                let frame = encode_publish(&topic, &payload);
                stream.write_all(&frame).await.context("writing frame")?;
            }

            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    let elapsed = Instant::now().duration_since(start);
    let seconds = elapsed.as_secs_f64();
    let msg_rate = config.messages as f64 / seconds;
    let bytes = (1 + 2 + 12 + config.payload_size) as f64 * config.messages as f64;
    let mb_per_sec = bytes / (1024.0 * 1024.0) / seconds;

    println!("Benchmark complete:");
    println!("  server: {}", config.server);
    println!("  topics: {}", config.topics);
    println!("  messages: {}", config.messages);
    println!("  parallel connections: {}", config.parallel);
    println!("  payload size: {} bytes", config.payload_size);
    println!("  elapsed: {:.2}s", seconds);
    println!("  throughput: {:.0} msg/s", msg_rate);
    println!("  bandwidth: {:.2} MB/s", mb_per_sec);

    Ok(())
}
