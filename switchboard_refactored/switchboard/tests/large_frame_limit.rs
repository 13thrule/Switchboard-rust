use std::time::Duration;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use switchboard::connection::Connection;
use switchboard::router::Router;

#[tokio::test]
async fn rejects_oversized_prefixed_frame() {
    // Bind server to an ephemeral port
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let router = Router::new();
    let router_server = router.clone();

    // Accept one connection and run the server connection handler
    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.expect("accept");
        let conn = Connection::new(stream, peer, router_server);
        // run until it returns (should exit on protocol error)
        let _ = conn.run().await;
    });

    // Connect a client socket
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");

    // Send a 4-byte length header that exceeds MAX_FRAME_BYTES (16MiB)
    let oversized: u32 = 16 * 1024 * 1024 + 1;
    let hdr = oversized.to_be_bytes();
    stream.write_all(&hdr).await.expect("write header");

    // Give server a moment to process and close the connection
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Attempt to read — expecting EOF / connection closed by server
    let mut buf = [0u8; 1];
    match stream.read(&mut buf).await {
        Ok(0) => { /* closed as expected */ }
        Ok(n) => panic!("expected closed connection, got {} bytes", n),
        Err(e) => panic!("read error: {}", e),
    }

    server.abort();
}
