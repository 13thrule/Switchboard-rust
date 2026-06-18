use prometheus::{Encoder, Gauge, IntCounter, Registry, TextEncoder};
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref CONNECTIONS: IntCounter = IntCounter::new("switchboard_connections_total", "Total accepted connections").unwrap();
    pub static ref PUBLISHES: IntCounter = IntCounter::new("switchboard_publishes_total", "Total published messages").unwrap();
    pub static ref LAST_PUBLISH_SIZE: Gauge = Gauge::new("switchboard_last_publish_size_bytes", "Size of last published message").unwrap();
}

pub fn init_registry() {
    let _ = REGISTRY.register(Box::new(CONNECTIONS.clone()));
    let _ = REGISTRY.register(Box::new(PUBLISHES.clone()));
    let _ = REGISTRY.register(Box::new(LAST_PUBLISH_SIZE.clone()));
}

pub async fn serve_metrics(addr: SocketAddr) -> JoinHandle<()> {
    use hyper::{Body, Request, Response, Server};
    use hyper::service::{make_service_fn, service_fn};

    init_registry();

    let make_svc = make_service_fn(|_conn| async {
        Ok::<_, Infallible>(service_fn(|_req: Request<Body>| async move {
            let encoder = TextEncoder::new();
            let metric_families = REGISTRY.gather();
            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer).unwrap();
            Ok::<_, Infallible>(Response::builder().status(200).body(Body::from(buffer)).unwrap())
        }))
    });

    let server = Server::bind(&addr).serve(make_svc);
    tokio::spawn(async move {
        if let Err(e) = server.await {
            eprintln!("metrics server error: {}", e);
        }
    })
}
