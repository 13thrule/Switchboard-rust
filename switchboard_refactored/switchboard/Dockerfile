FROM rust:1.70 as builder
WORKDIR /usr/src/switchboard
COPY . .
WORKDIR /usr/src/switchboard/switchboard_refactored/switchboard
RUN cargo build --release --bin switchboard

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/switchboard/switchboard_refactored/switchboard/target/release/switchboard /usr/local/bin/switchboard
EXPOSE 7777
ENTRYPOINT ["/usr/local/bin/switchboard"]
CMD ["--port","7777"]
