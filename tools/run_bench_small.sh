#!/usr/bin/env bash
set -euo pipefail

cd switchboard_refactored/switchboard
cargo build --release --bin bench_publisher

# Run a small bench suitable for CI/local testing
./target/release/bench_publisher --server 127.0.0.1:7777 --topics 10 --messages 1000 --parallel 2 --payload-size 32
