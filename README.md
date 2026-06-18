# Switchboard-rust
Switchboard is an ultra-low latency async pub/sub message broker in Rust. Traditional brokers bottleneck memory by deep-copying network frames for each subscriber and idling via wasteful polling loops. Switchboard uses a waker-driven Zero-Copy Pipeline and lock-free skip lists so thousands of apps read raw memory concurrently with 0% idle CPU.
