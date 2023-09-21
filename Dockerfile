FROM rust:1-slim-bullseye

COPY ./Cargo.toml /usr/local/kube-state-rs/Cargo.toml
COPY ./src /usr/local/kube-state-rs/src

RUN cd /usr/local/kube-state-rs && cargo build
RUN cp /usr/local/kube-state-rs/target/debug/kube-state-rs /usr/local/bin/kube-state-rs

RUN chmod +x /usr/local/bin/kube-state-rs

ENTRYPOINT ["/usr/local/bin/kube-state-rs"]
