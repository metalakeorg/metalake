# syntax=docker/dockerfile:1.4
# MetaLake's command line, for the end-to-end test: built from this repository, its root the context.

ARG RUST_VERSION=1.99.1

FROM rust:${RUST_VERSION}-slim-trixie AS builder
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p metalake-cli && cp target/release/metalake /metalake

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /metalake /usr/local/bin/metalake
ENTRYPOINT ["metalake"]
