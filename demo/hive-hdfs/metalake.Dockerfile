# syntax=docker/dockerfile:1.4
# MetaLake's command line for the demo, its UI embedded: built from this repository, its root the context.
#
#   docker compose -f demo/hive-hdfs/compose.yml build metalake

ARG RUST_VERSION=1.99.1

# The frontend, which `metalake ui` serves from the binary (ADR 0007).
FROM node:24-trixie-slim AS web
WORKDIR /web
COPY crates/metalake-ui/web/package.json crates/metalake-ui/web/pnpm-lock.yaml crates/metalake-ui/web/pnpm-workspace.yaml crates/metalake-ui/web/.pnpmfile.cjs ./
RUN corepack enable && pnpm install --frozen-lockfile
COPY crates/metalake-ui/web/ ./
RUN pnpm build

FROM rust:${RUST_VERSION}-slim-trixie AS builder
WORKDIR /src
# few enough compilers for a Docker of a few GB
ENV CARGO_BUILD_JOBS=6
COPY . .
COPY --from=web /web/dist crates/metalake-ui/web/dist
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p metalake-cli && cp target/release/metalake /metalake

# `socat` forwards the UI out of the container's loopback, as an SSH tunnel would.
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates socat && rm -rf /var/lib/apt/lists/*
COPY --from=builder /metalake /usr/local/bin/metalake
WORKDIR /lake
