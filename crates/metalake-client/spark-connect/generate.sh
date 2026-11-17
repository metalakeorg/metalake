#!/usr/bin/env bash
# Writes src/engine/spark/proto/spark.connect.rs from the protos in proto/, Spark 3.5.9's
# (connector/connect/common/src/main/protobuf at tag v3.5.9). Needs only Docker: protoc and Rust run in a container.
# Run from anywhere; check in what it writes.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
client="$(cd "$here/.." && (pwd -W 2>/dev/null || pwd))"
export MSYS_NO_PATHCONV=1
docker run --rm -v "$client:/client" -w /client/spark-connect/generate rust:1.94-bookworm bash -c '
  apt-get update -qq && apt-get install -y -qq protobuf-compiler >/dev/null
  rustup component add rustfmt >/dev/null 2>&1
  CARGO_TARGET_DIR=/tmp/target cargo run --quiet --release
  rustfmt --edition 2024 /client/src/engine/spark/proto/spark.connect.rs || true
'
echo "written: $client/src/engine/spark/proto/spark.connect.rs"
