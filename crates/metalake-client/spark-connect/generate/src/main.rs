//! Spark Connect's protocol as Rust: the messages, the client MetaLake sends statements with, and the server its tests
//! answer as. Written to `src/engine/spark/proto/`, checked in, so that no build needs `protoc`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let out = root.join("../src/engine/spark/proto");
    std::fs::create_dir_all(&out)?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(&out)
        .compile_protos(&[root.join("proto/spark/connect/base.proto")], &[root.join("proto")])?;
    Ok(())
}
