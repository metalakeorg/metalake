//! Compiles the crate again whenever `web/dist/` changes, files added included: `rust-embed` embeds what is there when it
//! compiles, and Cargo alone would not see a frontend built after a build without one. It never builds the frontend:
//! that is `pnpm build`'s, so that `cargo build` needs no Node (ADR 0007 §5.3).

fn main() {
    println!("cargo::rerun-if-changed=web/dist");
    println!("cargo::rerun-if-changed=web/dist/labextension");
    println!("cargo::rerun-if-changed=web/dist/labextension/static");
}
