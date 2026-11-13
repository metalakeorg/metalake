//! Writes the UI API's OpenAPI spec to `web/openapi.json`, which the frontend's types are generated from and a test
//! compares with the router's.
//!
//! ```text
//! cargo run -p metalake-ui --no-default-features --example openapi
//! ```

fn main() -> std::io::Result<()> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("web/openapi.json");
    std::fs::write(&path, metalake_ui::api::openapi())?;
    eprintln!("written: {}", path.display());
    Ok(())
}
