//! The UI served as `metalake ui` serves it, without the command line, which links the whole of MetaLake: how to try
//! the router, the redirect file and Ctrl-C in seconds.
//!
//! ```text
//! cargo run -p metalake-ui --example serve -- [--no-browser]
//! ```

use std::net::Ipv4Addr;

use metalake_ui::browser::{self, Redirect};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let no_browser = std::env::args().any(|arg| arg == "--no-browser");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/", listener.local_addr()?);
    println!("{url}");
    let redirect = if no_browser {
        None
    } else {
        let redirect = Redirect::write(&browser::directory()?, &url)?;
        eprintln!("redirect file: {}", redirect.path().display());
        browser::open(&redirect)?;
        Some(redirect)
    };
    eprintln!("built: {}; Ctrl-C stops it", metalake_ui::built());
    metalake_ui::serve(listener, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    let path = redirect.as_ref().map(|redirect| redirect.path().to_owned());
    drop(redirect);
    if let Some(path) = path {
        eprintln!("redirect file removed: {}", !path.exists());
    }
    Ok(())
}
