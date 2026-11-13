//! The UI served as `metalake ui` serves it, without the command line, which links the whole of MetaLake: how to try
//! the router, the redirect file and Ctrl-C in seconds.
//!
//! ```text
//! cargo run -p metalake-ui --example serve -- [--no-browser] [--port N]
//! ```
//!
//! The frontend's end-to-end tests run against it where the command line is not built (`METALAKE_UI_COMMAND`).

use std::net::Ipv4Addr;

use metalake_ui::browser::{self, Redirect};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let no_browser = args.iter().any(|arg| arg == "--no-browser");
    let port = args
        .iter()
        .position(|arg| arg == "--port")
        .and_then(|at| args.get(at + 1))
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(0);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
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
