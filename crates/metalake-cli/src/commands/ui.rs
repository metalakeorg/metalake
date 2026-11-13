//! `metalake ui`: the UI on this machine, in the foreground (ADR 0007 §6). It listens on the loopback, prints its URL,
//! opens it in the browser through a redirect file, and serves until Ctrl-C.

use std::io::ErrorKind;
use std::net::Ipv4Addr;

use metalake_client::Error;
use metalake_ui::browser::{self, Redirect};
use tokio::net::TcpListener;

use super::signal::stopped;
use crate::args::Ui;

pub async fn run(args: &Ui) -> Result<(), Error> {
    let port = args.port.unwrap_or(0);
    let listener = match TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await {
        Err(e) if e.kind() == ErrorKind::AddrInUse => {
            return Err(Error::usage(format!(
                "port {port} is taken: give another, or none for a free one"
            )));
        }
        bound => bound?,
    };
    let url = format!("http://{}/", listener.local_addr()?);
    if !metalake_ui::built() {
        eprintln!("this metalake was compiled without its UI's frontend: the page says how to build it");
    }
    // What was asked for, on stdout: a script, or a test, reads the URL from this line.
    println!("{url}");
    // Kept until the UI stops, then removed, however it stops.
    let _redirect = if args.no_browser { None } else { opened(&url) };
    eprintln!("serving MetaLake's UI: Ctrl-C stops it");
    metalake_ui::serve(listener, stopped()).await?;
    Ok(())
}

/// The redirect file, opened in the browser. Failing to is said, not fatal: the URL is printed.
fn opened(url: &str) -> Option<Redirect> {
    let redirect = browser::directory().and_then(|dir| Redirect::write(&dir, url));
    match redirect.and_then(|redirect| browser::open(&redirect).map(|()| redirect)) {
        Ok(redirect) => Some(redirect),
        Err(e) => {
            eprintln!("could not open a browser ({e}): open {url}");
            None
        }
    }
}
