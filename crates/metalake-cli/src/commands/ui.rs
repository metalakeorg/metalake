//! `metalake ui`: the UI of the lake, on this machine, in the foreground (ADR 0007 §6). It opens the lake as every verb
//! does, listens on the loopback at the lake's own port, prints its URL, which carries the run's token, opens it in the
//! browser through a redirect file, and serves until Ctrl-C.

use std::io::ErrorKind;
use std::net::Ipv4Addr;
use std::sync::Arc;

use metalake_client::{Error, Lake};
use metalake_ui::browser::{self, Redirect};
use tokio::net::TcpListener;

use super::signal::stopped;
use crate::args::Ui;

pub async fn run(args: &Ui, lake: Lake) -> Result<(), Error> {
    let listener = listen(args.port, lake.ui_port()).await?;
    // `METALAKE_UI_TOKEN` gives it, for a test or Vite's dev server; else a new one, which only this URL carries.
    let token = metalake_ui::Token::from_environment().map_err(|e| Error::usage(e.to_string()))?;
    let url = metalake_ui::url(listener.local_addr()?, &token);
    if !metalake_ui::built() {
        eprintln!("this metalake was compiled without its UI's frontend: the page says how to build it");
    }
    // What was asked for, on stdout: a script, or a test, reads the URL from this line.
    println!("{url}");
    // Kept until the UI stops, then removed, however it stops.
    let _redirect = if args.no_browser { None } else { opened(&url) };
    eprintln!("serving MetaLake's UI: Ctrl-C stops it");
    // Notebooks beside the lake's file, or where the UI was started for a lake its environment names alone.
    let mut notebook = metalake_ui::Notebook::from_environment();
    if let Some(directory) = lake.directory() {
        notebook = notebook.keeping(directory.join("notebooks"));
    }
    metalake_ui::serve(listener, Arc::new(lake), Arc::new(notebook), token, stopped()).await?;
    Ok(())
}

/// The port asked for, which must be free; else the lake's own, the same each run, or a free one when another holds it.
async fn listen(asked: Option<u16>, preferred: u16) -> Result<TcpListener, Error> {
    if let Some(port) = asked {
        return match TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await {
            Err(e) if e.kind() == ErrorKind::AddrInUse => Err(Error::usage(format!(
                "port {port} is taken: give another, or none for the lake's own"
            ))),
            bound => Ok(bound?),
        };
    }
    // Taken, or reserved as Windows reserves ranges of ports: either way, another.
    match TcpListener::bind((Ipv4Addr::LOCALHOST, preferred)).await {
        Ok(listener) => Ok(listener),
        Err(_) => Ok(TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?),
    }
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
