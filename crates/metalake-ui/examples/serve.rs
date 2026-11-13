//! The UI served as `metalake ui` serves it, without the command line, which links the whole of MetaLake: how to try
//! the router, the redirect file and Ctrl-C in seconds. No lake is behind it: its API says so (`api::Detached`); with
//! `--sample`, a lake in memory answers instead, of two branches and a few tables, which never change.
//!
//! ```text
//! cargo run -p metalake-ui --no-default-features --example serve -- [--no-browser] [--port N] [--sample]
//! ```
//!
//! The frontend's end-to-end tests run against it where the command line is not built (`METALAKE_UI_COMMAND`).

use std::net::Ipv4Addr;
use std::sync::Arc;

use metalake_ui::api::{Answer, Backend, Detached, Fault, Kind, Mark, Marks, Ref, Rows, Statement, Status, Table};
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
    let backend: Arc<dyn Backend> = if args.iter().any(|arg| arg == "--sample") {
        Arc::new(Sample::default())
    } else {
        Arc::new(Detached)
    };
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let token = metalake_ui::Token::from_environment()?;
    let url = metalake_ui::url(listener.local_addr()?, &token);
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
    let notebook = std::sync::Arc::new(metalake_ui::Notebook::from_environment());
    metalake_ui::serve(listener, backend, notebook, token, async {
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

/// A lake in memory: `main` and `dev`, each with its tables. It never changes: its one mark is said, and its events stay
/// open, as a lake's do while nothing is written.
struct Sample(Vec<(&'static str, Table)>);

impl Default for Sample {
    fn default() -> Self {
        let table = |on, namespace: &str, name: &str| {
            (
                on,
                Table {
                    namespace: vec![namespace.into()],
                    name: name.into(),
                },
            )
        };
        Sample(vec![
            table("main", "sales", "orders"),
            table("main", "sales", "customers"),
            table("main", "default", "events"),
            table("dev", "sales", "orders"),
            table("dev", "sales", "customers"),
            table("dev", "default", "events"),
            table("dev", "staging", "orders_v2"),
        ])
    }
}

impl Backend for Sample {
    fn status(&self) -> Answer<'_, Status> {
        Box::pin(async { Err(Fault::new(Kind::Usage, "a sample lake has no status")) })
    }

    fn refs(&self) -> Answer<'_, Vec<Ref>> {
        let one = |name: &str, parent: Option<&str>| Ref {
            name: name.into(),
            kind: "branch".into(),
            parent: parent.map(Into::into),
            fork_seq: parent.map(|_| 3),
            created_seq: if parent.is_some() { 4 } else { 1 },
            protected: parent.is_none(),
        };
        Box::pin(async move { Ok(vec![one("main", None), one("dev", Some("main"))]) })
    }

    fn tables<'a>(&'a self, on: &'a str) -> Answer<'a, Vec<Table>> {
        let tables: Vec<Table> = self
            .0
            .iter()
            .filter(|(r, _)| *r == on)
            .map(|(_, t)| t.clone())
            .collect();
        let known = on == "main" || on == "dev";
        Box::pin(async move {
            if known {
                Ok(tables)
            } else {
                Err(Fault::new(Kind::Usage, format!("no ref {on}")))
            }
        })
    }

    fn sql(&self, _: Statement) -> Answer<'_, Rows> {
        Box::pin(async {
            Err(Fault::new(
                Kind::Usage,
                "a sample lake runs no statement: run `metalake ui` in a lake",
            ))
        })
    }

    fn watch(&self) -> Answer<'_, Marks> {
        Box::pin(async {
            let now = futures_util::stream::iter([Ok(Mark {
                seq: 7,
                recent: 7,
                deleted: 0,
            })]);
            let marks = futures_util::StreamExt::chain(now, futures_util::stream::pending());
            Ok(Box::pin(marks) as Marks)
        })
    }
}
