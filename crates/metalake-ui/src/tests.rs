//! The router over files of a test's own, built or not, and over the files this crate embedded.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use tower::ServiceExt;

use crate::assets::{File, Files};
use crate::frontend::{Coding, codings, matches};
use crate::{built, router, router_with};

const PAGE: &str = "<!doctype html><title>MetaLake</title>";
const SCRIPT: &str = "console.log('the application')";

/// Files in memory, each with a hash of its own.
struct Memory(HashMap<&'static str, File>);

impl Memory {
    fn new(files: &[(&'static str, &'static str, &[u8])]) -> Self {
        let files = files
            .iter()
            .enumerate()
            .map(|(index, (path, media_type, bytes))| {
                let file = File {
                    bytes: Cow::Owned(bytes.to_vec()),
                    media_type: Cow::Borrowed(media_type),
                    sha256: [index as u8 + 1; 32],
                };
                (*path, file)
            })
            .collect();
        Memory(files)
    }
}

impl Files for Memory {
    fn get(&self, path: &str) -> Option<File> {
        self.0.get(path).cloned()
    }
}

/// A built frontend: its page, a script with both compressed copies, a style with none, and an icon.
fn built_router() -> Router {
    router_with(Arc::new(Memory::new(&[
        ("index.html", "text/html", PAGE.as_bytes()),
        ("assets/app-1a2b.js", "text/javascript", SCRIPT.as_bytes()),
        ("assets/app-1a2b.js.br", "application/octet-stream", b"brotli bytes"),
        ("assets/app-1a2b.js.gz", "application/gzip", b"gzip bytes"),
        ("assets/app-1a2b.css", "text/css", b"body{}"),
        ("favicon.svg", "image/svg+xml", b"<svg/>"),
    ])))
}

fn unbuilt_router() -> Router {
    router_with(Arc::new(Memory::new(&[])))
}

struct Answer {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

impl Answer {
    fn header(&self, name: header::HeaderName) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

async fn send(router: Router, method: Method, path: &str, headers: &[(header::HeaderName, &str)]) -> Answer {
    let mut request = Request::builder().method(method).uri(path);
    for (name, value) in headers {
        request = request.header(name, *value);
    }
    let response = router.oneshot(request.body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap().to_vec();
    Answer { status, headers, body }
}

async fn get(path: &str) -> Answer {
    send(built_router(), Method::GET, path, &[]).await
}

// The routes and the files (2a).

#[tokio::test]
async fn root_serves_page() {
    let answer = get("/").await;
    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(answer.header(header::CONTENT_TYPE), Some("text/html; charset=utf-8"));
    assert_eq!(answer.body, PAGE.as_bytes());
}

#[tokio::test]
async fn route_serves_page() {
    for path in ["/refs", "/refs/main", "/catalog/main/sales/orders"] {
        let answer = get(path).await;
        assert_eq!(answer.status, StatusCode::OK, "{path}");
        assert_eq!(answer.body, PAGE.as_bytes(), "{path}");
    }
}

#[tokio::test]
async fn file_served_with_type() {
    let script = get("/assets/app-1a2b.js").await;
    assert_eq!(script.status, StatusCode::OK);
    assert_eq!(
        script.header(header::CONTENT_TYPE),
        Some("text/javascript; charset=utf-8")
    );
    assert_eq!(script.body, SCRIPT.as_bytes());
    let icon = get("/favicon.svg").await;
    assert_eq!(icon.header(header::CONTENT_TYPE), Some("image/svg+xml"));
}

#[tokio::test]
async fn missing_file_not_found() {
    let answer = get("/assets/gone-9z9z.js").await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    assert_eq!(answer.header(header::CACHE_CONTROL), Some("no-store"));
}

#[tokio::test]
async fn api_never_falls_back() {
    for path in ["/api", "/api/", "/api/v1/status", "/api/refs/main"] {
        let answer = get(path).await;
        assert_eq!(answer.status, StatusCode::NOT_FOUND, "{path}");
        assert_eq!(answer.header(header::CONTENT_TYPE), Some("application/json"), "{path}");
        assert!(answer.body.starts_with(br#"{"kind":"usage""#), "{path}");
    }
    let posted = send(built_router(), Method::POST, "/api/v1/sql", &[]).await;
    assert_eq!(posted.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn write_method_refused() {
    for method in [Method::POST, Method::PUT, Method::DELETE, Method::PATCH] {
        let answer = send(built_router(), method.clone(), "/", &[]).await;
        assert_eq!(answer.status, StatusCode::METHOD_NOT_ALLOWED, "{method}");
        assert_eq!(answer.header(header::ALLOW), Some("GET, HEAD"), "{method}");
    }
}

#[tokio::test]
async fn dot_segment_not_found() {
    for path in ["/../Cargo.toml", "/assets/../index.html", "/./index.html"] {
        assert_eq!(get(path).await.status, StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
async fn unbuilt_route_says_how_to_build() {
    let answer = send(unbuilt_router(), Method::GET, "/refs/main", &[]).await;
    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(answer.header(header::CACHE_CONTROL), Some("no-store"));
    assert!(
        String::from_utf8(answer.body)
            .unwrap()
            .contains("pnpm --dir crates/metalake-ui/web build")
    );
    let file = send(unbuilt_router(), Method::GET, "/assets/app-1a2b.js", &[]).await;
    assert_eq!(file.status, StatusCode::NOT_FOUND);
}

/// The files this crate embedded: the application when `pnpm build` ran before it compiled, the page that says how to
/// build it otherwise. CI runs both.
#[tokio::test]
async fn embedded_frontend_matches_built() {
    let answer = send(router(), Method::GET, "/", &[]).await;
    if built() {
        assert_eq!(answer.status, StatusCode::OK);
        let page = String::from_utf8(answer.body).unwrap();
        assert!(page.contains(r#"<div id="root">"#), "{page}");
        // The page's script, as `pnpm build` wrote it and compressed it.
        let script = page
            .split('"')
            .find(|part| part.starts_with("/assets/") && part.ends_with(".js"))
            .expect("the page names its script");
        let plain = send(router(), Method::GET, script, &[]).await;
        let brotli = send(router(), Method::GET, script, &[(header::ACCEPT_ENCODING, "gzip, br")]).await;
        assert_eq!(
            plain.header(header::CACHE_CONTROL),
            Some("public, max-age=31536000, immutable")
        );
        assert_eq!(brotli.header(header::CONTENT_ENCODING), Some("br"));
        assert!(
            brotli.body.len() < plain.body.len() / 2,
            "{} of {}",
            brotli.body.len(),
            plain.body.len()
        );
    } else {
        assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    }
}

// How a file is served (2b).

#[tokio::test]
async fn hashed_asset_cached_forever() {
    let answer = get("/assets/app-1a2b.css").await;
    assert_eq!(
        answer.header(header::CACHE_CONTROL),
        Some("public, max-age=31536000, immutable")
    );
}

#[tokio::test]
async fn page_and_unhashed_revalidated() {
    for path in ["/", "/refs", "/favicon.svg"] {
        assert_eq!(
            get(path).await.header(header::CACHE_CONTROL),
            Some("no-cache"),
            "{path}"
        );
    }
}

#[tokio::test]
async fn brotli_preferred() {
    let answer = send(
        built_router(),
        Method::GET,
        "/assets/app-1a2b.js",
        &[(header::ACCEPT_ENCODING, "gzip, deflate, br, zstd")],
    )
    .await;
    assert_eq!(answer.header(header::CONTENT_ENCODING), Some("br"));
    assert_eq!(
        answer.header(header::CONTENT_TYPE),
        Some("text/javascript; charset=utf-8")
    );
    assert_eq!(answer.header(header::VARY), Some("Accept-Encoding"));
    assert_eq!(answer.body, b"brotli bytes");
}

#[tokio::test]
async fn gzip_when_brotli_refused() {
    for accept in ["gzip", "br;q=0, gzip", "gzip;q=1, br;q=0.5"] {
        let answer = send(
            built_router(),
            Method::GET,
            "/assets/app-1a2b.js",
            &[(header::ACCEPT_ENCODING, accept)],
        )
        .await;
        assert_eq!(answer.header(header::CONTENT_ENCODING), Some("gzip"), "{accept}");
        assert_eq!(answer.body, b"gzip bytes", "{accept}");
    }
}

#[tokio::test]
async fn identity_when_nothing_accepted() {
    for accept in [None, Some("identity"), Some("*;q=0"), Some("deflate")] {
        let headers: Vec<_> = accept
            .map(|value| (header::ACCEPT_ENCODING, value))
            .into_iter()
            .collect();
        let answer = send(built_router(), Method::GET, "/assets/app-1a2b.js", &headers).await;
        assert_eq!(answer.header(header::CONTENT_ENCODING), None, "{accept:?}");
        assert_eq!(answer.body, SCRIPT.as_bytes(), "{accept:?}");
    }
}

#[tokio::test]
async fn identity_when_no_compressed_copy() {
    let answer = send(
        built_router(),
        Method::GET,
        "/assets/app-1a2b.css",
        &[(header::ACCEPT_ENCODING, "br, gzip")],
    )
    .await;
    assert_eq!(answer.header(header::CONTENT_ENCODING), None);
    assert_eq!(answer.body, b"body{}");
}

#[tokio::test]
async fn compressed_copy_not_served_by_name() {
    for path in ["/assets/app-1a2b.js.br", "/assets/app-1a2b.js.gz"] {
        assert_eq!(get(path).await.status, StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
async fn entity_tag_per_coding() {
    let plain = get("/assets/app-1a2b.js").await;
    let brotli = send(
        built_router(),
        Method::GET,
        "/assets/app-1a2b.js",
        &[(header::ACCEPT_ENCODING, "br")],
    )
    .await;
    let plain = plain.header(header::ETAG).unwrap().to_owned();
    let brotli = brotli.header(header::ETAG).unwrap().to_owned();
    assert!(
        plain.starts_with('"') && plain.ends_with('"') && plain.len() == 66,
        "{plain}"
    );
    assert_ne!(plain, brotli);
}

#[tokio::test]
async fn held_entity_not_modified() {
    let first = get("/").await;
    let tag = first.header(header::ETAG).unwrap().to_owned();
    for given in [
        tag.clone(),
        format!("W/{tag}"),
        format!("\"other\", {tag}"),
        "*".to_owned(),
    ] {
        let answer = send(built_router(), Method::GET, "/", &[(header::IF_NONE_MATCH, &given)]).await;
        assert_eq!(answer.status, StatusCode::NOT_MODIFIED, "{given}");
        assert!(answer.body.is_empty(), "{given}");
        assert_eq!(answer.header(header::ETAG), Some(tag.as_str()), "{given}");
        assert_eq!(answer.header(header::CACHE_CONTROL), Some("no-cache"), "{given}");
    }
    let changed = send(
        built_router(),
        Method::GET,
        "/",
        &[(header::IF_NONE_MATCH, "\"other\"")],
    )
    .await;
    assert_eq!(changed.status, StatusCode::OK);
}

#[tokio::test]
async fn list_header_across_field_lines() {
    let split = send(
        built_router(),
        Method::GET,
        "/assets/app-1a2b.js",
        &[(header::ACCEPT_ENCODING, "br;q=0"), (header::ACCEPT_ENCODING, "gzip")],
    )
    .await;
    assert_eq!(split.header(header::CONTENT_ENCODING), Some("gzip"));
    let tag = get("/").await.header(header::ETAG).unwrap().to_owned();
    let held = send(
        built_router(),
        Method::GET,
        "/",
        &[(header::IF_NONE_MATCH, "\"other\""), (header::IF_NONE_MATCH, &tag)],
    )
    .await;
    assert_eq!(held.status, StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn head_without_body() {
    let answer = send(built_router(), Method::HEAD, "/assets/app-1a2b.js", &[]).await;
    assert_eq!(answer.status, StatusCode::OK);
    assert!(answer.body.is_empty());
    assert_eq!(
        answer.header(header::CONTENT_LENGTH),
        Some(SCRIPT.len().to_string().as_str())
    );
    assert_eq!(
        answer.header(header::CONTENT_TYPE),
        Some("text/javascript; charset=utf-8")
    );
}

#[test]
fn codings_by_weight_then_preference() {
    use Coding::*;
    assert_eq!(codings(None), [Identity]);
    assert_eq!(codings(Some("gzip, br")), [Brotli, Gzip, Identity]);
    assert_eq!(codings(Some("GZIP;Q=0.9, Br;q=0.5")), [Identity, Gzip, Brotli]);
    assert_eq!(codings(Some("x-gzip")), [Gzip, Identity]);
    assert_eq!(codings(Some("*")), [Brotli, Gzip, Identity]);
    assert_eq!(codings(Some("br, *;q=0")), [Brotli]);
    assert_eq!(codings(Some("identity;q=0")), [Identity]);
}

#[test]
fn if_none_match_weak_comparison() {
    assert!(matches("\"a\"", "\"a\""));
    assert!(matches("W/\"a\"", "\"a\""));
    assert!(matches(" \"b\" , \"a\" ", "\"a\""));
    assert!(matches("*", "\"a\""));
    assert!(!matches("\"b\"", "\"a\""));
}

// The server's life (milestone 3).

mod life {
    use std::time::{Duration, Instant};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    use super::built_router;
    use crate::{GRACE, serve_router};

    /// A server of `built_router` with `grace`, its address, and what stops it.
    async fn started(
        grace: Duration,
    ) -> (
        std::net::SocketAddr,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<std::io::Result<()>>,
    ) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let served = tokio::spawn(serve_router(
            listener,
            built_router(),
            async move {
                let _ = stopped.await;
            },
            grace,
        ));
        (address, stop, served)
    }

    #[tokio::test]
    async fn stop_ends_serving_and_frees_port() {
        let (address, stop, served) = started(GRACE).await;
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.unwrap();
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");

        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), served)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(TcpStream::connect(address).await.is_err(), "the port is free");
    }

    /// A browser's connection that never finishes its request holds the process no longer than the grace.
    #[tokio::test]
    async fn grace_bounds_open_connection() {
        let grace = Duration::from_millis(300);
        let (address, stop, served) = started(grace).await;
        let mut held = TcpStream::connect(address).await.unwrap();
        held.write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stopping = Instant::now();
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), served)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            stopping.elapsed() < grace + Duration::from_secs(1),
            "{:?}",
            stopping.elapsed()
        );
        drop(held);
    }
}
