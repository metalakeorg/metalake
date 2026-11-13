//! The router over files of a test's own, built or not, and over the files this crate embedded.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode, header};
use tower::ServiceExt;

use crate::api::Detached;
use crate::assets::{File, Files};
use crate::frontend::{Coding, codings, matches};
use crate::{Token, built, frontend_router, router};

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
    frontend_router(Arc::new(Memory::new(&[
        ("index.html", "text/html", PAGE.as_bytes()),
        ("assets/app-1a2b.js", "text/javascript", SCRIPT.as_bytes()),
        ("assets/app-1a2b.js.br", "application/octet-stream", b"brotli bytes"),
        ("assets/app-1a2b.js.gz", "application/gzip", b"gzip bytes"),
        ("assets/app-1a2b.css", "text/css", b"body{}"),
        ("favicon.svg", "image/svg+xml", b"<svg/>"),
    ])))
}

/// The router a run serves, with the files this crate embedded, on port 4173.
fn embedded() -> Router {
    router(Arc::new(Detached), Token::given("t".repeat(32)).unwrap(), 4173)
}

/// The `Host` a browser sends to it.
const HOST: &str = "127.0.0.1:4173";

fn unbuilt_router() -> Router {
    frontend_router(Arc::new(Memory::new(&[])))
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
    let answer = send(embedded(), Method::GET, "/", &[(header::HOST, HOST)]).await;
    if built() {
        assert_eq!(answer.status, StatusCode::OK);
        let page = String::from_utf8(answer.body).unwrap();
        assert!(page.contains(r#"<div id="root">"#), "{page}");
        // The page's script, as `pnpm build` wrote it and compressed it.
        let script = page
            .split('"')
            .find(|part| part.starts_with("/assets/") && part.ends_with(".js"))
            .expect("the page names its script");
        let plain = send(embedded(), Method::GET, script, &[(header::HOST, HOST)]).await;
        let brotli = send(
            embedded(),
            Method::GET,
            script,
            &[(header::HOST, HOST), (header::ACCEPT_ENCODING, "gzip, br")],
        )
        .await;
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

// Who may use the UI (milestone 4).

mod access {
    use std::sync::Arc;

    use axum::Router;
    use axum::http::{Method, StatusCode, header};

    use super::{Answer, HOST, Memory, PAGE, send};
    use crate::api::Detached;
    use crate::guard::Access;
    use crate::{Token, router_with};

    const SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef";

    /// The guarded router of a run on port 4173, whose token is `SECRET`.
    fn guarded() -> Router {
        let files = Memory::new(&[
            ("index.html", "text/html", PAGE.as_bytes()),
            ("favicon.svg", "image/svg+xml", b"<svg/>"),
        ]);
        let access = Access {
            token: Token::given(SECRET.to_owned()).unwrap(),
            port: 4173,
        };
        router_with(Arc::new(files), Arc::new(Detached), access)
    }

    fn bearer() -> String {
        format!("Bearer {SECRET}")
    }

    async fn ask(method: Method, path: &str, headers: &[(header::HeaderName, &str)]) -> Answer {
        super::send(guarded(), method, path, headers).await
    }

    #[tokio::test]
    async fn other_host_forbidden_everywhere() {
        let bearer = bearer();
        for host in [
            "evil.example:4173",
            "127.0.0.1.evil.com:4173",
            "127.0.0.1:9999",
            "127.0.0.1",
            "[::1]:4173",
        ] {
            for path in ["/", "/favicon.svg", "/api/v1/version"] {
                let headers = [(header::HOST, host), (header::AUTHORIZATION, bearer.as_str())];
                let answer = ask(Method::GET, path, &headers).await;
                assert_eq!(answer.status, StatusCode::FORBIDDEN, "{host} {path}");
            }
        }
        let none = send(guarded(), Method::GET, "/", &[]).await;
        assert_eq!(none.status, StatusCode::FORBIDDEN, "no Host");
    }

    #[tokio::test]
    async fn own_host_by_address_or_name() {
        for host in [HOST, "localhost:4173", "LOCALHOST:4173"] {
            let answer = ask(Method::GET, "/", &[(header::HOST, host)]).await;
            assert_eq!(answer.status, StatusCode::OK, "{host}");
        }
    }

    #[tokio::test]
    async fn frontend_needs_no_token() {
        let answer = ask(Method::GET, "/refs/main", &[(header::HOST, HOST)]).await;
        assert_eq!(answer.status, StatusCode::OK);
        assert_eq!(answer.body, PAGE.as_bytes());
    }

    #[tokio::test]
    async fn api_without_token_unauthorized() {
        let wrong = format!("Bearer {}", SECRET.replace('0', "1"));
        let basic = format!("Basic {SECRET}");
        let cases = [
            None,
            Some("Bearer"),
            Some(wrong.as_str()),
            Some(basic.as_str()),
            Some(SECRET),
        ];
        for authorization in cases {
            let mut headers = vec![(header::HOST, HOST)];
            headers.extend(authorization.map(|value| (header::AUTHORIZATION, value)));
            let answer = ask(Method::GET, "/api/v1/version", &headers).await;
            assert_eq!(answer.status, StatusCode::UNAUTHORIZED, "{authorization:?}");
            assert_eq!(
                answer.header(header::WWW_AUTHENTICATE),
                Some("Bearer"),
                "{authorization:?}"
            );
            assert!(answer.body.starts_with(br#"{"kind":"usage""#), "{authorization:?}");
        }
    }

    #[tokio::test]
    async fn api_with_token_answers() {
        for scheme in ["Bearer", "bearer", "BEARER"] {
            let authorization = format!("{scheme} {SECRET}");
            let headers = [(header::HOST, HOST), (header::AUTHORIZATION, authorization.as_str())];
            let answer = ask(Method::GET, "/api/v1/version", &headers).await;
            assert_eq!(answer.status, StatusCode::OK, "{scheme}");
            assert_eq!(answer.body, br#"{"api":1}"#, "{scheme}");
        }
    }

    /// The token's layer wraps the API's whole router: a path that does not exist is 401 without it, telling nothing,
    /// and 404 with it; never the frontend's page.
    #[tokio::test]
    async fn every_api_path_behind_token() {
        for path in ["/api", "/api/", "/api/v1", "/api/v1/sql", "/api/v2/version"] {
            let without = ask(Method::GET, path, &[(header::HOST, HOST)]).await;
            assert_eq!(without.status, StatusCode::UNAUTHORIZED, "{path}");
        }
        let bearer = bearer();
        for path in ["/api/v1/sql", "/api/v2/version"] {
            let headers = [(header::HOST, HOST), (header::AUTHORIZATION, bearer.as_str())];
            let with = ask(Method::GET, path, &headers).await;
            assert_eq!(with.status, StatusCode::NOT_FOUND, "{path}");
            assert_eq!(with.header(header::CONTENT_TYPE), Some("application/json"), "{path}");
        }
    }

    #[tokio::test]
    async fn write_from_other_origin_forbidden() {
        let bearer = bearer();
        let origins = [
            None,
            Some("http://evil.example"),
            Some("null"),
            Some("http://127.0.0.1:9999"),
            Some("https://127.0.0.1:4173"),
        ];
        for origin in origins {
            let mut headers = vec![(header::HOST, HOST), (header::AUTHORIZATION, bearer.as_str())];
            headers.extend(origin.map(|value| (header::ORIGIN, value)));
            for method in [Method::POST, Method::PUT, Method::DELETE, Method::PATCH] {
                let answer = ask(method.clone(), "/api/v1/version", &headers).await;
                assert_eq!(answer.status, StatusCode::FORBIDDEN, "{method} {origin:?}");
            }
        }
    }

    #[tokio::test]
    async fn write_from_own_origin_passes() {
        let bearer = bearer();
        for origin in ["http://127.0.0.1:4173", "http://localhost:4173"] {
            let headers = [
                (header::HOST, HOST),
                (header::AUTHORIZATION, bearer.as_str()),
                (header::ORIGIN, origin),
            ];
            let answer = ask(Method::POST, "/api/v1/version", &headers).await;
            // Past the guard: the route itself answers, and takes GET alone.
            assert_eq!(answer.status, StatusCode::METHOD_NOT_ALLOWED, "{origin}");
        }
    }

    #[tokio::test]
    async fn every_answer_secured() {
        let requests = [
            ("/", HOST),
            ("/favicon.svg", HOST),
            ("/", "evil.example"),
            ("/api/v1/version", HOST),
            ("/assets/gone.js", HOST),
        ];
        for (path, host) in requests {
            let answer = ask(Method::GET, path, &[(header::HOST, host)]).await;
            let policy = answer.header(header::CONTENT_SECURITY_POLICY).unwrap_or_default();
            assert!(policy.starts_with("default-src 'self'"), "{path} {host}: {policy}");
            assert!(policy.contains("frame-ancestors 'none'"), "{path} {host}: {policy}");
            // Inline styles, for the SQL editor; never an inline script, which `default-src 'self'` refuses.
            assert!(
                policy.contains("style-src 'self' 'unsafe-inline';"),
                "{path} {host}: {policy}"
            );
            assert!(!policy.contains("script-src"), "{path} {host}: {policy}");
            assert_eq!(
                answer.header(header::X_CONTENT_TYPE_OPTIONS),
                Some("nosniff"),
                "{path} {host}"
            );
            assert_eq!(
                answer.header(header::REFERRER_POLICY),
                Some("no-referrer"),
                "{path} {host}"
            );
        }
    }
}

// The API over a lake (milestone 6).

mod api {
    use std::sync::Arc;

    use axum::Router;
    use axum::http::{Method, StatusCode, header};

    use super::{Answer, HOST, Memory, send};
    use crate::api::{self, Backend, Detached, Fault, Kind, Mark, Marks, Named, Ref, Status, Table};
    use crate::guard::Access;
    use crate::{Token, router_with};

    const SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef";

    /// A lake that answers what it is given: its status, or a fault for every path; on `main`, two tables; its marks.
    struct Given(Result<Status, Fault>);

    impl Given {
        fn lake() -> Self {
            Given(Ok(status()))
        }

        fn or<T>(&self, value: T) -> Result<T, Fault> {
            self.0.as_ref().map(|_| value).map_err(Fault::clone)
        }
    }

    impl Backend for Given {
        fn status(&self) -> api::Answer<'_, Status> {
            let answer = self.0.clone();
            Box::pin(async move { answer })
        }

        fn refs(&self) -> api::Answer<'_, Vec<Ref>> {
            let main = Ref {
                name: "main".into(),
                kind: "branch".into(),
                parent: None,
                fork_seq: None,
                created_seq: 1,
                protected: true,
            };
            let dev = Ref {
                name: "dev/1".into(),
                kind: "branch".into(),
                parent: Some("main".into()),
                fork_seq: Some(4),
                created_seq: 5,
                protected: false,
            };
            let answer = self.or(vec![main, dev]);
            Box::pin(async move { answer })
        }

        fn tables<'a>(&'a self, on: &'a str) -> api::Answer<'a, Vec<Table>> {
            let table = |namespace: &str, name: &str| Table {
                namespace: vec![namespace.into()],
                name: name.into(),
            };
            let answer = match on {
                "main" => self.or(vec![table("sales", "orders"), table("default", "t")]),
                "dev/1" => self.or(vec![table("default", "t")]),
                _ => Err(Fault::new(Kind::Usage, format!("no ref {on}"))),
            };
            Box::pin(async move { answer })
        }

        fn watch(&self) -> api::Answer<'_, Marks> {
            let marks = vec![
                Ok(Mark { seq: 7, deleted: 0 }),
                Ok(Mark { seq: 9, deleted: 0 }),
                Err(Fault::new(Kind::Unknown, "the connection was lost")),
            ];
            let answer = self.or(Box::pin(futures_util::stream::iter(marks)) as Marks);
            Box::pin(async move { answer })
        }
    }

    fn status() -> Status {
        Status {
            postgres: None,
            local: Some("/home/a/lake/postgres".into()),
            schema: "metalake".into(),
            warehouse: Some("file:///home/a/lake/data".into()),
            format_version: "0".into(),
            writer_version: "0.2.0".into(),
            protocol_version: Some("0".into()),
            refs: vec![Named {
                name: "main".into(),
                kind: "branch".into(),
            }],
        }
    }

    fn over(backend: impl Backend) -> Router {
        let access = Access {
            token: Token::given(SECRET.to_owned()).unwrap(),
            port: 4173,
        };
        router_with(Arc::new(Memory::new(&[])), Arc::new(backend), access)
    }

    async fn ask(router: Router, path: &str) -> Answer {
        let bearer = format!("Bearer {SECRET}");
        let headers = [(header::HOST, HOST), (header::AUTHORIZATION, bearer.as_str())];
        send(router, Method::GET, path, &headers).await
    }

    fn json(answer: &Answer) -> serde_json::Value {
        serde_json::from_slice(&answer.body).unwrap()
    }

    #[tokio::test]
    async fn status_answers_lake() {
        let answer = ask(over(Given::lake()), "/api/v1/status").await;
        assert_eq!(answer.status, StatusCode::OK);
        assert_eq!(answer.header(header::CACHE_CONTROL), Some("no-store"));
        let body = json(&answer);
        assert_eq!(body["schema"], "metalake");
        assert_eq!(body["postgres"], serde_json::Value::Null);
        assert_eq!(body["refs"][0]["name"], "main");
        assert_eq!(body["refs"][0]["kind"], "branch");
    }

    /// Each kind of failure with its status, said as the command line's `--format json` says it.
    #[tokio::test]
    async fn fault_by_kind() {
        let cases = [
            (Kind::Usage, StatusCode::BAD_REQUEST, "usage"),
            (Kind::Failed, StatusCode::INTERNAL_SERVER_ERROR, "failed"),
            (Kind::Conflict, StatusCode::CONFLICT, "conflict"),
            (Kind::Unknown, StatusCode::BAD_GATEWAY, "unknown"),
        ];
        for (kind, code, name) in cases {
            let fault = Fault {
                code: Some("40001".into()),
                retryable: true,
                ..Fault::new(kind, "the catalog said no")
            };
            let answer = ask(over(Given(Err(fault))), "/api/v1/status").await;
            assert_eq!(answer.status, code, "{name}");
            let body = json(&answer);
            assert_eq!(body["kind"], name);
            assert_eq!(body["message"], "the catalog said no");
            assert_eq!(body["code"], "40001");
            assert_eq!(body["retryable"], true);
            assert!(body.get("rule").is_none(), "absent, not null: {body}");
        }
    }

    #[tokio::test]
    async fn detached_says_no_lake() {
        for path in [
            "/api/v1/status",
            "/api/v1/refs",
            "/api/v1/refs/main/tables",
            "/api/v1/events",
        ] {
            let answer = ask(over(Detached), path).await;
            assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{path}");
            assert!(
                json(&answer)["message"].as_str().unwrap().contains("without a lake"),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn refs_answers_branches_and_tags() {
        let answer = ask(over(Given::lake()), "/api/v1/refs").await;
        assert_eq!(answer.status, StatusCode::OK);
        let body = json(&answer);
        assert_eq!(body[0]["name"], "main");
        assert_eq!(body[0]["parent"], serde_json::Value::Null);
        assert_eq!(body[1]["parent"], "main");
        assert_eq!(body[1]["fork_seq"], 4);
    }

    /// A ref by its name, percent-encoded as a path's segment: one with a slash is one segment.
    #[tokio::test]
    async fn tables_of_ref() {
        let main = json(&ask(over(Given::lake()), "/api/v1/refs/main/tables").await);
        assert_eq!(main[0]["namespace"], serde_json::json!(["sales"]));
        assert_eq!(main[0]["name"], "orders");
        let dev = ask(over(Given::lake()), "/api/v1/refs/dev%2F1/tables").await;
        assert_eq!(dev.status, StatusCode::OK);
        assert_eq!(json(&dev).as_array().unwrap().len(), 1);
        let none = ask(over(Given::lake()), "/api/v1/refs/nope/tables").await;
        assert_eq!(none.status, StatusCode::BAD_REQUEST);
        assert_eq!(json(&none)["message"], "no ref nope");
    }

    /// The marks as server-sent events, each a `mark`; a fault is the last event.
    #[tokio::test]
    async fn events_stream_marks() {
        let answer = ask(over(Given::lake()), "/api/v1/events").await;
        assert_eq!(answer.status, StatusCode::OK);
        assert_eq!(answer.header(header::CONTENT_TYPE), Some("text/event-stream"));
        assert_eq!(answer.header(header::CACHE_CONTROL), Some("no-store"));
        let body = String::from_utf8(answer.body).unwrap();
        let events: Vec<&str> = body.split("\n\n").filter(|e| !e.is_empty()).collect();
        assert_eq!(
            events,
            [
                "event: mark\ndata: {\"seq\":7,\"deleted\":0}",
                "event: mark\ndata: {\"seq\":9,\"deleted\":0}",
                "event: fault\ndata: {\"kind\":\"unknown\",\"message\":\"the connection was lost\",\"retryable\":false}",
            ]
        );
    }

    /// Every path of the spec is behind the token: the spec and the router are walked together, so that a path added
    /// to one is checked.
    #[tokio::test]
    async fn every_spec_path_behind_token() {
        let spec: serde_json::Value = serde_json::from_str(&api::openapi()).unwrap();
        let paths = spec["paths"].as_object().unwrap();
        assert!(paths.len() >= 5, "{paths:?}");
        for (path, _) in paths {
            assert!(path.starts_with("/api/v1/"), "{path}");
            let path = &path.replace("{ref}", "main");
            let without = send(over(Given::lake()), Method::GET, path, &[(header::HOST, HOST)]).await;
            assert_eq!(without.status, StatusCode::UNAUTHORIZED, "{path}");
            assert_eq!(ask(over(Given::lake()), path).await.status, StatusCode::OK, "{path}");
        }
    }

    /// The spec the frontend's types are generated from is the one the router answers by. Written again with
    /// `cargo run -p metalake-ui --no-default-features --example openapi`.
    #[test]
    fn spec_committed() {
        let committed = include_str!("../web/openapi.json").replace("\r\n", "\n");
        assert!(
            committed == api::openapi(),
            "web/openapi.json is not the router's: cargo run -p metalake-ui --no-default-features --example openapi"
        );
    }
}
