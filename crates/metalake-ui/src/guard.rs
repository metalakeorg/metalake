//! Who may use the UI (ADR 0007 §4.2, research/ui.md part 2). Each refusal stops what the others do not: under DNS
//! rebinding a page is same-origin with itself, so only the `Host` tells it apart; another process of the machine forges
//! every header, so only the run's token does; and a page of another site may make the browser send a request, which
//! the token's header, needing a preflight that is never granted, and the `Origin` refuse.
//!
//! | Refused | Answer | Where |
//! |---|---|---|
//! | a `Host` other than `127.0.0.1:<port>` or `localhost:<port>` | 403 | every path |
//! | no `Authorization: Bearer <the run's token>` | 401 | under `/api` |
//! | a method that writes, from an `Origin` other than the UI's own, or from none | 403 | under `/api` |
//!
//! And every answer, a refusal included, carries the headers of [`secured`].

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;

use crate::token::Token;

/// What the guard checks a request against: the run's token, and the port the UI listens on.
#[derive(Debug, Clone)]
pub(crate) struct Access {
    pub token: Token,
    pub port: u16,
}

impl Access {
    /// The hosts the UI answers as, with its port: the loopback by its address and by its name.
    fn host(&self, given: &str) -> bool {
        let port = self.port;
        [format!("127.0.0.1:{port}"), format!("localhost:{port}")]
            .iter()
            .any(|host| host.eq_ignore_ascii_case(given))
    }

    /// The origins the UI's own pages have.
    fn origin(&self, given: &str) -> bool {
        let port = self.port;
        [format!("http://127.0.0.1:{port}"), format!("http://localhost:{port}")]
            .iter()
            .any(|origin| origin.eq_ignore_ascii_case(given))
    }
}

/// Every path: a request for another host is refused, as a page that rebound its name to the loopback sends.
pub(crate) async fn host(State(access): State<Arc<Access>>, request: Request, next: Next) -> Response {
    let given = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    match given {
        Some(given) if access.host(given) => next.run(request).await,
        _ => refuse(
            StatusCode::FORBIDDEN,
            "this UI answers only as 127.0.0.1 or localhost, on its own port",
        ),
    }
}

/// Under `/api`: a request without the run's token as its bearer is refused.
pub(crate) async fn bearer(State(access): State<Arc<Access>>, request: Request, next: Next) -> Response {
    if bearer_of(request.headers()).is_some_and(|given| access.token.matches(given)) {
        return next.run(request).await;
    }
    let mut response = refuse(
        StatusCode::UNAUTHORIZED,
        "this request lacks the run's token: open the URL `metalake ui` printed",
    );
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

/// Under `/api`: a method that writes is refused unless it comes from the UI's own pages. A browser always sends
/// `Origin` with one, so a request without it is not the UI's frontend.
pub(crate) async fn origin(State(access): State<Arc<Access>>, request: Request, next: Next) -> Response {
    let safe = matches!(*request.method(), Method::GET | Method::HEAD | Method::OPTIONS);
    let own = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|given| access.origin(given));
    if safe || own {
        next.run(request).await
    } else {
        refuse(
            StatusCode::FORBIDDEN,
            "a write to this UI comes from its own pages alone",
        )
    }
}

/// The headers every answer carries: no frame may hold the UI, no script but its own runs in it, and nothing it
/// answers is read as another type, sent as a referrer, or opened by another site.
pub(crate) async fn secured(mut response: Response) -> Response {
    let headers = response.headers_mut();
    for (name, value) in [
        (
            header::CONTENT_SECURITY_POLICY,
            "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
        ),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (header::REFERRER_POLICY, "no-referrer"),
        (
            header::HeaderName::from_static("cross-origin-opener-policy"),
            "same-origin",
        ),
        (
            header::HeaderName::from_static("cross-origin-resource-policy"),
            "same-origin",
        ),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    response
}

/// The token a request bears, by the `Bearer` scheme in any case (RFC 9110 §11.1).
fn bearer_of(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
}

/// A refusal, in the shape of the client's errors. `message` holds no character JSON would escape.
fn refuse(status: StatusCode, message: &str) -> Response {
    let body = format!(r#"{{"kind":"usage","message":"{message}"}}"#);
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
