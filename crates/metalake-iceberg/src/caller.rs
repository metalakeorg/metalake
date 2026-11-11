//! Who calls (REST-10, REST-11): a request that bears the server's own token as its bearer is a `writer`'s, one that bears
//! none is anonymous, and one that bears another is refused with 401, so that an engine given the wrong token says so at
//! once rather than later, when it asks for what only a writer is given. A lake's own server holds its lake's token,
//! kept where that lake's user alone reads it, which that user's engines bear (REST-11).

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::rest::errors::Failure;

/// Who a request is from, as the request's extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caller {
    /// It bears the server's token.
    Writer,
    /// It bears none.
    Anonymous,
}

/// The bearer of a request, if it bears one.
pub(crate) fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

/// Who a request bearing `bearer` is, to a server holding `token`; `None` when it bears a token that is not the server's.
pub(crate) fn caller(bearer: Option<&str>, token: &str) -> Option<Caller> {
    match bearer {
        None => Some(Caller::Anonymous),
        Some(given) if same(given.as_bytes(), token.as_bytes()) => Some(Caller::Writer),
        Some(_) => None,
    }
}

/// The layer that says who each request is from, before any route.
pub(crate) async fn identify(State(token): State<Arc<str>>, mut request: Request, next: Next) -> Response {
    match caller(bearer(request.headers()), &token) {
        Some(found) => {
            request.extensions_mut().insert(found);
            next.run(request).await
        }
        None => Failure::unknown_token().into_response(),
    }
}

/// Whether two secrets are equal, in a time that does not say how much of them is.
pub(crate) fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |differ, (x, y)| differ | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use axum::routing::get;
    use axum::{Extension, Router};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    /// The status and body of one request to `address`.
    async fn ask(address: std::net::SocketAddr, authorization: Option<&str>) -> (u16, String) {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let header = authorization
            .map(|a| format!("Authorization: {a}\r\n"))
            .unwrap_or_default();
        let request = format!("GET /who HTTP/1.1\r\nHost: x\r\n{header}Connection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.unwrap();
        let status = answer.split_whitespace().nth(1).unwrap().parse().unwrap();
        let body = answer
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        (status, body)
    }

    #[tokio::test]
    async fn caller_over_http() {
        let token: Arc<str> = "s3cret".into();
        let app = Router::new()
            .route(
                "/who",
                get(|Extension(caller): Extension<Caller>| async move { format!("{caller:?}") }),
            )
            .layer(axum::middleware::from_fn_with_state(token, identify));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });

        assert_eq!(ask(address, Some("Bearer s3cret")).await, (200, "Writer".into()));
        assert_eq!(ask(address, None).await, (200, "Anonymous".into()));
        let (status, body) = ask(address, Some("Bearer wrong")).await;
        assert_eq!(status, 401);
        assert!(body.contains("NotAuthorizedException"), "{body}");
    }

    #[test]
    fn caller_by_bearer() {
        assert_eq!(caller(Some("the-token"), "the-token"), Some(Caller::Writer));
        assert_eq!(caller(None, "the-token"), Some(Caller::Anonymous));
        assert_eq!(caller(Some("another"), "the-token"), None, "another token is refused");
        assert_eq!(caller(Some(""), "the-token"), None);
    }

    #[test]
    fn token_comparison() {
        assert!(same(b"abc", b"abc"));
        assert!(!same(b"abc", b"abd"));
        assert!(!same(b"abc", b"abcd"));
    }
}
