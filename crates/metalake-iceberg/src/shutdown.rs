//! `POST /metalake/shutdown`: how the process that started a server stops it, on any system and with no signal. The
//! route exists only when the server was given a token, and answers only to a request that bears it; it is MetaLake's
//! own, outside Iceberg's routes, as Jupyter's `/api/shutdown` is outside notebooks'.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use tokio::sync::Notify;

use crate::caller::{bearer, same};

/// The route's path.
pub const PATH: &str = "/metalake/shutdown";

#[derive(Clone)]
struct Shutdown {
    token: Arc<str>,
    stop: Arc<Notify>,
}

/// The route, which wakes `stop` when a request bears `token` as its bearer.
pub(crate) fn router(token: &str, stop: Arc<Notify>) -> Router {
    let shutdown = Shutdown {
        token: token.into(),
        stop,
    };
    Router::new().route(PATH, post(shut)).with_state(shutdown)
}

async fn shut(State(shutdown): State<Shutdown>, headers: HeaderMap) -> StatusCode {
    match bearer(&headers) {
        Some(given) if same(given.as_bytes(), shutdown.token.as_bytes()) => {
            shutdown.stop.notify_one();
            StatusCode::ACCEPTED
        }
        _ => StatusCode::UNAUTHORIZED,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    /// The status line's code of one request to `address`.
    async fn post(address: std::net::SocketAddr, authorization: Option<&str>) -> u16 {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let header = authorization
            .map(|a| format!("Authorization: {a}\r\n"))
            .unwrap_or_default();
        let request =
            format!("POST {PATH} HTTP/1.1\r\nHost: x\r\n{header}Content-Length: 0\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.unwrap();
        answer.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    #[tokio::test]
    async fn shutdown_requires_token() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(Notify::new());
        let notified = stop.clone();
        let served = tokio::spawn(async move {
            axum::serve(listener, router("s3cret", stop))
                .with_graceful_shutdown(async move { notified.notified().await })
                .await
        });

        assert_eq!(post(address, None).await, 401);
        assert_eq!(post(address, Some("Bearer wrong!")).await, 401);
        assert_eq!(post(address, Some("Basic s3cret")).await, 401);
        assert!(!served.is_finished(), "a request without the token stops nothing");

        assert_eq!(post(address, Some("Bearer s3cret")).await, 202);
        tokio::time::timeout(Duration::from_secs(5), served)
            .await
            .expect("it stops")
            .unwrap()
            .unwrap();
    }
}
