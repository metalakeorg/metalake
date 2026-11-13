//! JupyterLab on the UI's origin, under [`PREFIX`] (ADR 0008 §5). Every request there goes to the Notebook's own
//! JupyterLab, never to a host or port a request names; with the browser's `Host` and its path kept, which Jupyter's
//! origin and cookie checks need (tried: rewriting `Host` is refused). A websocket's upgrade is tunneled as bytes once
//! Jupyter has switched protocols, so that its subprotocol, frames and pings pass untouched. The UI's bearer never
//! reaches Jupyter; Jupyter's own `token` scheme does, and Jupyter decides who it answers.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::Response;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use crate::notebook::{Notebook, PREFIX};

/// One request under [`PREFIX`], answered by the Notebook's JupyterLab.
pub(crate) async fn forward(State(notebook): State<Arc<Notebook>>, mut request: Request) -> Response {
    // One host name: Jupyter's websocket breaks when the page is on `localhost` and its checks see `127.0.0.1`.
    if let Some(port) = request
        .headers()
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
        .and_then(|host| host.strip_prefix("localhost:"))
    {
        let path = request.uri().path_and_query().map_or("/", |p| p.as_str());
        return redirect(&format!("http://127.0.0.1:{port}{path}"));
    }
    let Some((port, _)) = notebook.reach().await else {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "JupyterLab is not running: open the Notebook page",
        );
    };
    let bearer = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .get(..7)
                .is_some_and(|scheme| scheme.eq_ignore_ascii_case("bearer "))
        });
    if bearer {
        request.headers_mut().remove(header::AUTHORIZATION);
    }
    // To Jupyter, as the path alone: it serves under the same prefix.
    let path = request.uri().path_and_query().map_or(PREFIX, |p| p.as_str()).to_owned();
    *request.uri_mut() = Uri::try_from(path).expect("a request's own path");
    let upgrading = request.headers().contains_key(header::UPGRADE);
    let browser = upgrading.then(|| hyper::upgrade::on(&mut request));

    let Ok(stream) = TcpStream::connect(("127.0.0.1", port)).await else {
        return refused(StatusCode::BAD_GATEWAY, "JupyterLab does not answer");
    };
    let Ok((mut sender, connection)) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await else {
        return refused(StatusCode::BAD_GATEWAY, "JupyterLab does not answer");
    };
    tokio::spawn(connection.with_upgrades());
    let mut response = match sender.send_request(request).await {
        Ok(response) => response,
        Err(_) => return refused(StatusCode::BAD_GATEWAY, "JupyterLab did not answer"),
    };
    if response.status() == StatusCode::SWITCHING_PROTOCOLS
        && let Some(browser) = browser
    {
        let jupyter = hyper::upgrade::on(&mut response);
        tokio::spawn(async move {
            if let (Ok(browser), Ok(jupyter)) = (browser.await, jupyter.await) {
                let (mut browser, mut jupyter) = (TokioIo::new(browser), TokioIo::new(jupyter));
                let _ = tokio::io::copy_bidirectional(&mut browser, &mut jupyter).await;
            }
        });
    }
    response.map(Body::new)
}

fn redirect(to: &str) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::TEMPORARY_REDIRECT;
    if let Ok(location) = HeaderValue::from_str(to) {
        response.headers_mut().insert(header::LOCATION, location);
    }
    response
}

fn refused(status: StatusCode, message: &str) -> Response {
    let body = format!(r#"{{"kind":"failed","message":"{message}","retryable":true}}"#);
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
