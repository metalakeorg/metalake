//! The token services a credential asks: a form posted or a URL read, answering an OAuth2 access token.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use metalake_pg::Password;
use serde_json::Value;

use super::error;

/// How long a token service may take to answer.
const WAIT: Duration = Duration::from_secs(20);

pub(crate) fn client() -> reqwest::Client {
    reqwest::Client::builder().timeout(WAIT).build().unwrap_or_default()
}

/// The JSON a request answers, or why it was refused, as OAuth2's `error_description` says.
pub(crate) async fn json(request: reqwest::RequestBuilder, what: &str) -> metalake_pg::Result<Value> {
    let answer = request.send().await.map_err(|e| error(format!("{what}: {e}")))?;
    let ok = answer.status().is_success();
    let text = answer
        .text()
        .await
        .map_err(|e| error(format!("{what}'s answer: {e}")))?;
    let body: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
    if !ok {
        let said = body["error_description"]
            .as_str()
            .or(body["error"]["message"].as_str())
            .or(body["error"].as_str())
            .or(body.as_str())
            .unwrap_or("refused");
        return Err(error(format!("{what} refused: {said}")));
    }
    Ok(body)
}

/// An OAuth2 answer's token, and when it expires: `expires_in` seconds from now, or `expires_on` in seconds since the
/// epoch, as a number or a string.
pub(crate) fn token(body: &Value, what: &str) -> metalake_pg::Result<Password> {
    let value = body["access_token"]
        .as_str()
        .ok_or_else(|| error(format!("{what} answered no access_token")))?
        .to_string();
    let seconds = |v: &Value| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()));
    let now = Instant::now();
    let expires = match (seconds(&body["expires_in"]), seconds(&body["expires_on"])) {
        (Some(within), _) => Some(now + Duration::from_secs(within)),
        (None, Some(on)) => {
            let epoch = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Some(now + Duration::from_secs(on.saturating_sub(epoch)))
        }
        // Each lasts minutes: one that says nothing is kept for none.
        (None, None) => Some(now),
    };
    Ok(Password { value, expires })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    use super::*;

    /// A server of one answer: the request it got, as text, and its address.
    pub(crate) fn answering(status: u16, body: &str) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let (sent, got) = mpsc::channel();
        let body = body.to_string();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
                request.push_str(&line);
                if line == "\r\n" {
                    break;
                }
            }
            let mut content = vec![0; length];
            reader.read_exact(&mut content).unwrap();
            request.push_str(&String::from_utf8(content).unwrap());
            sent.send(request).unwrap();
            let mut stream = stream;
            write!(
                stream,
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        (address, got)
    }

    #[test]
    fn expiry_read() {
        let lasting = token(&serde_json::json!({"access_token": "t", "expires_in": 3599}), "x").unwrap();
        assert_eq!(lasting.value, "t");
        let left = lasting.expires.unwrap() - Instant::now();
        assert!(left > Duration::from_secs(3590), "{left:?}");
        let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let on = token(
            &serde_json::json!({"access_token": "t", "expires_on": (epoch + 600).to_string()}),
            "x",
        )
        .unwrap();
        assert!(on.expires.unwrap() - Instant::now() > Duration::from_secs(590));
        assert!(token(&serde_json::json!({"token": "t"}), "x").is_err());
    }
}
