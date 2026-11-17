//! What Spark Connect says when it refuses, as MetaLake says it: Spark's message, its error class and SQL state from
//! the `google.rpc.ErrorInfo` its status carries, and which refusals are asked again.

use std::collections::HashMap;
use std::time::Duration;

use prost::Message;
use tonic::{Code, Status};

use crate::Error;

/// How many times a call Spark could not answer now is asked again, and the first wait, four times longer each time
/// up to [`LONGEST`]: as PySpark's, cut to seconds, since a face waits on it.
pub(super) const AGAIN: u32 = 5;
pub(super) const FIRST: Duration = Duration::from_millis(50);
pub(super) const LONGEST: Duration = Duration::from_secs(5);

/// `google.rpc.Status`, the details a gRPC status carries.
#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

/// `google.rpc.ErrorInfo`: Spark's exception class as `reason`; `errorClass`, `sqlState` among its metadata.
#[derive(Clone, PartialEq, Message)]
struct ErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
    #[prost(map = "string, string", tag = "3")]
    metadata: HashMap<String, String>,
}

/// The `ErrorInfo` a status carries, if it does.
fn info(status: &Status) -> Option<ErrorInfo> {
    let details = RpcStatus::decode(status.details()).ok()?;
    details
        .details
        .iter()
        .find(|any| any.type_url.ends_with("google.rpc.ErrorInfo"))
        .and_then(|any| ErrorInfo::decode(any.value.as_slice()).ok())
}

/// Whether Spark asks to be asked again: it is not reached now, or a stream it held was taken by another.
pub(super) fn retryable(status: &Status) -> bool {
    status.code() == Code::Unavailable
        || (status.code() == Code::Internal && status.message().contains("INVALID_CURSOR.DISCONNECTED"))
}

/// Whether the Spark session is not the one statements ran in: the server restarted, and what was set on it is gone.
pub(super) fn session_changed(status: &Status) -> bool {
    status.message().contains("INVALID_HANDLE.SESSION_CHANGED")
        || info(status).is_some_and(|i| {
            i.metadata
                .get("errorClass")
                .is_some_and(|c| c.starts_with("INVALID_HANDLE"))
        })
}

/// A refusal in Spark's words: its message, with its error class and SQL state when the message does not say them.
pub(super) fn said(url: &str, status: &Status) -> Error {
    let mut message = status.message().trim().to_string();
    if message.is_empty() {
        message = format!("{:?}", status.code());
    }
    if let Some(info) = info(status) {
        if let Some(class) = info
            .metadata
            .get("errorClass")
            .filter(|c| !message.contains(c.as_str()))
        {
            message = format!("[{class}] {message}");
        }
        if let Some(state) = info.metadata.get("sqlState").filter(|s| !message.contains(s.as_str())) {
            message = format!("{message} (SQLSTATE {state})");
        }
    }
    match status.code() {
        Code::Unauthenticated | Code::PermissionDenied => {
            Error::usage(format!("spark at {url} does not let this side in: {message}"))
        }
        Code::Unavailable => Error::failed(format!("spark at {url} is not reached: {message}")),
        _ => Error::failed(format!("spark at {url}: {message}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_info(code: Code, message: &str, metadata: &[(&str, &str)]) -> Status {
        let info = ErrorInfo {
            reason: "org.apache.spark.sql.AnalysisException".into(),
            domain: "org.apache.spark".into(),
            metadata: metadata.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        };
        let details = RpcStatus {
            code: code as i32,
            message: message.into(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/google.rpc.ErrorInfo".into(),
                value: info.encode_to_vec(),
            }],
        };
        Status::with_details(code, message, details.encode_to_vec().into())
    }

    #[test]
    fn a_refusal_says_its_class_and_state_once() {
        let status = with_info(
            Code::Internal,
            "The table or view `nope` cannot be found.",
            &[("errorClass", "TABLE_OR_VIEW_NOT_FOUND"), ("sqlState", "42P01")],
        );
        assert_eq!(
            said("sc://s", &status).to_string(),
            "spark at sc://s: [TABLE_OR_VIEW_NOT_FOUND] The table or view `nope` cannot be found. (SQLSTATE 42P01)"
        );
        let status = with_info(
            Code::Internal,
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `nope` cannot be found. SQLSTATE: 42P01",
            &[("errorClass", "TABLE_OR_VIEW_NOT_FOUND"), ("sqlState", "42P01")],
        );
        assert!(!said("sc://s", &status).to_string().contains("(SQLSTATE"));
        assert!(
            said("sc://s", &Status::internal("plain"))
                .to_string()
                .ends_with(": plain")
        );
    }

    #[test]
    fn what_is_asked_again_and_what_restarts_the_session() {
        assert!(retryable(&Status::unavailable("down")));
        assert!(retryable(&Status::internal("[INVALID_CURSOR.DISCONNECTED] taken")));
        assert!(!retryable(&Status::internal("[TABLE_OR_VIEW_NOT_FOUND]")));
        assert!(session_changed(&Status::internal(
            "[INVALID_HANDLE.SESSION_CHANGED] restarted"
        )));
        assert!(!session_changed(&Status::internal("other")));
        assert!(matches!(
            said("sc://s", &Status::unauthenticated("no")).kind,
            crate::Kind::Usage
        ));
    }
}
