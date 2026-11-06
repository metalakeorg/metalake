//! A failure, as an Iceberg client reads it (REST-2). The code is a contract: a client rebuilds its commit on a 409, and on
//! a 5xx it cannot know whether its commit went in and cleans nothing. So 409 means that nothing was applied, and what is
//! not known is a 5xx.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use metalake_pg::ErrorKind;
use serde_json::json;

use crate::apply::{ApplyError, Refused};

/// What a request failed with: its code, the name of the exception a client raises, and what to tell the person.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    code: StatusCode,
    kind: &'static str,
    message: String,
}

impl Failure {
    fn new(code: StatusCode, kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn no_route() -> Self {
        Self::new(StatusCode::NOT_FOUND, "NoSuchEndpointException", "no such route")
    }

    pub(crate) fn no_namespace(name: &str) -> Self {
        let message = format!("no namespace {name}");
        Self::new(StatusCode::NOT_FOUND, "NoSuchNamespaceException", message)
    }
}

/// The code and exception of what the protocol answered.
fn of_protocol(kind: ErrorKind) -> (StatusCode, &'static str) {
    match kind {
        ErrorKind::Conflict | ErrorKind::Serialization => (StatusCode::CONFLICT, "CommitFailedException"),
        ErrorKind::Invalid | ErrorKind::Unsupported => (StatusCode::BAD_REQUEST, "BadRequestException"),
        ErrorKind::Permission => (StatusCode::FORBIDDEN, "ForbiddenException"),
        ErrorKind::Unknown => (StatusCode::GATEWAY_TIMEOUT, "CommitStateUnknownException"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "ServerErrorException"),
    }
}

impl From<metalake::Error> for Failure {
    fn from(error: metalake::Error) -> Self {
        use metalake::Error as E;
        let (code, kind) = match &error {
            E::Protocol(e) => of_protocol(e.kind()),
            E::NotFound(_) => (StatusCode::NOT_FOUND, "NoSuchTableException"),
            E::Invalid(_) | E::Unsupported(_) | E::Location(_) => (StatusCode::BAD_REQUEST, "BadRequestException"),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "ServerErrorException"),
        };
        Self::new(code, kind, error.to_string())
    }
}

impl From<Refused> for Failure {
    fn from(refused: Refused) -> Self {
        match refused {
            Refused::Invalid(message) => Self::new(StatusCode::BAD_REQUEST, "BadRequestException", message),
            Refused::Conflict(message) => Self::new(StatusCode::CONFLICT, "CommitFailedException", message),
        }
    }
}

impl From<ApplyError> for Failure {
    fn from(error: ApplyError) -> Self {
        match error {
            ApplyError::Refused(refused) => refused.into(),
            ApplyError::Core(core) => core.into(),
        }
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let error = json!({"error": {"message": self.message, "type": self.kind, "code": self.code.as_u16()}});
        (self.code, Json(error)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest2_protocol_error_status() {
        for (kind, code) in [
            (ErrorKind::Conflict, 409),
            (ErrorKind::Serialization, 409),
            (ErrorKind::Invalid, 400),
            (ErrorKind::Unsupported, 400),
            (ErrorKind::Permission, 403),
            (ErrorKind::Unknown, 504),
            (ErrorKind::Corrupt, 500),
            (ErrorKind::Postgres, 500),
        ] {
            assert_eq!(of_protocol(kind).0.as_u16(), code, "{kind:?}");
        }
    }

    #[test]
    fn rest2_refused_status_matches_protocol() {
        let conflict = Failure::from(Refused::Conflict("moved".into()));
        assert_eq!((conflict.code, conflict.kind), of_protocol(ErrorKind::Conflict));
        let invalid = Failure::from(Refused::Invalid("no".into()));
        assert_eq!((invalid.code, invalid.kind), of_protocol(ErrorKind::Invalid));
    }

    #[tokio::test]
    async fn error_response_body() {
        let response = Failure::no_namespace("a.b").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body,
            json!({"error": {"message": "no namespace a.b", "type": "NoSuchNamespaceException", "code": 404}})
        );
    }
}
