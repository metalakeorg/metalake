//! A statement sent to Spark through Spark Connect: gRPC, one `ExecutePlan` a statement, its rows answered as Arrow.
//! It is sent as PySpark's `spark.sql` sends it, a `SqlCommand`: Spark runs a command (DDL, DML, `SHOW`, `SET`) at
//! once and answers its rows within; a query it answers as a relation, which a second `ExecutePlan` runs.
//!
//! Statements on a ref run in a Spark session of that ref, which the lake's catalogs are set on under their own names,
//! each asking the lake's Iceberg server for itself on that ref (`catalog@ref`): what `USING` sends reads the same names
//! MetaLake does. Spark keeps a catalog as it was first set, so a ref has a session of its own.
//!
//! The protocol is Spark 3.5's (`proto/`, written by `spark-connect/generate.sh`); a newer server answers it too.

use std::collections::{BTreeSet, HashMap};
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig};
use url::Url;

use super::EngineConfig;
use crate::{Error, Result};

#[allow(
    clippy::all,
    clippy::pedantic,
    missing_docs,
    rustdoc::all,
    unreachable_pub,
    dead_code,
    reason = "written by tonic from Spark's protos"
)]
pub(crate) mod proto {
    include!("proto/spark.connect.rs");
}

mod errors;

use proto::execute_plan_request::{RequestOption, request_option};
use proto::execute_plan_response::ResponseType;
use proto::spark_connect_service_client::SparkConnectServiceClient;
use proto::{
    Command, ConfigRequest, ExecutePlanRequest, ExecutePlanResponse, KeyValue, Plan, ReattachExecuteRequest,
    ReattachOptions, Relation, ReleaseExecuteRequest, SqlCommand, UserContext, command, config_request, plan, relation,
    release_execute_request,
};

/// Spark Connect's client, each call carrying the engine's token.
type Client = SparkConnectServiceClient<InterceptedService<Channel, Bearer>>;

/// Where Spark Connect answers when its url names no port.
const PORT: u16 = 15002;
/// The largest message Spark may answer, an Arrow batch: as PySpark takes it.
const MESSAGE: usize = 128 * 1024 * 1024;
/// How this side names itself to Spark.
const CLIENT: &str = "metalake";

/// Where a Spark Connect server answers, from its `sc://host[:port][/;use_ssl=true]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Endpoint {
    host: String,
    port: u16,
    tls: bool,
}

impl Endpoint {
    /// Refuses what is not Spark Connect's url, or holds what never goes in one here: a token is in `token_env`.
    pub(crate) fn parse(url: &str) -> Result<Self> {
        let parsed = Url::parse(url).map_err(|e| Error::usage(format!("a Spark engine's url {url}: {e}")))?;
        if parsed.scheme() != "sc" {
            return Err(Error::usage(format!(
                "a Spark engine's url is Spark Connect's, sc://host:{PORT}, not {url}"
            )));
        }
        let host = parsed
            .host_str()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| Error::usage(format!("a Spark engine's url names no host: {url}")))?
            .to_string();
        let mut tls = false;
        let path = parsed.path().trim_start_matches('/');
        for parameter in path.split(';').filter(|p| !p.is_empty()) {
            match parameter.split_once('=') {
                Some(("use_ssl", value)) => {
                    tls = value.parse().map_err(|_| {
                        Error::usage(format!("a Spark engine's url: use_ssl is true or false, not {value}"))
                    })?
                }
                Some(("token", _)) => {
                    return Err(Error::usage(
                        "a Spark engine's token is never in its url: name the variable that holds it, token_env",
                    ));
                }
                _ => {
                    return Err(Error::usage(format!(
                        "a Spark engine's url takes use_ssl alone, not `{parameter}`"
                    )));
                }
            }
        }
        Ok(Self {
            host,
            port: parsed.port().unwrap_or(PORT),
            tls,
        })
    }

    fn uri(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://{}:{}", self.host, self.port)
    }
}

/// The token each call carries, when the engine signs in by one.
#[derive(Debug, Clone)]
struct Bearer(Option<MetadataValue<tonic::metadata::Ascii>>);

impl Interceptor for Bearer {
    fn call(&mut self, mut request: tonic::Request<()>) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        if let Some(token) = &self.0 {
            request.metadata_mut().insert("authorization", token.clone());
        }
        Ok(request)
    }
}

/// A Spark the lake names, ready to be sent statements: clones share its sessions, which are released when the last
/// is dropped.
#[derive(Debug, Clone)]
pub struct Spark {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    url: String,
    channel: Channel,
    bearer: Bearer,
    user: String,
    /// The Spark session of each ref statements ran on, by the ref's name: a ref's catalogs are set on a session of
    /// its own, so that each catalog, which Spark keeps as it was first set, is that ref's.
    sessions: Mutex<HashMap<String, Held>>,
}

/// A Spark session, and the lake's catalogs set on it.
#[derive(Debug, Clone)]
struct Held {
    id: String,
    catalogs: BTreeSet<String>,
}

/// A catalog of the lake as Spark is given it: its name, and the session's settings that make it.
pub(crate) type CatalogSettings = (String, Vec<(String, String)>);

/// Why a statement was not run: Spark's session is not the one it was, or anything else.
enum Failed {
    SessionChanged,
    Error(Error),
}

impl From<Error> for Failed {
    fn from(e: Error) -> Self {
        Self::Error(e)
    }
}

impl Spark {
    /// The engine `config` names, its secret read from the environment now: a variable named and unset is said here,
    /// before anything is sent. Spark is reached at the first statement.
    pub fn new(name: &str, config: &EngineConfig) -> Result<Self> {
        let endpoint = Endpoint::parse(&config.url)?;
        let bearer = match &config.token_env {
            Some(variable) => {
                let token = std::env::var(variable).map_err(|_| {
                    Error::usage(format!("the engine `{name}` signs in by ${variable}, which is not set"))
                })?;
                let value = format!("Bearer {token}")
                    .parse()
                    .map_err(|_| Error::usage(format!("the engine `{name}`: its token is not a header's value")))?;
                Bearer(Some(value))
            }
            None => Bearer(None),
        };
        let mut channel =
            Channel::from_shared(endpoint.uri()).map_err(|e| Error::usage(format!("spark at {}: {e}", config.url)))?;
        if endpoint.tls {
            channel = channel
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(|e| Error::failed(format!("spark at {}: {e}", config.url)))?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                url: config.url.clone(),
                channel: channel.connect_lazy(),
                bearer,
                user: config.user.clone().unwrap_or_else(|| CLIENT.to_string()),
                sessions: Mutex::new(HashMap::new()),
            }),
        })
    }

    fn client(&self) -> Client {
        SparkConnectServiceClient::with_interceptor(self.inner.channel.clone(), self.inner.bearer.clone())
            .max_decoding_message_size(MESSAGE)
            .max_encoding_message_size(MESSAGE)
    }

    fn user_context(&self) -> UserContext {
        UserContext {
            user_id: self.inner.user.clone(),
            user_name: self.inner.user.clone(),
            ..Default::default()
        }
    }

    /// Runs one statement on `main`, no catalog of the lake set: Spark's own, as a check asks.
    pub async fn sql(&self, text: &str) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        self.sql_on(crate::MAIN, &[], text).await
    }

    /// Runs one statement in the Spark session of the ref `on`, `catalogs` set on it first if they are not, and
    /// answers its rows. A statement Spark refuses fails with Spark's own words. If Spark restarted, and so lost the
    /// session and what was set on it, a session is begun again, once.
    pub async fn sql_on(
        &self,
        on: &str,
        catalogs: &[CatalogSettings],
        text: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        match self.attempt(on, catalogs, text).await {
            Ok(rows) => Ok(rows),
            Err(Failed::Error(e)) => Err(e),
            Err(Failed::SessionChanged) => {
                self.forget(on);
                match self.attempt(on, catalogs, text).await {
                    Ok(rows) => Ok(rows),
                    Err(Failed::Error(e)) => Err(e),
                    Err(Failed::SessionChanged) => Err(Error::failed(format!(
                        "spark at {}: its session changed twice under one statement",
                        self.inner.url
                    ))),
                }
            }
        }
    }

    async fn attempt(
        &self,
        on: &str,
        catalogs: &[CatalogSettings],
        text: &str,
    ) -> std::result::Result<(SchemaRef, Vec<RecordBatch>), Failed> {
        let session = self.session(on);
        for (catalog, settings) in catalogs {
            if !session.catalogs.contains(catalog) {
                self.set(&session.id, settings).await?;
                self.mark(on, catalog);
            }
        }
        let command = Plan {
            op_type: Some(plan::OpType::Command(Command {
                command_type: Some(command::CommandType::SqlCommand(SqlCommand {
                    sql: text.to_string(),
                    ..Default::default()
                })),
            })),
        };
        match self.execute(&session.id, command).await? {
            Answer::Rows(schema, batches) => Ok((schema, batches)),
            // A query: Spark answers the relation to run.
            Answer::Relation(relation) => {
                let root = Plan {
                    op_type: Some(plan::OpType::Root(relation)),
                };
                match self.execute(&session.id, root).await? {
                    Answer::Rows(schema, batches) => Ok((schema, batches)),
                    Answer::Relation(_) => Err(Error::failed(format!(
                        "spark at {}: a query answered a query to run",
                        self.inner.url
                    ))
                    .into()),
                }
            }
        }
    }

    /// The session of `on`, begun if there is none.
    fn session(&self, on: &str) -> Held {
        let mut sessions = self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner());
        sessions
            .entry(on.to_string())
            .or_insert_with(|| Held {
                id: uuid::Uuid::new_v4().to_string(),
                catalogs: BTreeSet::new(),
            })
            .clone()
    }

    fn mark(&self, on: &str, catalog: &str) {
        let mut sessions = self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(held) = sessions.get_mut(on) {
            held.catalogs.insert(catalog.to_string());
        }
    }

    fn forget(&self, on: &str) {
        let mut sessions = self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner());
        sessions.remove(on);
    }

    /// Asks `call` of a client, again while Spark says it cannot answer now, waiting longer each time.
    async fn again<T, F, Fut>(&self, mut call: F) -> std::result::Result<T, Status>
    where
        F: FnMut(Client) -> Fut,
        Fut: Future<Output = std::result::Result<T, Status>>,
    {
        let mut wait = errors::FIRST;
        for _ in 0..errors::AGAIN {
            match call(self.client()).await {
                Err(status) if errors::retryable(&status) => {
                    tokio::time::sleep(wait).await;
                    wait = (wait * 4).min(errors::LONGEST);
                }
                answered => return answered,
            }
        }
        call(self.client()).await
    }

    fn refused(&self, status: &Status) -> Failed {
        if errors::session_changed(status) {
            Failed::SessionChanged
        } else {
            Failed::Error(errors::said(&self.inner.url, status))
        }
    }

    /// Sets `settings` on session `id`.
    async fn set(&self, id: &str, settings: &[(String, String)]) -> std::result::Result<(), Failed> {
        let request = ConfigRequest {
            session_id: id.to_string(),
            user_context: Some(self.user_context()),
            operation: Some(config_request::Operation {
                op_type: Some(config_request::operation::OpType::Set(config_request::Set {
                    pairs: settings
                        .iter()
                        .map(|(key, value)| KeyValue {
                            key: key.clone(),
                            value: Some(value.clone()),
                        })
                        .collect(),
                })),
            }),
            client_type: Some(CLIENT.to_string()),
        };
        self.again(|mut client| {
            let request = request.clone();
            async move { client.config(request).await }
        })
        .await
        .map_err(|status| self.refused(&status))?;
        Ok(())
    }

    /// Runs `plan` in session `id`, reattachable: Spark ends a stream after two minutes or a gibibyte, and only
    /// `ResultComplete` says the answer is all there; until then the stream is taken up again where it ended.
    async fn execute(&self, id: &str, plan: Plan) -> std::result::Result<Answer, Failed> {
        let operation = uuid::Uuid::new_v4().to_string();
        let request = ExecutePlanRequest {
            session_id: id.to_string(),
            user_context: Some(self.user_context()),
            operation_id: Some(operation.clone()),
            plan: Some(plan),
            client_type: Some(CLIENT.to_string()),
            request_options: vec![RequestOption {
                request_option: Some(request_option::RequestOption::ReattachOptions(ReattachOptions {
                    reattachable: true,
                })),
            }],
            tags: Vec::new(),
        };
        let mut stream = self
            .again(|mut client| {
                let request = request.clone();
                async move { client.execute_plan(request).await }
            })
            .await
            .map_err(|status| self.refused(&status))?
            .into_inner();
        let mut collected = Collected::default();
        let mut last: Option<String> = None;
        let mut taken_up = 0;
        loop {
            match stream.message().await {
                Ok(Some(response)) => {
                    last = Some(response.response_id.clone());
                    collected.take(response)?;
                }
                Ok(None) if collected.complete => break,
                ended => {
                    if let Err(status) = &ended {
                        if !errors::retryable(status) {
                            return Err(self.refused(status));
                        }
                    }
                    taken_up += 1;
                    if taken_up > errors::AGAIN * 4 {
                        return Err(Error::failed(format!(
                            "spark at {}: its answer ended {taken_up} times before it was all there",
                            self.inner.url
                        ))
                        .into());
                    }
                    let request = ReattachExecuteRequest {
                        session_id: id.to_string(),
                        user_context: Some(self.user_context()),
                        operation_id: operation.clone(),
                        client_type: Some(CLIENT.to_string()),
                        last_response_id: last.clone(),
                    };
                    stream = self
                        .again(|mut client| {
                            let request = request.clone();
                            async move { client.reattach_execute(request).await }
                        })
                        .await
                        .map_err(|status| self.refused(&status))?
                        .into_inner();
                }
            }
        }
        // What Spark holds of the answer, to take it up again, is let go: what is taken is all.
        let release = ReleaseExecuteRequest {
            session_id: id.to_string(),
            user_context: Some(self.user_context()),
            operation_id: operation,
            client_type: Some(CLIENT.to_string()),
            release: Some(release_execute_request::Release::ReleaseAll(
                release_execute_request::ReleaseAll {},
            )),
        };
        let _ = self.client().release_execute(release).await;
        Ok(collected.answer())
    }
}

impl Drop for Inner {
    /// Each session this side began is released, if the server knows how (Spark 4.0 on); a 3.5 server keeps them.
    fn drop(&mut self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let sessions: Vec<String> = self
            .sessions
            .get_mut()
            .map(|s| s.values().map(|h| h.id.clone()).collect())
            .unwrap_or_default();
        if sessions.is_empty() {
            return;
        }
        let channel = self.channel.clone();
        let bearer = self.bearer.clone();
        let user = self.user.clone();
        runtime.spawn(async move {
            for id in sessions {
                let _ = release_session(channel.clone(), bearer.clone(), &user, id).await;
            }
        });
    }
}

/// `ReleaseSession`, Spark 4.0's, which the 3.5 protocol this is written from lacks: its two messages by hand.
#[derive(Clone, PartialEq, prost::Message)]
struct ReleaseSessionRequest {
    #[prost(string, tag = "1")]
    session_id: String,
    #[prost(message, optional, tag = "2")]
    user_context: Option<UserContext>,
    #[prost(string, optional, tag = "3")]
    client_type: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ReleaseSessionResponse {
    #[prost(string, tag = "1")]
    session_id: String,
}

async fn release_session(
    channel: Channel,
    mut bearer: Bearer,
    user: &str,
    id: String,
) -> std::result::Result<(), Status> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.map_err(|e| Status::unavailable(e.to_string()))?;
    let request = bearer.call(tonic::Request::new(()))?;
    let (metadata, extensions, ()) = request.into_parts();
    let request = tonic::Request::from_parts(
        metadata,
        extensions,
        ReleaseSessionRequest {
            session_id: id,
            user_context: Some(UserContext {
                user_id: user.to_string(),
                user_name: user.to_string(),
                ..Default::default()
            }),
            client_type: Some(CLIENT.to_string()),
        },
    );
    let path =
        tonic::codegen::http::uri::PathAndQuery::from_static("/spark.connect.SparkConnectService/ReleaseSession");
    let codec: tonic_prost::ProstCodec<ReleaseSessionRequest, ReleaseSessionResponse> =
        tonic_prost::ProstCodec::default();
    grpc.unary(request, path, codec).await?;
    Ok(())
}

/// What one `ExecutePlan` answered: rows, or for a query sent as a command, the relation that runs it.
enum Answer {
    Rows(SchemaRef, Vec<RecordBatch>),
    Relation(Relation),
}

/// An answer as its responses arrive.
#[derive(Default)]
struct Collected {
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
    relation: Option<Relation>,
    complete: bool,
}

impl Collected {
    fn take(&mut self, response: ExecutePlanResponse) -> Result<()> {
        match response.response_type {
            Some(ResponseType::ArrowBatch(batch)) => self.rows(&batch.data)?,
            Some(ResponseType::SqlCommandResult(result)) => match result.relation {
                Some(Relation {
                    rel_type: Some(relation::RelType::LocalRelation(local)),
                    ..
                }) => {
                    if let Some(data) = local.data {
                        self.rows(&data)?;
                    }
                }
                Some(answered @ Relation { rel_type: Some(_), .. }) => self.relation = Some(answered),
                _ => {}
            },
            Some(ResponseType::ResultComplete(_)) => self.complete = true,
            _ => {}
        }
        Ok(())
    }

    fn rows(&mut self, data: &[u8]) -> Result<()> {
        let (schema, batches) = arrow(data)?;
        self.schema.get_or_insert(schema);
        self.batches.extend(batches);
        Ok(())
    }

    fn answer(self) -> Answer {
        match self.relation {
            Some(relation) => Answer::Relation(relation),
            None => Answer::Rows(self.schema.unwrap_or_else(|| Arc::new(Schema::empty())), self.batches),
        }
    }
}

/// An Arrow IPC stream, as Spark answers each batch: its schema, then its batches.
fn arrow(data: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let reader = StreamReader::try_new(Cursor::new(data), None)
        .map_err(|e| Error::failed(format!("spark's answer is not Arrow: {e}")))?;
    let schema = reader.schema();
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::failed(format!("spark's answer is not Arrow: {e}")))?;
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_endpoint_as_spark_connect_writes_it() {
        let e = Endpoint::parse("sc://spark.example").unwrap();
        assert_eq!((e.host.as_str(), e.port, e.tls), ("spark.example", 15002, false));
        assert_eq!(e.uri(), "http://spark.example:15002");
        let e = Endpoint::parse("sc://spark.example:443/;use_ssl=true").unwrap();
        assert_eq!(e.uri(), "https://spark.example:443");
        assert!(Endpoint::parse("sc://spark.example/;use_ssl=yes").is_err());
        assert!(
            Endpoint::parse("sc://spark.example/;token=s")
                .unwrap_err()
                .to_string()
                .contains("token_env")
        );
        assert!(Endpoint::parse("sc://spark.example/;x-cluster=1").is_err());
        assert!(Endpoint::parse("grpc://spark.example").is_err());
        assert!(Endpoint::parse("sc:///").is_err());
    }

    #[test]
    fn a_token_variable_unset_is_said_before_anything_is_sent() {
        let config = EngineConfig {
            kind: super::super::EngineKind::Spark,
            url: "sc://127.0.0.1".into(),
            user: None,
            token_env: Some("METALAKE_TEST_SPARK_TOKEN_UNSET".into()),
            password_env: None,
            catalog: None,
            lake_url: None,
        };
        let error = Spark::new("s", &config).unwrap_err().to_string();
        assert!(error.contains("$METALAKE_TEST_SPARK_TOKEN_UNSET"), "{error}");
    }
}
