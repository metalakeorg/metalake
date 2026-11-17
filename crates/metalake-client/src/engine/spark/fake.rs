//! A Spark Connect server that answers as the tests say, on a port of its own: what it hears is kept to be asked.
//!
//! A command is answered with its rows within; `select …` with the relation that runs it, which answers `n` = 1, 2;
//! `select long …` answers 1 and ends its stream before `ResultComplete`, so that it is taken up again, which answers 3.
//! `fail` is refused as Spark refuses a missing table; `changed`, the first time, as a server that restarted; `slow`
//! never answers.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::StreamWriter;
use futures_util::stream::Stream;
use prost::Message;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Code, Request, Response, Status};

use super::proto::execute_plan_response::{ArrowBatch, ResponseType, ResultComplete, SqlCommandResult};
use super::proto::spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer};
use super::proto::*;

/// What the fake heard.
#[derive(Debug, Default)]
pub(super) struct Heard {
    /// Each statement sent as a command, with its session.
    pub statements: Vec<(String, String)>,
    /// Each setting, with its session.
    pub settings: Vec<(String, String, String)>,
    pub interrupted: Vec<String>,
    pub released: Vec<String>,
    /// Each reattach, with the last response it said it had.
    pub reattached: Vec<Option<String>>,
    /// The `authorization` each call bore.
    pub tokens: Vec<Option<String>>,
    changed: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct Fake {
    pub heard: Arc<Mutex<Heard>>,
}

type Answers = Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;

fn ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut data = Vec::new();
    let mut writer = StreamWriter::try_new(&mut data, &batch.schema()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    data
}

fn numbers(values: &[i64]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    ipc(&RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap())
}

fn response(id: &str, answer: ResponseType) -> Result<ExecutePlanResponse, Status> {
    Ok(ExecutePlanResponse {
        response_id: id.to_string(),
        response_type: Some(answer),
        ..Default::default()
    })
}

fn complete(id: &str) -> Result<ExecutePlanResponse, Status> {
    response(id, ResponseType::ResultComplete(ResultComplete {}))
}

fn stream(answers: Vec<Result<ExecutePlanResponse, Status>>) -> Answers {
    Box::pin(futures_util::stream::iter(answers))
}

/// As Spark refuses a table it does not have: its message, and an `ErrorInfo` with its class and state.
fn missing() -> Status {
    #[derive(Clone, PartialEq, Message)]
    struct RpcStatus {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
        #[prost(message, repeated, tag = "3")]
        details: Vec<prost_types::Any>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct ErrorInfo {
        #[prost(string, tag = "1")]
        reason: String,
        #[prost(string, tag = "2")]
        domain: String,
        #[prost(map = "string, string", tag = "3")]
        metadata: std::collections::HashMap<String, String>,
    }
    let message = "The table or view `nope` cannot be found.";
    let info = ErrorInfo {
        reason: "org.apache.spark.sql.AnalysisException".into(),
        domain: "org.apache.spark".into(),
        metadata: [("errorClass", "TABLE_OR_VIEW_NOT_FOUND"), ("sqlState", "42P01")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    let details = RpcStatus {
        code: Code::Internal as i32,
        message: message.into(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".into(),
            value: info.encode_to_vec(),
        }],
    };
    Status::with_details(Code::Internal, message, details.encode_to_vec().into())
}

impl Fake {
    fn bore<T>(&self, request: &Request<T>) {
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        self.heard.lock().unwrap().tokens.push(token);
    }
}

#[tonic::async_trait]
impl SparkConnectService for Fake {
    type ExecutePlanStream = Answers;
    type ReattachExecuteStream = Answers;

    async fn execute_plan(&self, request: Request<ExecutePlanRequest>) -> Result<Response<Answers>, Status> {
        self.bore(&request);
        let request = request.into_inner();
        let op = request.plan.and_then(|p| p.op_type);
        let answers = match op {
            Some(plan::OpType::Command(Command {
                command_type: Some(command::CommandType::SqlCommand(SqlCommand { sql, .. })),
            })) => {
                let changed = {
                    let mut heard = self.heard.lock().unwrap();
                    heard.statements.push((request.session_id.clone(), sql.clone()));
                    let changed = sql == "changed" && !heard.changed;
                    heard.changed |= changed;
                    changed
                };
                if changed {
                    return Err(Status::internal(
                        "[INVALID_HANDLE.SESSION_CHANGED] The existing Spark server driver instance has restarted.",
                    ));
                }
                match sql.as_str() {
                    "fail" => return Err(missing()),
                    "slow" => Box::pin(futures_util::stream::pending()) as Answers,
                    query if query.starts_with("select") => stream(vec![
                        response(
                            "r1",
                            ResponseType::SqlCommandResult(SqlCommandResult {
                                relation: Some(Relation {
                                    common: None,
                                    rel_type: Some(relation::RelType::Sql(Sql {
                                        query: query.to_string(),
                                        ..Default::default()
                                    })),
                                }),
                            }),
                        ),
                        complete("r2"),
                    ]),
                    _ => {
                        let schema = Arc::new(Schema::new(vec![Field::new("said", DataType::Utf8, false)]));
                        let batch =
                            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["done"]))]).unwrap();
                        stream(vec![
                            response(
                                "r1",
                                ResponseType::SqlCommandResult(SqlCommandResult {
                                    relation: Some(Relation {
                                        common: None,
                                        rel_type: Some(relation::RelType::LocalRelation(LocalRelation {
                                            data: Some(ipc(&batch)),
                                            schema: None,
                                        })),
                                    }),
                                }),
                            ),
                            complete("r2"),
                        ])
                    }
                }
            }
            Some(plan::OpType::Root(Relation {
                rel_type: Some(relation::RelType::Sql(Sql { query, .. })),
                ..
            })) => {
                let first = response(
                    "b1",
                    ResponseType::ArrowBatch(ArrowBatch {
                        row_count: 1,
                        data: numbers(&[1]),
                    }),
                );
                if query.starts_with("select long") {
                    stream(vec![first])
                } else {
                    stream(vec![
                        first,
                        response(
                            "b2",
                            ResponseType::ArrowBatch(ArrowBatch {
                                row_count: 1,
                                data: numbers(&[2]),
                            }),
                        ),
                        complete("b3"),
                    ])
                }
            }
            _ => return Err(Status::unimplemented("not a plan the fake knows")),
        };
        Ok(Response::new(answers))
    }

    async fn reattach_execute(&self, request: Request<ReattachExecuteRequest>) -> Result<Response<Answers>, Status> {
        self.bore(&request);
        self.heard
            .lock()
            .unwrap()
            .reattached
            .push(request.into_inner().last_response_id);
        Ok(Response::new(stream(vec![
            response(
                "b2",
                ResponseType::ArrowBatch(ArrowBatch {
                    row_count: 1,
                    data: numbers(&[3]),
                }),
            ),
            complete("b3"),
        ])))
    }

    async fn config(&self, request: Request<ConfigRequest>) -> Result<Response<ConfigResponse>, Status> {
        self.bore(&request);
        let request = request.into_inner();
        if let Some(config_request::operation::OpType::Set(set)) = request.operation.and_then(|o| o.op_type) {
            let mut heard = self.heard.lock().unwrap();
            for pair in set.pairs {
                heard
                    .settings
                    .push((request.session_id.clone(), pair.key, pair.value.unwrap_or_default()));
            }
        }
        Ok(Response::new(ConfigResponse {
            session_id: request.session_id,
            ..Default::default()
        }))
    }

    async fn interrupt(&self, request: Request<InterruptRequest>) -> Result<Response<InterruptResponse>, Status> {
        if let Some(interrupt_request::Interrupt::OperationId(id)) = request.into_inner().interrupt {
            self.heard.lock().unwrap().interrupted.push(id);
        }
        Ok(Response::new(InterruptResponse::default()))
    }

    async fn release_execute(
        &self,
        request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        self.heard
            .lock()
            .unwrap()
            .released
            .push(request.into_inner().operation_id);
        Ok(Response::new(ReleaseExecuteResponse::default()))
    }

    async fn analyze_plan(&self, _: Request<AnalyzePlanRequest>) -> Result<Response<AnalyzePlanResponse>, Status> {
        Err(Status::unimplemented("analyze_plan"))
    }

    async fn add_artifacts(
        &self,
        _: Request<tonic::Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        Err(Status::unimplemented("add_artifacts"))
    }

    async fn artifact_status(
        &self,
        _: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented("artifact_status"))
    }
}

/// The fake, served on a port of its own: its `sc://` url.
pub(super) async fn serve(fake: Fake) -> String {
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = incoming.local_addr().unwrap().port();
    tokio::spawn(
        Server::builder()
            .add_service(SparkConnectServiceServer::new(fake))
            .serve_with_incoming(incoming),
    );
    format!("sc://127.0.0.1:{port}")
}
