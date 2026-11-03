//! Differential testing (ADR 0003 §5): one commit, expressed for the protocol and for the reference, applied to a
//! catalog of each; both must answer alike and leave the same rows.

use serde_json::{Value, json};
use tokio_postgres::Client;
use uuid::Uuid;

use crate::records::{FileAdd, FileRef, TableExpect, VectorSet};
use crate::{Difference, Error, compare, quote_ident};

/// What a commit does to one table.
#[derive(Debug, Clone, Default)]
pub struct TableChange {
    pub table_id: Uuid,
    pub adds: Vec<FileAdd>,
    pub removes: Vec<i64>,
    pub vectors: Vec<VectorSet>,
    /// `Some(None)` expects the table to have no snapshot on the ref.
    pub expected: Option<Option<i64>>,
    /// Definition changes, in the reference's shape (`create`, `rename`, `fields`…).
    pub definition: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct Commit {
    pub ref_id: i32,
    pub operation: String,
    pub tables: Vec<TableChange>,
    pub app: Option<(String, i64)>,
    /// Revision 0.1: namespace changes, in the reference's shape (`[{"namespace": [...], "create": true}]`).
    pub namespaces: Option<Value>,
}

/// How a catalog answered: the sequence of the commit, or the SQLSTATE of its refusal. Two answers are alike when
/// their outcomes are equal; the message of a refusal is kept to explain a difference, not compared.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Committed(i64),
    Refused(String),
}

#[derive(Debug, Clone)]
pub struct Answer {
    pub outcome: Outcome,
    pub message: Option<String>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_array(values: &[Option<Vec<u8>>]) -> Value {
    Value::Array(
        values
            .iter()
            .map(|v| v.as_deref().map_or(Value::Null, |b| json!(hex(b))))
            .collect(),
    )
}

impl Commit {
    /// The commit as the reference's `commit(jsonb)` takes it.
    pub fn reference_json(&self) -> Value {
        let tables: Vec<Value> = self
            .tables
            .iter()
            .map(|t| {
                let mut entry = json!({"table_id": t.table_id});
                let e = entry.as_object_mut().unwrap();
                if let Some(expected) = t.expected {
                    e.insert("expected_snapshot".into(), json!(expected));
                }
                if !t.adds.is_empty() {
                    let adds: Vec<Value> = t
                        .adds
                        .iter()
                        .map(|f| {
                            let mut a = json!({
                                "file_id": f.file_id, "path": f.path, "spec_id": f.spec_id, "partition": f.partition,
                                "sort_order_id": f.sort_order_id, "record_count": f.record_count,
                                "file_size": f.file_size, "footer_size": f.footer_size, "first_row_id": f.first_row_id,
                                "stat_field_ids": f.stat_field_ids, "lower": hex_array(&f.lower),
                                "upper": hex_array(&f.upper), "null_count": f.null_count, "nan_count": f.nan_count,
                                "value_count": f.value_count,
                            });
                            if let Some(content) = &f.content {
                                a["inline"] = json!(hex(content));
                            }
                            if let Some(footer) = &f.footer {
                                a["footer"] = json!(hex(footer));
                            }
                            a
                        })
                        .collect();
                    e.insert("add".into(), Value::Array(adds));
                }
                if !t.removes.is_empty() {
                    e.insert("remove".into(), json!(t.removes));
                }
                if !t.vectors.is_empty() {
                    let dvs: Vec<Value> = t
                        .vectors
                        .iter()
                        .map(|v| {
                            json!({"file_id": v.file_id, "base_seq": v.base_seq, "path": v.path,
                                   "content_offset": v.content_offset, "content_size": v.content_size,
                                   "cardinality": v.cardinality, "inline": v.inline.as_deref().map(hex)})
                        })
                        .collect();
                    e.insert("dvs".into(), Value::Array(dvs));
                }
                if let Some(Value::Object(definition)) = &t.definition {
                    for (k, v) in definition {
                        e.insert(k.clone(), v.clone());
                    }
                }
                entry
            })
            .collect();
        let mut c = json!({"ref": self.ref_id, "author": "test", "actor": "test", "operation": self.operation,
                           "tables": tables});
        if let Some(namespaces) = &self.namespaces {
            c["namespaces"] = namespaces.clone();
        }
        if let Some((app, version)) = &self.app {
            c["app_id"] = json!(app);
            c["app_version"] = json!(version);
        }
        c
    }
}

fn answer(result: Result<i64, tokio_postgres::Error>) -> Result<Answer, Error> {
    match result {
        Ok(seq) => Ok(Answer {
            outcome: Outcome::Committed(seq),
            message: None,
        }),
        Err(e) => match e.as_db_error() {
            Some(db) => Ok(Answer {
                outcome: Outcome::Refused(db.code().code().to_string()),
                message: Some(format!("{} ({})", db.message(), db.where_().unwrap_or(""))),
            }),
            None => Err(e.into()),
        },
    }
}

/// Applies a commit through the reference's `commit(jsonb)`, on a connection whose search_path is its catalog.
pub async fn commit_reference(client: &Client, commit: &Commit) -> Result<Answer, Error> {
    let result = client
        .query_one("select commit($1)", &[&commit.reference_json()])
        .await
        .map(|r| r.get::<_, i64>(0));
    answer(result)
}

/// Applies a commit through the protocol of the catalog in `schema`.
pub async fn commit_protocol(client: &Client, schema: &str, commit: &Commit) -> Result<Answer, Error> {
    let files: Vec<FileAdd> = commit.tables.iter().flat_map(|t| t.adds.clone()).collect();
    let removed: Vec<FileRef> = commit
        .tables
        .iter()
        .flat_map(|t| {
            t.removes.iter().map(|&f| FileRef {
                table_id: t.table_id,
                file_id: f,
            })
        })
        .collect();
    let vectors: Vec<VectorSet> = commit.tables.iter().flat_map(|t| t.vectors.clone()).collect();
    let expected: Vec<TableExpect> = commit
        .tables
        .iter()
        .filter_map(|t| {
            t.expected.map(|s| TableExpect {
                table_id: t.table_id,
                snapshot_id: s,
            })
        })
        .collect();
    let definitions: serde_json::Map<String, Value> = commit
        .tables
        .iter()
        .filter_map(|t| t.definition.clone().map(|d| (t.table_id.to_string(), d)))
        .collect();
    let changes = (!definitions.is_empty()).then_some(Value::Object(definitions));
    let (app_id, app_version) = commit.app.clone().unzip();
    let call = metalake_pg::Commit {
        ref_id: commit.ref_id,
        operation: commit.operation.clone(),
        files,
        removed,
        vectors,
        expected,
        changes,
        author: Some("test".into()),
        actor: Some("test".into()),
        app_id,
        app_version,
        namespaces: commit.namespaces.clone(),
        ..Default::default()
    };
    match metalake_pg::Protocol::new(schema).commit(client, &call).await {
        Ok(result) => Ok(Answer {
            outcome: Outcome::Committed(result.commit_seq),
            message: None,
        }),
        Err(e) => match e.code() {
            Some(code) => Ok(Answer {
                outcome: Outcome::Refused(code.to_string()),
                message: Some(e.to_string()),
            }),
            None => Err(e.into()),
        },
    }
}

/// The two catalogs of a differential test, each with its own connection.
pub struct Pair {
    pub protocol: String,
    pub reference: String,
    pub on_protocol: Client,
    pub on_reference: Client,
}

impl Pair {
    /// Applies a commit to both catalogs and checks that they answered alike and hold the same rows.
    pub async fn commit(&self, commit: &Commit) -> Result<Outcome, Error> {
        let by_reference = commit_reference(&self.on_reference, commit).await?;
        let by_protocol = commit_protocol(&self.on_protocol, &self.protocol, commit).await?;
        if by_reference.outcome != by_protocol.outcome {
            return Err(format!(
                "the reference answered {by_reference:?} and the protocol {by_protocol:?} to {:#}",
                commit.reference_json()
            )
            .into());
        }
        if let Outcome::Refused(_) = by_reference.outcome {
            self.align_sequences().await?;
        }
        self.same_rows().await?;
        Ok(by_reference.outcome)
    }

    /// How many sequence numbers a refused commit consumes is the implementation's freedom (CMT-3, ID-4): after a
    /// refusal, the protocol's sequences continue from the reference's, so that later commits compare.
    async fn align_sequences(&self) -> Result<(), Error> {
        let (p, r) = (quote_ident(&self.protocol), quote_ident(&self.reference));
        let mut sql = String::new();
        for s in ["commit_seq_gen", "file_id_gen", "ref_id_gen"] {
            sql += &format!("select setval('{p}.{s}', last_value, is_called) from {r}.{s};");
        }
        self.on_protocol.batch_execute(&sql).await?;
        Ok(())
    }

    pub async fn same_rows(&self) -> Result<(), Error> {
        let differences: Vec<Difference> = compare(&self.on_protocol, &self.protocol, &self.reference).await?;
        if differences.is_empty() {
            Ok(())
        } else {
            Err(format!("the catalogs differ (protocol, reference): {differences:#?}").into())
        }
    }

    /// Runs the same statements in both catalogs, with the search_path of each.
    pub async fn both(&self, sql: &str) -> Result<(), Error> {
        self.on_reference.batch_execute(sql).await?;
        self.on_protocol
            .batch_execute(&format!(
                "set search_path = {}; {sql}; reset search_path",
                quote_ident(&self.protocol)
            ))
            .await?;
        Ok(())
    }

    pub async fn drop(self) -> Result<(), Error> {
        self.on_protocol
            .batch_execute(&format!(
                "drop schema {} cascade; drop schema {} cascade",
                quote_ident(&self.protocol),
                quote_ident(&self.reference)
            ))
            .await?;
        Ok(())
    }
}

/// Copies every row and sequence of one catalog into another of the same format: the reference sets a scene up,
/// and the protocol continues from it.
pub async fn clone_catalog(client: &Client, from: &str, to: &str) -> Result<(), Error> {
    let (from_q, to_q) = (quote_ident(from), quote_ident(to));
    let tables: Vec<String> = client
        .query(
            "select table_name::text from information_schema.tables \
             where table_schema = $1 and table_type = 'BASE TABLE' and table_name <> 'meta'",
            &[&from],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    let all: Vec<String> = tables.iter().map(|t| format!("{to_q}.{}", quote_ident(t))).collect();
    let mut sql = format!(
        "begin; set local session_replication_role = replica; truncate {};",
        all.join(", ")
    );
    for t in &tables {
        let t = quote_ident(t);
        sql += &format!("insert into {to_q}.{t} select * from {from_q}.{t};");
    }
    let sequences: Vec<String> = client
        .query(
            "select sequence_name::text from information_schema.sequences where sequence_schema = $1",
            &[&from],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    for s in sequences {
        let s = quote_ident(&s);
        sql += &format!("select setval('{to_q}.{s}', last_value, is_called) from {from_q}.{s};");
    }
    sql += "commit;";
    client.batch_execute(&sql).await?;
    Ok(())
}
