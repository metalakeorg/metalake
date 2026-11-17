//! What Flink's SQL Gateway (REST v3) sends and answers, as its source writes it (`rest/serde`), not as its generated
//! spec names it; and its rows as Arrow. Each value is read as the text the JSON holds, a DECIMAL's digits all kept,
//! and cast strictly to the column's Arrow type; a type Arrow has no cast to from text keeps its text, a nested one as
//! JSON. A streaming result carries each row's kind first: Flink answers a changelog, `+I`, `-U`, `+U`, `-D`.

use std::sync::Arc;

use base64::Engine as _;
use datafusion::arrow::array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{Error, Result};

/// `POST /v3/sessions`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Opened {
    pub session_handle: String,
}

/// `POST /v3/sessions/{s}/statements`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Submitted {
    pub operation_handle: String,
}

/// `GET …/operations/{o}/result/{token}?rowFormat=JSON`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Fetched {
    pub result_type: ResultType,
    #[serde(default)]
    pub is_query_result: bool,
    pub results: Option<Results>,
    pub next_result_uri: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum ResultType {
    NotReady,
    Payload,
    Eos,
}

#[derive(Debug, Deserialize)]
pub(super) struct Results {
    #[serde(default)]
    pub columns: Vec<Column>,
    #[serde(default)]
    pub data: Vec<Row>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Column {
    pub name: String,
    pub logical_type: LogicalType,
}

/// A column's type as `LogicalTypeJsonSerializer` writes it: its root, and what the root takes.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct LogicalType {
    #[serde(rename = "type")]
    pub root: String,
    #[serde(default = "nullable")]
    pub nullable: bool,
    pub precision: Option<u8>,
    pub scale: Option<i8>,
}

fn nullable() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub(super) struct Row {
    pub kind: String,
    pub fields: Vec<Box<RawValue>>,
}

/// `{"errors": [...]}`, Flink REST's error body.
#[derive(Debug, Deserialize)]
pub(super) struct Failed {
    #[serde(default)]
    pub errors: Vec<String>,
}

/// A row's kind as a changelog column says it.
fn kind(kind: &str) -> &'static str {
    match kind {
        "UPDATE_BEFORE" => "-U",
        "UPDATE_AFTER" => "+U",
        "DELETE" => "-D",
        _ => "+I",
    }
}

/// A timestamp's unit from its precision: milliseconds up to 3, microseconds up to 6, else nanoseconds.
fn unit(precision: Option<u8>) -> TimeUnit {
    match precision.unwrap_or(6) {
        0..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

/// The Arrow type a Flink type is cast to from its text; none keeps the text.
pub(super) fn arrow_type(t: &LogicalType) -> Option<DataType> {
    Some(match t.root.as_str() {
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "DECIMAL" => DataType::Decimal128(t.precision.unwrap_or(10), t.scale.unwrap_or(0)),
        "CHAR" | "VARCHAR" => DataType::Utf8,
        "DATE" => DataType::Date32,
        "TIME_WITHOUT_TIME_ZONE" => DataType::Time64(TimeUnit::Microsecond),
        "TIMESTAMP_WITHOUT_TIME_ZONE" | "TIMESTAMP_WITH_LOCAL_TIME_ZONE" => {
            DataType::Timestamp(unit(t.precision), None)
        }
        _ => return None,
    })
}

/// A value as its text: a string's own, a number's digits as written, `null` none, anything else its JSON.
fn text(value: &RawValue) -> Option<String> {
    let raw = value.get().trim();
    if raw == "null" {
        return None;
    }
    if raw.starts_with('"') {
        return serde_json::from_str::<String>(raw).ok();
    }
    Some(raw.to_string())
}

/// A page's rows as one batch: `with_kind` puts each row's kind first, as a streaming result has it.
pub(super) fn arrow_of(columns: &[Column], rows: &[Row], with_kind: bool) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let mut fields = Vec::with_capacity(columns.len() + 1);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len() + 1);
    if with_kind {
        fields.push(Field::new("kind", DataType::Utf8, false));
        arrays.push(Arc::new(
            rows.iter().map(|r| Some(kind(&r.kind))).collect::<StringArray>(),
        ));
    }
    for (at, column) in columns.iter().enumerate() {
        let texts: Vec<Option<String>> = rows
            .iter()
            .map(|row| row.fields.get(at).and_then(|v| text(v)))
            .collect();
        let array: ArrayRef = match column.logical_type.root.as_str() {
            "BINARY" | "VARBINARY" => {
                let decoded: Vec<Option<Vec<u8>>> = texts
                    .iter()
                    .map(|t| {
                        t.as_ref()
                            .map(|t| base64::engine::general_purpose::STANDARD.decode(t))
                            .transpose()
                    })
                    .collect::<std::result::Result<_, _>>()
                    .map_err(|e| Error::failed(format!("flink's answer: a binary value is not base64: {e}")))?;
                Arc::new(BinaryArray::from_iter(decoded))
            }
            _ => {
                let texts: ArrayRef = Arc::new(texts.into_iter().collect::<StringArray>());
                arrow_type(&column.logical_type)
                    .and_then(|to| cast_with_options(&texts, &to, &strict).ok())
                    .unwrap_or(texts)
            }
        };
        fields.push(Field::new(
            &column.name,
            array.data_type().clone(),
            column.logical_type.nullable || array.null_count() > 0,
        ));
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    if rows.is_empty() || arrays.is_empty() {
        return Ok((schema, Vec::new()));
    }
    Ok((schema.clone(), vec![RecordBatch::try_new(schema, arrays)?]))
}

/// What a failed call says: the deepest `Caused by:` of the trace Flink sends, or its last error.
pub(super) fn cause(body: &str) -> String {
    let errors = serde_json::from_str::<Failed>(body)
        .map(|f| f.errors)
        .unwrap_or_else(|_| vec![body.to_string()]);
    let deepest = errors
        .iter()
        .flat_map(|e| e.lines())
        .filter_map(|line| line.trim().strip_prefix("Caused by: "))
        .last()
        .map(str::to_string);
    deepest
        .or_else(|| errors.last().cloned())
        .unwrap_or_else(|| body.to_string())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Decimal128Array, Int64Array, TimestampMillisecondArray};

    use super::*;

    fn read(json: &str) -> Fetched {
        serde_json::from_str(json).unwrap()
    }

    /// As the Gateway answers `SELECT 1` (Flink's documents) and a page of a streaming aggregate.
    #[test]
    fn a_page_reads_as_the_gateway_writes_it() {
        let page = read(
            r#"{"resultType":"PAYLOAD","isQueryResult":true,"resultKind":"SUCCESS_WITH_CONTENT",
                "results":{"columns":[{"name":"EXPR$0","logicalType":{"type":"INTEGER","nullable":false},"comment":null}],
                "rowFormat":"JSON","data":[{"kind":"INSERT","fields":[1]}]},
                "nextResultUri":"/v3/sessions/s/operations/o/result/1?rowFormat=JSON"}"#,
        );
        assert_eq!(page.result_type, ResultType::Payload);
        assert!(page.is_query_result);
        let results = page.results.unwrap();
        let (schema, batches) = arrow_of(&results.columns, &results.data, false).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            read(r#"{"resultType":"NOT_READY","nextResultUri":"/x"}"#).result_type,
            ResultType::NotReady
        );
        assert!(read(r#"{"resultType":"EOS"}"#).next_result_uri.is_none());
    }

    #[test]
    fn each_type_is_arrow_and_a_decimal_keeps_its_digits() {
        let page = read(
            r#"{"resultType":"PAYLOAD","results":{"columns":[
                {"name":"big","logicalType":{"type":"BIGINT","nullable":true}},
                {"name":"d","logicalType":{"type":"DECIMAL","nullable":true,"precision":38,"scale":3}},
                {"name":"ts","logicalType":{"type":"TIMESTAMP_WITHOUT_TIME_ZONE","nullable":true,"precision":3}},
                {"name":"b","logicalType":{"type":"VARBINARY","nullable":true,"length":2147483647}},
                {"name":"m","logicalType":{"type":"MAP","nullable":true}}],
              "data":[
                {"kind":"INSERT","fields":[9007199254740993,12345678901234567890123456789012345.678,"2020-12-30T12:13:14.123","AQI=",{"a":1}]},
                {"kind":"INSERT","fields":[null,null,null,null,null]}]}}"#,
        );
        let results = page.results.unwrap();
        let (_, batches) = arrow_of(&results.columns, &results.data, false).unwrap();
        let batch = &batches[0];
        let big = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(big.value(0), 9_007_199_254_740_993, "2^53 + 1, exact");
        assert!(big.is_null(1));
        let d = batch.column(1).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(d.value_as_string(0), "12345678901234567890123456789012345.678");
        let ts = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_609_330_394_123);
        let b = batch.column(3).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(b.value(0), [1, 2]);
        let m = batch.column(4).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(m.value(0), r#"{"a":1}"#);
    }

    #[test]
    fn a_streaming_result_says_each_rows_kind_first() {
        let page = read(
            r#"{"resultType":"PAYLOAD","results":{"columns":[{"name":"n","logicalType":{"type":"BIGINT"}}],
              "data":[{"kind":"INSERT","fields":[1]},{"kind":"UPDATE_BEFORE","fields":[1]},
                      {"kind":"UPDATE_AFTER","fields":[2]},{"kind":"DELETE","fields":[2]}]}}"#,
        );
        let results = page.results.unwrap();
        let (schema, batches) = arrow_of(&results.columns, &results.data, true).unwrap();
        assert_eq!(schema.field(0).name(), "kind");
        let kinds = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let kinds: Vec<&str> = (0..kinds.len()).map(|i| kinds.value(i)).collect();
        assert_eq!(kinds, ["+I", "-U", "+U", "-D"]);
    }

    #[test]
    fn a_failure_says_its_deepest_cause() {
        let body = r#"{"errors":["Internal server error.","<Exception on server side:
org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to fetchResults.
Caused by: org.apache.flink.table.gateway.service.utils.SqlExecutionException: Failed to execute the operation x.
Caused by: org.apache.flink.table.api.ValidationException: Object 'nope' not found
	at org.apache.flink...
End of exception on server side>"]}"#;
        assert_eq!(
            cause(body),
            "org.apache.flink.table.api.ValidationException: Object 'nope' not found"
        );
        assert_eq!(cause(r#"{"errors":["Bad request."]}"#), "Bad request.");
        assert_eq!(cause("not json"), "not json");
    }
}
