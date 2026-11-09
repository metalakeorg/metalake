//! `metalake iceberg`: when Iceberg clients find each table's metadata written (REST-9). Every table is served to them
//! and written when one loads it; a table set `on-commit` on a ref is written by the Iceberg service after each commit.

use serde_json::json;

use crate::args::{IcebergTables, IcebergVerb};
use crate::out::Out;
use metalake_client::{Error, Lake};

pub async fn run(verb: &IcebergVerb, lake: &Lake, out: &Out) -> Result<(), Error> {
    let iceberg = lake.iceberg();
    let (args, when, commit) = match verb {
        IcebergVerb::Status { on } => {
            let columns = ["table", "head", "written", "behind", "since", "metadata"];
            return out.records(&columns, &iceberg.status(on).await?).await;
        }
        IcebergVerb::OnCommit(args) => (args, "on-commit", iceberg.on_commit(&args.on, &names(args)).await?),
        IcebergVerb::OnLoad(args) => (args, "on-load", iceberg.on_load(&args.on, &names(args)).await?),
    };
    let done = json!({"ref": args.on, "metadata": when, "tables": args.tables.join(", "), "commit": commit});
    out.records(&["ref", "metadata", "tables", "commit"], &[done]).await
}

fn names(args: &IcebergTables) -> Vec<&str> {
    args.tables.iter().map(String::as_str).collect()
}
