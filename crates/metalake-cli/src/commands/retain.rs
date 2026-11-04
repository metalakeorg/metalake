//! `metalake retain`: what a lake keeps, a policy declared once, which `maintain` then acts on. With none, a ref keeps
//! every snapshot (RET-1), and nothing is ever taken away.

use metalake::Retention;
use serde_json::json;

use crate::args::Retain;
use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

pub async fn run(args: &Retain, lake: &Lake, out: &Out) -> Result<(), Failure> {
    let criteria = args.max_age.is_some() || args.min_snapshots.is_some() || args.min_seq.is_some();
    if args.on.is_some() && !criteria {
        return Err(Failure::usage(
            "a ref needs what to keep: --max-age, --min-snapshots or --min-seq",
        ));
    }
    if args.on.is_none() && (criteria || args.grace.is_none()) {
        return Err(Failure::usage(
            "--max-age, --min-snapshots and --min-seq are of a ref; --grace alone is of the lake",
        ));
    }

    if let Some(grace) = args.grace {
        lake.catalog.set_collect_grace(grace).await?;
    }
    if let Some(name) = &args.on {
        let refs = lake.catalog.refs().await?;
        let on = refs
            .iter()
            .find(|r| r.name() == name)
            .ok_or_else(|| Failure::failed(format!("no ref {name}")))?;
        on.set_retention(&Retention {
            min_seq: args.min_seq,
            max_age: args.max_age,
            min_snapshots: args.min_snapshots,
        })
        .await?;
    }

    let seconds = |d: Option<std::time::Duration>| d.map(|d| d.as_secs());
    let set = json!({
        "ref": args.on,
        "max_age_seconds": seconds(args.max_age),
        "min_snapshots": args.min_snapshots,
        "min_seq": args.min_seq,
        "grace_seconds": seconds(args.grace),
    });
    let columns = ["ref", "max_age_seconds", "min_snapshots", "min_seq", "grace_seconds"];
    out.records(&columns, &[set]).await
}
