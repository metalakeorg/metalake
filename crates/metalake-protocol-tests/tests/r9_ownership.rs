//! R9: ownership (revision 0.5, OWN-1, OWN-2, RET-4, CMT-9), the protocol against the reference. A table holds files
//! under its location and files elsewhere, as a Hive table registered in place; it adopts a prefix; a path is refused
//! twice; a compaction and an expiring queue only what a table owns; a name mapping moves the catalog to format 1.
//! After every commit, and after each expiring, both catalogs answer alike and hold the same rows.

use metalake_protocol_tests::differential::{Commit, Outcome, Pair, TableChange, clone_catalog};
use metalake_protocol_tests::records::stored_file;
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, install_reference, quote_ident};
use serde_json::{Value, json};
use uuid::Uuid;

const MAIN: i32 = 1;

async fn fresh() -> Result<Pair, Error> {
    let (protocol, reference) = (fresh_schema("p"), fresh_schema("r"));
    let on_protocol = connect().await?;
    let on_reference = connect().await?;
    install_protocol(&on_protocol, &protocol).await?;
    install_reference(&on_protocol, &reference).await?;
    on_reference
        .batch_execute(&format!("set search_path = {}", quote_ident(&reference)))
        .await?;
    clone_catalog(&on_protocol, &reference, &protocol).await?;
    let pair = Pair {
        protocol,
        reference,
        on_protocol,
        on_reference,
    };
    pair.same_rows().await?;
    Ok(pair)
}

fn change(t: Uuid, paths: &[&str], removes: &[i64], definition: Option<Value>) -> Commit {
    Commit {
        ref_id: MAIN,
        operation: if removes.is_empty() { "append" } else { "replace" }.into(),
        tables: vec![TableChange {
            table_id: t,
            adds: paths
                .iter()
                .map(|p| stored_file(t, (*p).into(), 10 * removes.len().max(1) as i64, 1, 0, 9))
                .collect(),
            removes: removes.to_vec(),
            definition,
            ..TableChange::default()
        }],
        app: None,
        namespaces: None,
    }
}

/// What each catalog queued, and the format it is at.
async fn state(pair: &Pair, schema: &str) -> Result<(Vec<String>, String), Error> {
    let on = if schema == pair.protocol {
        &pair.on_protocol
    } else {
        &pair.on_reference
    };
    let s = quote_ident(schema);
    let row = on
        .query_one(
            &format!(
                "select array(select path from {s}.deletion_queue order by path),
                        (select value from {s}.meta where key = 'format_version')"
            ),
            &[],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

#[tokio::test]
async fn own1_ret4_cmt9_match_reference() -> Result<(), Error> {
    let pair = fresh().await?;
    let t = Uuid::from_u128(0xa9);
    let create = json!({
        "create": {"namespace": ["db"], "name": "ventas", "location": "gs://lake/ventas"},
        "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
    });
    let committed = |o: Outcome| matches!(o, Outcome::Committed(_));

    // A table whose files lie in Hive's directory, and one under its location.
    let hive = ["gs://hive/ventas/dt=1/a.parquet", "gs://hive/ventas/dt=2/b.parquet"];
    assert!(committed(pair.commit(&change(t, &hive, &[], Some(create))).await?));
    assert!(committed(
        pair.commit(&change(t, &["gs://lake/ventas/c.parquet"], &[], None))
            .await?
    ));

    // CMT-9: a path the table already holds, and the same path twice in one commit.
    let again = pair.commit(&change(t, &[hive[0]], &[], None)).await?;
    let twice = pair
        .commit(&change(t, &["gs://hive/x.parquet", "gs://hive/x.parquet"], &[], None))
        .await?;
    assert_eq!(
        (again, twice),
        (Outcome::Refused("22023".into()), Outcome::Refused("22023".into()))
    );

    // OWN-2: a prefix of a store the table holds nothing on is refused; Hive's is taken, with a trailing / or not.
    let elsewhere = pair
        .commit(&change(t, &[], &[], Some(json!({"own": ["az://other/x"]}))))
        .await?;
    assert_eq!(elsewhere, Outcome::Refused("22023".into()));

    // RET-4: a compaction of a referenced file and an owned one; an expiring queues the owned one only.
    let ids: Vec<i64> = pair
        .on_protocol
        .query_one(
            &format!(
                "select array(select file_id from {}.data_file where path in ($1, $2) order by path)",
                quote_ident(&pair.protocol)
            ),
            &[&hive[0], &"gs://lake/ventas/c.parquet"],
        )
        .await?
        .get(0);
    assert!(committed(
        pair.commit(&change(t, &["gs://lake/ventas/d.parquet"], &ids, None))
            .await?
    ));
    pair.both("select set_retention(1, min_snapshots => 0); select expire()")
        .await?;
    pair.same_rows().await?;
    let (queued, _) = state(&pair, &pair.protocol).await?;
    assert_eq!(
        queued,
        ["gs://lake/ventas/c.parquet"],
        "the referenced Hive file is forgotten, not queued"
    );
    assert_eq!(state(&pair, &pair.reference).await?.0, queued);

    // Adopted, the rest of Hive's directory is owned: once compacted away, it is queued.
    assert!(committed(
        pair.commit(&change(t, &[], &[], Some(json!({"own": ["gs://hive/ventas/"]}))))
            .await?
    ));
    let b: i64 = pair
        .on_protocol
        .query_one(
            &format!(
                "select file_id from {}.data_file where path = $1",
                quote_ident(&pair.protocol)
            ),
            &[&hive[1]],
        )
        .await?
        .get(0);
    assert!(committed(
        pair.commit(&change(t, &["gs://lake/ventas/e.parquet"], &[b], None))
            .await?
    ));
    pair.both("select expire()").await?;
    pair.same_rows().await?;
    assert_eq!(
        state(&pair, &pair.protocol).await?.0,
        ["gs://hive/ventas/dt=2/b.parquet", "gs://lake/ventas/c.parquet"]
    );

    // §18: a name mapping moves the catalog to format 1, in both.
    let mapping = json!({"properties": {"schema.name-mapping.default": "[{\"field-id\": 1, \"names\": [\"k\"]}]"}});
    assert!(committed(pair.commit(&change(t, &[], &[], Some(mapping))).await?));
    assert_eq!(state(&pair, &pair.protocol).await?.1, "1");
    assert_eq!(state(&pair, &pair.reference).await?.1, "1");

    pair.drop().await
}
