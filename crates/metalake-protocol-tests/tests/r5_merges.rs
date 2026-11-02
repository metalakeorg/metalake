//! R5: merges the random sequences of `r5_branches` reach only by chance, once each. MRG-5: a file's vector changed
//! on a branch and on its parent; the merge is refused without a merged vector or with one that does not hold both
//! sides, and goes through with one that does, or by itself when both vectors are inline. MRG-3: the two sides add a
//! field of the same name, and the merge, which would leave the table with two, is refused.

use metalake_protocol_tests::differential::{clone_catalog, Commit, Outcome, Pair, TableChange};
use metalake_protocol_tests::records::{stored_file, VectorSet};
use metalake_protocol_tests::{connect, fresh_schema, install_protocol, install_reference, quote_ident, Error};
use serde_json::json;
use uuid::Uuid;

const T: Uuid = Uuid::from_u128(0x9000);
const LATEST: i64 = 9223372036854775806;

async fn commit(pair: &Pair, r: i32, change: TableChange) -> Result<(), Error> {
    let c = Commit {
        ref_id: r,
        operation: "update".into(),
        tables: vec![change],
        app: None,
        namespaces: None,
    };
    match pair.commit(&c).await? {
        Outcome::Committed(_) => Ok(()),
        refused => Err(format!("{c:?} was refused: {refused:?}").into()),
    }
}

/// A vector of `cardinality` rows on file `fid` as ref `r` sees it, inline or in a file.
async fn vector(pair: &Pair, r: i32, fid: i64, cardinality: i64, inline: bool) -> Result<VectorSet, Error> {
    let base_seq: Option<i64> = pair
        .on_reference
        .query_one("select (file_dv($1, $2, $3, $4)).begin_seq", &[&r, &T, &LATEST, &fid])
        .await?
        .get(0);
    let sql = "select dv_encode(array(select generate_series($1::bigint, $2::bigint - 1)))";
    // The two sides delete different rows, so that only a union holds both.
    let blob: Option<Vec<u8>> = if inline {
        Some(
            pair.on_reference
                .query_one(sql, &[&(i64::from(r) * 3), &(i64::from(r) * 3 + cardinality)])
                .await?
                .get(0),
        )
    } else {
        None
    };
    Ok(VectorSet {
        table_id: T,
        file_id: fid,
        base_seq,
        path: blob.is_none().then(|| format!("gs://dv/{r}.puffin")),
        content_offset: blob.is_none().then_some(4),
        content_size: blob.as_ref().map_or(40, |b| b.len() as i64),
        cardinality,
        inline: blob,
    })
}

/// A pair with table T and one file on main, and a branch of main.
async fn forked() -> Result<(Pair, i32, i64), Error> {
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
    let create = json!({
        "create": {"namespace": ["db"], "name": "t", "location": "gs://lake"},
        "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
        "cluster_key": [1],
    });
    commit(
        &pair,
        1,
        TableChange {
            table_id: T,
            definition: Some(create),
            ..TableChange::default()
        },
    )
    .await?;
    let file = stored_file(T, "gs://lake/f.parquet".into(), 100, 1, 0, 9);
    commit(
        &pair,
        1,
        TableChange {
            table_id: T,
            adds: vec![file],
            ..TableChange::default()
        },
    )
    .await?;
    let fid: i64 = pair
        .on_reference
        .query_one("select max(file_id) from data_file", &[])
        .await?
        .get(0);
    let branch: i32 = pair
        .on_reference
        .query_one("select create_ref('b', 1)", &[])
        .await?
        .get(0);
    let p = quote_ident(&pair.protocol);
    let mirrored: i32 = pair
        .on_protocol
        .query_one(&format!("select {p}.create_ref(name => 'b', parent => 1)"), &[])
        .await?
        .get(0);
    if branch != mirrored {
        return Err(format!("the branch is {branch} and {mirrored}").into());
    }
    Ok((pair, branch, fid))
}

/// The file's vector changed on both sides.
async fn diverged(inline: bool) -> Result<(Pair, i32, i64), Error> {
    let (pair, branch, fid) = forked().await?;
    for (r, cardinality) in [(1, 3), (branch, 5)] {
        let v = vector(&pair, r, fid, cardinality, inline).await?;
        commit(
            &pair,
            r,
            TableChange {
                table_id: T,
                vectors: vec![v],
                ..TableChange::default()
            },
        )
        .await?;
    }
    Ok((pair, branch, fid))
}

/// Merges on both with `merged` supplied, and returns whether both merged (they must agree).
async fn merge(pair: &Pair, branch: i32, merged: Option<VectorSet>) -> Result<bool, Error> {
    let dvs: Vec<_> = merged
        .iter()
        .map(|v| {
            json!({"table_id": v.table_id, "file_id": v.file_id, "path": v.path, "content_offset": v.content_offset,
                   "content_size": v.content_size, "cardinality": v.cardinality, "inline": null})
        })
        .collect();
    let by_reference = pair
        .on_reference
        .query_one("select merge($1, $2)", &[&branch, &json!({"dvs": dvs})])
        .await;
    let vectors: Vec<VectorSet> = merged.into_iter().collect();
    let sql = format!(
        "select commit_seq from {}.merge(branch => $1, vectors => $2)",
        quote_ident(&pair.protocol)
    );
    let by_protocol = pair.on_protocol.query_one(&sql, &[&branch, &vectors]).await;
    let state = |r: &Result<tokio_postgres::Row, tokio_postgres::Error>| match r {
        Ok(_) => "merged".to_string(),
        Err(e) => e.code().map_or(format!("{e}"), |c| c.code().to_string()),
    };
    let (a, b) = (state(&by_reference), state(&by_protocol));
    if a != b {
        return Err(format!("the reference {a} and the protocol {b}").into());
    }
    if a != "merged" {
        let (p, r) = (quote_ident(&pair.protocol), quote_ident(&pair.reference));
        pair.on_protocol
            .batch_execute(&format!(
                "select setval('{p}.commit_seq_gen', last_value, is_called) from {r}.commit_seq_gen"
            ))
            .await?;
    }
    pair.same_rows().await?;
    Ok(a == "merged")
}

fn supplied(fid: i64, cardinality: i64) -> VectorSet {
    VectorSet {
        table_id: T,
        file_id: fid,
        base_seq: None,
        path: Some("gs://dv/merged.puffin".into()),
        content_offset: Some(4),
        content_size: 40,
        cardinality,
        inline: None,
    }
}

#[tokio::test]
async fn a_vector_both_sides_changed_needs_one_that_holds_both() -> Result<(), Error> {
    let (pair, branch, fid) = diverged(false).await?;
    if merge(&pair, branch, None).await? {
        return Err("merged without a merged vector".into());
    }
    if merge(&pair, branch, Some(supplied(fid, 4))).await? {
        return Err("merged with a vector short of the branch's 5 rows".into());
    }
    if !merge(&pair, branch, Some(supplied(fid, 5))).await? {
        return Err("refused a vector that holds both sides".into());
    }
    pair.drop().await
}

#[tokio::test]
async fn two_inline_vectors_merge_by_themselves() -> Result<(), Error> {
    let (pair, branch, _) = diverged(true).await?;
    if !merge(&pair, branch, None).await? {
        return Err("refused two inline vectors".into());
    }
    pair.drop().await
}

#[tokio::test]
async fn a_merge_that_leaves_two_fields_one_name_is_refused() -> Result<(), Error> {
    let (pair, branch, _) = forked().await?;
    let p = quote_ident(&pair.protocol);
    for r in [1, branch] {
        // Each side draws its own id (ID-5), in both catalogs alike.
        let by_reference: i32 = pair
            .on_reference
            .query_one("select next_id($1, 'field')", &[&T])
            .await?
            .get(0);
        pair.on_protocol
            .batch_execute(&format!("set search_path = {p}"))
            .await?;
        let by_protocol: i32 = pair
            .on_protocol
            .query_one("select next_id($1, 'field')", &[&T])
            .await?
            .get(0);
        pair.on_protocol.batch_execute("reset search_path").await?;
        if by_reference != by_protocol {
            return Err(format!("next_id drew {by_reference} and {by_protocol}").into());
        }
        let fields =
            json!({"fields": [{"field_id": by_reference, "new": true, "position": 2, "name": "c", "type": "int"}]});
        commit(
            &pair,
            r,
            TableChange {
                table_id: T,
                definition: Some(fields),
                ..TableChange::default()
            },
        )
        .await?;
    }
    if merge(&pair, branch, None).await? {
        return Err("merged two fields named c".into());
    }
    pair.drop().await
}
