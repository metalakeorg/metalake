//! C6a (ADR 0003 §7): the core's deletion vectors against the reference's `dv_encode` and `dv_positions` (DV-2),
//! byte for byte both ways, on random sets of positions and on the edges of Roaring's layout: none, one, a full
//! container (a bitset), positions across 2^16 and 2^32, and far ones; damaged blobs are refused by both.

use metalake::Error as CoreError;
use metalake::vectors::DeletionVector;
use metalake_protocol_tests::{Error, connect, fresh_schema, install_reference, quote_ident};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

fn cases() -> usize {
    std::env::var("CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(64)
}

#[tokio::test]
async fn vector_encoding_matches_reference() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c6");
    install_reference(&on, &schema).await?;
    on.batch_execute(&format!("set search_path = {}", quote_ident(&schema)))
        .await?;

    let mut sets: Vec<Vec<i64>> = vec![
        vec![],
        vec![0],
        (0..5_000).collect(),
        vec![65_535, 65_536, 131_071],
        vec![(1 << 32) - 1, 1 << 32, (1 << 32) + 1],
        vec![3, 1 << 40, (1 << 62) + 7],
    ];
    let mut runner = TestRunner::deterministic();
    let spread = prop_oneof![0i64..200_000, 0i64..(1 << 34), 0i64..(1 << 62)];
    let strategy = proptest::collection::btree_set(spread, 0..300);
    for _ in 0..cases() {
        sets.push(strategy.new_tree(&mut runner).unwrap().current().into_iter().collect());
    }

    for positions in &sets {
        let ours = DeletionVector::from_positions(positions.iter().map(|p| *p as u64));
        let blob = ours.to_blob();
        let theirs: Vec<u8> = on.query_one("select dv_encode($1)", &[positions]).await?.get(0);
        assert_eq!(blob, theirs, "the blob of {} positions", positions.len());
        let read: Vec<i64> = on
            .query_one(
                "select coalesce(array_agg(p order by p), '{}') from dv_positions($1) p",
                &[&blob],
            )
            .await?
            .get(0);
        assert_eq!(&read, positions);
        let back = DeletionVector::from_blob(&theirs).unwrap();
        assert_eq!(back.positions().map(|p| p as i64).collect::<Vec<_>>(), *positions);
        assert_eq!(back.cardinality(), positions.len() as u64);
    }

    // Damage the reference refuses, the core refuses too.
    let blob = DeletionVector::from_positions([1, 2, 70_000]).to_blob();
    let mut damaged = Vec::new();
    for at in [0, 4, 9, 20, blob.len() - 1] {
        let mut b = blob.clone();
        b[at] ^= 0x10;
        damaged.push(b);
    }
    damaged.push(blob[..blob.len() - 1].to_vec());
    for b in &damaged {
        let refused = on.query("select dv_positions($1)", &[b]).await;
        assert!(refused.is_err(), "the reference takes {b:02x?}");
        assert!(
            matches!(DeletionVector::from_blob(b), Err(CoreError::Corrupt(_))),
            "{b:02x?}"
        );
    }

    on.batch_execute(&format!(
        "reset search_path; drop schema {} cascade",
        quote_ident(&schema)
    ))
    .await?;
    Ok(())
}
