//! R3: each faulty definition change, once, on a fresh pair of catalogs; both must refuse it with the same SQLSTATE.
//! The random sequences of `r3_commit_definitions` reach most of these only by chance.

use metalake_protocol_tests::differential::{Commit, Outcome, Pair, TableChange, clone_catalog};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, install_reference, quote_ident};
use serde_json::{Value, json};
use uuid::Uuid;

const MAIN: i32 = 1;
const SEED: u128 = 0x4000;
const OTHER: u128 = 0x4001;

fn commit(t: u128, definition: Value) -> Commit {
    let change = TableChange {
        table_id: Uuid::from_u128(t),
        definition: Some(definition),
        ..TableChange::default()
    };
    Commit {
        ref_id: MAIN,
        operation: "update".into(),
        tables: vec![change],
        app: None,
        namespaces: None,
    }
}

fn create(name: &str) -> Value {
    json!({
        "create": {"namespace": ["db"], "name": name, "location": "gs://lake"},
        "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true},
                   {"field_id": 2, "new": true, "position": 2, "name": "v", "type": "int", "required": false}],
        "cluster_key": [1],
    })
}

/// Two tables, `seed` and `other`, on main of both catalogs.
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
    for (t, name) in [(SEED, "seed"), (OTHER, "other")] {
        if !matches!(pair.commit(&commit(t, create(name))).await?, Outcome::Committed(_)) {
            return Err(format!("{name} was not created").into());
        }
    }
    Ok(pair)
}

#[tokio::test]
async fn invalid_definitions_match_reference() -> Result<(), Error> {
    let faults = [
        ("a create with an id that exists (ID-3)", commit(SEED, create("fresh"))),
        (
            "a create with a name that is taken (TBL-1)",
            commit(0x4002, create("seed")),
        ),
        (
            "a rename to a name that is taken (TBL-1)",
            commit(SEED, json!({"rename": {"namespace": ["db"], "name": "other"}})),
        ),
        (
            "a new field with a name that exists (CMT-7)",
            commit(
                SEED,
                json!({"fields": [{"field_id": 3, "new": true, "position": 3, "name": "v", "type": "int"}]}),
            ),
        ),
        (
            "a change to a field that is not visible (CMT-7)",
            commit(
                SEED,
                json!({"fields": [{"field_id": 777, "new": false, "position": 3, "name": "w", "type": "int"}]}),
            ),
        ),
        (
            "a narrowing type change (TYP-3)",
            commit(
                SEED,
                json!({"fields": [{"field_id": 1, "new": false, "position": 1, "name": "k", "type": "int",
                                         "required": true}]}),
            ),
        ),
        (
            "a drop of a field that is not visible (CMT-7)",
            commit(SEED, json!({"drop_fields": [777]})),
        ),
        (
            "a new field whose id was not drawn (ID-5)",
            commit(
                SEED,
                json!({"fields": [{"field_id": 900, "new": true, "position": 3, "name": "w", "type": "int"}]}),
            ),
        ),
        (
            "a cluster key on a field that does not exist",
            commit(SEED, json!({"cluster_key": [1, 999]})),
        ),
    ];
    let pair = fresh().await?;
    for (what, c) in &faults {
        match pair.commit(c).await {
            Ok(Outcome::Refused(_)) => {}
            Ok(outcome) => return Err(format!("{what}: both answered {outcome:?}").into()),
            Err(e) => return Err(format!("{what}: {e}").into()),
        }
    }
    pair.drop().await
}
