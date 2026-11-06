//! C1 (ADR 0003 §7): the protocol's SDK, `metalake-pg`, against a real catalog.

use std::time::Duration;

use metalake_pg::{ErrorKind, Pool, check_versions, versions};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};

/// A catalog the SDK installed speaks its versions; one of another format, or of another protocol until migrated, is
/// refused as unsupported; one of a newer writer is read, not written.
#[tokio::test]
async fn the_sdk_refuses_versions_it_does_not_speak() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = quote_ident(&schema);

    let found = check_versions(&on, &schema).await?;
    assert_eq!(
        (
            found.format_version.as_str(),
            found.writer_version.as_str(),
            found.protocol_version.as_deref()
        ),
        ("0", "2", Some("0"))
    );
    assert!(found.writable());

    for (key, value) in [
        ("format_version", "1"),
        ("protocol_version", "1"),
        ("protocol_version", "x"),
    ] {
        on.execute(
            &format!("update {p}.meta set value = $1 where key = $2"),
            &[&value, &key],
        )
        .await?;
        let refused = check_versions(&on, &schema).await;
        assert!(
            refused.as_ref().is_err_and(|e| e.kind() == ErrorKind::Unsupported),
            "{key} {value}: {refused:?}"
        );
        on.execute(&format!("update {p}.meta set value = '0' where key = $1"), &[&key])
            .await?;
    }

    on.batch_execute(&format!("update {p}.meta set value = '3' where key = 'writer_version'"))
        .await?;
    let newer = check_versions(&on, &schema).await?;
    assert!(!newer.writable());
    assert_eq!(versions(&on, &schema).await?, newer);

    on.batch_execute(&format!("drop schema {p} cascade")).await?;
    Ok(())
}

/// A pool with nothing free refuses in the end, and says so, rather than leave its caller waiting for ever.
#[tokio::test]
async fn a_pool_with_nothing_free_refuses_after_its_wait() -> Result<(), Error> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = Pool::waiting(&url, 1, Duration::from_millis(200))?;
    let held = pool.get().await?;
    let Err(refused) = pool.get().await else {
        panic!("a pool of one gave a second connection");
    };
    assert_eq!(refused.kind(), ErrorKind::Postgres);
    assert!(refused.to_string().contains("no connection of 1"), "{refused}");
    drop(held);
    let _freed = pool.get().await?;
    Ok(())
}
