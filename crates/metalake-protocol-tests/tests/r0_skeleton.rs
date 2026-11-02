//! R0: the protocol and the reference install side by side, and the harness sees any difference between them.

use metalake_protocol_tests::{compare, connect, fresh_schema, install_protocol, install_reference, Error};

#[tokio::test]
async fn both_install_into_an_empty_database() -> Result<(), Error> {
    let client = connect().await?;
    let (protocol, reference) = (fresh_schema("protocol"), fresh_schema("reference"));
    install_protocol(&client, &protocol).await?;
    install_reference(&client, &reference).await?;
    for (schema, expected) in [(&protocol, Some("0")), (&reference, None)] {
        let format: String = client
            .query_one(
                &format!("select value from \"{schema}\".meta where key = 'format_version'"),
                &[],
            )
            .await?
            .get(0);
        let version: Option<String> = client
            .query_opt(
                &format!("select value from \"{schema}\".meta where key = 'protocol_version'"),
                &[],
            )
            .await?
            .map(|r| r.get(0));
        assert_eq!(format, "0");
        assert_eq!(version.as_deref(), expected, "{schema}: protocol_version");
    }
    Ok(())
}

#[tokio::test]
async fn two_fresh_catalogs_are_equal() -> Result<(), Error> {
    let client = connect().await?;
    let (protocol, reference) = (fresh_schema("protocol"), fresh_schema("reference"));
    install_protocol(&client, &protocol).await?;
    install_reference(&client, &reference).await?;
    assert_eq!(compare(&client, &protocol, &reference).await?, vec![]);
    Ok(())
}

#[tokio::test]
async fn a_changed_row_is_reported() -> Result<(), Error> {
    let client = connect().await?;
    let (protocol, reference) = (fresh_schema("protocol"), fresh_schema("reference"));
    install_protocol(&client, &protocol).await?;
    install_reference(&client, &reference).await?;
    client
        .batch_execute(&format!(
            "insert into \"{reference}\".ref (ref_id, name, kind, created_seq) values (2, 'b', 'branch', 1);
             update \"{protocol}\".meta set value = '1' where key = 'format_version';"
        ))
        .await?;
    let differences = compare(&client, &protocol, &reference).await?;
    let tables: Vec<&str> = differences.iter().map(|d| d.table.as_str()).collect();
    assert_eq!(tables, ["meta", "ref"]);
    assert_eq!(differences[1].only_in_first, Vec::<String>::new());
    assert_eq!(differences[1].only_in_second, ["(2,b,branch,1,f,f)"]);
    Ok(())
}

#[tokio::test]
async fn a_failed_install_is_rolled_back() -> Result<(), Error> {
    let client = connect().await?;
    let schema = fresh_schema("protocol");
    install_protocol(&client, &schema).await?;
    // The same schema again: creating it fails, the transaction is rolled back and the connection stays usable.
    assert!(install_protocol(&client, &schema).await.is_err());
    let count: i64 = client
        .query_one(
            "select count(*) from information_schema.schemata where schema_name = $1",
            &[&schema],
        )
        .await?
        .get(0);
    assert_eq!(count, 1);
    Ok(())
}
