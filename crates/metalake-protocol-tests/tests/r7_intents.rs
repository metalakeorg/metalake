//! R7: intents (RET-7). An expired intent no file refers to is an orphan; forget_intents forgets only orphans.

use metalake_protocol_tests::{connect, fresh_schema, install_protocol, quote_ident, Error};

#[tokio::test]
async fn only_orphans_are_forgotten() -> Result<(), Error> {
    let schema = fresh_schema("p");
    let on = connect().await?;
    install_protocol(&on, &schema).await?;
    let p = quote_ident(&schema);
    on.batch_execute(&format!(
        "insert into {p}.intent values
           ('gs://lake/old.parquet', gen_random_uuid(), 1, now() - interval '1 hour'),
           ('gs://lake/new.parquet', gen_random_uuid(), 1, now() + interval '1 hour')"
    ))
    .await?;
    let orphans: Vec<String> = on
        .query_one(&format!("select array(select {p}.orphans())"), &[])
        .await?
        .get(0);
    let forgot: i64 = on
        .query_one(
            &format!("select {p}.forget_intents(array['gs://lake/old.parquet', 'gs://lake/new.parquet'])"),
            &[],
        )
        .await?
        .get(0);
    let left: Vec<String> = on
        .query_one(&format!("select array(select path from {p}.intent)"), &[])
        .await?
        .get(0);
    on.batch_execute(&format!("drop schema {p} cascade")).await?;
    if orphans != ["gs://lake/old.parquet"] || forgot != 1 || left != ["gs://lake/new.parquet"] {
        return Err(format!("orphans {orphans:?}, forgot {forgot}, left {left:?}").into());
    }
    Ok(())
}
