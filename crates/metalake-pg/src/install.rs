//! `metalake init` and `metalake migrate` (ADR 0003 §2.1): the protocol installed in Postgres, and upgraded there.
//! Both run in one transaction of their own, so a catalog is complete or untouched.

use tokio_postgres::Client;

use crate::sources::{Kind, created, sources};
use crate::version::{FORMAT_VERSION, PROTOCOL_VERSION};
use crate::{Error, ErrorKind, Result, quote_ident};

/// The roles a catalog is given to (§2.7), which `init`'s caller has created.
#[derive(Debug, Clone)]
pub struct Roles {
    pub owner: String,
    pub writer: String,
    pub reader: String,
}

/// Creates a catalog in a new schema: the tables of the specification, the protocol, `meta` and `main`, and, if
/// given, its warehouse and the roles. Installs in one database take turns, since two transactions creating the same
/// extension at once collide.
pub async fn init(client: &mut Client, schema: &str, warehouse: Option<&str>, roles: Option<&Roles>) -> Result<()> {
    let q = quote_ident(schema);
    let tx = client.transaction().await?;
    tx.batch_execute(&format!(
        "select pg_advisory_xact_lock(hashtext('metalake.install')); \
         create schema {q}; set local search_path = {q}"
    ))
    .await?;
    for source in sources(schema, &[Kind::Table, Kind::Reference, Kind::Protocol]) {
        tx.batch_execute(&source.sql)
            .await
            .map_err(|e| Error::from(e).context(source.name))?;
    }
    if let Some(w) = warehouse {
        tx.execute(&format!("insert into {q}.meta values ('warehouse', $1)"), &[&w])
            .await?;
    }
    if let Some(r) = roles {
        tx.execute(
            &format!("select {q}.grant_roles($1, $2, $3)"),
            &[&r.owner, &r.writer, &r.reader],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Upgrades the catalog in `schema` to these sources, with every ref held exclusively: refuses a format it does not
/// know or a protocol newer than its own; drops, by name, what the protocol's sources create (views, functions,
/// composite types), never a table, a row or a function of the format; installs them again; migrates the format
/// where the text says so (§18); and gives the roles stored in `meta` their rights again, since replacing the
/// functions dropped them. Migrating twice changes nothing. Returns the sequence of the format's migration, if any.
pub async fn migrate(client: &mut Client, schema: &str) -> Result<Option<i64>> {
    let q = quote_ident(schema);
    let tx = client.transaction().await?;
    tx.batch_execute(&format!(
        "select {q}.lock_ref(r.ref_id) from {q}.ref r order by r.ref_id"
    ))
    .await?;
    let meta = |key: &'static str| {
        let tx = &tx;
        let q = &q;
        async move {
            let row = tx
                .query_opt(&format!("select value from {q}.meta where key = $1"), &[&key])
                .await?;
            Ok::<_, Error>(row.map(|r| r.get::<_, String>(0)))
        }
    };
    let format = meta("format_version").await?;
    if format.as_deref() != Some(FORMAT_VERSION) {
        return Err(Error::new(ErrorKind::Unsupported, format!("format_version {format:?}")));
    }
    let protocol = meta("protocol_version").await?;
    if protocol
        .as_deref()
        .is_some_and(|v| v.parse::<u64>().map_or(true, |v| v > PROTOCOL_VERSION.parse().unwrap()))
    {
        return Err(Error::new(
            ErrorKind::Unsupported,
            format!("protocol_version {protocol:?} is newer"),
        ));
    }

    let installed = sources(schema, &[Kind::Reference, Kind::Protocol]);
    let mut drops = String::new();
    for (kind, name) in installed.iter().flat_map(|s| created(&s.sql)) {
        let name = name.trim_start_matches(&format!("{q}."));
        drops += &match kind {
            "function" => format!(
                "select format('drop function if exists %s cascade', p.oid::regprocedure) from pg_proc p \
                  where p.pronamespace = '{q}'::regnamespace and p.proname = '{name}' union all "
            ),
            kind => format!("select 'drop {kind} if exists {q}.{name} cascade' union all "),
        };
    }
    let statements: Vec<String> = tx
        .query(&format!("{drops}select 'select 1'"), &[])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    tx.batch_execute(&statements.join(";")).await?;
    tx.batch_execute(&format!("set local search_path = {q}")).await?;
    for source in installed {
        tx.batch_execute(&source.sql)
            .await
            .map_err(|e| Error::from(e).context(source.name))?;
    }
    let migrated: Option<i64> = tx.query_one("select migrate()", &[]).await?.get(0);

    let roles = (
        meta("owner_role").await?,
        meta("writer_role").await?,
        meta("reader_role").await?,
    );
    if let (Some(owner), Some(writer), Some(reader)) = roles {
        tx.execute(
            &format!("select {q}.grant_roles($1, $2, $3)"),
            &[&owner, &writer, &reader],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(migrated)
}
