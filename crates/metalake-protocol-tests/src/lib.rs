//! The protocol's test harness (ADR 0003 §5).
//!
//! It installs, in one Postgres database, a catalog with the protocol (`sql/v0/` over the tables of `spec/v0/schema/`)
//! and a catalog with the reference of the specification (`spec/v0/schema/` and `spec/v0/reference/`), and compares
//! their rows. The reference is the oracle: whatever both are asked to do, they must end with the same rows.

pub mod differential;
pub mod records;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use tokio_postgres::{Client, NoTls};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Columns that differ between two catalogs by nature, not by what they hold: when a transaction ran.
const VOLATILE_COLUMNS: &[(&str, &str)] = &[
    ("commit", "xid"),
    ("commit", "committed_at"),
    ("deletion_queue", "expired_at"),
];

/// Rows of `meta` that identify a catalog, or belong to the protocol alone.
const OWN_META_KEYS: &[&str] = &["catalog_uuid", "created_at", "protocol_version"];

/// The root of the repository.
pub fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The `.sql` files of a directory of the repository, in file-name order (bytewise, as the runner of the
/// specification loads them).
fn sources(dir: &str) -> Result<Vec<(PathBuf, String)>, Error> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(repository().join(dir))?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<_, _>>()?;
    paths.retain(|p| p.extension().is_some_and(|x| x == "sql"));
    paths.sort();
    paths
        .into_iter()
        .map(|p| Ok((p.clone(), std::fs::read_to_string(&p)?)))
        .collect()
}

pub use metalake_pg::{quote_ident, render};

/// A schema name no other test uses.
pub fn fresh_schema(prefix: &str) -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// A connection to the database of `DATABASE_URL`. Docker forwards the port to the host some time after the server
/// is ready inside the container, and a connection attempted before that hangs until it is reset: each attempt
/// gives up after three seconds, and the first minute is retried.
pub async fn connect() -> Result<Client, Error> {
    let url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is not set: run sql/test.sh")?;
    let mut config: tokio_postgres::Config = url.parse()?;
    config.connect_timeout(std::time::Duration::from_secs(3));
    let mut attempts = 0;
    let (client, connection) = loop {
        match config.connect(NoTls).await {
            Ok(c) => break c,
            Err(_) if attempts < 20 => attempts += 1,
            Err(e) => return Err(e.into()),
        }
    };
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection: {e}");
        }
    });
    Ok(client)
}

/// Loads `files` into a new schema, in one transaction: the schema exists complete or not at all. Installs in one
/// database take turns, because two transactions creating the same extension at once collide.
async fn install(client: &Client, schema: &str, files: Vec<(PathBuf, String)>) -> Result<(), Error> {
    let quoted = quote_ident(schema);
    let start = format!(
        "begin; select pg_advisory_xact_lock(hashtext('metalake.install')); \
         create schema {quoted}; set local search_path = {quoted};"
    );
    if let Err(e) = client.batch_execute(&start).await {
        client.batch_execute("rollback").await?;
        return Err(e.into());
    }
    for (path, sql) in files {
        if let Err(e) = client.batch_execute(&sql).await {
            client.batch_execute("rollback").await?;
            return Err(format!("{}: {e:?}", path.display()).into());
        }
    }
    // Revision 0.1 (NS-2): the tests put their tables in namespace db, which every catalog starts with.
    if let Err(e) = client
        .batch_execute("insert into namespace values ('{db}', false, 1, 0)")
        .await
    {
        client.batch_execute("rollback").await?;
        return Err(e.into());
    }
    client.batch_execute("commit").await?;
    Ok(())
}

/// A catalog with the protocol, installed by the SDK as `metalake init` does, in namespace `db`, which the tests put
/// their tables in (revision 0.1, NS-2). The SDK installs in a transaction of its own, on a connection of its own.
pub async fn install_protocol(_client: &Client, schema: &str) -> Result<(), Error> {
    let mut own = connect().await?;
    metalake_pg::init(&mut own, schema, None, None).await?;
    own.batch_execute(&format!(
        "insert into {}.namespace values ('{{db}}', false, 1, 0)",
        quote_ident(schema)
    ))
    .await?;
    Ok(())
}

/// The protocol version these sources install.
pub const PROTOCOL_VERSION: &str = metalake_pg::PROTOCOL_VERSION;

/// `metalake migrate`, by the SDK, on a connection of its own. Returns the sequence of the format's migration, if
/// any.
pub async fn migrate_protocol(_client: &Client, schema: &str) -> Result<Option<i64>, Error> {
    let mut own = connect().await?;
    Ok(metalake_pg::migrate(&mut own, schema).await?)
}

/// A catalog with the reference of the specification.
pub async fn install_reference(client: &Client, schema: &str) -> Result<(), Error> {
    let mut files = sources("spec/v0/schema")?;
    files.extend(sources("spec/v0/reference")?);
    install(client, schema, files).await
}

/// The rows of one table of a catalog that differ from the other's.
#[derive(Debug, PartialEq)]
pub struct Difference {
    pub table: String,
    pub only_in_first: Vec<String>,
    pub only_in_second: Vec<String>,
}

/// The base tables of a schema.
async fn tables(client: &Client, schema: &str) -> Result<Vec<String>, Error> {
    let rows = client
        .query(
            "select table_name::text from information_schema.tables \
             where table_schema = $1 and table_type = 'BASE TABLE' order by 1",
            &[&schema],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// The rows of a table as text, sorted, without its volatile columns and identifying `meta` rows.
async fn rows(client: &Client, schema: &str, table: &str) -> Result<Vec<String>, Error> {
    let columns: Vec<String> = client
        .query(
            "select column_name::text from information_schema.columns \
             where table_schema = $1 and table_name = $2 order by ordinal_position",
            &[&schema, &table],
        )
        .await?
        .iter()
        .map(|r| r.get::<_, String>(0))
        .filter(|c| !VOLATILE_COLUMNS.contains(&(table, c.as_str())))
        .collect();
    let list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let filter = if table == "meta" {
        format!(
            "where key not in ({})",
            OWN_META_KEYS
                .iter()
                .map(|k| format!("'{k}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        String::new()
    };
    let sql = format!(
        "select coalesce(array_agg(r order by r), '{{}}') from (select row({list})::text as r from {}.{} {filter}) x",
        quote_ident(schema),
        quote_ident(table)
    );
    Ok(client.query_one(&sql, &[]).await?.get(0))
}

/// Every difference between the rows of two catalogs, table by table. Two catalogs with different tables are an
/// error, not a difference.
pub async fn compare(client: &Client, first: &str, second: &str) -> Result<Vec<Difference>, Error> {
    let names = tables(client, first).await?;
    let others = tables(client, second).await?;
    if names != others {
        return Err(format!("the catalogs have different tables: {names:?} and {others:?}").into());
    }
    let mut differences = Vec::new();
    for table in names {
        let mut counts: BTreeMap<String, i64> = BTreeMap::new();
        for row in rows(client, first, &table).await? {
            *counts.entry(row).or_default() += 1;
        }
        for row in rows(client, second, &table).await? {
            *counts.entry(row).or_default() -= 1;
        }
        let only = |sign: i64| -> Vec<String> {
            counts
                .iter()
                .filter(|(_, n)| n.signum() == sign)
                .flat_map(|(row, n)| std::iter::repeat_n(row.clone(), n.unsigned_abs() as usize))
                .collect()
        };
        let (only_in_first, only_in_second) = (only(1), only(-1));
        if !only_in_first.is_empty() || !only_in_second.is_empty() {
            differences.push(Difference {
                table,
                only_in_first,
                only_in_second,
            });
        }
    }
    Ok(differences)
}
