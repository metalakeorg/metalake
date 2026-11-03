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

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A source of the protocol rendered for one catalog: every object it names is qualified with the catalog's schema.
pub fn render(source: &str, schema: &str) -> String {
    source.replace("@schema@", &quote_ident(schema))
}

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

/// A catalog with the protocol: the tables of the specification and the protocol rendered for `schema`.
pub async fn install_protocol(client: &Client, schema: &str) -> Result<(), Error> {
    let mut files = sources("spec/v0/schema")?;
    files.extend(protocol_files(schema)?);
    install(client, schema, files).await
}

/// The protocol's files for the catalog in `schema`, in the order they install: the reference's it reuses, and its
/// own, rendered.
fn protocol_files(schema: &str) -> Result<Vec<(PathBuf, String)>, Error> {
    let mut files = Vec::new();
    let reused = reused()?;
    files.extend(
        reused
            .iter()
            .filter(|(_, before, _)| before.is_none())
            .map(|(p, _, s)| (p.clone(), s.clone())),
    );
    for (path, sql) in sources("sql/v0")? {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
        for (p, _, s) in reused
            .iter()
            .filter(|(_, before, _)| before.as_deref() == Some(name.as_str()))
        {
            files.push((p.clone(), s.clone()));
        }
        files.push((path, render(&sql, schema)));
    }
    Ok(files)
}

/// The protocol version these files install.
pub const PROTOCOL_VERSION: i64 = 0;

/// `metalake migrate` (ADR 0003 §2.1): in one transaction, with every ref of the catalog held exclusively, refuses a
/// format it does not know or a protocol newer than its own (0A000); drops what the protocol installed (views,
/// functions, composite types), never a table or a row; installs the protocol again; and migrates the format where
/// the text says so (§18). Migrating twice changes nothing. Returns the sequence of the format's migration, if any.
pub async fn migrate_protocol(client: &Client, schema: &str) -> Result<Option<i64>, Error> {
    let q = quote_ident(schema);
    client.batch_execute("begin").await?;
    let result = async {
        client
            .batch_execute(&format!(
                "select {q}.lock_ref(r.ref_id) from {q}.ref r order by r.ref_id"
            ))
            .await?;
        let versions: Vec<(String, Option<String>)> = client
            .query(
                &format!("select key, value from {q}.meta where key in ('format_version', 'protocol_version')"),
                &[],
            )
            .await?
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        let get = |k: &str| versions.iter().find(|(key, _)| key == k).and_then(|(_, v)| v.clone());
        if get("format_version").as_deref() != Some("0") {
            return Err(unsupported(format!("format_version {:?}", get("format_version"))));
        }
        let protocol = get("protocol_version").map(|v| v.parse::<i64>().unwrap_or(i64::MAX));
        if protocol.is_some_and(|v| v > PROTOCOL_VERSION) {
            return Err(unsupported(format!("protocol_version {protocol:?}")));
        }
        // What the protocol's own files create, by name: the format's functions (bound_range, under an index) stay.
        let files = protocol_files(schema)?;
        let mut drops = String::new();
        for (kind, name) in files.iter().flat_map(|(_, sql)| created(sql)) {
            let name = name.trim_start_matches(&format!("{q}.")).to_string();
            drops += &match kind {
                "function" => format!(
                    "select format('drop function if exists %s cascade', p.oid::regprocedure) from pg_proc p
                      where p.pronamespace = '{q}'::regnamespace and p.proname = '{name}' union all "
                ),
                k => format!("select 'drop {k} if exists {q}.{name} cascade' union all "),
            };
        }
        let statements: Vec<String> = client
            .query(&format!("{drops}select 'select 1'"), &[])
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
        client.batch_execute(&statements.join(";")).await?;
        client.batch_execute(&format!("set local search_path = {q}")).await?;
        for (path, sql) in files {
            client
                .batch_execute(&sql)
                .await
                .map_err(|e| format!("{}: {e:?}", path.display()))?;
        }
        let migrated: Option<i64> = client.query_one("select migrate()", &[]).await?.get(0);
        // Replacing the functions dropped their rights: the roles of the catalog, if it has any, get them again.
        let roles = client
            .query_opt(
                &format!(
                    "select o.value, w.value, r.value from {q}.meta o, {q}.meta w, {q}.meta r
                      where o.key = 'owner_role' and w.key = 'writer_role' and r.key = 'reader_role'"
                ),
                &[],
            )
            .await?;
        if let Some(r) = roles {
            let (o, w, rd): (String, String, String) = (r.get(0), r.get(1), r.get(2));
            client
                .execute(&format!("select {q}.grant_roles($1, $2, $3)"), &[&o, &w, &rd])
                .await?;
        }
        Ok(migrated)
    }
    .await;
    match result {
        Ok(m) => {
            client.batch_execute("commit").await?;
            Ok(m)
        }
        Err(e) => {
            client.batch_execute("rollback").await?;
            Err(e)
        }
    }
}

/// The views, functions and types a file of the protocol creates: (kind, name as written).
fn created(sql: &str) -> Vec<(&'static str, &str)> {
    sql.lines()
        .filter_map(|l| {
            let l = l.trim_start();
            let rest = l
                .strip_prefix("create or replace ")
                .or_else(|| l.strip_prefix("create "))?;
            let (kind, rest) = [("function", "function "), ("type", "type "), ("view", "view ")]
                .into_iter()
                .find_map(|(k, p)| rest.strip_prefix(p).map(|r| (k, r)))?;
            let name = rest.split(|c: char| c == '(' || c.is_whitespace()).next()?;
            Some((kind, name))
        })
        .collect()
}

fn unsupported(what: String) -> Error {
    format!("0A000 metalake unsupported: {what}").into()
}

/// The files of the reference the protocol installs as they are (`sql/v0/reference-reused.txt`): before all of its
/// own, or, for `name before file`, just before that file of its own.
fn reused() -> Result<Vec<(PathBuf, Option<String>, String)>, Error> {
    let list = std::fs::read_to_string(repository().join("sql/v0/reference-reused.txt"))?;
    list.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|line| {
            let (name, before) = match line.split_once(" before ") {
                Some((n, b)) => (n, Some(b.trim().to_string())),
                None => (line, None),
            };
            let path = repository().join("spec/v0/reference").join(name);
            Ok((path.clone(), before, std::fs::read_to_string(&path)?))
        })
        .collect()
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
