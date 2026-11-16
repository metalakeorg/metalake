# Changelog

What each version published on PyPI as `metalake` brings to whoever installs it. The work behind each line is in its
pull request; the decisions, in the ADRs.

## 0.7.0

### SQL

- **Manage branches and tags in SQL.** `CREATE BRANCH dev [FROM main@1234]`, `MERGE BRANCH dev [DRY RUN]`, `USE BRANCH dev`, `SHOW BRANCHES`, `SHOW LOG`, `ALTER BRANCH dev RETAIN 30 DAYS` and `DROP BRANCH dev FORCE`, tags alike, in Query, `metalake sql` and `lake.sql`. In Query, `USE BRANCH` and `USE TAG` move the branch selector, and the next run starts there (PR_I5)

### Iceberg

- **Connect any Iceberg engine to a branch or tag of the lake.** The engine reads and writes that ref's namespaces and tables with no ref named in a statement; a tag is read only. Use `metalake iceberg connect <engine> --ref <ref>` ([#67](https://github.com/metalakeorg/metalake/pull/67))
- **Follow the notebook's Ref selector from one connection.** An engine from `lake.iceberg.duckdb()`, `pyiceberg()`, `spark()` or `flink()` moves with the selector and `lake.on(…)`, from its next statement, with no new connection; a write from a table loaded before a move goes to the new ref, never the old one. `ref=` pins one to a ref ([#68](https://github.com/metalakeorg/metalake/pull/68))

### Engines

- **Run `USING duckdb` on the ref the session is on.** With Query's branch selector on `dev`, `USING duckdb` reads and writes `dev`, tables only there included. Engines named in `metalake.toml` still read the ref their own catalog is connected to ([#67](https://github.com/metalakeorg/metalake/pull/67))
- **Run DuckDB in a process of its own.** `USING duckdb` sends each statement to a worker process; if DuckDB crashes there (a CPU it was not built for, a bad extension), only that statement fails, saying how DuckDB ended, and the UI, `metalake sql` or your Python keep running. The next `USING duckdb` starts it again ([#66](https://github.com/metalakeorg/metalake/pull/66))

## 0.6.0

Released on 2026-11-15.

Run SQL on more than one engine from the same query: DuckDB in-process, or engines named once for the lake.

### Query

- **Run several statements in one go.** Each statement gets its own result, browsed with `‹ Result 2 of 5 ›`; a failure stops the run and marks the rest as not run ([#62](https://github.com/metalakeorg/metalake/pull/62))

### Engines

- **Run statements on another engine with `USING`.** `USING duckdb SELECT …` runs one statement on DuckDB; `USING duckdb` alone switches until `USING metalake`. Each result names its engine. Works in Query, `metalake sql` and `lake.sql`; DuckDB runs in-process with `pip install "metalake[duckdb]"` ([#63](https://github.com/metalakeorg/metalake/pull/63))
- **Name remote engines once in `metalake.toml`.** Manage them with `metalake engine add|list|remove|check` or `lake.engine(name)`, and reach them with `USING <name>`. Secrets are never written to the file, only the variable that holds them ([#61](https://github.com/metalakeorg/metalake/pull/61), [#63](https://github.com/metalakeorg/metalake/pull/63))
- **Point an engine at an Iceberg server elsewhere** with `metalake iceberg connect <engine> --uri`, or `lake.iceberg.properties(…, uri=)` in Python ([#61](https://github.com/metalakeorg/metalake/pull/61))

## 0.5.0

### Upgrading from 0.4.0

- **A catalog's Postgres must keep each commit's time** (`track_commit_timestamp = on`, rule TT-1 of the spec):
  `metalake init` refuses one that does not, and a lake on such a server is read but no longer written until it is
  turned on. On Cloud SQL, RDS or Azure it is a database flag; on your own Postgres, `ALTER SYSTEM` and a restart
  (#55).

### SQL

- **`CREATE TABLE … AS SELECT`**, the table created and filled in one commit (#56).
- **`ALTER TABLE`:** add, drop and rename a column, widen its type, drop its `NOT NULL`, and rename the table (#56).

### Pipelines

- **A pipeline on a lake whose catalog is in the cloud:** the catalog's identity (`auth = "gcp-iam"`, `"aws-iam"`,
  `"azure-entra"`) and its verified TLS reach the pipeline, a pipeline that fails retries, and its state says
  `failing`, with the log of why (#54).

### UI

- **Catalog and Query:** a tag is picked from the branch's own tags; tables open to their columns, with an icon for
  each type, and a column's name or a table's path goes into the editor with one click; empty schemas are listed; a
  light and dark theme, switched from the sidebar (#57).

## 0.4.0

- **Catalog:** a new page to browse the lake by branch or tag: each catalog with its schemas and commits, each schema
  with its tables and properties, and each table with its columns, a sample of rows, its details and snapshots
  (#50, #51, #52).
- **Query:** the lake's tags listed below the branches; a tag picked is browsed and queried read only (#49).

## 0.3.1

- **UI:** Query's results copy to the clipboard as JSON, and show in pages of 100 (#46, #47).

## 0.3.0

- **UI:** `metalake ui` on your lake, with a live catalog and a SQL editor that reads and writes by branch (#33, #34).
- **Notebook:** JupyterLab in the UI, `pip install "metalake[notebook]"`, with `lake` ready and a branch selector
  (#35–#39, #42, #43).

## 0.2.0

### Upgrading from 0.1.0

- **A lake made by 0.1.0 is refused until it is migrated:** run `metalake migrate` (or `metalake.migrate()`) once.
  The protocol's sources changed, and revision 0.3 of the format raises the writer version to 3: a client of 0.1.0
  still reads a migrated catalog, and no longer writes it.

### Iceberg engines

- **A lake's Iceberg REST server**, ensured by the client on this machine and run by `metalake iceberg serve` (#11).
- **Engines connected in one line:** `lake.iceberg.spark()`, `duckdb()`, `pyiceberg()`, `flink()`, and
  `metalake iceberg connect <engine>` for Trino and the rest; their jars pinned and fetched once (#19).
- **Credentials vended for a table's files** on S3, Azure and GCS, narrowed to the table and to reading or writing,
  refreshed through the catalog (#19, spec 0.4 in #13).
- **A commit's requirements on a table's definition are checked** (REST-12, #14).
- Iceberg's REST Compatibility Kit gates every change; time travel by snapshot log (#1); first loads that arrive
  together generate the metadata once (#2).

### The catalog's Postgres in the cloud

- **TLS in libpq's modes**, `sslmode` up to `verify-full` and `sslrootcert`, so a managed Postgres's own connection
  string works as given (#23).
- **A cloud's token for the client's own identity**, no password: `auth = "aws-iam"` (RDS, Aurora), `"gcp-iam"`
  (Cloud SQL), `"azure-entra"` (Azure Database for PostgreSQL), in `metalake.toml`, `METALAKE_POSTGRES_AUTH`,
  `metalake init --auth` and `metalake.init(auth=)`. A token goes only to a server whose certificate is checked (#23).

### Streaming and pipelines

- **`table_changes`** in SQL: a range of a table's commits as a table (#7).
- **Changes, follow and a writer** in Python (#8) and on the command line, with `ingest` (#9); following a table from
  the client (#4).
- **Continuous pipelines**, their lifecycle and their verbs in every face (#10); a pipeline's MetaLake tables found by
  name (#6).

### DataFusion across processes

- **MetaLake's plans across processes:** the scan node and its codecs (#20), a scan as a `DataSourceExec` (#27).
- **Ballista** reads (#21), writes and `table_changes` (#22).
- **The router:** each statement runs locally or on the lake's cluster, by `metalake.engine` or by its measure (#24).

### The UI

- **`metalake ui`:** the web UI as a process (#28), served by the crate that embeds its frontend (#26, #25), to whom
  may use it (#29). Every wheel ships it, with its licenses (#30).

### Packaging

- The command line's image takes this system's certificates (#16).
