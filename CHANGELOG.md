# Changelog

What each version published on PyPI as `metalake` brings to whoever installs it. The work behind each line is in its
pull request; the decisions, in the ADRs.

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
