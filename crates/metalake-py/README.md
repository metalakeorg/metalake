# MetaLake

An open lakehouse table format whose metadata lives in Postgres. Tables are Parquet in your storage; snapshots,
schemas, statistics, branches and deletes are rows in a transactional catalog. SQL runs on Apache DataFusion.

**Status: alpha.** The format and the command line can still change.

```
pip install metalake
metalake init ./lake
metalake sql "create table t (k bigint); insert into t values (1), (2); select * from t"
```

That is all it takes: the package carries Postgres, which starts when a command needs it and stays up until
`metalake stop`.

## Branches and time

A branch or a tag covers the whole lake and copies nothing.

```
metalake branch create dev
metalake sql "insert into dev.default.t values (3)"
metalake merge dev --check
metalake merge dev -m "add a row"
metalake log
metalake sql 'select * from "main@4".default.t'
```

## Cloud storage and your own Postgres

```
metalake init ./lake gs://bucket/prefix
metalake init ./lake s3://bucket/prefix
metalake init postgresql://user@host/db s3://bucket/prefix
```

Credentials come from the environment (`AWS_*`, `GOOGLE_*`, `AZURE_*`), never from a file. A password for Postgres
is `PGPASSWORD`.

## Upkeep

```
metalake retain main --max-age 30d --min-snapshots 10
metalake maintain
```

Until a ref says what it keeps, it keeps everything. `maintain` runs once; schedule it as you schedule anything else.

## For programs

stdout holds what was asked for and stderr the rest. `--format json` prints one object per line, and exit codes are
stable: 0 done, 1 failed, 2 wrong use, 3 a conflict that changed nothing, 4 a write whose fate is unknown.

## Platforms

Windows and macOS 11 or later on x86-64 and ARM, Linux with glibc 2.34 or later on x86-64 and ARM.
