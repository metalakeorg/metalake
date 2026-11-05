# MetaLake

Open-source lakehouse table format & protocol inside a PostgreSQL catalog

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
