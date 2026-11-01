# MetaLake preliminary-study prototypes

These are **throwaway prototypes** from MetaLake's preliminary study. They exist to answer design questions with
measurements before the Rust implementation is written: how to store branch visibility, how to commit, how to merge,
how to version the schema, how to write safely, how to collect garbage, how to store statistics, and whether an
Iceberg projection is faithful. They are not the implementation and will not be maintained. The results they produced
are recorded in [`docs/adr/0001-metalake.md`](../docs/adr/0001-metalake.md) §13.

Everything runs in a stock `postgres:17` image (plus `python:3.12-slim` for the two Python scripts). Benchmark
scripts print one `result<TAB>...` line per measurement; SQL test files abort on the first unexpected result and
print a final `WARNING: ... OK` line when every scenario passes.

## What each prototype is

| Files | Question |
|---|---|
| `schema.sql` | The catalog model and the three **visibility options**, plus the **commit modes**. Base of everything else. |
| `read_o{1,2,3}_full.sql`, `read_o{1,2,3}_pruned.sql` | Reading a ref's live file set (full, and pruned by min/max key) under each option. |
| `commit_cas.sql`, `commit_lock.sql`, `commit_global.sql`, `commit_delete.sql` | pgbench scripts for the commit modes. |
| `bench.sh` | Seeds, checks that o1/o2/o3 see the same files in every ref, measures read latency per branch depth, fork cost, and concurrent commit throughput per mode. |
| `merge.sql`, `merge_tests.sql`, `merge_bench.sh` | **Merging** a branch into its parent: conflict detection and the merge itself; 12 scenarios; cost by diff size. |
| `columns.sql`, `columns_tests.sql` | **Schema with branches**: column versions per ref, field ids, type promotion, schema merge; 9 scenarios. |
| `write.sql`, `write_tests.sql`, `commit_idempotent.sql` | The **write protocol**: upload intents, idempotent commit, orphan detection. |
| `maintenance.sql`, `maintenance_tests.sql` | **Maintenance**: retention horizons and garbage collection with branches (hidden deleted branches). |
| `stats.sql`, `stats_e{1,2,3}_{c3,c7,s15,all}.sql`, `scale_bench.sh` | Size and planning at scale (o1 only), and the three **statistics shapes**. |
| `projection.py` | **Iceberg projection**: writes Iceberg v2 metadata from the Postgres rows and checks it with PyIceberg. |
| `commit_payload.py` | The **commit payload** taken from the Parquet footer the writer already holds. |

### Visibility options (o1 / o2 / o3)

Branches form a tree with inheritance. The question is how to store which files a `(ref, table, snapshot)` sees:

- **o1 · validity range per ref with inheritance.** Each file row lives in its ref with `(begin_seq, end_seq)`. A
  branch stores only what it adds, plus **tombstones** for inherited files it removes. Forking is one row in `ref`
  (`fork_seq` = the last parent seq the branch sees). Reading walks the ancestor chain (`visible()`).
- **o2 · deltas.** Each commit stores what it adds (`'a'`) and removes (`'r'`); the live set is computed by walking.
- **o3 · copy on fork** (DuckLake with branches). The branch copies the parent's live rows when it forks; reading is
  one level, forking is proportional to table size.

### Commit modes (on o1)

- **cas** — the writer read the head earlier; the update succeeds only if the head did not move (optimistic).
- **lock** — head lock (`select ... for update` on the `head` row): commits to one table are serialised inside the
  database, an append never conflicts.
- **global** — head lock plus a global advisory lock: the commit sequence has no gaps across the whole catalog.
- **delete** — a real conflict: the commit fails (SQLSTATE 40001) if a file it removes is no longer live.

### Merge

`conflicts(b)` compares what branch `b` changed with what its parent changed since `fork_seq` (both removed the same
file; rows deleted in a file the other removed; a file removed in which the other deleted rows). `merge_branch(b)`
applies the diff to the parent in one transaction if there are none — moving rows, not copying them, unioning deletion
vectors — and leaves `b` as an empty child of the parent. Scenarios E1–E12 cover append/append, remove/append,
compaction on either side, deletion vectors, fast-forward, nested branches and multi-table atomicity.

### Schema with branches

Columns follow the same model as files: each column version is a row of its ref with a validity range; field ids
come from one catalog-wide sequence. `schema_conflicts(b)` flags a field changed differently on both sides and two
fields ending up with the same name; promotions follow Iceberg (int→long, float→double). Scenarios C1–C9.

### Write protocol

1. `register_intent()` records each file's path and expiry before uploading. 2. The files are uploaded and their
payload computed from the footer. 3. `commit_write()` is one transaction: it checks `(ref, app) → version`
idempotency (like Delta's `txn`), opens the commit under the head lock, registers the files, deletes their intents
and records the version. Retrying a lost commit is safe. `orphans()` lists expired intents whose file never got in.
Scenarios W1, W2, W4, W6, plus a concurrent retry check with pgbench.

### Maintenance

Each ref has a retention horizon. A file is collectable when no retained snapshot of its ref sees it and no live
child branch sees it at its fork point. A deleted branch with live descendants stays hidden (its children read
through it). Scenarios G1–G6.

### Statistics shapes (e1 / e2 / e3)

Per-file, per-column min/max/null counts for 20 columns (10 integer, 10 text): **e1** one typed row per
`(file, column)` with a single range index for every column; **e2** one `jsonb` per file; **e3** arrays per file.
`scale_bench.sh` measures their size and pruning latency (an indexed column, an unindexed integer column, a text
column, and all 20 columns of 800 files).

### Iceberg projection

`projection.py` reads the live set of every ref, writes content-addressed manifests (what a branch inherits untouched
reuses `main`'s manifests), one manifest list per head snapshot and a `metadata.json` with one Iceberg branch per
ref. PyIceberg then reads it with `StaticTable` and plans each ref, full and pruned; the files must match
`visible()` exactly. Finally it commits 4 files + 1 removal on main and re-projects, reporting how many manifests
were rewritten.

### Commit payload

`commit_payload.py` writes a 10⁶-row Parquet file in memory and derives everything a commit registers (rows, bytes,
footer size, row groups, per-column min/max/nulls aggregated from the row groups) from the footer alone.

## Running

From Git Bash on Windows, `export MSYS_NO_PATHCONV=1` first and use `C:/metalake/bench` as the mount source; on
Linux/macOS use `"$PWD"`. The mount can be read-only.

### SQL test suites

Each suite runs in a fresh database:

```bash
docker run -d --name mlx-pg -e POSTGRES_HOST_AUTH_METHOD=trust -v "$PWD":/b:ro postgres:17
# wait until `docker exec mlx-pg pg_isready -U postgres` succeeds, then one of:
docker exec mlx-pg psql -X -q -U postgres -v ON_ERROR_STOP=1 -f /b/schema.sql -f /b/merge.sql -f /b/merge_tests.sql postgres
docker exec mlx-pg psql -X -q -U postgres -v ON_ERROR_STOP=1 -f /b/schema.sql -f /b/merge.sql -f /b/columns.sql -f /b/columns_tests.sql postgres
docker exec mlx-pg psql -X -q -U postgres -v ON_ERROR_STOP=1 -f /b/schema.sql -f /b/merge.sql -f /b/write.sql -f /b/write_tests.sql postgres
docker exec mlx-pg psql -X -q -U postgres -v ON_ERROR_STOP=1 -f /b/schema.sql -f /b/merge.sql -f /b/maintenance.sql -f /b/maintenance_tests.sql postgres
docker rm -f mlx-pg
```

Expected last lines: `merge: the 12 scenarios, OK`, `schema: the 9 scenarios, OK`, `write: W1, W2, W4 and W6, OK`,
`maintenance: G1–G6, OK`.

Concurrent idempotency (after the write suite, in the same database): 8 clients retrying the same `(app, version)`
must leave exactly one new snapshot.

```bash
docker exec mlx-pg pgbench -n -U postgres -c 8 -j 4 -t 20 -D v=5 -f /b/commit_idempotent.sql postgres
docker exec mlx-pg psql -X -At -U postgres -c "select count(*) from snapshot" postgres   # one more than before
```

### Benchmarks

The bench scripts start their own Postgres inside the container, so they run as the `postgres` user with no server:

```bash
# Visibility options and commit modes (the defaults are the full-scale run; these are tiny)
docker run --rm -u postgres -v "$PWD":/b:ro \
  -e N=20000 -e DEPTH=5 -e K=200 -e M=200 -e POST=200 -e DUR=2 -e CLIENTS="1 8" -e SB=256MB -e ECS=1GB \
  postgres:17 bash /b/bench.sh

# Merge cost by diff size
docker run --rm -u postgres -v "$PWD":/b:ro -e N=50000 -e SB=256MB -e ECS=1GB postgres:17 bash /b/merge_bench.sh

# Size and planning at scale, and the statistics shapes
docker run --rm -u postgres -v "$PWD":/b:ro -e N=100000 -e NS=20000 -e DUR=1 -e SB=256MB -e ECS=1GB \
  postgres:17 bash /b/scale_bench.sh
```

Environment variables (each script documents its own in its header):

| Variable | Used by | Meaning (default) |
|---|---|---|
| `N` | all three | files on main (10⁶; 10⁷ for `scale_bench.sh`) |
| `COMMITS` | `bench.sh` | commits on main (`N/100`) |
| `DEPTH` | `bench.sh` | chained branches (5) |
| `K`, `M` | `bench.sh` | new files / tombstones per branch (1000) |
| `POST` | `bench.sh` | files added and deleted on main after forking (1000) |
| `DUR` | `bench.sh`, `scale_bench.sh` | seconds per pgbench run (10 / 5) |
| `CLIENTS` | `bench.sh` | pgbench client counts (`1 8 32 64`) |
| `NS` | `scale_bench.sh` | files with statistics (10⁶) |
| `SB`, `ECS` | all three | `shared_buffers`, `effective_cache_size` (8GB/20GB or 6GB/18GB) |
| `PGDATA` | all three | data directory (`/tmp/pg`) |

`bench.sh` must report `correctness failures=0`. The full-scale numbers in the ADR were taken on a GKE test cluster
(n2-highmem-4 node, a pod of 3 vCPU / 24 GiB, Postgres 17.11), on a dedicated benchmark node pool.

### Iceberg projection

```bash
docker network create mlx-net
docker run -d --name mlx-pg --network mlx-net -e POSTGRES_HOST_AUTH_METHOD=trust -v "$PWD":/b:ro postgres:17
# wait for it, then load and seed (o1 only):
docker exec mlx-pg psql -X -q -U postgres -v ON_ERROR_STOP=1 -f /b/schema.sql -f /b/merge.sql \
  -c "select seed(20000, 200, 2, 200, 200, 200, false)" -c "call build_indexes()" postgres
docker run --rm --network mlx-net -v "$PWD":/b:ro python:3.12-slim bash -c '
  pip install -q "pyiceberg[pyarrow]==0.12.0" "psycopg[binary]" &&
  mkdir -p /tmp/out && python /b/projection.py "host=mlx-pg user=postgres dbname=postgres" /tmp/out 2000'
docker rm -f mlx-pg && docker network rm mlx-net
```

The last argument is the number of files per manifest (default 10000). Expect `correctness failures=0` and a final
`re-project main after a commit` line.

### Commit payload

```bash
docker run --rm -v "$PWD":/b:ro python:3.12-slim bash -c 'pip install -q pyarrow && python /b/commit_payload.py'
```
