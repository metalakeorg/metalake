# A proof of value: a Hive warehouse on HDFS, taken over by MetaLake

The demo shows a technology entering a legacy environment and the value that follows, measured in that environment.
It is not a benchmark. A benchmark compares two systems doing the same thing, and much of what MetaLake does here
Hive on HDFS cannot do at all.

The company is fictional, **Tienda**, an online shop. Its data platform is a Hive warehouse on HDFS of the kind a
Hadoop shop runs:

- raw files land from outside;
- Hive ETL refines them into curated tables and marts;
- small files pile up, a few tables are ORC, and one is transactional.

The demo has two parts. The migration is the **bridge**: it is measured by what crossing costs (bytes copied,
time, downtime, tables left behind) and scores nothing by itself. The **value** is what the company can do once it is
across, measured on the migrated estate against the same estate as Hive ran it:

- its queries;
- its development and audit;
- its loads;
- what it opens.

## 1. The scoreboard

Each act of part two, on the migrated estate, scores in one of three columns, the way vendors' proofs of value and
Forrester's TEI studies report:

| Column | What it holds | How it is measured |
|---|---|---|
| **Better** | What both do, MetaLake better | A direct delta: the same work done both ways |
| **New** | What the warehouse cannot do | The cost of today's workaround, or that there is none, against one statement |
| **Enables** | Options opened, priced or not | What becomes possible with no copy and no change |

Then the **risks**: what the demo finds MetaLake cannot yet do, each with its plan.

**Success criteria**, fixed before the first measure, as a proof of value fixes them:

The bridge:

1. The migration copies no byte of a Parquet table, and the warehouse keeps working throughout.

The value:

2. A day's load is published to every table at once, or not at all, and a bad one is undone without repair.
3. A team gets a development copy of the whole warehouse in seconds and at no storage cost.
4. An auditor's question, *what did `ventas` say at the close of January?*, is answered exactly.
5. Spark, Trino and DuckDB read the same tables with no copy.
6. The number of files on HDFS falls, and the rows stay the same.

## 2. The company's warehouse

The warehouse has four Hive databases: three landing tables feed the eight below. Each table is chosen for what it puts to the test.

| Table | Format, layout | Written by | It puts to the test |
|---|---|---|---|
| `raw.eventos` | CSV text, `dt`/`hora`, external | The web (the generator), 4 drops a day | The landing zone; not Parquet, so a copy |
| `raw.clientes`, `raw.productos`, `raw.stock` | CSV text, `dt`, external | The shop's systems, once a day | Where the dimensions and the stock come from |
| `curated.ventas` | Parquet, `dt` | One insert per drop: 4 files a day | The happy path, at scale, with small files |
| `curated.clientes` | Parquet, unpartitioned | A first load, then the day's new customers appended | A dimension that grows |
| `curated.productos` | Parquet, unpartitioned | A first load; `ALTER TABLE ADD COLUMNS (marca)` on day 45; then new products with `marca` | Schema evolution: old files lack the column |
| `curated.inventario` | Parquet, `dt`/`almacen` | A daily snapshot per warehouse; unknown warehouses as Hive's null | Two partition levels and `__HIVE_DEFAULT_PARTITION__` |
| `marts.ventas_diarias` | Parquet, unpartitioned | `INSERT OVERWRITE` of the whole table, every day | Rewritten files: the files a source drops |
| `marts.top_clientes` | ORC, unpartitioned | `INSERT OVERWRITE`, every day | Not Parquet: a copy |
| `ops.pedidos` | ORC, transactional (ACID) | Inserts and `UPDATE`s of an order's status | Hive's ACID: a copy |

### The data

- **History:** 90 days, 2024-01-01 to 2024-03-30, from a seeded generator, so every run is the same.
- **Volume:** about 5,000 customers, 500 products, and 3,000 purchases a day out of about 20,000 events. That is
  about 270,000 sales and 1.8 million events: small enough for one PC, and large enough for files and partitions to
  matter.
- **Files:**
  - `curated.ventas`: 90 partitions × 4 files;
  - `curated.inventario`: 90 days × 3 warehouses;
  - about 1,000 files in all before compaction.

### How the warehouse is written

The way Hive writes it is the way a production Hive writes it:

1. The generator writes each drop's CSV. `hdfs dfs -put` lands it under `raw.eventos`, and
   `ALTER TABLE … ADD PARTITION` makes it seen.
2. A **drop** runs `INSERT INTO curated.ventas PARTITION (dt) SELECT … FROM raw.eventos WHERE dt = … AND hora = …`.
   It is a dynamic partition, one file per drop.
3. The **close of a day** writes:
   - the day's `inventario`;
   - the new customers;
   - `ventas_diarias` and `top_clientes`, each rebuilt with `INSERT OVERWRITE`;
   - `pedidos` changes, inserted and updated.
4. **History is loaded in bulk:**
   - the first 60 days by one statement per drop hour, over every day at once, so each partition still gets its 4
     files;
   - the last 30 days day by day, so the warehouse has lived like one;
   - `ALTER TABLE` falls inside that history, on day 45.
5. **`day.sh` adds one more day,** to test coexistence after the migration: Hive keeps writing, and MetaLake follows.

## 3. Part one, the bridge: the migration, by phase

The pipeline a company follows to cross. It reports the cost of crossing:

- bytes copied;
- time;
- downtime;
- tables in place, copied, or left with a gap.

It does not fill the scoreboard. The value comes on the other side (§5).

| Phase | What | With |
|---|---|---|
| 0. Inventory | Every table classified as in place, copy, or a gap (§4) | Hive's `SHOW`/`DESCRIBE FORMATTED`; `REGISTER TABLE … CHECK` |
| 1. The lake | Catalog in Cloud SQL, schema `tienda`; warehouse on the same HDFS | `metalake init` |
| 2. In place | The Parquet tables, no byte copied | `REGISTER TABLE` |
| 3. Coexistence | Hive writes; MetaLake follows | `day.sh`, then `REGISTER TABLE t` again |
| 4. Reconciliation | Rows and sums per partition, Hive against MetaLake | `reconcile.py` |
| 5. Copies | The tables that cannot be in place | Hive rewrites them as Parquet in a new directory, then `REGISTER TABLE … ADOPT` |
| 6. Cut-over | Writers move to MetaLake; sources adopted; Hive retired | The ETL rewritten against MetaLake's SQL |

## 4. What the demo will find, on purpose

These gaps are measured and reported as risks, not hidden:

| Table | What happens today | The surface it asks for |
|---|---|---|
| `curated.productos` | A run after `ALTER TABLE ADD COLUMNS` is refused: the column is new | Schema evolution when registering |
| `marts.ventas_diarias` | `INSERT OVERWRITE` leaves the old files in the table | A sync mode that drops what the source dropped |
| `marts.top_clientes`, `ops.pedidos`, `raw.eventos` | ORC, ACID and CSV are not registered | A copy (phase 5) |
| All eight | One statement per table, its directory found by hand | `REGISTER SCHEMA … FROM hive`, from the metastore |

## 5. Part two, the value: the acts on the migrated estate

Once across, the same files are Hive's and MetaLake's at once (coexistence), so each act measures both on the same
estate. The acts are grouped by the column they score in.

**Better**: performance and operation, the same work done both ways.

| Act | Hive, measured | MetaLake, measured |
|---|---|---|
| Planning | Metastore partitions, then an HDFS listing, before a byte is read | Files and column statistics from the catalog, no listing |
| Latency | A day, a month, a product: Hive's time | The same queries: MetaLake's time, files pruned |
| Small files | Files on HDFS, which the NameNode holds in memory | `maintain` compacts: files before and after, the same rows |
| Upkeep | Old files and orphans, cleaned by hand | `maintain` expires and sweeps; `strays` reports |

**New**: what Hive cannot do. The measure is the workaround's cost against one statement.

| Act | Hive's workaround, measured | MetaLake, measured |
|---|---|---|
| Development | Copy the data: GB and minutes, or share production | A branch of the whole lake: 0 bytes, seconds |
| A day's load (write, audit, publish) | A half-done load is seen; a bad one repaired by hand | Loaded on a branch, reconciled, one `MERGE` publishes every table; a bad load is a dropped branch |
| Several tables at once | `ventas` and `ventas_diarias` drift apart | One commit for both |
| Audit | No answer, or a snapshot copied at month end | Tag `cierre_enero`; `AS OF` |
| Rollback | Restore from a backup, if there is one | Read, or branch from, the commit before |

**Enables**: options opened, at no copy.

| Act | What opens |
|---|---|
| Engines | Spark, Trino and DuckDB on the same tables through Iceberg REST; `USING` from SQL |
| Changes | `changes` and `follow`: CDC for downstream at no cost |
| Governed by SQL | Branches, tags, retention and registration as SQL statements, in one catalog |
| The next store | A lake whose catalog holds where the bytes are, so a later move to object storage is a store, not a rewrite |

## 6. Where it lives

```
demo/hive-hdfs/
  compose.yml            the cluster (exists)
  DESIGN.md              this
  company/
    generate.py          the seeded data: customers, products, events per drop
    hive/
      schema.sql         the four databases and their tables
      drop.sql           one drop: raw → curated.ventas
      close.sql          one day's close: inventario, clientes, productos, marts, pedidos
      backfill.sql       the first 60 days, one statement per drop hour, products before the brands
      alter.sql          day 45: productos gains marca
      backfill_after_brand.sql  the rest of the 60 days: products with brands, marts, orders
    lib.sh               Hive's and HDFS's command lines, the calendar, one day's run
    land.sh              the CSV onto HDFS, its partitions found or added
    history.sh           the 90 days: bulk, then day by day
    day.sh               one more day
  migration/             inventory, reconciliation (phase 0, 4)
  acts/                  one script per act, and the scoreboard
```

## 7. Order of construction (steps of 15 minutes at most)

1. `generate.py`: customers, products, events; seeded; one day or one drop at a time.
2. `schema.sql`, and Hive's ACID switched on in `compose.yml`. The risk: `ops.pedidos` needs it, and the image ships
   it off.
3. `land.sh` and `drop.sql`: one drop, end to end, checked against the generator's counts.
4. `close.sql` and `alter.sql`.
5. `backfill.sql` and `history.sh`: the 90 days; their time and file counts recorded as the baseline.
6. `day.sh`.

Then the migration (phases 0 to 6), and then the acts and the scoreboard, each a design of its own when it comes.

## 8. Risks of the build itself

- **Hive's ACID with an embedded Derby metastore** may not run. If not, `ops.pedidos` stays ORC without
  transactions, and the gap is said so.
- **Tez in local mode** may be slow: a statement takes seconds. Ninety days day by day would take an hour, hence the
  bulk backfill of the first 60.
- **One PC.** Timings are this machine's. Every figure is reported with its scale, and none is extrapolated.
