# MetaLake table format · version 0

**Status: draft.** Sections are added as the reference and the conformance suite that check them are built.

## 1. Conventions

- The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be read as described in RFC 2119.
- Every normative rule has an id, such as **VIS-3**. The conformance suite names the rules each case checks.
- **Catalog:** the set of tables of §4 in one Postgres schema.
- **Ref:** a branch or a tag. `main` is the ref every catalog starts with.
- **Sequence:** a `commit_seq` value (§3). "At sequence `s`" means the state that includes every commit whose
  rows are valid at `s`.
- **Validity range:** the pair `begin_seq`, `end_seq` of a row. A row is valid at `s` when
  `begin_seq <= s < end_seq`.
- **`OPEN`** is the `bigint` value `9223372036854775807`. A row whose `end_seq` is `OPEN` is **live**.
- **Reference:** `v0/schema.sql` and `v0/reference.sql`. Where this text and the reference disagree, the text
  is wrong and is corrected.

## 2. Catalog layout

- **LAY-1.** A catalog is one Postgres schema that holds exactly the tables of §4, under those names. No
  table has a tenant column. A database MAY hold any number of catalogs.
- **LAY-2.** The table `meta(key text primary key, value text not null)` holds at least:
  - `format_version`, which is `0` for this specification;
  - `catalog_uuid`, a UUID assigned when the catalog is created;
  - `created_at`, the creation time in ISO 8601 with a time zone.
- **LAY-3.** A reader MUST refuse a catalog whose `format_version` it does not know, or that has none. The
  reference raises SQLSTATE `0A000` with the message prefix `metalake unsupported:`.
- **LAY-4.** Every sequence number is a `bigint`. The end of a validity range is never `NULL`: an open end is
  `OPEN`. Every validity range satisfies `begin_seq < end_seq`.

## 3. Identity

- **ID-1.** `commit_seq` values are drawn from the catalog sequence `commit_seq_gen`, which starts at 1 and
  never cycles. A `commit_seq` identifies one commit in the catalog.
- **ID-2.** `ref_id` values are drawn from `ref_id_gen`, which starts at 2. `ref_id = 1` is `main`, which
  exists from the creation of the catalog with `created_seq = 0`.
- **ID-3.** `table_id` is a UUID assigned when the table is created and never reused.
- **ID-4.** `file_id` values are drawn from `file_id_gen`. A `file_id` is unique in the catalog, across
  tables.
- **ID-5.** Field ids, partition spec ids and sort order ids are unique per table. They are drawn from the
  table's row in `table_counter`, which only increases, under a lock on that row. They are never drawn per
  ref, so two branches never assign the same id to different things.

## 4. Tables

Columns are Postgres types. Primary keys, uniqueness rules and checks are normative. Other indexes in the
reference DDL are recommendations.

### 4.1 Refs and commits

| Table | Columns | Notes |
|---|---|---|
| `ref` | `ref_id int` pk, `name text` unique, `kind text` (`branch` or `tag`), `created_seq bigint`, `protected bool`, `deleted bool` | |
| `ref_base` | `ref_id int`, `begin_seq bigint`, `end_seq bigint`, `parent_ref_id int`, `fork_seq bigint`; pk `(ref_id, begin_seq)` | Where a ref inherits from (§5). `main` has none. |
| `commit` | `commit_seq bigint` pk, `ref_id int`, `xid xid8`, `committed_at timestamptz` null, `author text`, `actor text`, `operation text`, `app_id text` null, `app_version bigint` null, `definition text` null, `message text` null | One row per commit. |

### 4.2 Tables

| Table | Columns | Notes |
|---|---|---|
| `tbl` | `table_id uuid` pk, `location text`, `created_seq bigint` | Immutable. `location` is the prefix new files are written under. |
| `table_counter` | `table_id uuid` pk, `last_field_id int`, `last_spec_id int`, `last_sort_order_id int` | The per-table id counters (ID-5). |

### 4.3 Data

| Table | Columns | Notes |
|---|---|---|
| `data_file` | `table_id uuid`, `file_id bigint`, `ref_id int`, `begin_seq bigint`, `end_seq bigint`, `path text` null, `format text`, `record_count bigint`, `file_size bigint`; pk `(table_id, file_id, begin_seq)` | One row per ref that has owned the file (§5). Immutable apart from closing `end_seq`. |
| `tombstone` | `table_id uuid`, `file_id bigint`, `ref_id int`, `begin_seq bigint`, `end_seq bigint`; pk `(table_id, file_id, ref_id, begin_seq)` | A ref hides a file it inherits. |

## 5. Visibility

Every read goes through this section.

### 5.1 Ref bases

- **VIS-1.** A `ref_base` row says that, during its validity range, its ref inherits from `parent_ref_id` as
  that ref was at `fork_seq`. A ref has at most one live `ref_base` row, `fork_seq <= begin_seq`, and a ref is
  never its own parent.
- **VIS-2.** `ref_base` rows are versioned. A change of base (a merge, a re-parenting) closes the live row at
  the change's `commit_seq` and opens a new one that begins there. Rows are never deleted while a retained
  read can reach them, so a read at an earlier sequence keeps the base it had.

### 5.2 The chain

- **VIS-3.** The chain of a read of ref `r` at sequence `s` is a list of levels `(ref_i, s_i)`:
  1. level 0 is `(r, s)`;
  2. level i+1 is `(parent_ref_id, fork_seq)` of the `ref_base` row of `ref_i` valid at `s_i`;
  3. the chain ends at a level whose ref has no `ref_base` row valid at its sequence.

  A ref never reaches itself through its chain. The reference raises SQLSTATE `XX000` with the message prefix
  `metalake corrupt:` if it does.

### 5.3 Data files

- **VIS-4.** A `data_file` row is **valid at level i** when its `ref_id` is `ref_i` and it is valid at `s_i`.
- **VIS-5.** A `tombstone` row **applies at level j** when its `ref_id` is `ref_j` and it is valid at `s_j`.
- **VIS-6.** The files of table `t` visible at `(r, s)` are the rows of `t` valid at some level i for which no
  tombstone of the same file applies at a level `j < i`. The result is a set: no file appears twice.
- **VIS-7.** A file has at most one live row. When a file changes owner, as when a merge moves a branch's files
  to its parent, the old owner's row is closed at the commit that moves it and the new owner's
  row begins there. A ref removes a file it owns by closing the row, and a file it inherits with a tombstone; it never has a live
  tombstone of a file it owns.
- **VIS-8.** Reads at a sequence are stable. A commit only adds rows that begin at its own `commit_seq` and
  closes live rows at it, so any read at a sequence below that `commit_seq` returns what it returned before
  the commit.
