# MetaLake table format · version 0

**Status: draft.** Sections are added as the reference and the conformance suite that check them are built.

## 1. Conventions

- The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be read as described in RFC 2119.
- Every normative rule has an id, such as **VIS-3**. The conformance suite names the rules each case checks.
  Rules prefixed `ENG` bind the engines that read a catalog, not the catalog; the suite does not check them.
- **Catalog:** the set of tables of §4 in one Postgres schema.
- **Ref:** a branch or a tag. `main` is the ref every catalog starts with.
- **Sequence:** a `commit_seq` value (§3). "At sequence `s`" means the state that includes every commit whose
  rows are valid at `s`.
- **Validity range:** the pair `begin_seq`, `end_seq` of a row. A row is valid at `s` when
  `begin_seq <= s < end_seq`.
- **`OPEN`** is the `bigint` value `9223372036854775807`. A row whose `end_seq` is `OPEN` is **live**.
- **`LATEST`** is `OPEN - 1`: a read at `LATEST` sees the current state.
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
- **ID-5.** Field ids, partition spec ids, partition field ids and sort order ids are unique per table. They
  are drawn from the table's row in `table_counter`, which only increases, under a lock on that row. They are
  never drawn per ref, so two branches never assign the same id to different things. The first field id is 1,
  the first spec id 0, the first partition field id 1000 and the first sort order id 1; sort order id 0 is
  the unsorted order and has no row.

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
| `table_counter` | `table_id uuid` pk, `last_field_id int`, `last_spec_id int`, `last_partition_field_id int`, `last_sort_order_id int` | The per-table id counters (ID-5). |
| `table_name` (V) | `table_id uuid`, `namespace text[]`, `name text`, `dropped bool` | The name of a table, and whether it is dropped (§6). |
| `table_property` (V) | `table_id uuid`, `key text`, `value text` null, `dropped bool` | Table properties, as in Iceberg. |

### 4.3 Schema, partitioning and ordering

| Table | Columns | Notes |
|---|---|---|
| `field` (V) | `table_id uuid`, `field_id int`, `parent_field_id int` null, `position int`, `name text`, `type text`, `required bool`, `initial_default jsonb` null, `write_default jsonb` null, `doc text` null, `dropped bool` | One row per version of a field, nested fields included (§7). |
| `partition_spec` (V) | `table_id uuid`, `spec_id int`, `fields jsonb` | The default partition spec (§6). |
| `sort_order` (V) | `table_id uuid`, `sort_order_id int`, `fields jsonb` | The default sort order (§6). |
| `cluster_key` (V) | `table_id uuid`, `field_ids int[]` | The fields whose statistics are indexed for pruning (§6). |

Tables marked (V) are **versioned**: each also has `ref_id int`, `begin_seq bigint` and `end_seq bigint`,
and its rows follow §5.5.

### 4.4 Snapshots and writes

| Table | Columns | Notes |
|---|---|---|
| `snapshot` | `table_id uuid`, `snapshot_id bigint`, `ref_id int`, `parent_snapshot_id bigint` null, `schema_seq bigint`, `spec_id int`, `sort_order_id int`, `summary jsonb`; pk `(table_id, snapshot_id)` | One row per table per commit (§9). |
| `head` | `ref_id int`, `table_id uuid`, `snapshot_id bigint`; pk `(ref_id, table_id)` | The lock row of a `(ref, table)` (§9). |
| `idempotency` | `ref_id int`, `app_id text`, `version bigint`, `commit_seq bigint`; pk `(ref_id, app_id)` | Idempotent commits (CMT-5). |
| `intent` | `path text` pk, `table_id uuid`, `ref_id int`, `expires_at timestamptz` | Files that may have been uploaded without being committed (CMT-10). |

### 4.5 Data

| Table | Columns | Notes |
|---|---|---|
| `data_file` | `table_id uuid`, `file_id bigint`, `ref_id int`, `begin_seq bigint`, `end_seq bigint`, `path text` null, `format text`, `spec_id int`, `partition jsonb`, `sort_order_id int` null, `record_count bigint`, `file_size bigint`, `footer_size int`, `first_row_id bigint` null, `stat_field_ids int[]`, `lower bytea[]`, `upper bytea[]`, `null_count bigint[]`, `nan_count bigint[]`, `value_count bigint[]` null; pk `(table_id, file_id, begin_seq)` | One row per ref that has owned the file (§5). Immutable apart from closing `end_seq`. Statistics follow §8. |
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

### 5.4 Stable sequences

- **VIS-11.** A sequence `s` is **stable** when no commit with `commit_seq <= s` can still commit. A read that
  must be repeatable, across transactions or readers, uses a stable sequence; a read at `LATEST` is consistent
  within one transaction but not repeatable. To obtain one, a reader takes `L`, the last value drawn from
  `commit_seq_gen`, and then `X = pg_snapshot_xmax(pg_current_snapshot())`. `L` is stable once a later
  snapshot has `pg_snapshot_xmin >= X`. This holds because a commit has a transaction id, from locking its ref
  row, before it draws its `commit_seq` (CMT-4).

### 5.5 Versioned objects

- **VIS-9.** For a versioned table, the **key** of a row is:

  | Table | Key |
  |---|---|
  | `table_name` | `table_id` |
  | `table_property` | `(table_id, key)` |
  | `field` | `(table_id, field_id)` |
  | `partition_spec`, `sort_order`, `cluster_key` | `table_id` |

  At `(r, s)`, the visible version of a key is its row valid at the lowest level of the chain. If that row is
  `dropped`, the key is not visible. A ref changes an object by closing its own live row, if it has one, and
  adding a new row; it never changes rows of other refs.
- **VIS-10.** A ref has at most one live row per key.

## 6. Tables

The rules of this section and of §7 follow the Iceberg table specification, format version 3, as published
with Apache Iceberg 1.10.0
([`format/spec.md`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/format/spec.md)).

- **TBL-1.** A table exists at `(r, s)` when its `table_name` key is visible (VIS-9). Its name is the pair
  `(namespace, name)` of that version. A rename adds a version with the new name and keeps the `table_id`; a
  drop adds a version with `dropped = true`. Both are seen only by the ref that made them and the refs that
  inherit from it.
- **TBL-2.** The properties of a table at `(r, s)` are its visible `table_property` keys. A property is
  removed with a `dropped` version.
- **TBL-3.** The visible `partition_spec` version is the table's default spec. A `spec_id` denotes one list
  of fields forever: every row with the same `table_id` and `spec_id` has the same `fields`, and a file
  written with a spec that is no longer the default is read with it. `fields` is a JSON array of
  `{"source_ids": [int], "field_id": int, "name": text, "transform": text}`.
  - Transforms are `identity`, `bucket[N]`, `truncate[W]`, `year`, `month`, `day`, `hour` and `void`, with
    `N` and `W` positive, applied only to the source types Iceberg allows for them. A catalog never holds
    another transform.
  - A source field is a primitive field that is not contained in a list or a map.
- **TBL-4.** The visible `sort_order` version is the table's default sort order. With none, the default is
  the unsorted order, id 0. A `sort_order_id` denotes one list of fields forever. `fields` is a JSON array of
  `{"source_ids": [int], "transform": text, "direction": "asc" | "desc", "null_order": "nulls-first" | "nulls-last"}`,
  with the transforms and sources of TBL-3.
- **TBL-5.** The visible `cluster_key` version names between one and four distinct fields of the table, each
  a visible primitive field not contained in a list or a map.
- **ENG-1.** A file's `sort_order_id` declares that the file is sorted by that order. An engine MAY trust it; a
  writer MUST NOT declare an order the file does not have.
- **ENG-2.** A `bucket[N]` field in a file's partition spec declares that the file holds only rows of its
  bucket. An engine MUST NOT treat two tables as co-partitioned unless both use Iceberg's `bucket` hash with the
  same `N` over the same source type; an engine that cannot verify it treats them as not co-partitioned.

## 7. Types and schemas

- **TYP-1.** `field.type` is one of Iceberg's type strings in canonical form: `unknown`, `boolean`, `int`,
  `long`, `float`, `double`, `decimal(P,S)` with `1 <= P <= 38` and no spaces, `date`, `time`, `timestamp`,
  `timestamptz`, `timestamp_ns`, `timestamptz_ns`, `string`, `uuid`, `fixed[L]` with `L >= 1`, `binary`,
  `variant`, `geometry(C)`, `geography(C,A)`, and `struct`, `list` and `map` for nested fields. A geospatial
  type whose parameters are the defaults (`OGC:CRS84`, and `spherical` for `geography`) is written `geometry`
  or `geography`. `A` is one of `spherical`, `vincenty`, `thomas`, `andoyer` and `karney`.
- **TYP-2.** The visible fields of a table form one tree:
  - a field with no parent is a column of the table; a field with a parent belongs to that parent, which is
    visible and is a `struct`, `list` or `map`;
  - a `list` has exactly one child, named `element`; a `map` has exactly two, `key` and `value`, and `key` is
    required; a primitive has none;
  - siblings have distinct names and distinct positions; `position` orders them;
  - field ids are between 1 and 2147483447.
- **TYP-3.** A field's type changes only as Iceberg v3 allows: `unknown` to any type, `int` to `long`,
  `float` to `double`, `date` to `timestamp` or `timestamp_ns`, and `decimal(P,S)` to `decimal(P',S)` with
  `P' > P`. Any other change is a new field with a new id.
- **TYP-4.** `date` does not change to `timestamp` or `timestamp_ns` while it is the source of a partition
  field, in any spec of the table, whose transform is `identity` or `bucket[N]`: those would produce different
  partition values.
- **TYP-5.** Defaults follow Iceberg:
  - only fields of a struct (top-level columns included) have defaults; the children of lists and maps have
    none;
  - `initial_default` never changes across the versions of a field;
  - a required field added after the table was created has a non-null `initial_default` and `write_default`;
  - fields of type `unknown`, `variant`, `geometry` or `geography` have null defaults, and `unknown` fields are
    optional;
  - the default of a `struct` field is null or the empty object `{}`;
  - defaults use Iceberg's JSON single-value serialization: a JSON boolean for `boolean`, a JSON integer for
    `int` and `long`, a JSON number for `float` and `double`, a JSON string for the other primitives, a JSON
    array for a `list`, and a JSON object with `keys` and `values` arrays for a `map`.
- **TYP-6.** The schema of a table at `(r, s)` is its visible fields rendered as an Iceberg JSON `struct` type,
  ordered by `position` at every level, with `id`, `name`, `required`, `type`, and `doc`, `initial-default`
  and `write-default` when they are not null. `list` and `map` render with `element-id`, `element-required`
  and `element`, or `key-id`, `key`, `value-id`, `value-required` and `value`.

## 8. Statistics

### 8.1 What a file carries

- **STA-1.** Every `data_file` row has `record_count`, `file_size` and `footer_size`, exact and non-negative.
  `footer_size` is the length of the Parquet footer (`FileMetaData`), so a reader can fetch it in one request.
- **STA-2.** Column statistics are parallel arrays: `lower`, `upper`, `null_count`, `nan_count` and, when
  present, `value_count` have one element per field of `stat_field_ids`, in the same order. The field ids are
  distinct. A `NULL` element means unknown. `nan_count` is `NULL` for fields that are not `float` or `double`;
  `value_count` counts nulls and NaNs, as in Iceberg.
- **STA-3.** Readers find a field's statistics by its position in `stat_field_ids`, never by a fixed position.
  Writers MUST give bounds for every field of the table's cluster key at commit time and SHOULD place them
  first, in key order, so that indexes on the first positions serve pruning. Writers SHOULD give bounds for
  every primitive field.
- **STA-4.** `partition` is a JSON array with one element per field of the file's partition spec `spec_id`, in
  order, each in Iceberg's JSON single-value serialization. `spec_id` is a spec of the table, and
  `sort_order_id`, when not `NULL`, is 0 or a sort order of the table.

### 8.2 Bound encoding

Bounds are `bytea` in an **order-preserving encoding**: for two values `a` and `b` of one field,
`encode(a) < encode(b)` exactly when `a < b`, comparing `bytea` as Postgres does (unsigned bytes, a prefix
first). Pruning is then plain SQL over the catalog.

- **STA-5.** The encoding of each type:

  | Type | Encoding |
  |---|---|
  | `boolean` | 1 byte: `0x00` false, `0x01` true |
  | `int`, `long` | the value as a 64-bit two's complement integer, big-endian, sign bit flipped (8 bytes) |
  | `date` | days from 1970-01-01 as a 32-bit two's complement integer, big-endian, sign bit flipped (4 bytes) |
  | `time` | microseconds from midnight, as `long` (8 bytes) |
  | `timestamp`, `timestamptz` | microseconds from 1970-01-01 00:00:00 UTC, as `long` (8 bytes) |
  | `timestamp_ns`, `timestamptz_ns` | nanoseconds from 1970-01-01 00:00:00 UTC, as `long` (8 bytes) |
  | `float`, `double` | the value as an IEEE 754 double, big-endian; if the sign bit is 0, flip it; if it is 1, flip every bit (8 bytes) |
  | `decimal(P,S)` | the unscaled value as a 128-bit two's complement integer, big-endian, sign bit flipped (16 bytes) |
  | `string` | the UTF-8 bytes |
  | `uuid` | the 16 bytes, big-endian |
  | `fixed[L]`, `binary` | the bytes |

  `string` bounds therefore order by Unicode code point, and `float` and `double` bounds by IEEE 754
  `totalOrder`: `-0.0` precedes `+0.0`, as Iceberg requires.
- **STA-6.** NaN is never a bound. A bound is the least or greatest of the non-null, non-NaN values of the
  field in the file; a file whose values are all null or NaN has `NULL` bounds for that field.
- **STA-7.** The encoding does not change under the promotions of TYP-3: `int` and `long` share one encoding,
  a `float` is encoded as the double it promotes to, and a decimal's unscaled value keeps its scale. The
  exception is `date` to `timestamp` or `timestamp_ns`: bounds written before the promotion keep their 4 bytes,
  and a reader converts a 4-byte bound of a timestamp field from days to the field's unit.
- **STA-8.** `string` and `binary` bounds MAY be truncated: a `string` to its first 16 code points, a `binary`
  to its first 16 bytes. A truncated `lower` is the prefix. A truncated `upper` is the prefix with its last code
  point (or byte) incremented, after dropping trailing code points that are U+10FFFF (or bytes that are
  `0xFF`); the increment skips the surrogate range U+D800 to U+DFFF. If nothing is left to increment, the
  `upper` bound is `NULL`.
- **STA-9.** `variant` bounds are `NULL` in v0. `geometry` and `geography` bounds are the corners of the
  bounding box, as in Iceberg: `lower` holds the minimum and `upper` the maximum of each coordinate, as the
  concatenation `x:y`, `x:y:z`, or `x:y:z:m` (with `z` NaN when only `m` is set), each coordinate encoded as a
  `double` above. For `geography`, `x` of `lower` MAY be greater than `x` of `upper`: the box crosses the
  antimeridian.

### 8.3 Pruning

- **STA-10.** A visible file is a candidate for a predicate `lo <= field <= hi` (either end MAY be open)
  unless its bounds prove that no value of the field in the file is in that range: `upper < lo` or
  `lower > hi`, comparing bounds as STA-7 reads them. A file with no bounds for the field is always a
  candidate, and so is every file for a `geometry`, `geography` or `variant` field, whose bounds are not
  ordered values. The reference function `prune(r, t, s, field_id, lo, hi)` returns exactly the candidates.

## 9. Commits

A commit changes one or more tables of one ref. Its operations are: add files, remove files, create, rename
and drop a table, add, change and drop fields, set or remove properties, and set the partition spec, the sort
order and the cluster key.

### 9.1 Snapshots

- **CMT-1.** Every table a commit touches gets one `snapshot` row with `snapshot_id = commit_seq`, the ref,
  `parent_snapshot_id` (the table's current snapshot on the ref before the commit, `NULL` for a new table),
  `schema_seq` (the `commit_seq` of the latest commit that changed the table's fields, as seen by this
  snapshot), the default `spec_id` and `sort_order_id` (0 when unsorted) after the commit, and a `summary`
  with `operation`, `added-data-files`, `deleted-data-files`, `added-records` and `deleted-records`.
- **CMT-2.** The snapshot of table `t` at `(r, s)` is found through the chain: at the lowest level i that has
  one, the greatest `snapshot_id` of `ref_i` that is at most `s_i` and at least the `begin_seq` of the
  `ref_base` row that level reads through (0 for a ref without one). The **current** snapshot is the one at
  `LATEST`. `head.snapshot_id` is always the current snapshot of a ref that has committed to the table.

### 9.2 Protocol

- **CMT-3.** A commit is one transaction. It writes everything or nothing. Its `commit` row records the ref,
  the author and actor, the operation, the optional `app_id`, `app_version`, `definition` and `message`, and
  `xid = pg_current_xact_id()`. Sequence numbers of failed commits are lost; `commit_seq` may have gaps.
- **CMT-4.** Before drawing its `commit_seq`, a commit locks:
  1. its ref's row: `FOR KEY SHARE`, or `FOR UPDATE` if it creates, renames or drops a table;
  2. the `head` rows of the existing tables it touches, `FOR UPDATE`, in `table_id` order. A missing head is
     first inserted with the table's current snapshot.

  So commits of one `(ref, table)` serialise, their snapshots form one chain of increasing ids, and no commit
  is lost. Tags and deleted refs receive no commits.
- **CMT-5.** With an `app_id`, a commit is idempotent: it inserts `(ref, app_id, 0, 0)` into `idempotency`
  if absent and locks that row first. If its `app_version` is at most the stored `version`, it changes nothing
  and returns the stored `commit_seq`. Otherwise it stores its own `app_version` and `commit_seq`.
  `app_version` is positive.
- **CMT-6.** A table entry MAY carry `expected_snapshot`. If the current snapshot differs, the commit fails
  with a conflict. This is how an Iceberg `assert-ref-snapshot-id` requirement is honoured.
- **CMT-7.** Checked under the locks, against the current state, a commit fails with a conflict when it:

  | The commit… | Conflicts when… |
  |---|---|
  | removes a file | the file is not visible on the ref |
  | adds a field named `n` under a parent | a visible sibling is already named `n` |
  | changes or drops field `f` | `f` is not visible, or the type change is not allowed (TYP-3, TYP-4) |
  | creates a table, or renames one, to `(namespace, name)` | another visible table has that name |
  | renames, drops or otherwise changes a table | the table is not visible on the ref |
  | only adds files | never |

  A conflict raises SQLSTATE `40001` with the message prefix `metalake conflict:`. The writer MAY retry with
  a refreshed view.
- **CMT-8.** A commit writes, with `begin_seq = commit_seq`:
  - its added files, owned by the ref;
  - for a removed file the ref owns, the closing of its row (`end_seq = commit_seq`); for one it inherits, a
    tombstone;
  - for a changed versioned object, the closing of the ref's own live row of that key, if any, and a new row;
  - its snapshots (CMT-1), and `head.snapshot_id = commit_seq` for every touched table.
- **CMT-9.** A commit is refused, with SQLSTATE `22023` and the message prefix `metalake invalid:`, if the
  current state of a touched table would break a rule of §6, §7 or §8, if an added file lacks statistics for
  a field of the cluster key (STA-3), or if a new field id was not drawn from the table's counter. A created
  table takes its field, spec and partition field ids from the commit, and its counters start after them; its
  default spec is spec 0, unpartitioned unless the commit gives one. Only what the commit changes is checked: the
  definitions of the tables it touches and the files it adds. Files committed earlier are not checked again, so
  the cost of a commit does not grow with the number of files in the table.
- **CMT-10.** Writers SHOULD register an `intent` row (path, table, ref, expiry) in its own transaction before
  uploading a file. A commit deletes the intents of the files it adds. After committing, it sends
  `NOTIFY metalake` with the payload `<schema>:<commit_seq>`.
