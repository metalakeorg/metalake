# MetaLake table format · version 0

**Status:** final, tagged `spec-v0` on 2026-11-02. Later changes follow §18. Revision 0.1 (namespaces as
objects, `writer_version`) is in progress: its rules are marked *0.1*.

## 1. Conventions

- The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be read as described in RFC 2119.
- Every normative rule has an id, such as **VIS-3**. The conformance suite names the rules each case checks.
  Rules prefixed `ENG` bind the engines that read a catalog, not the catalog; the suite does not check them.
  Rules prefixed `ICE` are checked by the Iceberg harness rather than by the SQL suite (§16).
- **Catalog:** the set of tables of §4 in one Postgres schema.
- **Ref:** a branch or a tag. `main` is the ref every catalog starts with.
- **Sequence:** a `commit_seq` value (§3). "At sequence `s`" means the state that includes every commit whose
  rows are valid at `s`.
- **Validity range:** the pair `begin_seq`, `end_seq` of a row. A row is valid at `s` when
  `begin_seq <= s < end_seq`.
- **`OPEN`** is the `bigint` value `9223372036854775807`. A row whose `end_seq` is `OPEN` is **live**.
- **`LATEST`** is `OPEN - 1`: a read at `LATEST` sees the current state.
- **Reference:** the DDL in `v0/schema/` and the behaviour in `v0/reference/`, both SQL, loaded in file-name
  order. Where this text and the reference disagree, the text is wrong and is corrected.

## 2. Catalog layout

- **LAY-1.** A catalog is one Postgres schema that holds exactly the tables of §4, under those names. No
  table has a tenant column. A database MAY hold any number of catalogs.
- **LAY-2.** The table `meta(key text primary key, value text not null)` holds at least:
  - `format_version`, which is `0` for this specification;
  - `catalog_uuid`, a UUID assigned when the catalog is created;
  - `created_at`, the creation time in ISO 8601 with a time zone;
  - *0.1:* `writer_version`, `1` for this revision (LAY-5).
- **LAY-3.** A reader MUST refuse a catalog whose `format_version` it does not know, or that has none. The
  reference raises SQLSTATE `0A000` with the message prefix `metalake unsupported:`.
- **LAY-4.** Every sequence number is a `bigint`. The end of a validity range is never `NULL`: an open end is
  `OPEN`. Every validity range satisfies `begin_seq < end_seq`.
- **LAY-5.** *0.1.* A writer MUST refuse to commit to a catalog whose `writer_version` it does not know; a
  missing `writer_version` is `0`. A writer of revision 0.1 commits only to `writer_version` `1`, after migrating a
  catalog of version 0 (§18). The reference raises SQLSTATE `0A000`. Readers do not read `writer_version`.

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
| `namespace` (V) | `namespace text[]`, `dropped bool` | *0.1.* A namespace (NS-1). |
| `namespace_property` (V) | `namespace text[]`, `key text`, `value text` null, `dropped bool` | *0.1.* Namespace properties (NS-4). |

### 4.3 Schema, partitioning and ordering

| Table | Columns | Notes |
|---|---|---|
| `field` (V) | `table_id uuid`, `field_id int`, `parent_field_id int` null, `position int`, `name text`, `type text`, `required bool`, `initial_default jsonb` null, `write_default jsonb` null, `doc text` null, `dropped bool` | One row per version of a field, nested fields included (§7). |
| `partition_spec` (V) | `table_id uuid`, `spec_id int`, `fields jsonb` | The default partition spec (§6). |
| `sort_order` (V) | `table_id uuid`, `sort_order_id int`, `fields jsonb` | The default sort order (§6). |
| `cluster_key` (V) | `table_id uuid`, `field_ids int[]` | The fields whose statistics are indexed for pruning (§6). |

Tables marked (V) are **versioned**: each also has `ref_id int`, `begin_seq bigint` and `end_seq bigint`,
and its rows follow §5.4.

### 4.4 Snapshots and writes

| Table | Columns | Notes |
|---|---|---|
| `snapshot` | `table_id uuid`, `snapshot_id bigint`, `ref_id int`, `parent_snapshot_id bigint` null, `schema_seq bigint`, `spec_id int`, `sort_order_id int`, `summary jsonb`; pk `(table_id, snapshot_id)` | One row per table per commit (§9). |
| `head` | `ref_id int`, `table_id uuid`, `snapshot_id bigint`; pk `(ref_id, table_id)` | The lock row of a `(ref, table)` (§9). |
| `idempotency` | `ref_id int`, `app_id text`, `version bigint`, `commit_seq bigint`; pk `(ref_id, app_id)` | Idempotent commits (CMT-5). |
| `intent` | `path text` pk, `table_id uuid`, `ref_id int`, `expires_at timestamptz` | Files that may have been uploaded without being committed (CMT-10). |
| `retention` | `ref_id int` pk, `min_seq bigint` null, `max_age interval` null, `min_snapshots int` null | What a ref keeps (§13). |

### 4.5 Data

| Table | Columns | Notes |
|---|---|---|
| `data_file` | `table_id uuid`, `file_id bigint`, `ref_id int`, `begin_seq bigint`, `end_seq bigint`, `path text` null, `format text`, `spec_id int`, `partition jsonb`, `sort_order_id int` null, `record_count bigint`, `file_size bigint`, `footer_size int`, `first_row_id bigint` null, `stat_field_ids int[]`, `lower bytea[]`, `upper bytea[]`, `null_count bigint[]`, `nan_count bigint[]`, `value_count bigint[]` null; pk `(table_id, file_id, begin_seq)` | One row per ref that has owned the file (§5). Immutable apart from closing `end_seq`. Statistics follow §8. |
| `tombstone` | `table_id uuid`, `file_id bigint`, `ref_id int`, `begin_seq bigint`, `end_seq bigint`; pk `(table_id, file_id, ref_id, begin_seq)` | A ref hides a file it inherits. |
| `deletion_vector` (V) | `table_id uuid`, `file_id bigint`, `path text` null, `content_offset bigint` null, `content_size bigint`, `cardinality bigint`, `inline bytea` null | The deleted rows of a file (§10). |
| `inline_file` | `table_id uuid`, `file_id bigint`, `content bytea`; pk `(table_id, file_id)` | The bytes of an inline file (§11). |
| `file_footer` | `table_id uuid`, `file_id bigint`, `footer bytea`; pk `(table_id, file_id)` | OPTIONAL: a file's Parquet footer (§11). |

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
- **VIS-7.** A file has at most one live row. When a file changes owner, as when a merge hands a branch's files
  to its parent (MRG-4), the old owner's row is closed at that commit and the new owner's row begins there. A
  ref removes a file it owns by closing its row, and a file it inherits with a tombstone; it never has a live
  tombstone of a file it owns.
- **VIS-8.** Reads at a sequence are stable. A commit only adds rows that begin at its own `commit_seq` and
  closes live rows at it, so any read at a sequence below that `commit_seq` returns what it returned before
  the commit.

### 5.4 Versioned objects

- **VIS-9.** For a versioned table, the **key** of a row is:

  | Table | Key |
  |---|---|
  | `table_name` | `table_id` |
  | `table_property` | `(table_id, key)` |
  | `field` | `(table_id, field_id)` |
  | `deletion_vector` | `(table_id, file_id)` |
  | `partition_spec`, `sort_order`, `cluster_key` | `table_id` |
  | `namespace` | `namespace` (*0.1*) |
  | `namespace_property` | `(namespace, key)` (*0.1*) |

  At `(r, s)`, the visible version of a key is its row valid at the lowest level of the chain. If that row is
  `dropped`, the key is not visible. A ref changes an object by closing its own live row, if it has one, and
  adding a new row; it never changes rows of other refs.
- **VIS-10.** A ref has at most one live row per key.

### 5.5 Stable sequences

- **VIS-11.** A sequence `s` is **stable** when no commit with `commit_seq <= s` can still commit. A read that
  must be repeatable, across transactions or readers, uses a stable sequence; a read at `LATEST` is consistent
  within one transaction but not repeatable. To obtain one, a reader takes `L`, the last value drawn from
  `commit_seq_gen`, and then `X = pg_snapshot_xmax(pg_current_snapshot())`. `L` is stable once a later
  snapshot has `pg_snapshot_xmin >= X`. This holds because a commit has a transaction id, from locking its ref
  row, before it draws its `commit_seq` (CMT-4).

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
*0.1.* A table lives in a namespace, an object of its own:

- **NS-1.** A namespace is a non-empty array of non-empty strings, compared exactly. It exists at `(r, s)` when its
  `namespace` key is visible (VIS-9). A namespace of more than one level has a parent: the array without its last
  element.
- **NS-2.** A commit that creates a table, or renames one, leaves the table's namespace existing on the ref; a
  commit that creates a namespace leaves its parent existing. A namespace that never existed on the ref makes the
  commit invalid (`22023`); one whose visible version is `dropped`, by a commit that won the race, is a conflict
  (`40001`).
- **NS-3.** A namespace is dropped with a `dropped` version, and only when, once the commit is applied, no table
  and no namespace under it exist on the ref; otherwise the commit is a conflict. A dropped namespace MAY be
  created again. Namespaces are not renamed.
- **NS-4.** The properties of a namespace at `(r, s)` are its visible `namespace_property` keys; a property is
  removed with a `dropped` version. Three keys are reserved and informative to clients: `location`, the prefix
  under which tables of the namespace are created by default; `comment`; and `kind`, where `foreign` marks a
  namespace whose tables live in another system. The format gives them no other meaning.
- **NS-5.** NS-2 and NS-3 are checked by commits and merges. History written before a catalog was migrated
  (§18) need not satisfy them, and readers do not check them.
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

Counts, NaN handling, truncation and geospatial bounds follow the Iceberg table specification, format version 3
(Apache Iceberg 1.10.0); the bound encoding of §8.2 is MetaLake's own.

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
- **CMT-4.** Before drawing its `commit_seq`, a commit is ordered:
  1. against its ref: commits of a ref are serialised against a merge into it, `create_ref` from it and
     `delete_ref` of it, as by a reader-writer lock that grants in order of arrival, and not against each other.
     The reference locks the ref's row `FOR KEY SHARE`, or `FOR UPDATE` when the commit gives a table a name; an
     implementation MAY use any mechanism with this order, and every writer of a catalog uses the same one;
  2. against commits that give the same name: a commit that creates or renames a table holds that name on the
     ref until it ends;
  3. against commits of the same tables: it locks the `head` rows of the existing tables it touches,
     `FOR UPDATE`, in `table_id` order. A missing head is first inserted with the table's current snapshot.

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
  | sets the deletion vector of a file | the file is not visible, or its visible vector is not the one the commit started from (DV-4) |
  | adds a field named `n` under a parent | a visible sibling is already named `n` |
  | changes or drops field `f` | `f` is not visible, or the type change is not allowed (TYP-3, TYP-4) |
  | creates a table, or renames one, to `(namespace, name)` | another visible table has that name |
  | *0.1:* creates a table in, or renames one into, a namespace | the namespace's visible version is dropped (NS-2) |
  | *0.1:* creates a namespace | it exists on the ref |
  | *0.1:* drops a namespace, or sets its properties | its visible version is dropped |
  | *0.1:* drops a namespace | a table or a namespace under it exists once the commit is applied (NS-3) |
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
  a field of the cluster key (STA-3), if a new field id was not drawn from the table's counter, if it adds a
  file whose `intent` has expired (RET-7), or, *0.1*, if it names a namespace that never existed on the ref, or one
  that is not a namespace (NS-1, NS-2). A commit MAY change namespaces only; it then writes no snapshot. A created table takes its field, spec and partition field ids from
  the commit, and its counters start after them; its default spec is spec 0, unpartitioned unless the commit
  gives one. Only what the commit changes is checked: the definitions of the tables it touches and the files it
  adds. Files committed earlier are not checked again, so
  the cost of a commit does not grow with the number of files in the table.
- **CMT-10.** Writers SHOULD register an `intent` row (path, table, ref, expiry) in its own transaction before
  uploading a file. A commit deletes the intents of the files it adds. After committing, it sends
  `NOTIFY metalake` with the payload `<schema>:<commit_seq>`.
- **CMT-11.** A commit whose `operation` is `replace` (a compaction or a flush) only removes and adds files.
  The live rows of the files it removes (DV-5) equal the rows of the files it adds, which carry no vectors.

## 10. Deletion vectors

The rules of this section follow the Puffin specification published with Apache Iceberg 1.10.0
([`format/puffin-spec.md`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/format/puffin-spec.md)),
blob type `deletion-vector-v1`.

- **DV-1.** A `deletion_vector` row deletes the rows of its data file whose positions, counted from 0, its
  bitmap holds. It is a versioned object keyed by `(table_id, file_id)` (§5.4). At `(r, s)` a file's vector is
  its visible version, and it applies only while the file is visible; a file has at most one vector. A ref
  that removes a file closes its own live vector of that file, if any, at the same commit.
- **DV-2.** The vector is a `deletion-vector-v1` blob: the length of what follows up to the checksum as 4
  bytes big-endian, the magic `D1 D3 39 64`, the positions as a 64-bit Roaring bitmap in its portable
  serialization (the number of 32-bit bitmaps as 8 bytes little-endian, then for each, by increasing key, the
  key as 4 bytes little-endian and the 32-bit Roaring bitmap in its standard serialization), and a CRC-32 of the
  magic and the bitmap as 4 bytes big-endian. Positions are below 2⁶³ and below the file's `record_count`.
  `cardinality` is the number of positions.
- **DV-3.** The blob is stored in one of two ways:
  - in a Puffin file at `path`, with `content_offset` and `content_size` exactly the blob's offset and length in
    that file's footer, and `inline` `NULL`;
  - in `inline`, holding the blob's bytes, with `path` and `content_offset` `NULL` and
    `content_size = length(inline)`.
- **DV-4.** A commit that sets a file's vector names `base_seq`, the `begin_seq` of the visible vector it
  started from, or none if the file had none. If the file's visible vector is another one, the commit fails
  with a conflict (CMT-7). The new vector holds every position of the one it replaces, so `cardinality` never
  decreases. The reference checks the positions themselves only for inline vectors, whose bytes it has.
  A vector MAY delete rows of a file the same commit adds; it then names no base.
- **DV-5.** The live rows of a file at `(r, s)` are its `record_count` minus the `cardinality` of its vector.

## 11. Inline files and footers

- **INL-1.** An inline file is a `data_file` row with `path = NULL`; its complete Parquet bytes are the
  `content` of its `inline_file` row, and `file_size = length(content)`. A `data_file` has `path = NULL`
  exactly when it has an `inline_file` row. It follows every other rule of a file. Its size is at most the table
  property `inline.max-bytes`, 65536 when the property is absent.
- **INL-2.** Inline files are flushed by a `replace` commit (CMT-11) that removes them and adds stored files.
  Implementations MUST flush a table on a ref when its live inline files exceed the property
  `inline.flush-bytes` in total (1048576 when absent) or the oldest of them was committed more than
  `inline.flush-age-seconds` ago (3600 when absent). The reference function `inline_flush_due(r, t)` says
  whether that is the case.
- **FTR-1.** A `file_footer` row is OPTIONAL. When present, `footer` is the file's Parquet footer
  (`FileMetaData`) byte for byte, so its length is the file's `footer_size`. A file ends with its footer, the
  footer's length as 4 bytes little-endian and the magic `PAR1`: a reader with `footer_size` fetches the last
  `footer_size + 8` bytes in one request, and a reader with the `file_footer` row fetches none.

## 12. Branches and merges

### 12.1 Branch operations

- **BR-1.** Creating a branch `b` from ref `p` is one transaction. It locks `p`'s `ref` row `FOR UPDATE`, which
  waits for every commit in flight on `p` (CMT-4). It takes `fork_seq`, the last value drawn from
  `commit_seq_gen`, or an earlier retained read of `p` the caller names (RET-6). It then draws a `commit_seq` `c`
  and writes the `ref` row with `created_seq = c`, a `ref_base` row `(b, begin_seq = c, parent_ref_id = p,
  fork_seq)` and a `commit` row of `b` with operation `branch`. Nothing else is written: `b` sees `p` as it was
  at `fork_seq`.
- **BR-2.** A tag is created the same way, with `kind = tag`. It never takes commits (CMT-4) and is never
  merged.
- **BR-3.** Deleting a ref sets `deleted = true`. `main` and protected refs are not deleted. A ref's name is
  never reused, so a name always denotes one ref, past or present.

### 12.2 Merge

- **MRG-1.** A branch `b` merges into the ref its live `ref_base` row points at, its parent `p`; merging
  into any other ref is not in v0. The merge is one transaction: it locks `p`'s `ref` row and then `b`'s
  `FOR UPDATE`, then `p`'s heads of the tables `b` changed, in `table_id` order, then draws `commit_seq` `m`
  and writes a `commit` row of `p` with operation `merge`. A branch with nothing to merge is left as it is.
- **MRG-2.** What the merge compares:
  - **`b`'s changes** are its live rows: its files, tombstones, vectors and object versions. They are all it
    changed since its base, because a merge closes them (MRG-4).
  - **`p`'s changes** since the fork are the differences between what `p` sees at `fork_seq` and what it sees
    at `LATEST`, for the files and keys `b` changed: whether a file is visible, a file's vector, a key's
    visible version. They do not depend on how `p` got there, whether by its own commits or by merges into
    it.
- **MRG-3.** The outcome, file by file and key by key. Any conflict fails the merge with SQLSTATE `40001`
  and changes nothing.

  | `b` | `p` since the fork | Outcome |
  |---|---|---|
  | adds files | anything | `b`'s files join `p` |
  | removes file `f` | leaves `f` visible and its vector unchanged | `f` leaves `p` |
  | removes `f` | removes `f`, or changes its vector | conflict |
  | sets the vector of `f` | leaves `f` visible and its vector unchanged | `b`'s vector |
  | sets the vector of `f` | changes the vector of `f` | the union of both vectors (MRG-5) |
  | sets the vector of `f` | removes `f` | conflict |
  | changes key `k` | leaves `k` as it was | `b`'s version |
  | changes `k` | changes `k` to the same content | that version |
  | changes `k` | changes `k` otherwise, drops it, or drops the table | conflict |
  | changes a table `p` no longer has | | conflict |

  A key is a field, a table name, a property, the partition spec, the sort order, the cluster key or, *0.1*, a
  namespace or a namespace property (VIS-9). After applying, every touched table MUST satisfy §6–§8, table names
  MUST stay unique on `p`, and, *0.1*, every table and namespace the merge brought or touched MUST satisfy NS-2 and
  NS-3 on `p`; otherwise the merge is a conflict, for instance when `b` and `p` gave two different fields one name,
  or `b` created a table in a namespace `p` dropped.
- **MRG-4.** Applying, at `m`:
  - each of `b`'s live rows is closed at `m`, and a copy of it opens on `p` at `m`; a copy of a vector or a
    version first closes `p`'s own live row of the same key;
  - a tombstone of `b` closes `p`'s own live row of the file, or, if `p` inherits the file, opens a tombstone
    of `p`;
  - `p` gets a snapshot (CMT-1, operation `merge`) and its head for every table that changed;
  - `b`'s live `ref_base` row closes at `m` and a new one opens: `(b, m, p, fork_seq = m)`. `b` is now an empty
    child of `p`, and its reads below `m` are unchanged (VIS-8). Its heads become its current snapshots.
- **MRG-5.** When both sides changed the vector of a file, the merged vector holds every position of both. If
  both are inline, the merge writes their union inline. Otherwise the caller MUST supply the merged vector,
  stored as any vector (DV-3); without it the merge is a conflict. A supplied vector has at least the
  cardinality of each side, and, where the bytes are inline, every position of both.

## 13. Retention and collection

### 13.1 What is kept

- **RET-1.** A ref's `retention` row, if any, keeps a snapshot of the ref when at least one of its set
  criteria holds:
  - `min_seq`: the snapshot's id is at least `min_seq`;
  - `max_age`: its commit was committed at most `max_age` ago;
  - `min_snapshots`: it is among the ref's `min_snapshots` latest snapshots.

  A ref without a `retention` row, or whose row sets no criterion, keeps every snapshot. A ref's snapshots
  are the distinct `snapshot_id` values of its `snapshot` rows.
- **RET-2.** A ref is **live** when it is not deleted. The **retained reads** are, for every live ref `r`,
  `(r, LATEST)` and `(r, s)` for every snapshot `s` that `r` keeps. The **reach points** are the levels of the
  chains of the retained reads (VIS-3). A deleted ref that a reach point still reaches is **hidden**: its rows
  stay, because a live ref reads through them. A live ref keeps what it reads alive: a branch keeps its
  parent's state at its fork, and a merged branch, an empty child at the merge (MRG-4), keeps the parent's state
  at the merge until it is deleted.
- **RET-3.** A row of `data_file`, `tombstone`, `deletion_vector`, `ref_base` or a versioned table is
  **collectable** when no reach point `(R, s)` has `R` equal to the row's ref and the row valid at `s`. A
  `snapshot` row is collectable when no retained read uses it as its snapshot of the table (CMT-2). A `commit`
  row is collectable when its `commit_seq` is not the id of a remaining snapshot, the `created_seq` of a ref,
  or a commit recorded in `idempotency`. Collecting never changes the result of a retained read.

### 13.2 Collecting

- **RET-4.** An object in storage is deleted only when nothing that remains refers to it:
  - a data file's object (its `path`, or its `inline_file` row), with its `file_footer`, when every
    `data_file` row of the file is collectable;
  - a Puffin file when no remaining `deletion_vector` row has its `path`.
- **RET-5.** A collector locks the `ref` row of every ref `FOR SHARE`, computes what is collectable, deletes
  the objects, then deletes the rows, all while holding the locks. Objects go first, so a failure leaves rows
  that no retained read reaches, never objects that nothing refers to. Changing a ref's retention locks its
  `ref` row `FOR NO KEY UPDATE`, and creating a ref locks its parent's `FOR UPDATE` (BR-1), so neither runs
  during a collection; commits are not blocked.
- **RET-6.** A ref created at an earlier sequence (BR-1) is created at a retained read of its parent: one of
  the snapshots the parent keeps. Together with RET-5, nothing a collector decided to delete can become
  reachable again.

### 13.3 Orphans

- **RET-7.** A path is an **orphan** when its `intent` has expired and no `data_file` or `deletion_vector`
  row refers to it. Orphans are found from `intent` alone, without listing storage: a collector deletes the
  object, then the `intent` row. A commit that adds a file whose `intent` has expired is refused (CMT-9), so a
  writer that missed its deadline never commits a path that a collector may have deleted.

## 14. Time and changes

- **TT-1.** Implementations MUST run with `track_commit_timestamp = on`. A commit's time is its transaction's
  commit timestamp. `commit.committed_at` is `NULL` until a filler copies that timestamp into it, after the
  commit; until then readers take it from `pg_xact_commit_timestamp` of the commit's `xid`.
- **TT-2.** The order of commits in a catalog is `(committed_at, commit_seq)`. `commit_seq` alone is not that
  order: a commit may draw a lower `commit_seq` and commit later. Within one `(ref, table)`, both orders agree
  (CMT-4).
- **TT-3.** A read of table `t` on ref `r` as of time `T` reads `(r, s)`, where `s` is:
  - the greatest snapshot of `t` on `r` committed at or before `T`, among those since the `ref_base` row that
    was in force at `T` (the row whose `begin_seq` committed at or before `T` and whose `end_seq` had not
    committed by `T`);
  - if there is none, the `begin_seq` of that row, so that `r` reads its parent as of its fork;
  - nothing, if `r` was created after `T`.

  The sequence is found per table, because one sequence cannot describe the catalog at a time (TT-2). The
  answer is stable once every commit with a timestamp up to `T` is visible: implementations MUST refuse a `T`
  later than one second before the current time.
- **CHG-1.** The change feed delivers commits in the order of TT-2 from a cursor `(committed_at, commit_seq)`,
  the last commit delivered, and only those committed more than one second ago. Postgres takes a commit's
  timestamp just before making it visible, so a commit not yet visible will carry a timestamp later than that
  horizon: every commit is delivered once, in order, with no gaps.
  Commits removed by collection (RET-3) are no longer delivered: a consumer keeps up within the retention.

## 15. Partition values

The rules of this section follow the Iceberg table specification, format version 3 (Apache Iceberg 1.10.0):
partition transforms, the truncate details and Appendix B.

- **PAR-1.** Each transform maps a value to a partition value; a null value maps to null:
  - `identity`: the value itself;
  - `bucket[N]`: `(h & 2147483647) mod N`, where `h` is the 32-bit Murmur3 hash, x86 variant, seed 0, as a
    signed integer, of the value's hash serialization: `int`, `long`, `time` (microseconds from midnight) and
    timestamps (microseconds from the epoch, nanoseconds divided by 1000 rounding down) as the 8-byte
    little-endian `long`; `date` as the 8-byte little-endian `long` of its days from 1970-01-01; `decimal` as
    the unscaled value in the fewest bytes of two's complement, big-endian; `string` as UTF-8; `uuid` as its 16
    bytes, big-endian; `fixed` and `binary` as the bytes;
  - `truncate[W]`: for `int` and `long`, `v - (((v mod W) + W) mod W)`; for `decimal`, the same over the unscaled
    value, so `W` counts in units of the scale; for `string`, the first `W` code points; for `binary`, the first
    `W` bytes;
  - `year`, `month`, `day`, `hour`: the number of whole years, months, days or hours from 1970-01-01T00:00 UTC to
    the value, rounding down; a timestamp counts in UTC;
  - `void`: null.
- **PAR-2.** Every row of a file has the same partition value for each field of the file's spec, and
  `data_file.partition` holds it, in Iceberg's JSON single-value serialization of the transform's result type:
  the source type for `identity` and `truncate`, `int` for the others.
- **PAR-3.** A commit refuses a file whose partition values contradict its statistics. For a field with
  transform `T` over source field `f`, where the file has statistics for `f` (STA-7 reading them):
  - if the partition value is null, the file has no non-null value of `f`: its bounds are `NULL`;
  - if it is not null, `null_count` of `f` is 0 where known;
  - for `identity`, `truncate`, `year`, `month`, `day` and `hour`, which keep order, `T(lower)` and `T(upper)`
    equal the partition value; for `string` and `binary`, whose bounds may be truncated (STA-8),
    `T(lower) <= value <= T(upper)`;
  - for `bucket[N]`, when `lower = upper` and `f` is neither `string` nor `binary`, `T(lower)` equals the value.

## 16. Iceberg projection

A MetaLake table is served to Iceberg clients as an Iceberg table: metadata, manifest lists and manifests derived
from the catalog, and an Iceberg REST `updateTable` turned into a MetaLake commit. The projection is a mapping,
not a copy: nothing in it is a source of truth. It follows the Iceberg table specification, format version 2,
and the REST catalog API
([`open-api/rest-catalog-open-api.yaml`](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml)),
as published with Apache Iceberg 1.10.0. Server-side scan planning (`planTableScan`) is not in v0. Rules prefixed
`ICE` are checked by the Iceberg harness (`v0/conformance/iceberg/`), with PyIceberg.

### 16.1 Outbound

- **ICE-1.** The table metadata of table `t`:
  - `format-version` 2: format 3 requires row lineage, which is not in v0; `table-uuid` is `table_id`;
    `location` is `tbl.location`; `last-column-id`, `last-partition-id` are the table's counters;
  - one schema per distinct `schema_seq` of the snapshots served, with `schema-id = schema_seq`, rendered as
    TYP-6 at a snapshot that has it; `current-schema-id` is that of `main`'s current snapshot;
  - every partition spec of the table (`source-id` for the single source), every sort order plus the unsorted
    order 0, and `main`'s defaults; `main`'s visible properties;
  - the snapshots served (ICE-2), with `sequence-number` equal to `snapshot-id`, `timestamp-ms` from the commit
    time (TT-1), `parent-snapshot-id`, `schema-id` and the summary, whose `operation` is mapped to Iceberg's:
    `merge` becomes `overwrite`;
  - a `refs` entry for every live ref that sees the table, of its kind, pointing at its current snapshot (CMT-2);
    `current-snapshot-id` is `main`'s.

  Schema ids are `schema_seq` values: a catalog projects while its `commit_seq` stays below 2³¹.
- **ICE-2.** Every remaining snapshot of the table is served, so that a client can rebase on any of them after a
  conflict. The manifests of snapshot `S`, owned by ref `R`, list exactly the files visible at `(R, S)`: status
  `ADDED` for the files `S`'s commit added on `R`, `EXISTING` for the rest, each with its data sequence number
  equal to its row's `begin_seq`, its spec, partition, record count, size and statistics (ICE-3).
- **ICE-3.** Statistics are served in Iceberg's single-value serialization of the field's current type: each
  MetaLake bound is decoded (§8, STA-7) and serialized as Iceberg writes it: `int`, `date` as 4 bytes
  little-endian, `long`, `time` and timestamps as 8, `float` and `double` as 4 and 8 bytes of IEEE 754
  little-endian, `decimal` as the unscaled value in the fewest bytes of two's complement big-endian, `uuid` as 16
  bytes big-endian, and the rest as their bytes. Counts are served as they are.
- **ICE-4.** A deletion vector is served as an Iceberg v2 position delete file that the projection materializes
  from the vector's positions, read inline or from its Puffin file: a Parquet file at
  `<location>/metalake-deletes/<file_id>-<begin_seq>.parquet` with one row per position, sorted, in the columns
  `file_path` (field id 2147483546, the data file's served path) and `pos` (2147483545). It is listed in a delete
  manifest with `record_count` its cardinality, `referenced_data_file` its data file, and the vector's
  `begin_seq` as data sequence number, so it applies to its data file (whose sequence number is not greater).
- **ICE-5.** Inline files are never served as catalog rows: the projection materializes an inline file at
  `<location>/metalake-inline/<file_id>.parquet` with its exact bytes.

- **ICE-7.** *0.1.* The projection serves the namespaces that exist on the ref it projects (NS-1). Listing the
  namespaces under a parent returns those whose parent it is; loading one returns its properties (NS-4).
  Creating a namespace, updating its properties and dropping it are commits under NS-2 and NS-3. Dropping a
  namespace that is not empty answers Iceberg's `NamespaceNotEmptyException` (HTTP 409). Namespaces are not
  renamed, as in the Iceberg REST specification.

### 16.2 Inbound

- **ICE-6.** An Iceberg REST `updateTable` becomes one MetaLake commit on the ref its `set-snapshot-ref` names:
  - an `assert-ref-snapshot-id` requirement becomes `expected_snapshot` (CMT-6), and a mismatch answers
    409 `CommitFailedException`;
  - the `ADDED` entries of the manifests the new snapshot added become added files, their bounds converted back
    from Iceberg's serialization to MetaLake's (STA-5); its `DELETED` entries become removed files;
  - other updates are refused in v0;
  - the answer is the projection after the commit, in which the snapshot carries MetaLake's id.

## 17. Conformance

An implementation conforms to version 0 when:

- every catalog it writes satisfies §2 to §15;
- every read it serves of a catalog equals the reference's read of that catalog at the same ref and sequence;
- its Iceberg projection, if it serves one, satisfies §16.

The SQL suite (`v0/run.sh`) checks every rule except those prefixed `ENG` and `ICE`; the Iceberg harness
(`v0/conformance/iceberg/run.sh`) checks those prefixed `ICE`. Both run against the reference. An implementation
shows conformance by running them with its own operations in place of the reference's.

## 18. Versioning

- `format_version` (LAY-2) is an integer. This text defines version 0.
- An additive change, one that a reader of the previous text can ignore without misreading the catalog (an
  optional column, a table, a property), keeps the version. LAY-1 then names the added tables, and readers
  ignore columns and tables they do not know.
- Any other change increments the version, and the text of the new version states how a catalog migrates from
  the previous one. A reader refuses a version it does not know (LAY-3).
- Rule ids are never reused or renumbered.
- *0.1.* `writer_version` (LAY-5) is an integer too. A change that readers of the previous text can ignore without
  misreading the catalog, but that its writers could break, keeps `format_version` and increments
  `writer_version`. Revision 0.1 is `format_version` 0, `writer_version` 1.
- *0.1.* **Migrating from version 0.** A writer of revision 0.1 migrates a catalog of version 0 in one
  transaction: it draws a `commit_seq` with operation `migrate` on `main`; creates on `main` every namespace, and
  every parent of one, in which a table exists on `main` at `LATEST`; on each other live branch, the namespaces of its
  tables that it does not see; and sets `writer_version` to `1`. A catalog created by revision 0.1 starts with
  `writer_version` `1` and no namespace.

## 19. Not in v0

- **Row lineage.** `data_file.first_row_id` has no meaning in v0: writers MAY leave it `NULL` and readers ignore
  it. Without it there is no projection to Iceberg format 3 and no native v3 deletion vector (§16).
- **Equality deletes**, in the catalog or from Iceberg writers: an inbound Iceberg delete file is refused.
- **Server-side scan planning** in the projection (`planTableScan`).
- **Inbound Iceberg updates** other than files (ICE-6): schema, spec, sort order and properties change through
  MetaLake commits only.
- **Merges** into a ref other than the parent, and merges that check rows or predicates rather than files and
  keys (§12).
- **`variant` bounds** (STA-9).
- **Catalog objects other than tables and namespaces**, such as views, functions and models; **renaming a
  namespace**, and dropping one together with its tables.
- **Table constraints**, **encryption**, **catalog databases other than Postgres**, and **transactions across
  catalogs**.
