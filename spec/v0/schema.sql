-- MetaLake v0 · reference DDL.
--
-- Run inside an empty schema: the schema becomes one catalog (LAY-1). 9223372036854775807 is OPEN (LAY-4).

create table meta (
  key text primary key,
  value text not null);

create sequence commit_seq_gen as bigint start 1 no cycle;
create sequence ref_id_gen as int start 2 no cycle;
create sequence file_id_gen as bigint start 1 no cycle;

-- §4.1 · refs and commits

create table ref (
  ref_id int primary key,
  name text not null unique,
  kind text not null check (kind in ('branch', 'tag')),
  created_seq bigint not null,
  protected boolean not null default false,
  deleted boolean not null default false);

create table ref_base (
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  parent_ref_id int not null references ref,
  fork_seq bigint not null,
  primary key (ref_id, begin_seq),
  check (begin_seq < end_seq),
  check (fork_seq <= begin_seq),
  check (parent_ref_id <> ref_id));
create unique index ref_base_live on ref_base (ref_id) where end_seq = 9223372036854775807;

create table commit (
  commit_seq bigint primary key,
  ref_id int not null references ref,
  xid xid8 not null,
  committed_at timestamptz,
  author text not null,
  actor text not null,
  operation text not null,
  app_id text,
  app_version bigint,
  definition text,
  message text);

-- §4.2 · tables

create table tbl (
  table_id uuid primary key,
  location text not null,
  created_seq bigint not null);

create table table_counter (
  table_id uuid primary key references tbl,
  last_field_id int not null default 0,
  last_spec_id int not null default -1,
  last_partition_field_id int not null default 999,
  last_sort_order_id int not null default 0);

-- Versioned tables (§5.4): the key, then ref_id, begin_seq and end_seq; one live row per key and ref (VIS-10).

create table table_name (
  table_id uuid not null references tbl,
  namespace text[] not null,
  name text not null,
  dropped boolean not null default false,
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  primary key (table_id, ref_id, begin_seq),
  check (begin_seq < end_seq));
create unique index table_name_live on table_name (table_id, ref_id) where end_seq = 9223372036854775807;
create index table_name_by_name on table_name (namespace, name);

create table table_property (
  table_id uuid not null references tbl,
  key text not null,
  value text,
  dropped boolean not null default false,
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  primary key (table_id, key, ref_id, begin_seq),
  check (begin_seq < end_seq));
create unique index table_property_live on table_property (table_id, key, ref_id)
  where end_seq = 9223372036854775807;

-- §4.3 · schema, partitioning and ordering

create table field (
  table_id uuid not null references tbl,
  field_id int not null,
  parent_field_id int,
  position int not null,
  name text not null,
  type text not null,
  required boolean not null,
  initial_default jsonb,
  write_default jsonb,
  doc text,
  dropped boolean not null default false,
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  primary key (table_id, field_id, ref_id, begin_seq),
  check (begin_seq < end_seq));
create unique index field_live on field (table_id, field_id, ref_id) where end_seq = 9223372036854775807;
create index field_ref on field (table_id, ref_id, begin_seq);

create table partition_spec (
  table_id uuid not null references tbl,
  spec_id int not null,
  fields jsonb not null,
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  primary key (table_id, ref_id, begin_seq),
  check (begin_seq < end_seq));
create unique index partition_spec_live on partition_spec (table_id, ref_id) where end_seq = 9223372036854775807;
create index partition_spec_id on partition_spec (table_id, spec_id);

create table sort_order (
  table_id uuid not null references tbl,
  sort_order_id int not null check (sort_order_id > 0),
  fields jsonb not null,
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  primary key (table_id, ref_id, begin_seq),
  check (begin_seq < end_seq));
create unique index sort_order_live on sort_order (table_id, ref_id) where end_seq = 9223372036854775807;
create index sort_order_id on sort_order (table_id, sort_order_id);

create table cluster_key (
  table_id uuid not null references tbl,
  field_ids int[] not null,
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  primary key (table_id, ref_id, begin_seq),
  check (begin_seq < end_seq));
create unique index cluster_key_live on cluster_key (table_id, ref_id) where end_seq = 9223372036854775807;

-- §4.4 · snapshots and writes

create table snapshot (
  table_id uuid not null references tbl,
  snapshot_id bigint not null,
  ref_id int not null references ref,
  parent_snapshot_id bigint,
  schema_seq bigint not null,
  spec_id int not null,
  sort_order_id int not null,
  summary jsonb not null,
  primary key (table_id, snapshot_id));
create index snapshot_ref on snapshot (table_id, ref_id, snapshot_id);

create table head (
  ref_id int not null references ref,
  table_id uuid not null references tbl,
  snapshot_id bigint not null,
  primary key (ref_id, table_id));

create table idempotency (
  ref_id int not null references ref,
  app_id text not null,
  version bigint not null,
  commit_seq bigint not null,
  primary key (ref_id, app_id));

create table intent (
  path text primary key,
  table_id uuid not null,
  ref_id int not null,
  expires_at timestamptz not null);

-- §4.5 · data

create table data_file (
  table_id uuid not null references tbl,
  file_id bigint not null,
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  path text,
  format text not null default 'parquet' check (format = 'parquet'),
  spec_id int not null,
  partition jsonb not null,
  sort_order_id int,
  record_count bigint not null check (record_count >= 0),
  file_size bigint not null check (file_size >= 0),
  footer_size int not null check (footer_size >= 0),
  first_row_id bigint,
  stat_field_ids int[] not null default '{}',
  lower bytea[] not null default '{}',
  upper bytea[] not null default '{}',
  null_count bigint[] not null default '{}',
  nan_count bigint[] not null default '{}',
  value_count bigint[],
  primary key (table_id, file_id, begin_seq),
  check (begin_seq < end_seq),
  check (jsonb_typeof(partition) = 'array'),
  -- STA-2: parallel statistic arrays.
  check (cardinality(lower) = cardinality(stat_field_ids) and cardinality(upper) = cardinality(stat_field_ids)
     and cardinality(null_count) = cardinality(stat_field_ids) and cardinality(nan_count) = cardinality(stat_field_ids)
     and coalesce(cardinality(value_count) = cardinality(stat_field_ids), true)));
-- VIS-7: one live row per file.
create unique index data_file_live on data_file (table_id, file_id) where end_seq = 9223372036854775807;
create index data_file_ref on data_file (table_id, ref_id, begin_seq);
-- STA-3: bounds of the first statistics position, the first cluster key field when writers follow STA-3.
create index data_file_first_bound on data_file (table_id, ref_id, (lower[1]), (upper[1]));

create table tombstone (
  table_id uuid not null references tbl,
  file_id bigint not null,
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  primary key (table_id, file_id, ref_id, begin_seq),
  check (begin_seq < end_seq));
create unique index tombstone_live on tombstone (table_id, file_id, ref_id) where end_seq = 9223372036854775807;
create index tombstone_ref on tombstone (table_id, ref_id, begin_seq);

-- The catalog starts with its identity and `main` (LAY-2, ID-2).
insert into meta values
  ('format_version', '0'),
  ('catalog_uuid', gen_random_uuid()::text),
  ('created_at', to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
insert into ref (ref_id, name, kind, created_seq) values (1, 'main', 'branch', 0);
