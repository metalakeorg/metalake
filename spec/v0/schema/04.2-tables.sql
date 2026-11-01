-- MetaLake v0 · reference DDL · tables (§4.2).

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
