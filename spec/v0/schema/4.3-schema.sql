-- MetaLake v0 · reference DDL · schema, partitioning and ordering (§4.3).

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
