-- MetaLake v0 · reference DDL · snapshots and writes (§4.4).

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
