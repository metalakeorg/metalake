-- MetaLake v0 · reference DDL · refs and commits (§4.1).

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
