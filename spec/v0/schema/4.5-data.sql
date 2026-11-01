-- MetaLake v0 · reference DDL · data (§4.5).

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
