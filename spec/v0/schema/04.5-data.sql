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
-- STA-3 and STA-10: the bounds of the first statistics position, the first cluster key field when writers follow
-- STA-3, indexed as an int8range of their first 8 bytes read as an integer (halved, so that a closed range never
-- overflows). Bounds keep their order in those bytes (STA-5), so the range overlaps a predicate's whenever the bounds
-- do, and a query rechecks the bounds themselves. A range of bytea would need a distance GiST cannot take from bytea.
-- - A missing bound leaves its side of the range open.
-- - Bounds that are not ordered values (STA-9) are indexed as the range between their keys and never pruned (STA-10).
-- - A query on a field promoted from date also takes the files whose bounds still have 4 bytes (STA-7).
-- The function uses only built-ins: index expressions run with a safe search_path, and it stays inlinable.
create extension if not exists btree_gist schema public;
create function bound_range(lower bytea, upper bytea) returns int8range language sql immutable parallel safe as $$
  select case when ((('x' || rpad(encode(substring(upper for 8), 'hex'), 16, '0'))::bit(64)::bigint # (-9223372036854775808)::bigint) >> 1) < ((('x' || rpad(encode(substring(lower for 8), 'hex'), 16, '0'))::bit(64)::bigint # (-9223372036854775808)::bigint) >> 1)
              then int8range(((('x' || rpad(encode(substring(upper for 8), 'hex'), 16, '0'))::bit(64)::bigint # (-9223372036854775808)::bigint) >> 1), ((('x' || rpad(encode(substring(lower for 8), 'hex'), 16, '0'))::bit(64)::bigint # (-9223372036854775808)::bigint) >> 1), '[]')
              else int8range(((('x' || rpad(encode(substring(lower for 8), 'hex'), 16, '0'))::bit(64)::bigint # (-9223372036854775808)::bigint) >> 1), ((('x' || rpad(encode(substring(upper for 8), 'hex'), 16, '0'))::bit(64)::bigint # (-9223372036854775808)::bigint) >> 1), '[]') end;
$$;
create index data_file_bound on data_file using gist (table_id, ref_id, bound_range(lower[1], upper[1]));

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

-- §10 · deletion vectors: versioned by (table_id, file_id); one live row per key and ref (VIS-10); DV-3.
create table deletion_vector (
  table_id uuid not null references tbl,
  file_id bigint not null,
  path text,
  content_offset bigint,
  content_size bigint not null check (content_size > 0),
  cardinality bigint not null check (cardinality >= 0),
  inline bytea,
  ref_id int not null references ref,
  begin_seq bigint not null,
  end_seq bigint not null default 9223372036854775807,
  primary key (table_id, file_id, ref_id, begin_seq),
  check (begin_seq < end_seq),
  check ((path is not null and content_offset is not null and inline is null)
      or (path is null and content_offset is null and inline is not null and content_size = length(inline))));
create unique index deletion_vector_live on deletion_vector (table_id, file_id, ref_id)
  where end_seq = 9223372036854775807;

-- §11 · inline files and footers
create table inline_file (
  table_id uuid not null references tbl,
  file_id bigint not null,
  content bytea not null,
  primary key (table_id, file_id));

create table file_footer (
  table_id uuid not null references tbl,
  file_id bigint not null,
  footer bytea not null,
  primary key (table_id, file_id));
