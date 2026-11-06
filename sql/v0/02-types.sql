-- MetaLake v0 · protocol · the records its calls carry (ADR 0003 §2.3).

-- A file a commit adds: a data_file row and, for an inline file, its bytes; a footer is optional (FTR-1).
create type @schema@.file_add as (
  table_id uuid,
  file_id bigint,
  path text,
  spec_id int,
  partition jsonb,
  sort_order_id int,
  record_count bigint,
  file_size bigint,
  footer_size int,
  first_row_id bigint,
  stat_field_ids int[],
  lower bytea[],
  upper bytea[],
  null_count bigint[],
  nan_count bigint[],
  value_count bigint[],
  content bytea,
  footer bytea);

-- A file a commit removes.
create type @schema@.file_ref as (
  table_id uuid,
  file_id bigint);

-- The deletion vector a commit sets on a file, replacing the one that began at base_seq (DV-4).
create type @schema@.vector_set as (
  table_id uuid,
  file_id bigint,
  base_seq bigint,
  path text,
  content_offset bigint,
  content_size bigint,
  cardinality bigint,
  inline bytea);

-- The snapshot a table must be at for the commit to apply (CMT-6).
create type @schema@.table_expect as (
  table_id uuid,
  snapshot_id bigint);

-- What a commit returns: its sequence, and whether it applied now or had already (CMT-5).
create type @schema@.commit_result as (
  commit_seq bigint,
  applied boolean);

-- A file a read opens (plan): where it is, what it holds, its inline bytes or stored footer, and the vector that
-- applies to it, if any (DV-1).
create type @schema@.planned_file as (
  file_id bigint,
  path text,
  spec_id int,
  partition jsonb,
  record_count bigint,
  file_size bigint,
  footer_size int,
  first_row_id bigint,
  content bytea,
  footer bytea,
  vector_begin_seq bigint,
  vector_path text,
  vector_offset bigint,
  vector_size bigint,
  vector_cardinality bigint,
  vector_inline bytea,
  begin_seq bigint);

-- A file as a list of files gives it (listed_files): what a read needs of it, and what a manifest does and a read does
-- not, its statistics and the sequence that added it.
create type @schema@.listed_file as (
  file_id bigint,
  begin_seq bigint,
  path text,
  spec_id int,
  partition jsonb,
  record_count bigint,
  file_size bigint,
  footer_size int,
  sort_order_id int,
  stat_field_ids int[],
  lower bytea[],
  upper bytea[],
  null_count bigint[],
  nan_count bigint[],
  value_count bigint[],
  content bytea,
  vector_begin_seq bigint,
  vector_path text,
  vector_offset bigint,
  vector_size bigint,
  vector_cardinality bigint,
  vector_inline bytea);
