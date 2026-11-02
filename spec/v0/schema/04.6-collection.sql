-- MetaLake v0 · reference DDL · revision 0.2: collection in two phases (§13).

-- RET-4, RET-9: objects nothing refers to any more, until the grace has passed and a sweeper deleted them.
create table deletion_queue (
  path text primary key,
  kind text not null check (kind in ('data', 'puffin')),
  expired_at timestamptz not null);
create index deletion_queue_due on deletion_queue (expired_at);

-- RET-3 (note): the closed rows of each ranged table, by ref and end, are the only candidates of expiring.
create index data_file_closed on data_file (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index tombstone_closed on tombstone (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index deletion_vector_closed on deletion_vector (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index ref_base_closed on ref_base (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index table_name_closed on table_name (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index table_property_closed on table_property (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index field_closed on field (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index partition_spec_closed on partition_spec (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index sort_order_closed on sort_order (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index cluster_key_closed on cluster_key (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index namespace_closed on namespace (ref_id, end_seq) where end_seq <> 9223372036854775807;
create index namespace_property_closed on namespace_property (ref_id, end_seq) where end_seq <> 9223372036854775807;

-- RET-4, RET-7, RET-8: a path's rows, to know whether anything still refers to it.
create index data_file_path on data_file (path);
create index deletion_vector_path on deletion_vector (path) where path is not null;
