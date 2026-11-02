-- S12 · the statements a merge of a branch that only adds and removes files requires (MRG-1 to MRG-4 for files),
-- set based. The reference merge() defines the right answer; this is the shape of a fast one, checked against it.
create function bench_merge(b int, t uuid) returns bigint language plpgsql as $$
declare
  open constant bigint := 9223372036854775807;
  latest constant bigint := 9223372036854775806;
  p int;
  f bigint;
  m bigint;
begin
  select parent_ref_id, fork_seq into p, f from ref_base where ref_id = b and end_seq = open;
  perform 1 from ref where ref_id = p for update;
  perform 1 from ref where ref_id = b for update;
  perform 1 from head where ref_id = p and table_id = t for update;
  -- MRG-3: a file b removed is still visible on p, with the vector it had at the fork.
  if exists (select 1 from tombstone x where x.table_id = t and x.ref_id = b and x.end_seq = open
              and (not file_visible(p, t, latest, x.file_id)
                   or (file_dv(p, t, f, x.file_id)).begin_seq is distinct from (file_dv(p, t, latest, x.file_id)).begin_seq)) then
    perform conflict('a file the branch removed changed on its parent');
  end if;
  m := nextval('commit_seq_gen');
  insert into commit values (m, p, pg_current_xact_id(), null, 'bench', 'bench', 'merge', null, null, null, null);
  -- MRG-4: b's tombstones close p's rows or become p's tombstones; b's files are copied to p.
  create temp table removed on commit drop as
    with x as (update tombstone set end_seq = m where table_id = t and ref_id = b and end_seq = open returning file_id)
    select file_id from x;
  with closed as (update data_file d set end_seq = m from removed x
                   where d.table_id = t and d.file_id = x.file_id and d.ref_id = p and d.end_seq = open returning d.file_id)
  insert into tombstone (table_id, file_id, ref_id, begin_seq)
  select t, x.file_id, p, m from removed x where x.file_id not in (select file_id from closed);
  create temp table moved on commit drop as
    with x as (update data_file set end_seq = m where table_id = t and ref_id = b and end_seq = open returning *)
    select * from x;
  insert into data_file
  select table_id, file_id, p, m, open, path, format, spec_id, partition, sort_order_id, record_count, file_size,
         footer_size, first_row_id, stat_field_ids, lower, upper, null_count, nan_count, value_count
    from moved;
  insert into snapshot
  select t, m, p, h.snapshot_id, s.schema_seq, s.spec_id, s.sort_order_id,
         jsonb_build_object('operation', 'merge', 'added-data-files', (select count(*) from moved),
           'deleted-data-files', (select count(*) from removed),
           'added-records', (select coalesce(sum(record_count), 0) from moved), 'deleted-records', 0)
    from head h join snapshot s on s.table_id = h.table_id and s.snapshot_id = h.snapshot_id
   where h.ref_id = p and h.table_id = t;
  update head set snapshot_id = m where ref_id = p and table_id = t;
  update ref_base set end_seq = m where ref_id = b and end_seq = open;
  insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (b, m, p, m);
  return m;
end $$;
