-- MetaLake v0 · protocol · time and changes (§14 of the text).
--
-- A commit's time is committed_at once the filler has copied it, and its transaction's commit timestamp until then
-- (TT-1). The feed reads the filled commits through an index in catalog order, from the cursor on, and the commits
-- not yet filled through one of their own: the filler keeps those few.

create index if not exists commit_by_time on @schema@.commit (committed_at, commit_seq) where committed_at is not null;
create index if not exists commit_unfilled on @schema@.commit (commit_seq) where committed_at is null;
-- A commit's vectors (CHG-3), as data_file_ref and tombstone_ref find its files.
create index if not exists deletion_vector_ref on @schema@.deletion_vector (table_id, ref_id, begin_seq);

-- TT-1: copies the commit timestamp into up to `lim` commits that lack it, oldest first, through the index of those
-- not yet filled; a commit whose timestamp is gone is refused (committed_time). Returns how many it filled.
create function @schema@.fill_committed_at(lim int default null) returns bigint language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
declare n bigint;
begin
  update commit c set committed_at = committed_time(c.commit_seq)
   where c.commit_seq in (select x.commit_seq from commit x where x.committed_at is null
                           order by x.commit_seq limit lim);
  get diagnostics n = row_count;
  return n;
end $$;

-- TT-3: the sequence at which to read table t on ref r as of time `at`, or NULL if r did not exist then. Within one
-- (ref, table) commit order and catalog order agree (CMT-4), so the snapshot is the first, newest first, committed
-- by then.
create function @schema@.as_of_time(r int, t uuid, at timestamptz) returns bigint language plpgsql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  base ref_base;
  own bigint;
begin
  if at > now() - interval '1 second' then
    perform refuse_invalid('TT-3', format('%s is less than one second ago', at));
  end if;
  select * into base from ref_base b
   where b.ref_id = r and committed_time(b.begin_seq) <= at
     and (b.end_seq = 9223372036854775807 or committed_time(b.end_seq) > at);
  own := (select s.snapshot_id from snapshot s
           where s.table_id = t and s.ref_id = r and s.snapshot_id >= coalesce(base.begin_seq, 0)
             and committed_time(s.snapshot_id) <= at
           order by s.snapshot_id desc limit 1);
  -- TT-3 (0.2): only a retained read: from the ref's floor up (RET-2).
  if coalesce(own, base.begin_seq) < retention_floor(r) then
    perform refuse_invalid('TT-3', format('%s is older than what ref %s keeps', at, r));
  end if;
  return coalesce(own, base.begin_seq);
end $$;

-- CHG-1: up to `lim` commits after the cursor (after_at, after_seq), in catalog order, committed more than one
-- second ago; given a table, only those that made a snapshot of it. A commit without a time is refused (TT-1).
-- LANGUAGE sql without SET and qualified throughout, like plan, so that the caller's query inlines it.
create function @schema@.changes(after_at timestamptz, after_seq bigint, lim int default 1000, tab uuid default null)
returns table (commit_seq bigint, ref_id int, committed_at timestamptz, operation text) language sql stable as $$
  select x.commit_seq, x.ref_id, x.at, x.operation
    from ((select c.commit_seq, c.ref_id, c.committed_at as at, c.operation
             from @schema@.commit c
            where c.committed_at is not null and (c.committed_at, c.commit_seq) > (after_at, after_seq)
              and c.committed_at < now() - interval '1 second'
              and (tab is null or exists (select 1 from @schema@.snapshot s
                                           where s.table_id = tab and s.snapshot_id = c.commit_seq))
            order by c.committed_at, c.commit_seq
            limit lim)
          union all
          select c.commit_seq, c.ref_id, y.at, c.operation
            from @schema@.commit c
           cross join lateral (select coalesce(pg_xact_commit_timestamp(@schema@.xid32(c.xid)),
                                               @schema@.no_time(c.commit_seq, c.xid)) as at) y
           where c.committed_at is null and (y.at, c.commit_seq) > (after_at, after_seq)
             and y.at < now() - interval '1 second'
             and (tab is null or exists (select 1 from @schema@.snapshot s
                                          where s.table_id = tab and s.snapshot_id = c.commit_seq))) x
   order by x.at, x.commit_seq
   limit lim;
$$;

-- CHG-2: the changes of table t on ref r between the retained reads (r, a) and (r, b): the difference of two plans by
-- file, and the files of both whose vector differs in content, with the vector at each as the reference gives it.
create function @schema@.table_changes(r int, t uuid, a bigint, b bigint)
returns table (change text, file_id bigint, path text, vector_before jsonb, vector_after jsonb) language plpgsql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  f bigint := retention_floor(r);
begin
  if not exists (select 1 from ref x where x.ref_id = r and not x.deleted) or a >= b or a < f or b < f then
    perform refuse_invalid('CHG-2', format('(%s, %s) and (%s, %s) are not two retained reads in order', r, a, r, b));
  end if;
  return query
    with va as materialized (select * from plan(r, t, a)), vb as materialized (select * from plan(r, t, b))
    select 'added'::text, y.file_id, y.path, null::jsonb, null::jsonb from vb y
     where not exists (select 1 from va x where x.file_id = y.file_id)
    union all
    select 'removed', x.file_id, x.path, null, null from va x
     where not exists (select 1 from vb y where y.file_id = x.file_id)
    union all
    select 'vector', y.file_id, y.path,
           (select to_jsonb(d) - 'ref_id' - 'begin_seq' - 'end_seq' from file_dv(r, t, a, y.file_id) d
             where d.file_id is not null),
           (select to_jsonb(d) - 'ref_id' - 'begin_seq' - 'end_seq' from file_dv(r, t, b, y.file_id) d
             where d.file_id is not null)
      from vb y join va x on x.file_id = y.file_id
     where (x.vector_path, x.vector_offset, x.vector_size, x.vector_cardinality, x.vector_inline)
           is distinct from (y.vector_path, y.vector_offset, y.vector_size, y.vector_cardinality, y.vector_inline);
end $$;

-- CHG-3: the changes of commit s to table t on ref r. It looks only at the files whose rows of r opened or closed at s,
-- each one against both reads, so that it costs what the commit touched. A ref whose base changed at s has no such rows
-- for what it inherits: there, every file of both reads is looked at. The closed rows are asked as `<> open` too, so that
-- a plan with s as a parameter can use their partial indexes.
create function @schema@.commit_changes(r int, t uuid, s bigint)
returns table (change text, file_id bigint, path text, spec_id int, partition jsonb, record_count bigint,
               file_size bigint, footer_size int, first_row_id bigint, content bytea, footer bytea,
               vector_before jsonb, vector_after jsonb)
language plpgsql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  open constant bigint := 9223372036854775807;
  rebased boolean := exists (select 1 from ref_base x where x.ref_id = r and x.begin_seq = s);
begin
  if not exists (select 1 from ref x where x.ref_id = r and not x.deleted) or s - 1 < retention_floor(r) then
    perform refuse_invalid('CHG-3', format('(%s, %s) and (%s, %s) are not two retained reads', r, s - 1, r, s));
  end if;
  return query
    with touched (id) as (
      select x.file_id from data_file x where x.table_id = t and x.ref_id = r and x.begin_seq = s
      union select x.file_id from data_file x where x.table_id = t and x.ref_id = r and x.end_seq = s and x.end_seq <> open
      union select x.file_id from tombstone x where x.table_id = t and x.ref_id = r and x.begin_seq = s
      union select x.file_id from tombstone x where x.table_id = t and x.ref_id = r and x.end_seq = s and x.end_seq <> open
      union select x.file_id from deletion_vector x where x.table_id = t and x.ref_id = r and x.begin_seq = s
      union select x.file_id from deletion_vector x where x.table_id = t and x.ref_id = r and x.end_seq = s and x.end_seq <> open
      union select x.file_id from plan(r, t, s - 1) x where rebased
      union select x.file_id from plan(r, t, s) x where rebased),
    reads as (
      select u.id, a as before, b as after,
             (select to_jsonb(d) - 'ref_id' - 'begin_seq' - 'end_seq' from file_dv(r, t, s - 1, u.id) d
               where d.file_id is not null) as vb,
             (select to_jsonb(d) - 'ref_id' - 'begin_seq' - 'end_seq' from file_dv(r, t, s, u.id) d
               where d.file_id is not null) as va
        from touched u
       cross join lateral visible_file(r, t, s - 1, u.id) a
       cross join lateral visible_file(r, t, s, u.id) b),
    changed as (
      select y.id, (y.before).file_id is null as added, (y.after).file_id is null as removed,
             case when (y.after).file_id is null then y.before else y.after end as f, y.vb, y.va
        from reads y
       where (y.before).file_id is not null or (y.after).file_id is not null)
    select case when v.added then 'added' when v.removed then 'removed' else 'vector' end, v.id, (v.f).path,
           (v.f).spec_id, (v.f).partition, (v.f).record_count, (v.f).file_size, (v.f).footer_size, (v.f).first_row_id,
           (select i.content from inline_file i where i.table_id = t and i.file_id = v.id),
           (select x.footer from file_footer x where x.table_id = t and x.file_id = v.id),
           v.vb, v.va
      from changed v
     where v.added or v.removed or v.vb is distinct from v.va;
end $$;
