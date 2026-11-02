-- MetaLake v0 · protocol · time and changes (§14 of the text).
--
-- A commit's time is committed_at once the filler has copied it, and its transaction's commit timestamp until then
-- (TT-1). The feed reads the filled commits through an index in catalog order, from the cursor on, and the commits
-- not yet filled through one of their own: the filler keeps those few.

create index if not exists commit_by_time on @schema@.commit (committed_at, commit_seq) where committed_at is not null;
create index if not exists commit_unfilled on @schema@.commit (commit_seq) where committed_at is null;

-- TT-1: copies the commit timestamp into the commits that lack it. Returns how many it filled.
create function @schema@.fill_committed_at() returns bigint language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
declare n bigint;
begin
  update commit set committed_at = pg_xact_commit_timestamp(xid32(xid)) where committed_at is null;
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
  return coalesce(own, base.begin_seq);
end $$;

-- CHG-1: up to `lim` commits after the cursor (after_at, after_seq), in catalog order, committed more than one
-- second ago. LANGUAGE sql without SET and qualified throughout, like plan, so that the caller's query inlines it.
create function @schema@.changes(after_at timestamptz, after_seq bigint, lim int default 1000)
returns table (commit_seq bigint, ref_id int, committed_at timestamptz, operation text) language sql stable as $$
  select x.commit_seq, x.ref_id, x.at, x.operation
    from ((select c.commit_seq, c.ref_id, c.committed_at as at, c.operation
             from @schema@.commit c
            where c.committed_at is not null and (c.committed_at, c.commit_seq) > (after_at, after_seq)
              and c.committed_at < now() - interval '1 second'
            order by c.committed_at, c.commit_seq
            limit lim)
          union all
          select c.commit_seq, c.ref_id, y.at, c.operation
            from @schema@.commit c
           cross join lateral (select pg_xact_commit_timestamp(@schema@.xid32(c.xid)) as at) y
           where c.committed_at is null and (y.at, c.commit_seq) > (after_at, after_seq)
             and y.at < now() - interval '1 second') x
   order by x.at, x.commit_seq
   limit lim;
$$;
