-- MetaLake v0 · reference behaviour · time and changes (§14).
--
-- It defines the right answer, not the fast one.

-- TT-1: copies the commit timestamp into the commits that lack it. Returns how many it filled.
create function fill_committed_at() returns bigint language plpgsql as $$
declare n bigint;
begin
  update commit set committed_at = pg_xact_commit_timestamp(xid32(xid)) where committed_at is null;
  get diagnostics n = row_count;
  return n;
end $$;

-- TT-3: the sequence at which to read table t on ref r as of time `at`, or NULL if r did not exist then: a ref's
-- first ref_base row begins at its creation commit (BR-1), so before it no row is in force.
create function as_of_time(r int, t uuid, at timestamptz) returns bigint language plpgsql stable as $$
declare
  base ref_base;
  own bigint;
begin
  if at > now() - interval '1 second' then
    perform invalid(format('%s is less than one second ago', at));
  end if;
  select * into base from ref_base b
   where b.ref_id = r and committed_time(b.begin_seq) <= at
     and (b.end_seq = 9223372036854775807 or committed_time(b.end_seq) > at);
  own := (select max(s.snapshot_id) from snapshot s
           where s.table_id = t and s.ref_id = r and s.snapshot_id >= coalesce(base.begin_seq, 0)
             and committed_time(s.snapshot_id) <= at);
  return coalesce(own, base.begin_seq);
end $$;

-- CHG-1: up to `lim` commits after the cursor (after_at, after_seq), in catalog order, committed more than one
-- second ago.
create function changes(after_at timestamptz, after_seq bigint, lim int default 1000)
returns table (commit_seq bigint, ref_id int, committed_at timestamptz, operation text) language sql stable as $$
  select c.commit_seq, c.ref_id, x.at, c.operation
    from commit c cross join lateral (select committed_time(c.commit_seq) as at) x
   where (x.at, c.commit_seq) > (after_at, after_seq) and x.at < now() - interval '1 second'
   order by x.at, c.commit_seq
   limit lim;
$$;
