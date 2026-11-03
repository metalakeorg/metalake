-- MetaLake v0 · reference behaviour · time and changes (§14).
--
-- It defines the right answer, not the fast one.

-- TT-1: copies the commit timestamp into up to `lim` commits that lack it, oldest first. Returns how many it filled.
create function fill_committed_at(lim int default null) returns bigint language plpgsql as $$
declare n bigint;
begin
  update commit c set committed_at = committed_time(c.commit_seq)
   where c.commit_seq in (select x.commit_seq from commit x where x.committed_at is null
                           order by x.commit_seq limit lim);
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
  -- TT-3 (0.2): only a retained read: from the ref's floor up (RET-2).
  if coalesce(own, base.begin_seq) < retention_floor(r) then
    perform invalid(format('TT-3: %s is older than what ref %s keeps', at, r));
  end if;
  return coalesce(own, base.begin_seq);
end $$;

-- CHG-1: up to `lim` commits after the cursor (after_at, after_seq), in catalog order, committed more than one
-- second ago; given a table, only those that made a snapshot of it.
create function changes(after_at timestamptz, after_seq bigint, lim int default 1000, tab uuid default null)
returns table (commit_seq bigint, ref_id int, committed_at timestamptz, operation text) language sql stable as $$
  select c.commit_seq, c.ref_id, x.at, c.operation
    from commit c cross join lateral (select committed_time(c.commit_seq) as at) x
   where (x.at, c.commit_seq) > (after_at, after_seq) and x.at < now() - interval '1 second'
     and (tab is null or exists (select 1 from snapshot s where s.table_id = tab and s.snapshot_id = c.commit_seq))
   order by x.at, c.commit_seq
   limit lim;
$$;

-- CHG-2 (0.2): the changes of table t on ref r between the retained reads (r, a) and (r, b), a < b: files added,
-- files removed, and files of both whose deletion vector differs, with the vector at each, as JSON.
create function table_changes(r int, t uuid, a bigint, b bigint)
returns table (change text, file_id bigint, path text, vector_before jsonb, vector_after jsonb)
language plpgsql stable as $$
declare
  f bigint := retention_floor(r);
begin
  if not exists (select 1 from ref x where x.ref_id = r and not x.deleted) or a >= b or a < f or b < f then
    perform invalid(format('CHG-2: (%s, %s) and (%s, %s) are not two retained reads in order', r, a, r, b));
  end if;
  return query
    with va as (select * from visible_files(r, t, a)), vb as (select * from visible_files(r, t, b)),
    dv as (select v.file_id,
                  (select to_jsonb(d) - 'ref_id' - 'begin_seq' - 'end_seq' from file_dv(r, t, a, v.file_id) d
                    where d.file_id is not null) as before,
                  (select to_jsonb(d) - 'ref_id' - 'begin_seq' - 'end_seq' from file_dv(r, t, b, v.file_id) d
                    where d.file_id is not null) as after
             from vb v where v.file_id in (select y.file_id from va y))
    select 'added', v.file_id, v.path, null::jsonb, null::jsonb from vb v
     where v.file_id not in (select y.file_id from va y)
    union all
    select 'removed', v.file_id, v.path, null, null from va v
     where v.file_id not in (select y.file_id from vb y)
    union all
    select 'vector', d.file_id, (select v.path from vb v where v.file_id = d.file_id), d.before, d.after from dv d
     where d.before is distinct from d.after;
end $$;
