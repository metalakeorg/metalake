-- MetaLake v0 · reference behaviour · visibility of data files (§5.1–§5.3).
--
-- It defines the right answer, not the fast one.

-- VIS-3: the chain of a read of r at s. `since` is the begin_seq of the ref_base row each level reads through,
-- 0 at a level without one (CMT-2).
create function chain(r int, s bigint) returns table (level int, ref_id int, seq bigint, since bigint)
language plpgsql stable as $$
declare
  seen int[] := '{}';
  parent int;
  fork bigint;
begin
  level := 0; ref_id := r; seq := s;
  loop
    if ref_id = any (seen) then
      raise exception 'metalake corrupt: the chain of ref % at % reaches itself', r, s using errcode = 'XX000';
    end if;
    seen := seen || ref_id;
    select b.parent_ref_id, b.fork_seq, b.begin_seq into parent, fork, since
      from ref_base b where b.ref_id = chain.ref_id and b.begin_seq <= chain.seq and chain.seq < b.end_seq;
    if not found then
      since := 0;
      return next;
      exit;
    end if;
    return next;
    ref_id := parent; seq := fork; level := level + 1;
  end loop;
end $$;

-- VIS-4 to VIS-6: the files of t visible at (r, s). The tombstones that apply are computed once, with the lowest
-- level each applies at; a row at level i is hidden by a tombstone at a level below i.
create function visible_files(r int, t uuid, s bigint) returns setof data_file language sql stable as $$
  with c as (select * from chain(r, s)),
  hidden as (
    select x.file_id, min(c.level) as level
      from tombstone x join c on x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     where x.table_id = t
     group by x.file_id)
  select f.*
    from c
    join data_file f on f.table_id = t and f.ref_id = c.ref_id and f.begin_seq <= c.seq and c.seq < f.end_seq
    left join hidden h on h.file_id = f.file_id and h.level < c.level
   where h.file_id is null;
$$;

-- VIS-6 for one file: the row through which file `fid` of t is visible at (r, s), if it is. It reads only that
-- file's rows, so a commit's checks do not grow with the table (CMT-9).
create function visible_file(r int, t uuid, s bigint, fid bigint) returns data_file language sql stable as $$
  with c as (select * from chain(r, s))
  select f.*
    from c join data_file f on f.table_id = t and f.file_id = fid and f.ref_id = c.ref_id
     and f.begin_seq <= c.seq and c.seq < f.end_seq
   where not exists (select 1 from c d join tombstone x on x.table_id = t and x.file_id = fid and x.ref_id = d.ref_id
                       and x.begin_seq <= d.seq and d.seq < x.end_seq where d.level < c.level);
$$;

create function file_visible(r int, t uuid, s bigint, fid bigint) returns boolean language sql stable as $$
  select (visible_file(r, t, s, fid)).file_id is not null;
$$;
