-- MetaLake v0 · reference behaviour.
--
-- It defines the right answer, not the fast one. Loaded after schema.sql, in the catalog's schema.

-- LAY-3: refuse a catalog of an unknown version.
create function open_catalog() returns uuid language plpgsql stable as $$
declare v text := (select value from meta where key = 'format_version');
begin
  if v is distinct from '0' then
    raise exception 'metalake unsupported: format_version %', coalesce(v, 'missing') using errcode = '0A000';
  end if;
  return (select value::uuid from meta where key = 'catalog_uuid');
end $$;

-- ID-3: a new table identity and its counters.
create function new_table(location text, created_seq bigint) returns uuid language sql as $$
  with t as (insert into tbl values (gen_random_uuid(), location, created_seq) returning table_id)
  insert into table_counter (table_id) select table_id from t returning table_id;
$$;

-- ID-5: the next field, spec or sort order id of a table.
create function next_id(t uuid, kind text) returns int language plpgsql as $$
declare id int;
begin
  if kind not in ('field', 'spec', 'sort_order') then
    raise exception 'metalake: unknown id kind %', kind;
  end if;
  update table_counter set
      last_field_id = last_field_id + (kind = 'field')::int,
      last_spec_id = last_spec_id + (kind = 'spec')::int,
      last_sort_order_id = last_sort_order_id + (kind = 'sort_order')::int
    where table_id = t
    returning case kind when 'field' then last_field_id when 'spec' then last_spec_id else last_sort_order_id end
    into id;
  if id is null then
    raise exception 'metalake: unknown table %', t;
  end if;
  return id;
end $$;

-- VIS-3: the chain of a read of r at s.
create function chain(r int, s bigint) returns table (level int, ref_id int, seq bigint) language plpgsql stable as $$
declare seen int[] := '{}';
begin
  level := 0; ref_id := r; seq := s;
  loop
    if ref_id = any (seen) then
      raise exception 'metalake corrupt: the chain of ref % at % reaches itself', r, s using errcode = 'XX000';
    end if;
    seen := seen || ref_id;
    return next;
    select b.parent_ref_id, b.fork_seq into ref_id, seq
      from ref_base b where b.ref_id = chain.ref_id and b.begin_seq <= chain.seq and chain.seq < b.end_seq;
    exit when not found;
    level := level + 1;
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
