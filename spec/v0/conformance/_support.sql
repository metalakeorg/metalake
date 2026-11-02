-- Assertions for the conformance cases. Not part of the reference.

-- Every case starts with namespace db on main (revision 0.1, NS-2), where the cases of version 0 put their tables.
insert into namespace values ('{db}', false, 1, 0);

create function expect(ok boolean, what text) returns void language plpgsql as $$
begin
  if ok is not true then raise exception 'expected: %', what; end if;
end $$;

-- `statement` must fail with `state`.
create function expect_error(statement text, state text, what text) returns void language plpgsql as $$
begin
  execute statement;
  raise exception 'expected % to fail with %, and it succeeded', what, state;
exception when others then
  if sqlstate <> state then
    raise exception 'expected % to fail with %, got %: %', what, state, sqlstate, sqlerrm;
  end if;
end $$;

-- The files of t visible at (r, s) are exactly `expected`, each once.
create function expect_files(r int, t uuid, s bigint, expected bigint[], what text) returns void language plpgsql as $$
declare got bigint[] := (select coalesce(array_agg(file_id order by file_id), '{}') from visible_files(r, t, s));
begin
  if got <> (select coalesce(array_agg(e order by e), '{}') from unnest(expected) e) then
    raise exception '%: expected files %, got %', what, expected, got;
  end if;
end $$;

-- Fixtures for cases that write rows directly. They act on the catalog's only table.

create function branch(r int, name text, b bigint, parent int, fork bigint) returns void language sql as $$
  insert into ref (ref_id, name, kind, created_seq) values (r, name, 'branch', b);
  insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (r, b, parent, fork);
$$;

-- A field version of the only table, live unless `e` is given.
create function fld(id int, name text, type text, r int, b bigint, parent int default null, pos int default null,
    req boolean default false, e bigint default 9223372036854775807, dropped boolean default false,
    init jsonb default null, wdef jsonb default null) returns void language sql as $$
  insert into field (table_id, field_id, parent_field_id, position, name, type, required, initial_default,
                     write_default, dropped, ref_id, begin_seq, end_seq)
  select table_id, id, parent, coalesce(pos, id), name, type, req, init, wdef, dropped, r, b, e from tbl;
$$;
create function names(r int, s bigint) returns text language sql as $$
  select string_agg(array_to_string(namespace || name, '.'), ' ' order by name) from visible_table_names(r, s);
$$;
create function columns(r int, s bigint) returns text language sql as $$
  select string_agg(field_id || ':' || name || ':' || type, ' ' order by field_id) from visible_fields(r, (select table_id from tbl), s);
$$;
create function errors(r int, s bigint) returns text[] language sql as $$
  select coalesce(array_agg(distinct split_part(e, ':', 1)), '{}') from table_errors(r, (select table_id from tbl), s) e;
$$;
