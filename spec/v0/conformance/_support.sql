-- Assertions for the conformance cases. Not part of the reference.

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
