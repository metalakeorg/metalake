-- MetaLake · preliminary study 2.6: garbage collection with branches, scenario by scenario. Empty database, after
-- schema.sql, merge.sql and maintenance.sql.
\set ON_ERROR_STOP 1
set client_min_messages = warning;
insert into ref values (1, 'main', null, null);

create or replace function collects(f bigint) returns boolean language sql as $$
  select exists (select 1 from collectable() x where x.id = f);
$$;
create or replace function set_horizon(r int) returns void language sql as $$
  insert into retention values (r, (select last_value from seq))
  on conflict (ref) do update set horizon = excluded.horizon;
$$;
create or replace function expect_collect(f bigint, expected boolean, what text) returns void language plpgsql as $$
begin
  if collects(f) <> expected then raise exception '% · file %: collectable=% (expected %)', what, f, collects(f), expected; end if;
end $$;

do $$
declare b int; c int;
begin
  -- G5 · main closes a file BEFORE forking: once past its horizon, it is collected (the branch never saw it).
  perform op_add(1, 1, '{11,12,13}');
  perform op_compact(1, 1, '{11}', 14);
  b := branch('b', 1);
  perform expect_collect(11, false, 'G5 before the horizon');
  perform set_horizon(1);
  perform expect_collect(11, true, 'G5 after the horizon');

  -- G1 · main compacts 12 AFTER forking b: even if main passes its horizon, b sees it; it is not collected.
  perform op_compact(1, 1, '{12}', 15);
  perform set_horizon(1);
  perform expect_collect(12, false, 'G1 b inherits it');
  if 12 not in (select visible(b, 1, (select last_value from seq))) then raise exception 'G1: b does not see 12'; end if;

  -- G4 · b covers 13 with a tombstone: main still sees it; it is not collected.
  perform op_remove(1, b, '{13}');
  perform expect_collect(13, false, 'G4 main sees it');

  -- G3 · b adds 21 and removes it: it is its own; collected once past b's horizon, not before.
  perform op_add(1, b, '{21}'); perform op_remove(1, b, '{21}');
  perform expect_collect(21, false, 'G3 before the horizon of b');
  perform set_horizon(b);
  perform expect_collect(21, true, 'G3 after the horizon of b');

  -- G6 · c, child of b: deleting b leaves it hidden (c reads through it); 12 is still not collected. Delete c too: collected.
  c := branch('c', b);
  update ref set deleted = true where id = b;
  perform expect_collect(12, false, 'G6 b deleted but c alive');
  if 12 not in (select visible(c, 1, (select last_value from seq))) then raise exception 'G6: c does not see 12'; end if;
  update ref set deleted = true where id = c;
  perform expect_collect(12, true, 'G6 without live descendants');

  -- G2 · with b (and c) deleted, b's own files are collected too.
  perform op_add(1, b, '{22}');
  perform expect_collect(22, true, 'G2 what belongs to a deleted branch');

  raise warning 'maintenance: G1–G6, OK';
end $$;
