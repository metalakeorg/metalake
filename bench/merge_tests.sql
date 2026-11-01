-- MetaLake · preliminary study 1.2: the merge matrix, scenario by scenario. On top of `schema.sql` + `merge.sql`,
-- in an empty database. Each scenario uses its own table and its own branch; if anything is not as expected, it aborts.
\set ON_ERROR_STOP 1
set client_min_messages = warning;

insert into ref values (1, 'main', null, null);

-- expect_visible(<ref>, <table>, <expected>): the live set is exactly that.
create or replace function expect_visible(r int, t int, expected bigint[], what text) returns void language plpgsql as $$
declare v bigint[] := (select coalesce(array_agg(x order by x), '{}') from visible(r, t, (select last_value from seq)) x);
begin
  if v <> (select coalesce(array_agg(x order by x), '{}') from unnest(expected) x) then
    raise exception '% · ref % table %: sees % and expected %', what, r, t, v, expected;
  end if;
end $$;

create or replace function expect_conflicts(b int, expected text, what text) returns void language plpgsql as $$
declare c text := (select string_agg(distinct x.what, ' · ') from conflicts(b) x);
begin
  if c is distinct from expected then raise exception '% · conflicts: % (expected %)', what, c, expected; end if;
end $$;

-- The base of each scenario: table t with files t*100+1 .. t*100+9 on main.
create or replace function base(t int) returns void language sql as $$
  select op_add(t, 1, array(select t * 100 + g from generate_series(1, 9) g)::bigint[]);
$$;
create or replace function b(t int) returns bigint[] language sql as $$
  select array(select t * 100 + g from generate_series(1, 9) g)::bigint[];
$$;

do $$
declare r int; c int; n int;
begin
  -- E1 · append and append: both get in.
  perform base(1); r := branch('e1', 1);
  perform op_add(1, r, '{1001}'); perform op_add(1, 1, '{1002}');
  perform expect_conflicts(r, null, 'E1');
  if merge_branch(r) <> 0 then raise exception 'E1 does not merge'; end if;
  perform expect_visible(1, 1, b(1) || '{1001,1002}', 'E1 main');
  perform expect_visible(r, 1, b(1) || '{1001,1002}', 'E1 the branch after merging');
  -- E10 · merge again: only what is new, the base moved forward.
  perform op_add(1, r, '{1003}');
  perform expect_conflicts(r, null, 'E10');
  if merge_branch(r) <> 0 then raise exception 'E10 does not merge'; end if;
  perform expect_visible(1, 1, b(1) || '{1001,1002,1003}', 'E10 main');

  -- E2 · the branch removes a file, main appends: no conflict (structural merge, per file).
  perform base(2); r := branch('e2', 1);
  perform op_remove(2, r, '{202}'); perform op_add(2, 1, '{2002}');
  perform expect_conflicts(r, null, 'E2');
  perform merge_branch(r);
  perform expect_visible(1, 2, array_remove(b(2), 202) || '{2002}', 'E2 main');

  -- E3 · both remove the same file: conflict, and main does not change.
  perform base(3); r := branch('e3', 1);
  perform op_remove(3, r, '{303}'); perform op_remove(3, 1, '{303}');
  perform expect_conflicts(r, 'removed in both', 'E3');
  if merge_branch(r) = 0 then raise exception 'E3 merged a conflict'; end if;
  perform expect_visible(1, 3, array_remove(b(3), 303), 'E3 main untouched');

  -- E4 · main compacts what the branch inherited without touching it; the branch appends: no conflict.
  perform base(4); r := branch('e4', 1);
  perform op_compact(4, 1, '{404,405}', 4005); perform op_add(4, r, '{4001}');
  perform expect_conflicts(r, null, 'E4');
  perform expect_visible(r, 4, b(4) || '{4001}', 'E4 the branch still sees what main compacted');
  perform merge_branch(r);
  perform expect_visible(1, 4, array_remove(array_remove(b(4), 404), 405) || '{4005,4001}', 'E4 main');

  -- E5 · both compact a common file: conflict.
  perform base(5); r := branch('e5', 1);
  perform op_compact(5, r, '{506,507}', 5001); perform op_compact(5, 1, '{507,508}', 5002);
  perform expect_conflicts(r, 'removed in both', 'E5');

  -- E6 · both delete rows of the same file: resolves itself, with the union of the deletes.
  perform base(6); r := branch('e6', 1);
  perform op_dv(6, r, 609, '{1,2}'); perform op_dv(6, 1, 609, '{3}');
  perform expect_conflicts(r, null, 'E6');
  perform merge_branch(r);
  if dv_of(1, 6, 609, (select last_value from seq)) <> '{1,2,3}' then
    raise exception 'E6: DV %', dv_of(1, 6, 609, (select last_value from seq));
  end if;

  -- E7 · the branch deletes rows of a file that main compacted: conflict.
  perform base(7); r := branch('e7', 1);
  perform op_dv(7, r, 701, '{5}'); perform op_compact(7, 1, '{701,702}', 7001);
  perform expect_conflicts(r, 'rows deleted in a file the other removed', 'E7');

  -- E8 · the branch compacts a file whose rows main deleted: conflict.
  perform base(8); r := branch('e8', 1);
  perform op_compact(8, r, '{801,802}', 8001); perform op_dv(8, 1, 801, '{5}');
  perform expect_conflicts(r, 'removed a file in which the other deleted rows', 'E8');

  -- E9 · fast-forward: main did not change.
  perform base(9); r := branch('e9', 1);
  perform op_add(9, r, '{9001}'); perform op_remove(9, r, '{901}');
  perform merge_branch(r);
  perform expect_visible(1, 9, array_remove(b(9), 901) || '{9001}', 'E9 main');

  -- E11 · nested: c child of b child of main; c into b, then b into main.
  perform base(11); r := branch('e11b', 1);
  perform op_add(11, r, '{11001}');
  c := branch('e11c', r);
  perform op_add(11, c, '{11002}'); perform op_remove(11, c, '{1101}');
  perform expect_visible(r, 11, b(11) || '{11001}', 'E11 b does not see what is in c');
  perform merge_branch(c);
  perform expect_visible(r, 11, array_remove(b(11), 1101) || '{11001,11002}', 'E11 b after merging c');
  perform expect_visible(1, 11, b(11), 'E11 main not yet');
  perform merge_branch(r);
  perform expect_visible(1, 11, array_remove(b(11), 1101) || '{11001,11002}', 'E11 main');

  -- E12 · several tables: if one conflicts, none gets in.
  perform base(12); perform base(13); r := branch('e12', 1);
  perform op_add(12, r, '{12001}'); perform op_remove(13, r, '{1303}'); perform op_remove(13, 1, '{1303}');
  if merge_branch(r) = 0 then raise exception 'E12 merged with a conflict'; end if;
  perform expect_visible(1, 12, b(12), 'E12 the table without conflict did not change either');

  raise warning 'merge: the 12 scenarios, OK';
end $$;
