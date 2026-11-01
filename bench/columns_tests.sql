-- MetaLake · preliminary study 2.1: the schema with branches, scenario by scenario. Empty database, after schema.sql,
-- merge.sql and columns.sql. Each scenario uses its own table and its own branch; if anything is not as expected, it aborts.
\set ON_ERROR_STOP 1
set client_min_messages = warning;
insert into ref values (1, 'main', null, null);

create or replace function cols(r int, t int) returns text language sql as $$
  select string_agg(name || ':' || type, ',' order by field_id) from schema_of(r, t, (select last_value from seq));
$$;
create or replace function expect(got text, expected text, what text) returns void language plpgsql as $$
begin
  if got is distinct from expected then raise exception '% · % (expected %)', what, got, expected; end if;
end $$;
-- Table t with columns a:int, b:string, c:float on main.
create or replace function base(t int) returns void language plpgsql as $$
begin
  perform col_add(t, 1, 'a', 'int'); perform col_add(t, 1, 'b', 'string'); perform col_add(t, 1, 'c', 'float');
end $$;
create or replace function fid(t int, n text) returns int language sql as $$
  select field_id from schema_of(1, t, (select last_value from seq)) where name = n;
$$;
create or replace function clashes(b int) returns text language sql as $$
  select string_agg(distinct what, ' · ') from schema_conflicts(b);
$$;

do $$
declare r int; ok boolean;
begin
  -- C1 · the branch adds x, main adds y: both get in, with different ids.
  perform base(1); r := branch('c1', 1);
  perform col_add(1, r, 'x', 'long'); perform col_add(1, 1, 'y', 'long');
  perform expect(cols(r, 1), 'a:int,b:string,c:float,x:long', 'C1 the branch does not see y');
  perform expect(clashes(r), null, 'C1');
  perform merge_schema(r);
  perform expect(cols(1, 1), 'a:int,b:string,c:float,x:long,y:long', 'C1 main');

  -- C2 · both add x: name conflict (different ids).
  perform base(2); r := branch('c2', 1);
  perform col_add(2, r, 'x', 'long'); perform col_add(2, 1, 'x', 'string');
  perform expect(clashes(r), 'the same name for two fields: x', 'C2');

  -- C3 · the branch renames a → a2; main does not touch it: it gets in.
  perform base(3); r := branch('c3', 1);
  perform col_change(3, r, fid(3, 'a'), 'a2', 'int');
  perform expect(cols(1, 3), 'a:int,b:string,c:float', 'C3 main does not see the rename before merging');
  perform merge_schema(r);
  perform expect(cols(1, 3), 'a2:int,b:string,c:float', 'C3 main');

  -- C4 · both rename a, differently: conflict.
  perform base(4); r := branch('c4', 1);
  perform col_change(4, r, fid(4, 'a'), 'a2', 'int'); perform col_change(4, 1, fid(4, 'a'), 'a3', 'int');
  perform expect(clashes(r), 'the field changed in both', 'C4');

  -- C5 · the branch promotes c float → double: it gets in. And double → float is not a promotion: rejected when made.
  perform base(5); r := branch('c5', 1);
  perform col_change(5, r, fid(5, 'c'), 'c', 'double');
  perform merge_schema(r);
  perform expect(cols(1, 5), 'a:int,b:string,c:double', 'C5 main');
  ok := false;
  begin perform col_change(5, 1, fid(5, 'c'), 'c', 'float'); exception when sqlstate '22000' then ok := true; end;
  if not ok then raise exception 'C5: double → float was not rejected'; end if;

  -- C6 · the branch drops c, main renames it: conflict.
  perform base(6); r := branch('c6', 1);
  perform col_drop(6, r, fid(6, 'c')); perform col_change(6, 1, fid(6, 'c'), 'c2', 'float');
  perform expect(clashes(r), 'the field changed in both', 'C6');

  -- C7 · the branch renames a → z, main adds another z: name conflict.
  perform base(7); r := branch('c7', 1);
  perform col_change(7, r, fid(7, 'a'), 'z', 'int'); perform col_add(7, 1, 'z', 'long');
  perform expect(clashes(r), 'the same name for two fields: z', 'C7');

  -- C8 · both make the same change (a int → long): no conflict.
  perform base(8); r := branch('c8', 1);
  perform col_change(8, r, fid(8, 'a'), 'a', 'long'); perform col_change(8, 1, fid(8, 'a'), 'a', 'long');
  perform expect(clashes(r), null, 'C8');
  perform merge_schema(r);
  perform expect(cols(1, 8), 'a:long,b:string,c:float', 'C8 main');

  -- C9 · the branch drops b and adds another b: it is a new field (new id); after merging, main has the new b.
  perform base(9); r := branch('c9', 1);
  perform col_drop(9, r, fid(9, 'b')); perform col_add(9, r, 'b', 'long');
  perform expect(clashes(r), null, 'C9');
  perform merge_schema(r);
  perform expect(cols(1, 9), 'a:int,c:float,b:long', 'C9 main');

  raise warning 'schema: the 9 scenarios, OK';
end $$;
