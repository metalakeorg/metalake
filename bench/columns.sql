-- MetaLake · preliminary study 2.1: the schema with branches. On top of `schema.sql` + `merge.sql`. Throwaway prototype.
--
-- Columns use the same model as files (o1): each version of a column is a row of its ref, with a validity range.
-- A branch stores only what changes (add, rename, promote the type, drop); everything else it inherits.
-- Field ids come from ONE sequence for the whole catalog: two branches that add columns never hand out the same id.

create sequence if not exists field_seq;
create table if not exists o1_column (
  tbl int not null, ref int not null, field_id int not null, name text not null, type text not null,
  dropped boolean not null default false,
  begin_seq bigint not null, end_seq bigint not null default 9223372036854775807);
create index if not exists o1_column_f on o1_column (tbl, field_id, ref);

-- The live schema of (r, t) at s: for each field, the version of the nearest level of the chain.
create or replace function schema_of(r int, t int, s bigint)
returns table (field_id int, name text, type text) language sql stable as $$
  with recursive chain (lvl, ref, upto_s) as (
    select 0, r, s
    union all
    select c.lvl + 1, x.parent, x.fork_seq from chain c join ref x on x.id = c.ref where x.parent is not null),
  versions as (
    select distinct on (k.field_id) k.field_id, k.name, k.type, k.dropped
      from chain c join o1_column k on k.tbl = t and k.ref = c.ref and k.begin_seq <= c.upto_s and k.end_seq > c.upto_s
     order by k.field_id, c.lvl)
  select field_id, name, type from versions where not dropped order by field_id;
$$;

-- Allowed promotions: Iceberg's (int→long, float→double; wider decimal is left out of the prototype).
create or replace function valid_promotion(src text, dst text) returns boolean language sql immutable as $$
  select src = dst or (src, dst) in (('int', 'long'), ('float', 'double'));
$$;

-- A new version of a field in r: one's own live version (if any) is closed and another is opened. Inherited ones are not touched.
create or replace function col_version(t int, r int, f int, n text, ty text, b boolean) returns void language plpgsql as $$
declare s bigint := begin_commit(t, r);
begin
  update o1_column set end_seq = s where tbl = t and ref = r and field_id = f and end_seq = 9223372036854775807;
  insert into o1_column values (t, r, f, n, ty, b, s);
end $$;

create or replace function col_add(t int, r int, n text, ty text) returns int language plpgsql as $$
declare f int;
begin
  if exists (select 1 from schema_of(r, t, (select last_value from seq)) e where e.name = n) then
    raise exception using errcode = '40001', message = format('there is already a column %s', n);
  end if;
  f := nextval('field_seq');
  perform col_version(t, r, f, n, ty, false);
  return f;
end $$;

create or replace function col_change(t int, r int, f int, n text, ty text) returns void language plpgsql as $$
declare v record;
begin
  select * into v from schema_of(r, t, (select last_value from seq)) e where e.field_id = f;
  if v is null then raise exception 'no field %', f; end if;
  if not valid_promotion(v.type, ty) then
    raise exception using errcode = '22000', message = format('%s → %s is not a valid promotion', v.type, ty);
  end if;
  if exists (select 1 from schema_of(r, t, (select last_value from seq)) e where e.name = n and e.field_id <> f) then
    raise exception using errcode = '40001', message = format('there is already a column %s', n);
  end if;
  perform col_version(t, r, f, n, ty, false);
end $$;

create or replace function col_drop(t int, r int, f int) returns void language plpgsql as $$
declare v record;
begin
  select * into v from schema_of(r, t, (select last_value from seq)) e where e.field_id = f;
  perform col_version(t, r, f, v.name, v.type, true);
end $$;

-- Where the schemas of b and of its parent conflict since the fork.
create or replace function schema_conflicts(b int) returns table (tbl int, field_id int, what text)
language sql stable as $$
  with r as (select parent as p, fork_seq as bif from ref where id = b),
  -- the latest version of each field changed in b, and in the parent since the fork
  cb as (select distinct on (k.tbl, k.field_id) k.tbl, k.field_id, k.name, k.type, k.dropped
           from o1_column k where k.ref = b order by k.tbl, k.field_id, k.begin_seq desc),
  cp as (select distinct on (k.tbl, k.field_id) k.tbl, k.field_id, k.name, k.type, k.dropped
           from o1_column k, r where k.ref = r.p and k.begin_seq > r.bif order by k.tbl, k.field_id, k.begin_seq desc),
  -- the parent's schema after merging: its own, with b's changes on top
  tbls as (select distinct tbl from cb),
  after_merge as (
    select coalesce(cb.tbl, t.tbl) as tbl, coalesce(cb.field_id, e.field_id) as field_id, coalesce(cb.name, e.name) as name,
           coalesce(cb.dropped, false) as dropped
      from tbls t
      cross join r
      cross join lateral schema_of(r.p, t.tbl, (select last_value from seq)) e
      full join cb on cb.tbl = t.tbl and cb.field_id = e.field_id)
  select cb.tbl, cb.field_id, 'the field changed in both'
    from cb join cp using (tbl, field_id)
   where (cb.name, cb.type, cb.dropped) is distinct from (cp.name, cp.type, cp.dropped)
  union all
  select tbl, min(field_id), 'the same name for two fields: ' || name
    from after_merge where not dropped group by tbl, name having count(distinct field_id) > 1;
$$;

-- Merges b's schema into its parent (in the product, inside the same transaction as `merge_branch`).
create or replace function merge_schema(b int) returns int language plpgsql as $$
declare p int; s bigint; n int;
begin
  select parent into p from ref where id = b;
  select count(*) into n from schema_conflicts(b);
  if n > 0 then return n; end if;
  s := nextval('seq');
  update o1_column k set end_seq = s
    from (select distinct tbl, field_id from o1_column where ref = b) x
   where k.ref = p and k.tbl = x.tbl and k.field_id = x.field_id and k.end_seq = 9223372036854775807;
  insert into o1_column
    select distinct on (tbl, field_id) tbl, p, field_id, name, type, dropped, s, 9223372036854775807
      from o1_column where ref = b order by tbl, field_id, begin_seq desc;
  delete from o1_column where ref = b;
  update ref set fork_seq = s where id = b;
  return 0;
end $$;
