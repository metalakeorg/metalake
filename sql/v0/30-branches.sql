-- MetaLake v0 · protocol · branches and merges (ADR 0003 §2.4).

-- RET-1: the snapshots ref r keeps.
create function @schema@.kept_snapshots(r int) returns setof bigint language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  with snaps as (select distinct snapshot_id as s from snapshot where ref_id = r),
  pol as (select * from retention where ref_id = r and (min_seq is not null or max_age is not null or min_snapshots is not null))
  select s from snaps
   where not exists (select 1 from pol)
      or s >= (select min_seq from pol)
      or committed_time(s) >= now() - (select max_age from pol)
      or s in (select s from snaps order by s desc limit coalesce((select min_snapshots from pol), 0));
$$;

-- BR-1, BR-2: a branch or a tag `name` of ref `parent`, seeing it at `at`, a snapshot the parent keeps (RET-6), or
-- by default at the last commit_seq drawn, read under the lock. Returns its ref_id.
create function @schema@.create_ref(name text, parent int, kind text default 'branch', at bigint default null,
                                    author text default 'metalake') returns int language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  p ref;
  fork bigint;
  c bigint;
  id int := nextval('ref_id_gen');
begin
  perform lock_ref(parent);
  select * into p from ref x where x.ref_id = parent;
  if p.ref_id is null or p.deleted then
    perform refuse_invalid('BR-1', format('ref %s cannot be forked', parent));
  end if;
  fork := (select last_value from commit_seq_gen);
  if at is not null then
    if not exists (select 1 from kept_snapshots(parent) k where k = at) then
      perform refuse_invalid('RET-6', format('%s is not a snapshot ref %s keeps', at, parent));
    end if;
    fork := at;
  end if;
  c := nextval('commit_seq_gen');
  insert into ref (ref_id, name, kind, created_seq) values (id, create_ref.name, create_ref.kind, c);
  insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (id, c, parent, fork);
  insert into commit values (c, id, pg_current_xact_id(), null, author, author, create_ref.kind, null, null, null, null);
  return id;
end $$;

-- BR-3
create function @schema@.delete_ref(ref_id int) returns void language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
#variable_conflict use_column
declare
  r constant int := $1;
  x ref;
begin
  perform lock_ref(r);
  select * into x from ref where ref.ref_id = r;
  if x.ref_id is null or x.ref_id = 1 or x.protected then
    perform refuse_invalid('BR-3', format('ref %s cannot be deleted', r));
  end if;
  update ref set deleted = true where ref.ref_id = r;
end $$;

-- The version of the vector of file fid of t at (r, s), visible or not: what a merge compares (MRG-5).
create function @schema@.vector_at(r int, t uuid, s bigint, fid bigint) returns deletion_vector language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select x.*
    from chain(r, s) c join deletion_vector x
      on x.table_id = t and x.file_id = fid and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   order by c.level limit 1;
$$;

-- MRG-5: whether a supplied vector holds both sides.
create function @schema@.vector_covers(v vector_set, a deletion_vector, b deletion_vector) returns boolean
language sql immutable set search_path = @schema@, pg_catalog, pg_temp as $$
  select v.cardinality >= greatest(a.cardinality, b.cardinality)
     and (v.inline is null or not exists (
            select dv_positions(x) from unnest(array[a.inline, b.inline]) x where x is not null
            except select dv_positions(v.inline)));
$$;

-- MRG-1 to MRG-5: merges branch `branch` into its parent, by sets of rows: each step is one statement over every
-- table the branch changed, not one per row. `vectors` are the merged vectors of files whose vector both sides
-- changed, as in commit. Returns (NULL, false) when the branch has nothing to merge.
create function @schema@.merge(branch int, vectors vector_set[] default '{}', author text default 'metalake',
                               message text default null) returns commit_result language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
#variable_conflict use_column
declare
  latest constant bigint := 9223372036854775806;
  open constant bigint := 9223372036854775807;
  b constant int := $1;
  vecs constant vector_set[] := coalesce($2, '{}');
  who constant text := coalesce($3, 'metalake');
  rb ref;
  base ref_base;
  p int;
  f bigint;
  m bigint;
  t uuid;
  tables uuid[];
  named uuid[];
  vt record;
  cond text;
  bad boolean;
  short boolean;
  moved data_file[];
  merged deletion_vector[];
  cur bigint;
  problems text;
begin
  select * into base from ref_base x where x.ref_id = b and x.end_seq = open;
  if base.ref_id is null then
    perform refuse_invalid('MRG-1', format('ref %s has no parent to merge into', b));
  end if;
  p := base.parent_ref_id;
  perform lock_ref(p);
  perform 1 from ref x where x.ref_id = p and x.kind = 'branch' and not x.deleted;
  if not found then
    perform refuse_invalid('MRG-1', format('ref %s does not take merges', p));
  end if;
  perform lock_ref(b);
  select * into rb from ref x where x.ref_id = b;
  select * into base from ref_base x where x.ref_id = b and x.end_seq = open;
  if rb.kind <> 'branch' or rb.deleted or base.parent_ref_id <> p then
    perform refuse_invalid('MRG-1', format('ref %s cannot be merged into %s', b, p));
  end if;
  f := base.fork_seq;

  tables := array(
    select x.table_id from data_file x where x.ref_id = b and x.end_seq = open
    union select x.table_id from tombstone x where x.ref_id = b and x.end_seq = open
    union select x.table_id from deletion_vector x where x.ref_id = b and x.end_seq = open
    union select x.table_id from field x where x.ref_id = b and x.end_seq = open
    union select x.table_id from table_name x where x.ref_id = b and x.end_seq = open
    union select x.table_id from table_property x where x.ref_id = b and x.end_seq = open
    union select x.table_id from partition_spec x where x.ref_id = b and x.end_seq = open
    union select x.table_id from sort_order x where x.ref_id = b and x.end_seq = open
    union select x.table_id from cluster_key x where x.ref_id = b and x.end_seq = open
    order by 1);
  if cardinality(tables) = 0 then
    return row(null, false)::commit_result;
  end if;
  named := array(select x.table_id from table_name x where x.ref_id = b and x.end_seq = open);

  -- MRG-1: the parent's heads of the tables the branch changed that it has, in table order.
  foreach t in array tables loop
    if table_visible(p, t, latest) then
      insert into head values (p, t, snapshot_at(p, t, latest)) on conflict do nothing;
      perform 1 from head h where h.ref_id = p and h.table_id = t for update;
    elsif table_visible(p, t, f) then
      perform refuse_conflict('MRG-1', format('table %s was dropped on ref %s', t, p));
    end if;
  end loop;

  m := nextval('commit_seq_gen');
  insert into commit values (m, p, pg_current_xact_id(), null, who, who, 'merge', null, null, null, $4);

  -- MRG-2: files the branch removed, against the parent's state.
  if exists (select 1 from tombstone x where x.table_id = any (tables) and x.ref_id = b and x.end_seq = open
                and not file_visible(p, x.table_id, latest, x.file_id)) then
    perform refuse_conflict('MRG-2', 'a file was removed on both sides');
  end if;
  if exists (select 1 from tombstone x
                cross join lateral (select (vector_at(p, x.table_id, f, x.file_id)).*) a
                cross join lateral (select (vector_at(p, x.table_id, latest, x.file_id)).*) n
              where x.table_id = any (tables) and x.ref_id = b and x.end_seq = open
                and (a.path, a.content_offset, a.content_size, a.cardinality, a.inline)
                    is distinct from (n.path, n.content_offset, n.content_size, n.cardinality, n.inline)) then
    perform refuse_conflict('MRG-2', 'a file was removed on one side and its rows deleted on the other');
  end if;
  with x as (select x.table_id, x.file_id from tombstone x
              where x.table_id = any (tables) and x.ref_id = b and x.end_seq = open),
  closed as (
    update data_file d set end_seq = m from x
     where d.table_id = x.table_id and d.file_id = x.file_id and d.ref_id = p and d.end_seq = open
    returning d.table_id, d.file_id)
  insert into tombstone (table_id, file_id, ref_id, begin_seq)
  select x.table_id, x.file_id, p, m from x
   where not exists (select 1 from closed c where c.table_id = x.table_id and c.file_id = x.file_id);
  update deletion_vector d set end_seq = m from tombstone x
   where x.table_id = any (tables) and x.ref_id = b and x.end_seq = open
     and d.table_id = x.table_id and d.file_id = x.file_id and d.ref_id = p and d.end_seq = open;
  update tombstone x set end_seq = m where x.table_id = any (tables) and x.ref_id = b and x.end_seq = open;

  -- Files the branch added move to the parent.
  with c as (update data_file x set end_seq = m where x.table_id = any (tables) and x.ref_id = b and x.end_seq = open
             returning x.*)
  select array_agg(c) into moved from c;
  insert into data_file
  select (jsonb_populate_record(null::data_file,
            to_jsonb(x) || jsonb_build_object('ref_id', p, 'begin_seq', m, 'end_seq', open))).*
    from unnest(coalesce(moved, '{}')) x;

  -- Versions of definitions (VIS-9): a key both sides changed to different versions conflicts (MRG-3).
  for vt in select * from (values ('table_name', '{table_id}'::text[]), ('table_property', '{table_id,key}'),
                                  ('field', '{table_id,field_id}'), ('partition_spec', '{table_id}'),
                                  ('sort_order', '{table_id}'), ('cluster_key', '{table_id}')) v(name, keys) loop
    cond := (select string_agg(format('y.%1$I = x.%1$I', k), ' and ') from unnest(vt.keys) k);
    execute format($q$
      select exists (
        select 1 from %1$I x
          left join lateral (select to_jsonb(y) - 'ref_id' - 'begin_seq' - 'end_seq' as j
                               from chain($2, $3) c join %1$I y
                                 on y.ref_id = c.ref_id and y.begin_seq <= c.seq and c.seq < y.end_seq and %2$s
                              order by c.level limit 1) a on true
          left join lateral (select to_jsonb(y) - 'ref_id' - 'begin_seq' - 'end_seq' as j
                               from chain($2, $4) c join %1$I y
                                 on y.ref_id = c.ref_id and y.begin_seq <= c.seq and c.seq < y.end_seq and %2$s
                              order by c.level limit 1) n on true
         where x.table_id = any ($1) and x.ref_id = $5 and x.end_seq = $6
           and a.j is distinct from n.j and n.j is distinct from to_jsonb(x) - 'ref_id' - 'begin_seq' - 'end_seq')$q$,
      vt.name, cond) into bad using tables, p, f, latest, b, open;
    if bad then
      perform refuse_conflict('MRG-3', format('%s changed on both sides', vt.name));
    end if;
    execute format('update %1$I y set end_seq = $1 from %1$I x
                     where x.table_id = any ($2) and x.ref_id = $3 and x.end_seq = $4
                       and y.ref_id = $5 and y.end_seq = $4 and %2$s', vt.name, cond)
      using m, tables, b, open, p;
    execute format('with moved as (update %1$I x set end_seq = $1
                                    where x.table_id = any ($2) and x.ref_id = $3 and x.end_seq = $4 returning x.*)
                    insert into %1$I select (jsonb_populate_record(null::%1$I,
                      to_jsonb(moved) || jsonb_build_object(''ref_id'', $5, ''begin_seq'', $1, ''end_seq'', $4))).*
                      from moved', vt.name)
      using m, tables, b, open, p;
  end loop;

  -- Vectors (DV-1, MRG-5): the branch's, unless both sides changed one; then the union of two inline vectors, or
  -- the merged vector the caller supplies, holding both.
  if exists (select 1 from deletion_vector x where x.table_id = any (tables) and x.ref_id = b and x.end_seq = open
                and not file_visible(p, x.table_id, latest, x.file_id)) then
    perform refuse_conflict('MRG-5', 'the rows of a file were deleted on one side and the file removed on the other');
  end if;
  with mine as (select x.* from deletion_vector x where x.table_id = any (tables) and x.ref_id = b and x.end_seq = open),
  sides as (
    select row(x.*)::deletion_vector as x, row(n.*)::deletion_vector as n, row(s.*)::vector_set as s,
           (a.path, a.content_offset, a.content_size, a.cardinality, a.inline)
             is distinct from (n.path, n.content_offset, n.content_size, n.cardinality, n.inline) as changed
      from mine x
      cross join lateral (select (vector_at(p, x.table_id, f, x.file_id)).*) a
      cross join lateral (select (vector_at(p, x.table_id, latest, x.file_id)).*) n
      left join lateral (select v.* from unnest(vecs) v
                          where v.table_id = x.table_id and v.file_id = x.file_id limit 1) s on true)
  select array_agg(case
           when not changed then x
           when (x).inline is not null and (n).inline is not null then
             (select row((x).table_id, (x).file_id, null, null, length(u.blob),
                         (select count(*) from dv_positions(u.blob)), u.blob, p, m, open)::deletion_vector
                from (select dv_encode(array(select dv_positions((x).inline) union select dv_positions((n).inline)))
                             as blob) u)
           else row((x).table_id, (x).file_id, (s).path, (s).content_offset, (s).content_size, (s).cardinality,
                    (s).inline, p, m, open)::deletion_vector end),
         bool_or(changed and not ((x).inline is not null and (n).inline is not null) and (s).file_id is null),
         bool_or(changed and not ((x).inline is not null and (n).inline is not null) and (s).file_id is not null
                 and not vector_covers(s, x, n))
    into merged, bad, short
    from sides;
  if bad then
    perform refuse_conflict('MRG-5', 'a vector changed on both sides and no merged vector was given');
  end if;
  if short then
    perform refuse_conflict('MRG-5', 'a merged vector does not hold both sides');
  end if;
  update deletion_vector d set end_seq = m from unnest(coalesce(merged, '{}')) x
   where d.table_id = x.table_id and d.file_id = x.file_id and d.ref_id in (p, b) and d.end_seq = open;
  insert into deletion_vector (table_id, file_id, path, content_offset, content_size, cardinality, inline, ref_id,
                               begin_seq)
  select x.table_id, x.file_id, x.path, x.content_offset, x.content_size, x.cardinality, x.inline, p, m
    from unnest(coalesce(merged, '{}')) x;

  -- MRG-3: each table is valid; CMT-1: its snapshot and head.
  foreach t in array tables loop
    problems := (select string_agg(x, '; ') from (
      select table_errors(p, t, latest) as x union all select dv_errors(p, t, latest, m)) z);
    if problems is not null then
      perform refuse_conflict('MRG-3', format('the merge leaves table %s invalid: %s', t, problems));
    end if;
    cur := (select h.snapshot_id from head h where h.ref_id = p and h.table_id = t);
    insert into snapshot values (t, m, p, cur,
      case when exists (select 1 from field x where x.table_id = t and x.ref_id = p and x.begin_seq = m) or cur is null then m
           else (select s.schema_seq from snapshot s where s.table_id = t and s.snapshot_id = cur) end,
      (select spec_id from default_spec(p, t, latest)),
      coalesce((select sort_order_id from default_sort_order(p, t, latest)), 0),
      jsonb_build_object('operation', 'merge',
        'added-data-files', (select count(*) from data_file x where x.table_id = t and x.ref_id = p and x.begin_seq = m),
        'deleted-data-files', (select count(*) from data_file x where x.table_id = t and x.ref_id = p and x.end_seq = m)
                            + (select count(*) from tombstone x where x.table_id = t and x.ref_id = p and x.begin_seq = m),
        'added-records', (select coalesce(sum(record_count), 0) from data_file x
                           where x.table_id = t and x.ref_id = p and x.begin_seq = m),
        'deleted-records', (select coalesce(sum((visible_file(p, t, m - 1, y.file_id)).record_count), 0)
                              from (select x.file_id from data_file x where x.table_id = t and x.ref_id = p and x.end_seq = m
                                    union select x.file_id from tombstone x
                                           where x.table_id = t and x.ref_id = p and x.begin_seq = m) y)));
    insert into head values (p, t, m) on conflict (ref_id, table_id) do update set snapshot_id = m;
  end loop;

  -- TBL-1: only a name the merge brought can collide.
  if exists (select 1 from visible_table_names(p, latest) v
              where (v.namespace, v.name) in (select w.namespace, w.name from visible_table_names(p, latest) w
                                               where w.table_id = any (named))
              group by v.namespace, v.name having count(*) > 1) then
    perform refuse_conflict('MRG-3', 'the merge gives two tables one name');
  end if;

  -- MRG-4: the branch becomes an empty child of the parent.
  update ref_base x set end_seq = m where x.ref_id = b and x.end_seq = open;
  insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (b, m, p, m);
  update head h set snapshot_id = snapshot_at(b, h.table_id, latest) where h.ref_id = b;
  perform pg_notify('metalake', current_schema() || ':' || m);
  return row(m, true)::commit_result;
end $$;
