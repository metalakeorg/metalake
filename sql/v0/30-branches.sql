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

-- MRG-6 (revision 0.1): whether namespace ns lies under one of the namespaces of a selection (a JSON array of arrays).
create function @schema@.under_any(ns text[], selection jsonb) returns boolean language sql immutable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select exists (select 1 from jsonb_array_elements(coalesce(selection, '[]')) j
                  where cardinality(ns) >= jsonb_array_length(j)
                    and ns[1:jsonb_array_length(j)] = array(select jsonb_array_elements_text(j)));
$$;

-- MRG-3: whether a definition key of `tables` that branch b changed, its parent p changed too since the fork f, to
-- something else; one statement per versioned table.
create function @schema@.version_conflict(p int, b int, f bigint, tables uuid[]) returns boolean language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  vt record;
  cond text;
  bad boolean;
begin
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
                               from chain($2, 9223372036854775806) c join %1$I y
                                 on y.ref_id = c.ref_id and y.begin_seq <= c.seq and c.seq < y.end_seq and %2$s
                              order by c.level limit 1) n on true
         where x.table_id = any ($1) and x.ref_id = $4 and x.end_seq = 9223372036854775807
           and a.j is distinct from n.j and n.j is distinct from to_jsonb(x) - 'ref_id' - 'begin_seq' - 'end_seq')$q$,
      vt.name, cond) into bad using tables, p, f, b;
    if bad then
      return true;
    end if;
  end loop;
  return false;
end $$;

-- MRG-3, MRG-8: whether a namespace or namespace property of branch b, among those the merge takes (`taken`) or
-- among the rest, changed on the parent p since the fork f to something else.
create function @schema@.namespace_conflict(p int, b int, f bigint, chosen jsonb, picked jsonb, selective boolean,
                                            taken boolean) returns boolean language sql stable
set search_path = @schema@, pg_catalog, pg_temp as $$
  select exists (
    select 1 from namespace x
      left join lateral (select (namespace_version(p, x.namespace, f)).*) a on true
      left join lateral (select (namespace_version(p, x.namespace, 9223372036854775806)).*) n on true
     where x.ref_id = b and x.end_seq = 9223372036854775807
       and exists (select 1 from jsonb_array_elements(chosen) e where e = to_jsonb(x.namespace)) = taken
       and (a.namespace, a.dropped) is distinct from (n.namespace, n.dropped)
       and (n.namespace, n.dropped) is distinct from (x.namespace, x.dropped))
  or exists (
    select 1 from namespace_property x
      left join lateral (select (namespace_property_version(p, x.namespace, x.key, f)).*) a on true
      left join lateral (select (namespace_property_version(p, x.namespace, x.key, 9223372036854775806)).*) n on true
     where x.ref_id = b and x.end_seq = 9223372036854775807
       and (not selective or under_any(x.namespace, picked)) = taken
       and (a.namespace, a.value, a.dropped) is distinct from (n.namespace, n.value, n.dropped)
       and (n.namespace, n.value, n.dropped) is distinct from (x.namespace, x.value, x.dropped));
$$;

-- MRG-1 to MRG-5: merges branch `branch` into its parent, by sets of rows: each step is one statement over every
-- table the branch changed, not one per row. `vectors` are the merged vectors of files whose vector both sides
-- changed, as in commit. Revision 0.1: `only_tables` and `only_namespaces` (a JSON array of arrays) select part of the branch,
-- whose rest is rebased onto the merge (MRG-6 to MRG-8); `split` allows commits that changed both sides of the
-- selection. Returns (NULL, false) when the branch has nothing to merge.
create function @schema@.merge(branch int, vectors vector_set[] default '{}', author text default 'metalake',
                               message text default null, only_tables uuid[] default null,
                               only_namespaces jsonb default null, split boolean default false)
returns commit_result language plpgsql
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
  redefined uuid[];
  revectored uuid[];
  vt record;
  cond text;
  bad boolean;
  short boolean;
  moved data_file[];
  merged deletion_vector[];
  cur bigint;
  problems text;
  -- Revision 0.1: the selection (MRG-6), the rest (MRG-8) and the namespaces.
  selective constant boolean := $5 is not null or $6 is not null;
  picked_spaces constant jsonb := coalesce($6, '[]');
  selected uuid[];
  rest uuid[];
  spaces jsonb;
  chosen jsonb;
  ns_rows namespace[];
  prop_rows namespace_property[];
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
  spaces := (select coalesce(jsonb_agg(distinct to_jsonb(y.namespace)), '[]') from (
               select x.namespace from namespace x where x.ref_id = b and x.end_seq = open
               union select x.namespace from namespace_property x where x.ref_id = b and x.end_seq = open) y);
  if cardinality(tables) = 0 and jsonb_array_length(spaces) = 0 then
    return row(null, false)::commit_result;
  end if;
  named := array(select x.table_id from table_name x where x.ref_id = b and x.end_seq = open);

  -- MRG-6: the selection, the namespaces it brings (those under a selected namespace, and those of the selected
  -- tables and their parents), and the rest.
  if not selective then
    selected := tables;
    chosen := spaces;
  else
    selected := array(
      select x from unnest(tables) x
       where x = any (coalesce($5, '{}'))
          or exists (select 1 from visible_table_names(b, latest) v
                      where v.table_id = x and under_any(v.namespace, picked_spaces)));
    chosen := (select coalesce(jsonb_agg(to_jsonb(x.namespace)), '[]') from namespace x
                where x.ref_id = b and x.end_seq = open
                  and (under_any(x.namespace, picked_spaces)
                       or exists (select 1 from visible_table_names(b, latest) v
                                   where v.table_id = any (selected)
                                     and cardinality(v.namespace) >= cardinality(x.namespace)
                                     and v.namespace[1:cardinality(x.namespace)] = x.namespace)));
    if cardinality(selected) = 0 and jsonb_array_length(chosen) = 0
       and not exists (select 1 from namespace_property x where x.ref_id = b and x.end_seq = open
                          and under_any(x.namespace, picked_spaces)) then
      perform refuse_invalid('MRG-6', 'the selection names nothing the branch changed');
    end if;
  end if;
  rest := array(select x from unnest(tables) x where not x = any (selected));

  -- MRG-7: a commit of the branch since its base that changed both sides of the selection.
  if cardinality(rest) > 0 and not coalesce($7, false) then
    cur := (select s.snapshot_id from snapshot s where s.ref_id = b and s.snapshot_id > base.begin_seq
             group by s.snapshot_id
            having bool_or(s.table_id = any (selected)) and bool_or(s.table_id = any (rest))
             order by 1 limit 1);
    if cur is not null then
      perform refuse_conflict('MRG-7', format('commit %s changed tables on both sides of the selection', cur));
    end if;
  end if;
  -- The tables whose definition, or whose vectors, the merge brings: only those can become invalid (MRG-3).
  redefined := array(
    select x.table_id from field x where x.ref_id = b and x.end_seq = open
    union select x.table_id from table_name x where x.ref_id = b and x.end_seq = open
    union select x.table_id from table_property x where x.ref_id = b and x.end_seq = open
    union select x.table_id from partition_spec x where x.ref_id = b and x.end_seq = open
    union select x.table_id from sort_order x where x.ref_id = b and x.end_seq = open
    union select x.table_id from cluster_key x where x.ref_id = b and x.end_seq = open);
  revectored := array(select x.table_id from deletion_vector x where x.ref_id = b and x.end_seq = open);

  -- MRG-1: the parent's heads of the tables the branch changed that it has, in table order.
  foreach t in array tables loop
    if table_visible(p, t, latest) then
      insert into head values (p, t, snapshot_at(p, t, latest)) on conflict do nothing;
      perform 1 from head h where h.ref_id = p and h.table_id = t for update;
    elsif table_visible(p, t, f) then
      perform refuse_conflict('MRG-1', format('table %s was dropped on ref %s', t, p));
    end if;
  end loop;

  -- MRG-8: the rest, compared as if merged, against what the parent changed since the fork.
  if cardinality(rest) > 0 then
    if exists (select 1 from tombstone x where x.table_id = any (rest) and x.ref_id = b and x.end_seq = open
                  and not file_visible(p, x.table_id, latest, x.file_id)) then
      perform refuse_conflict('MRG-8', 'a file of the rest was removed on both sides');
    end if;
    if exists (select 1 from tombstone x
                  cross join lateral (select (vector_at(p, x.table_id, f, x.file_id)).*) a
                  cross join lateral (select (vector_at(p, x.table_id, latest, x.file_id)).*) n
                where x.table_id = any (rest) and x.ref_id = b and x.end_seq = open
                  and (a.path, a.content_offset, a.content_size, a.cardinality, a.inline)
                      is distinct from (n.path, n.content_offset, n.content_size, n.cardinality, n.inline)) then
      perform refuse_conflict('MRG-8', 'a file of the rest was removed on one side and its rows deleted on the other');
    end if;
    if version_conflict(p, b, f, rest) then
      perform refuse_conflict('MRG-8', 'a definition of the rest changed on both sides');
    end if;
    if exists (select 1 from deletion_vector x where x.table_id = any (rest) and x.ref_id = b and x.end_seq = open
                  and file_visible(p, x.table_id, f, x.file_id) and not file_visible(p, x.table_id, latest, x.file_id)) then
      perform refuse_conflict('MRG-8', 'the rows of a file of the rest were deleted on one side and the file removed on the other');
    end if;
  end if;
  if namespace_conflict(p, b, f, chosen, picked_spaces, selective, false) then
    perform refuse_conflict('MRG-8', 'a namespace of the rest changed on both sides');
  end if;

  m := nextval('commit_seq_gen');
  insert into commit values (m, p, pg_current_xact_id(), null, who, who, 'merge', null, null, null, $4);

  -- MRG-2: files the branch removed, against the parent's state.
  if exists (select 1 from tombstone x where x.table_id = any (selected) and x.ref_id = b and x.end_seq = open
                and not file_visible(p, x.table_id, latest, x.file_id)) then
    perform refuse_conflict('MRG-2', 'a file was removed on both sides');
  end if;
  if exists (select 1 from tombstone x
                cross join lateral (select (vector_at(p, x.table_id, f, x.file_id)).*) a
                cross join lateral (select (vector_at(p, x.table_id, latest, x.file_id)).*) n
              where x.table_id = any (selected) and x.ref_id = b and x.end_seq = open
                and (a.path, a.content_offset, a.content_size, a.cardinality, a.inline)
                    is distinct from (n.path, n.content_offset, n.content_size, n.cardinality, n.inline)) then
    perform refuse_conflict('MRG-2', 'a file was removed on one side and its rows deleted on the other');
  end if;
  with x as (select x.table_id, x.file_id from tombstone x
              where x.table_id = any (selected) and x.ref_id = b and x.end_seq = open),
  closed as (
    update data_file d set end_seq = m from x
     where d.table_id = x.table_id and d.file_id = x.file_id and d.ref_id = p and d.end_seq = open
    returning d.table_id, d.file_id)
  insert into tombstone (table_id, file_id, ref_id, begin_seq)
  select x.table_id, x.file_id, p, m from x
   where not exists (select 1 from closed c where c.table_id = x.table_id and c.file_id = x.file_id);
  update deletion_vector d set end_seq = m from tombstone x
   where x.table_id = any (selected) and x.ref_id = b and x.end_seq = open
     and d.table_id = x.table_id and d.file_id = x.file_id and d.ref_id = p and d.end_seq = open;
  update tombstone x set end_seq = m where x.table_id = any (selected) and x.ref_id = b and x.end_seq = open;

  -- Files the branch added move to the parent.
  with c as (update data_file x set end_seq = m where x.table_id = any (selected) and x.ref_id = b and x.end_seq = open
             returning x.*)
  select array_agg(c) into moved from c;
  insert into data_file (table_id, file_id, ref_id, begin_seq, path, format, spec_id, partition, sort_order_id,
                         record_count, file_size, footer_size, first_row_id, stat_field_ids, lower, upper, null_count,
                         nan_count, value_count)
  select x.table_id, x.file_id, p, m, x.path, x.format, x.spec_id, x.partition, x.sort_order_id, x.record_count,
         x.file_size, x.footer_size, x.first_row_id, x.stat_field_ids, x.lower, x.upper, x.null_count, x.nan_count,
         x.value_count
    from unnest(coalesce(moved, '{}')) x;

  -- Versions of definitions (VIS-9): a key both sides changed to different versions conflicts (MRG-3).
  for vt in select * from (values ('table_name', '{table_id}'::text[]), ('table_property', '{table_id,key}'),
                                  ('field', '{table_id,field_id}'), ('partition_spec', '{table_id}'),
                                  ('sort_order', '{table_id}'), ('cluster_key', '{table_id}')) v(name, keys) loop
    -- Nothing to compare or move where the branch has no live version.
    execute format('select exists (select 1 from %I x where x.table_id = any ($3) and x.ref_id = $1 and x.end_seq = $2)',
                   vt.name)
      into bad using b, open, selected;
    continue when not bad;
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
      vt.name, cond) into bad using selected, p, f, latest, b, open;
    if bad then
      perform refuse_conflict('MRG-3', format('%s changed on both sides', vt.name));
    end if;
    execute format('update %1$I y set end_seq = $1 from %1$I x
                     where x.table_id = any ($2) and x.ref_id = $3 and x.end_seq = $4
                       and y.ref_id = $5 and y.end_seq = $4 and %2$s', vt.name, cond)
      using m, selected, b, open, p;
    execute format('with moved as (update %1$I x set end_seq = $1
                                    where x.table_id = any ($2) and x.ref_id = $3 and x.end_seq = $4 returning x.*)
                    insert into %1$I select (jsonb_populate_record(null::%1$I,
                      to_jsonb(moved) || jsonb_build_object(''ref_id'', $5, ''begin_seq'', $1, ''end_seq'', $4))).*
                      from moved', vt.name)
      using m, selected, b, open, p;
  end loop;

  -- Vectors (DV-1, MRG-5): the branch's, unless both sides changed one; then the union of two inline vectors, or
  -- the merged vector the caller supplies, holding both.
  if exists (select 1 from deletion_vector x where x.table_id = any (selected) and x.ref_id = b and x.end_seq = open
                and not file_visible(p, x.table_id, latest, x.file_id)) then
    perform refuse_conflict('MRG-5', 'the rows of a file were deleted on one side and the file removed on the other');
  end if;
  with mine as (select x.* from deletion_vector x where x.table_id = any (selected) and x.ref_id = b and x.end_seq = open),
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
  foreach t in array selected loop
    problems := (select string_agg(x, '; ') from (
      select table_errors(p, t, latest) as x where t = any (redefined)
      union all select dv_errors(p, t, latest, m) where t = any (revectored)) z);
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

  -- VIS-9, MRG-3 (revision 0.1): the chosen namespaces and the selected namespace properties, by exact path.
  if namespace_conflict(p, b, f, chosen, picked_spaces, selective, true) then
    perform refuse_conflict('MRG-3', 'a namespace changed on both sides');
  end if;
  ns_rows := array(select x from namespace x where x.ref_id = b and x.end_seq = open
                     and exists (select 1 from jsonb_array_elements(chosen) e where e = to_jsonb(x.namespace)));
  prop_rows := array(select x from namespace_property x where x.ref_id = b and x.end_seq = open
                       and (not selective or under_any(x.namespace, picked_spaces)));
  update namespace x set end_seq = m from unnest(ns_rows) y
   where x.namespace = y.namespace and x.ref_id in (p, b) and x.end_seq = open;
  insert into namespace (namespace, dropped, ref_id, begin_seq) select y.namespace, y.dropped, p, m from unnest(ns_rows) y;
  update namespace_property x set end_seq = m from unnest(prop_rows) y
   where x.namespace = y.namespace and x.key = y.key and x.ref_id in (p, b) and x.end_seq = open;
  insert into namespace_property (namespace, key, value, dropped, ref_id, begin_seq)
  select y.namespace, y.key, y.value, y.dropped, p, m from unnest(prop_rows) y;
  select z.problem into problems
    from namespace_errors(p, latest, array(select x from unnest(named) x where x = any (selected)), chosen) z limit 1;
  if problems is not null then
    perform refuse_conflict('NS-2', format('the merge breaks a namespace rule: %s', problems));
  end if;

  -- TBL-1: only a name the merge brought can collide.
  if cardinality(named) > 0 and exists (select 1 from visible_table_names(p, latest) v
              where (v.namespace, v.name) in (select w.namespace, w.name from visible_table_names(p, latest) w
                                               where w.table_id = any (named) and w.table_id = any (selected))
              group by v.namespace, v.name having count(*) > 1) then
    perform refuse_conflict('MRG-3', 'the merge gives two tables one name');
  end if;

  -- MRG-4: the branch becomes an empty child of the parent.
  update ref_base x set end_seq = m where x.ref_id = b and x.end_seq = open;
  insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (b, m, p, m);

  -- MRG-8: the rest, rebased: the union of vectors both sides changed, written on the branch; a rebase snapshot per
  -- table; and its validity on the branch.
  if cardinality(rest) > 0 then
    with mine as (select x.* from deletion_vector x where x.table_id = any (rest) and x.ref_id = b and x.end_seq = open),
    sides as (
      select row(x.*)::deletion_vector as x, row(n.*)::deletion_vector as n, row(s.*)::vector_set as s
        from mine x
        cross join lateral (select (vector_at(p, x.table_id, f, x.file_id)).*) a
        cross join lateral (select (vector_at(p, x.table_id, latest, x.file_id)).*) n
        left join lateral (select v.* from unnest(vecs) v
                            where v.table_id = x.table_id and v.file_id = x.file_id limit 1) s on true
       where (a.path, a.content_offset, a.content_size, a.cardinality, a.inline)
             is distinct from (n.path, n.content_offset, n.content_size, n.cardinality, n.inline))
    select array_agg(case
             when (x).inline is not null and (n).inline is not null then
               (select row((x).table_id, (x).file_id, null, null, length(u.blob),
                           (select count(*) from dv_positions(u.blob)), u.blob, b, m, open)::deletion_vector
                  from (select dv_encode(array(select dv_positions((x).inline) union select dv_positions((n).inline)))
                               as blob) u)
             else row((x).table_id, (x).file_id, (s).path, (s).content_offset, (s).content_size, (s).cardinality,
                      (s).inline, b, m, open)::deletion_vector end),
           bool_or(not ((x).inline is not null and (n).inline is not null) and (s).file_id is null),
           bool_or(not ((x).inline is not null and (n).inline is not null) and (s).file_id is not null
                   and not vector_covers(s, x, n))
      into merged, bad, short
      from sides;
    if bad then
      perform refuse_conflict('MRG-8', 'a vector of the rest changed on both sides and no merged vector was given');
    end if;
    if short then
      perform refuse_conflict('MRG-8', 'a merged vector of the rest does not hold both sides');
    end if;
    update deletion_vector d set end_seq = m from unnest(coalesce(merged, '{}')) x
     where d.table_id = x.table_id and d.file_id = x.file_id and d.ref_id = b and d.end_seq = open;
    insert into deletion_vector (table_id, file_id, path, content_offset, content_size, cardinality, inline, ref_id,
                                 begin_seq)
    select x.table_id, x.file_id, x.path, x.content_offset, x.content_size, x.cardinality, x.inline, b, m
      from unnest(coalesce(merged, '{}')) x;

    insert into snapshot
    select t, m, b, ps.id,
           case when exists (select 1 from field x where x.table_id = t and x.ref_id = b and x.end_seq = open) then m
                else (select s.schema_seq from snapshot s where s.table_id = t and s.snapshot_id = ps.id) end,
           (select spec_id from default_spec(b, t, latest)),
           coalesce((select sort_order_id from default_sort_order(b, t, latest)), 0),
           jsonb_build_object('operation', 'rebase')
      from unnest(rest) t,
           lateral (select coalesce(snapshot_at(p, t, latest),
                                    (select h.snapshot_id from head h where h.ref_id = b and h.table_id = t)) as id) ps;
    foreach t in array rest loop
      problems := (select string_agg(x, '; ') from (
        select table_errors(b, t, latest) as x union all select dv_errors(b, t, latest, m)) z);
      if problems is not null then
        perform refuse_conflict('MRG-8', format('the rebase leaves table %s invalid on the branch: %s', t, problems));
      end if;
    end loop;
  end if;
  if cardinality(rest) > 0 or jsonb_array_length(chosen) < jsonb_array_length(spaces) then
    if exists (select 1 from visible_table_names(b, latest) v
                where (v.namespace, v.name) in (select w.namespace, w.name from visible_table_names(b, latest) w
                                                 where w.table_id = any (named) and w.table_id = any (rest))
                group by v.namespace, v.name having count(*) > 1) then
      perform refuse_conflict('MRG-8', 'the rebase gives two tables one name on the branch');
    end if;
    select z.problem into problems from namespace_errors(b, latest,
      array(select x from unnest(named) x where x = any (rest)),
      (select coalesce(jsonb_agg(e), '[]') from jsonb_array_elements(spaces) e
        where not exists (select 1 from jsonb_array_elements(chosen) c where c = e))) z limit 1;
    if problems is not null then
      perform refuse_conflict('MRG-8', format('the rebase breaks a namespace rule on the branch: %s', problems));
    end if;
  end if;
  update head h set snapshot_id = snapshot_at(b, h.table_id, latest) where h.ref_id = b;
  perform pg_notify('metalake', current_schema() || ':' || m);
  return row(m, true)::commit_result;
end $$;

-- A merge tried without effect: everything a merge does, rolled back, and what it would have done. The commit
-- sequence it drew is not given back (CMT-3 allows gaps).
create function @schema@.merge_report(branch int, vectors vector_set[] default '{}', only_tables uuid[] default null,
                                      only_namespaces jsonb default null, split boolean default false) returns jsonb
language plpgsql set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  result commit_result;
  report jsonb;
  state text;
  rule text;
  what text;
begin
  begin
    result := merge(branch => $1, vectors => $2, only_tables => $3, only_namespaces => $4, split => $5);
    report := jsonb_build_object('ok', true, 'merges', result.commit_seq is not null,
      'merged', (select coalesce(jsonb_agg(s.table_id order by s.table_id), '[]') from snapshot s
                  where s.snapshot_id = result.commit_seq and s.summary ->> 'operation' = 'merge'),
      'rebased', (select coalesce(jsonb_agg(s.table_id order by s.table_id), '[]') from snapshot s
                   where s.snapshot_id = result.commit_seq and s.summary ->> 'operation' = 'rebase'));
    raise exception 'metalake dry run' using errcode = 'MLDRY';
  exception
    when sqlstate 'MLDRY' then
      return report;
    when others then
      get stacked diagnostics state = returned_sqlstate, rule = pg_exception_detail, what = message_text;
      return jsonb_build_object('ok', false, 'state', state, 'rule', nullif(rule, ''), 'message', what);
  end;
end $$;
