-- MetaLake v0 · protocol · commit (§9 of the text; ADR 0003 §2.4, §2.5).
--
-- One call writes the data and definitions of one or several tables in one transaction. Data is set-based: no loop
-- runs over files. Definitions are rare and small, travel as JSON in the reference's shape, and keep the
-- reference's logic. The checks are those of the reference, except what only the data shows (PAR-3, the positions
-- inside a vector), which the writer is trusted with.

-- The definition changes of one table, before the commit draws its sequence: what conflicts or is invalid.
create function @schema@.check_definition(r int, t uuid, e jsonb) returns void language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  latest constant bigint := 9223372036854775806;
  x jsonb;
  old field;
  fid bigint;
begin
  if e ? 'create' then
    if exists (select 1 from tbl where table_id = t) then
      perform refuse_invalid('ID-3', format('table id %s exists', t));
    end if;
    if table_by_name(r, array(select jsonb_array_elements_text(e -> 'create' -> 'namespace')), e -> 'create' ->> 'name',
                     latest) is not null then
      perform refuse_conflict('TBL-1', format('the name %s is taken', e -> 'create'));
    end if;
    return;
  end if;
  if e ? 'rename' and coalesce(table_by_name(r, array(select jsonb_array_elements_text(e -> 'rename' -> 'namespace')),
                                             e -> 'rename' ->> 'name', latest), t) <> t then
    perform refuse_conflict('TBL-1', format('the name %s is taken', e -> 'rename'));
  end if;
  for x in select * from jsonb_array_elements(coalesce(e -> 'fields', '[]')) loop
    if coalesce((x ->> 'new')::boolean, false) then
      if exists (select 1 from visible_fields(r, t, latest) v
                  where v.parent_field_id is not distinct from (x ->> 'parent_field_id')::int and v.name = x ->> 'name') then
        perform refuse_conflict('CMT-7', format('a field named %s exists', x ->> 'name'));
      end if;
    else
      select * into old from visible_fields(r, t, latest) v where v.field_id = (x ->> 'field_id')::int;
      if old.field_id is null then
        perform refuse_conflict('CMT-7', format('field %s is not visible', x ->> 'field_id'));
      end if;
      if not field_type_change_allowed(t, old.field_id, old.type, x ->> 'type') then
        perform refuse_conflict('TYP-3', format('field %s cannot change from %s to %s', old.field_id, old.type, x ->> 'type'));
      end if;
    end if;
  end loop;
  for fid in select jsonb_array_elements_text(coalesce(e -> 'drop_fields', '[]'))::bigint loop
    if not exists (select 1 from visible_fields(r, t, latest) v where v.field_id = fid) then
      perform refuse_conflict('CMT-7', format('field %s is not visible', fid));
    end if;
  end loop;
end $$;

-- The definition changes of one table, written at seq.
create function @schema@.apply_definition(r int, t uuid, seq bigint, e jsonb) returns void language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  open constant bigint := 9223372036854775807;
  x jsonb;
  fid bigint;
begin
  if e ? 'create' then
    insert into tbl values (t, e -> 'create' ->> 'location', seq);
    insert into table_counter values (t,
      coalesce((select max((y ->> 'field_id')::int) from jsonb_array_elements(e -> 'fields') y), 0),
      0,
      greatest(999, coalesce((select max((y ->> 'field_id')::int) from jsonb_array_elements(e -> 'partition_spec') y), 999)),
      case when e ? 'sort_order' then 1 else 0 end);
    insert into table_name values (t, array(select jsonb_array_elements_text(e -> 'create' -> 'namespace')),
                                   e -> 'create' ->> 'name', false, r, seq);
    insert into partition_spec values (t, 0, coalesce(e -> 'partition_spec', '[]'), r, seq);
    if e ? 'sort_order' then
      insert into sort_order values (t, 1, e -> 'sort_order', r, seq);
    end if;
  end if;

  if e ? 'rename' or coalesce((e ->> 'drop')::boolean, false) then
    update table_name set end_seq = seq where table_id = t and ref_id = r and end_seq = open;
    insert into table_name
      select t, coalesce(array(select jsonb_array_elements_text(e -> 'rename' -> 'namespace')), v.namespace),
             coalesce(e -> 'rename' ->> 'name', v.name), coalesce((e ->> 'drop')::boolean, false), r, seq
        from visible_table_names(r, seq - 1) v where v.table_id = t;
  end if;

  for x in select * from jsonb_array_elements(coalesce(e -> 'fields', '[]')) loop
    if coalesce((x ->> 'new')::boolean, false) and not e ? 'create' and (
         (x ->> 'field_id')::int > (select last_field_id from table_counter where table_id = t)
         or exists (select 1 from field f where f.table_id = t and f.field_id = (x ->> 'field_id')::int)) then
      perform refuse_invalid('ID-5', format('field id %s was not drawn for a new field', x ->> 'field_id'));
    end if;
    update field set end_seq = seq
     where table_id = t and field_id = (x ->> 'field_id')::int and ref_id = r and end_seq = open;
    insert into field (table_id, field_id, parent_field_id, position, name, type, required, initial_default,
                       write_default, doc, dropped, ref_id, begin_seq)
    values (t, (x ->> 'field_id')::int, (x ->> 'parent_field_id')::int, (x ->> 'position')::int, x ->> 'name',
            x ->> 'type', coalesce((x ->> 'required')::boolean, false), x -> 'initial_default',
            x -> 'write_default', x ->> 'doc', false, r, seq);
  end loop;
  -- JSON null defaults are SQL NULL.
  update field set initial_default = nullif(initial_default, 'null'), write_default = nullif(write_default, 'null')
   where table_id = t and ref_id = r and begin_seq = seq;

  for fid in select jsonb_array_elements_text(coalesce(e -> 'drop_fields', '[]'))::bigint loop
    update field set end_seq = seq where table_id = t and field_id = fid and ref_id = r and end_seq = open;
    insert into field (table_id, field_id, parent_field_id, position, name, type, required, initial_default,
                       write_default, doc, dropped, ref_id, begin_seq)
      select t, v.field_id, v.parent_field_id, v.position, v.name, v.type, v.required, v.initial_default,
             v.write_default, v.doc, true, r, seq
        from visible_fields(r, t, seq - 1) v where v.field_id = fid;
  end loop;

  for x in select jsonb_build_object('k', k, 'v', v) from jsonb_each(coalesce(e -> 'properties', '{}')) p(k, v) loop
    update table_property set end_seq = seq where table_id = t and key = x ->> 'k' and ref_id = r and end_seq = open;
    insert into table_property values (t, x ->> 'k', x ->> 'v', jsonb_typeof(x -> 'v') = 'null', r, seq);
  end loop;

  if e ? 'partition_spec' and not e ? 'create' then
    update partition_spec set end_seq = seq where table_id = t and ref_id = r and end_seq = open;
    insert into partition_spec values (t, next_id(t, 'spec'), e -> 'partition_spec', r, seq);
  end if;
  if e ? 'sort_order' and not e ? 'create' then
    update sort_order set end_seq = seq where table_id = t and ref_id = r and end_seq = open;
    insert into sort_order values (t, next_id(t, 'sort_order'), e -> 'sort_order', r, seq);
  end if;
  if e ? 'cluster_key' then
    update cluster_key set end_seq = seq where table_id = t and ref_id = r and end_seq = open;
    insert into cluster_key values (t, array(select y::int from jsonb_array_elements_text(e -> 'cluster_key') y), r, seq);
  end if;
end $$;

-- The first rule a commit's own rows break, or NULL (CMT-9): only what the commit wrote is checked.
create function @schema@.commit_problem(r int, seq bigint, adds file_add[], ids bigint[], vecs vector_set[],
                                         defined uuid[]) returns text language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
declare
  latest constant bigint := 9223372036854775806;
  problem text;
begin
  -- §6, §7: the definitions of the tables whose definition changed.
  select format('%s', e) into problem from unnest(defined) t, table_errors(r, t, latest) e limit 1;
  if problem is not null then return problem; end if;

  with a as (select f, fid from (select adds[n] as f, ids[n] as fid from generate_subscripts(adds, 1) n) u),
  -- Each table's cluster key, once.
  ck as (select t, k.field_ids from (select distinct (f).table_id as t from a) x,
                lateral visible_cluster_key(r, x.t, latest) k)
  select p into problem from (
    -- STA-2: distinct statistics fields, NaN counts only for float and double.
    select format('STA-2: file %s repeats a statistics field', a.fid) as p from a
     where (select count(distinct i) from unnest((a.f).stat_field_ids) i) < cardinality((a.f).stat_field_ids)
    union all
    select format('STA-2: file %s has a NaN count for field %s', a.fid, x.field) from a,
           unnest((a.f).stat_field_ids, (a.f).nan_count) x(field, n)
     where x.n is not null
       and coalesce((select base_type(y.type) from visible_fields(r, (a.f).table_id, latest) y
                      where y.field_id = x.field), '') not in ('float', 'double')
    -- STA-4: the file's spec, with the arity of its partition, and its sort order exist.
    union all
    select format('STA-4: file %s has partition %s for spec %s', a.fid, (a.f).partition, (a.f).spec_id) from a
     where not exists (select 1 from partition_spec p
                        where p.table_id = (a.f).table_id and p.spec_id = coalesce((a.f).spec_id, 0)
                          and jsonb_array_length(p.fields) = jsonb_array_length(coalesce((a.f).partition, '[]')))
    union all
    select format('STA-4: file %s has sort order %s', a.fid, (a.f).sort_order_id) from a
     where (a.f).sort_order_id <> 0
       and not exists (select 1 from sort_order o
                        where o.table_id = (a.f).table_id and o.sort_order_id = (a.f).sort_order_id)
    -- STA-3: bounds for every field of the cluster key.
    union all
    select format('STA-3: file %s has no statistics for cluster key field %s', a.fid, k) from a
      join ck on ck.t = (a.f).table_id, unnest(ck.field_ids) k
     where not k = any (coalesce((a.f).stat_field_ids, '{}'))
    -- INL-1: a path or inline bytes, of the declared size, under the table's limit.
    union all
    select format('INL-1: file %s has no path and no inline content, or both', a.fid) from a
     where ((a.f).path is null) <> ((a.f).content is not null)
    union all
    select format('INL-1: inline file %s has file_size %s for %s bytes', a.fid, (a.f).file_size, length((a.f).content))
      from a
     where (a.f).content is not null
       and ((a.f).file_size <> length((a.f).content)
            or length((a.f).content) > int_property(r, (a.f).table_id, latest, 'inline.max-bytes', 65536))
    -- FTR-1: a stored footer is the footer.
    union all
    select format('FTR-1: the footer of file %s has %s bytes, not %s', a.fid, length((a.f).footer), (a.f).footer_size)
      from a
     where (a.f).footer is not null and length((a.f).footer) <> (a.f).footer_size
    -- RET-7: no file whose intent expired.
    union all
    select format('RET-7: the intent of %s expired at %s', i.path, i.expires_at) from a
      join intent i on i.path = (a.f).path
     where i.expires_at < now()
  ) z limit 1;
  if problem is not null then return problem; end if;

  -- DV-2 and DV-4, on the vectors set now that apply: no more rows than the file has, no fewer than they replace.
  select p into problem from (
    select format('DV-2: the vector of file %s deletes more rows than it has', v.file_id) as p
      from unnest(vecs) v
     where (file_dv(r, v.table_id, latest, v.file_id)).begin_seq = seq
       and v.cardinality > (visible_file(r, v.table_id, latest, v.file_id)).record_count
    union all
    select format('DV-4: the vector of file %s drops positions of the one it replaces', v.file_id)
      from unnest(vecs) v
     where (file_dv(r, v.table_id, latest, v.file_id)).begin_seq = seq
       and v.cardinality < (file_dv(r, v.table_id, seq - 1, v.file_id)).cardinality
  ) z limit 1;
  return problem;
end $$;

create function @schema@.commit(
  ref_id int,
  operation text,
  files @schema@.file_add[] default '{}',
  removed @schema@.file_ref[] default '{}',
  vectors @schema@.vector_set[] default '{}',
  expected @schema@.table_expect[] default '{}',
  changes jsonb default null,
  author text default null,
  actor text default null,
  app_id text default null,
  app_version bigint default null,
  message text default null,
  definition text default null,
  namespaces jsonb default null)
returns @schema@.commit_result language plpgsql
set search_path = @schema@, pg_catalog, pg_temp as $$
#variable_conflict use_column
declare
  latest constant bigint := 9223372036854775806;
  open constant bigint := 9223372036854775807;
  -- The parameters, by position: their names are the API and match the columns they fill.
  r int := $1;
  op text := $2;
  adds file_add[] := coalesce($3, '{}');
  rems file_ref[] := coalesce($4, '{}');
  vecs vector_set[] := coalesce($5, '{}');
  exps table_expect[] := coalesce($6, '{}');
  defs jsonb := coalesce($7, '{}');
  who text := coalesce($8, session_user);
  via text := coalesce($9, coalesce($8, session_user));
  app text := $10;
  ver bigint := $11;
  spaces jsonb := coalesce($14, '[]');
  sp jsonb;
  ns text[];
  nv namespace;
  st text;
  tables uuid[];
  created uuid[];
  defined uuid[];
  rf ref;
  stored idempotency;
  seq bigint;
  ids bigint[];
  t uuid;
  problem text;
begin
  -- LAY-5: a writer of revision 0.1 commits to writer_version 1 only.
  if coalesce((select m.value from meta m where m.key = 'writer_version'), '0') <> '1' then
    raise exception 'metalake unsupported: writer_version %',
      coalesce((select m.value from meta m where m.key = 'writer_version'), '0') using errcode = '0A000';
  end if;

  -- CMT-5: idempotency.
  if app is not null then
    if ver is null or ver < 1 then
      perform refuse_invalid('CMT-5', 'app_version must be positive');
    end if;
    insert into idempotency values (r, app, 0, 0) on conflict do nothing;
    select * into stored from idempotency x where x.ref_id = r and x.app_id = app for update;
    if ver <= stored.version then
      return row(stored.commit_seq, false)::commit_result;
    end if;
  end if;

  tables := array(select distinct x from (
      select (f).table_id from unnest(adds) f union all select (x).table_id from unnest(rems) x
      union all select (v).table_id from unnest(vecs) v union all select (e).table_id from unnest(exps) e
      union all select k::uuid from jsonb_object_keys(defs) k) y(x) order by 1);
  created := array(select k::uuid from jsonb_each(defs) d(k, e) where e ? 'create');
  defined := array(select k::uuid from jsonb_object_keys(defs) k);

  -- CMT-4: the ref, shared; the names the commit gives, in their lock's order; then the heads of the existing tables,
  -- in table_id order.
  perform lock_ref_shared(r);
  -- NS-2, NS-3: the namespaces the commit changes, exclusively; those its new names and new namespaces live in,
  -- shared; in one order, exclusive first where both.
  perform lock_namespace(r, y.ns, y.exclusive)
     from (select z.ns, bool_or(z.exclusive) as exclusive from (
             select array(select jsonb_array_elements_text(e -> 'namespace')) as ns, true as exclusive
               from jsonb_array_elements(spaces) e
             union all
             select array(select jsonb_array_elements_text(n -> 'namespace')), false
               from jsonb_each(defs) d(k, e), lateral (select coalesce(e -> 'create', e -> 'rename') as n) w
              where n is not null
             union all
             select a.ns[1:cardinality(a.ns) - 1], false
               from (select array(select jsonb_array_elements_text(e -> 'namespace')) as ns
                       from jsonb_array_elements(spaces) e) a
              where cardinality(a.ns) > 1) z
            group by z.ns) y
    order by hashtext(r || ' ' || array_to_string(y.ns, '.'));
  perform lock_name(r, x.ns, x.name)
     from (select array(select jsonb_array_elements_text(n -> 'namespace')) as ns, n ->> 'name' as name
             from jsonb_each(defs) d(k, e), lateral (select coalesce(e -> 'create', e -> 'rename') as n) y
            where n is not null) x
    order by hashtext(r || ' ' || array_to_string(x.ns, '.') || ' ' || x.name);
  select * into rf from ref x where x.ref_id = r;
  if rf.ref_id is null or rf.deleted or rf.kind <> 'branch' then
    perform refuse_invalid('CMT-4', format('ref %s does not take commits', r));
  end if;
  foreach t in array tables loop
    continue when t = any (created);
    if not table_visible(r, t, latest) then
      perform refuse_conflict('CMT-7', format('table %s is not visible on ref %s', t, r));
    end if;
    -- A ref's first commit to a table it inherits starts its head from what it sees.
    perform 1 from head h where h.ref_id = r and h.table_id = t for update;
    if not found then
      insert into head values (r, t, snapshot_at(r, t, latest)) on conflict do nothing;
      perform 1 from head h where h.ref_id = r and h.table_id = t for update;
    end if;
  end loop;

  -- CMT-6, CMT-7, DV-4: requirements and conflicts, against the current state under the locks.
  foreach t in array defined loop
    perform check_definition(r, t, defs -> t::text);
  end loop;
  if exists (select 1 from unnest(exps) e join head h on h.ref_id = r and h.table_id = e.table_id
              where not e.table_id = any (created) and e.snapshot_id is distinct from h.snapshot_id) then
    perform refuse_conflict('CMT-6', 'a table is no longer at its expected snapshot');
  end if;
  if exists (select 1 from unnest(rems) x where not file_visible(r, x.table_id, latest, x.file_id)) then
    perform refuse_conflict('CMT-7', 'a removed file is not visible');
  end if;
  if exists (select 1 from unnest(vecs) v
              where not (v.base_seq is null and exists (select 1 from unnest(adds) f where (f).file_id = v.file_id))
                and not file_visible(r, v.table_id, latest, v.file_id)) then
    perform refuse_conflict('CMT-7', 'a file whose vector is set is not visible');
  end if;
  if exists (select 1 from unnest(vecs) v
              where not (v.base_seq is null and exists (select 1 from unnest(adds) f where (f).file_id = v.file_id))
                and (file_dv(r, v.table_id, latest, v.file_id)).begin_seq is distinct from v.base_seq) then
    perform refuse_conflict('DV-4', 'a vector is not the one the commit started from');
  end if;
  if op = 'replace' and (cardinality(vecs) > 0 or defs <> '{}') then
    perform refuse_invalid('CMT-11', 'a replace commit only removes and adds files');
  end if;

  -- CMT-11: a replace keeps the live rows of every table it touches.
  if op = 'replace' then
    select format('table %s: removes %s live rows and adds %s', t, removed, added) into problem
      from (select t, coalesce((select sum((visible_file(r, t, latest, x.id)).record_count
                                           - coalesce((file_dv(r, t, latest, x.id)).cardinality, 0))
                                  from (select distinct (y).file_id from unnest(rems) y where (y).table_id = t) x(id)), 0)
                      as removed,
                   coalesce((select sum((f).record_count) from unnest(adds) f where (f).table_id = t), 0) as added
              from unnest(tables) t) z
     where removed <> added limit 1;
    if problem is not null then
      perform refuse_invalid('CMT-11', 'a replace ' || problem);
    end if;
  end if;

  -- NS-1 to NS-3: the namespaces the commit changes, each once, against the current state under their locks.
  if jsonb_array_length(spaces) <> (select count(distinct y -> 'namespace') from jsonb_array_elements(spaces) y) then
    perform refuse_invalid('NS-1', 'a commit names a namespace twice');
  end if;
  for sp in select * from jsonb_array_elements(spaces) loop
    ns := array(select jsonb_array_elements_text(sp -> 'namespace'));
    if cardinality(ns) = 0 or array_position(ns, '') is not null then
      perform refuse_invalid('NS-1', format('%s is not a namespace', sp -> 'namespace'));
    end if;
    nv := namespace_version(r, ns, latest);
    if coalesce((sp ->> 'create')::boolean, false) then
      if nv.namespace is not null and not nv.dropped then
        perform refuse_conflict('NS-2', format('namespace %s exists', ns));
      end if;
    elsif nv.namespace is null then
      perform refuse_invalid('NS-2', format('namespace %s does not exist', ns));
    elsif nv.dropped then
      perform refuse_conflict('NS-2', format('namespace %s is dropped', ns));
    end if;
  end loop;

  -- CMT-3: the commit.
  seq := nextval('commit_seq_gen');
  insert into commit values (seq, r, pg_current_xact_id(), null, who, via, op, app, ver, $13, $12);

  -- CMT-8: namespaces first, so that tables of the same commit may go into them.
  for sp in select * from jsonb_array_elements(spaces) loop
    ns := array(select jsonb_array_elements_text(sp -> 'namespace'));
    if coalesce((sp ->> 'create')::boolean, false) or coalesce((sp ->> 'drop')::boolean, false) then
      update namespace n set end_seq = seq where n.namespace = ns and n.ref_id = r and n.end_seq = open;
      insert into namespace values (ns, coalesce((sp ->> 'drop')::boolean, false), r, seq);
    end if;
    update namespace_property n set end_seq = seq
      from jsonb_each(coalesce(sp -> 'properties', '{}')) p(k, v)
     where n.namespace = ns and n.key = p.k and n.ref_id = r and n.end_seq = open;
    insert into namespace_property
    select ns, p.k, p.v #>> '{}', jsonb_typeof(p.v) = 'null', r, seq
      from jsonb_each(coalesce(sp -> 'properties', '{}')) p(k, v);
  end loop;

  -- CMT-8: the rows. Definitions first, so that the tables they create exist for their files.
  foreach t in array defined loop
    perform apply_definition(r, t, seq, defs -> t::text);
  end loop;

  ids := array(select coalesce(a.file_id, nextval('file_id_gen')) from unnest(adds) with ordinality a order by a.ordinality);
  insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, sort_order_id, record_count,
                         file_size, footer_size, first_row_id, stat_field_ids, lower, upper, null_count, nan_count,
                         value_count)
  select (f).table_id, fid, r, seq, (f).path, coalesce((f).spec_id, 0), coalesce((f).partition, '[]'),
         (f).sort_order_id, (f).record_count, (f).file_size, (f).footer_size, (f).first_row_id,
         coalesce((f).stat_field_ids, '{}'), coalesce((f).lower, '{}'), coalesce((f).upper, '{}'),
         coalesce((f).null_count, '{}'), coalesce((f).nan_count, '{}'), (f).value_count
    from (select adds[n] as f, ids[n] as fid from generate_subscripts(adds, 1) n) u;
  insert into inline_file select (f).table_id, fid, (f).content from (select adds[n] as f, ids[n] as fid from generate_subscripts(adds, 1) n) u where (f).content is not null;
  insert into file_footer select (f).table_id, fid, (f).footer from (select adds[n] as f, ids[n] as fid from generate_subscripts(adds, 1) n) u where (f).footer is not null;

  with closed as (
    update data_file d set end_seq = seq from unnest(rems) x
     where d.table_id = x.table_id and d.file_id = x.file_id and d.ref_id = r and d.end_seq = open
    returning d.table_id, d.file_id)
  insert into tombstone (table_id, file_id, ref_id, begin_seq)
  select distinct x.table_id, x.file_id, r, seq from unnest(rems) x
   where not exists (select 1 from closed c where c.table_id = x.table_id and c.file_id = x.file_id);
  update deletion_vector d set end_seq = seq from unnest(rems) x
   where d.table_id = x.table_id and d.file_id = x.file_id and d.ref_id = r and d.end_seq = open;

  update deletion_vector d set end_seq = seq from unnest(vecs) v
   where d.table_id = v.table_id and d.file_id = v.file_id and d.ref_id = r and d.end_seq = open;
  insert into deletion_vector (table_id, file_id, path, content_offset, content_size, cardinality, inline, ref_id,
                               begin_seq)
  select v.table_id, v.file_id, v.path, v.content_offset, coalesce(v.content_size, length(v.inline)), v.cardinality,
         v.inline, r, seq
    from unnest(vecs) v;

  -- CMT-9: what the commit wrote is valid.
  problem := commit_problem(r, seq, adds, ids, vecs, defined);
  if problem is not null then
    perform refuse_invalid(split_part(problem, ':', 1), problem);
  end if;
  -- NS-2, NS-3: the tables it created or renamed, and the namespaces it changed, once applied.
  if cardinality(created) > 0 or jsonb_array_length(spaces) > 0
     or exists (select 1 from jsonb_each(defs) d(k, e) where e ? 'rename') then
    select z.state, z.problem into st, problem from namespace_errors(r, latest,
      array(select k::uuid from jsonb_each(defs) d(k, e) where e ? 'create' or e ? 'rename'),
      (select coalesce(jsonb_agg(e -> 'namespace'), '[]') from jsonb_array_elements(spaces) e)) z
     order by z.state desc limit 1;
    if problem is not null then
      if st = '40001' then
        perform refuse_conflict(split_part(problem, ':', 1), problem);
      else
        perform refuse_invalid(split_part(problem, ':', 1), problem);
      end if;
    end if;
  end if;

  -- CMT-1: a snapshot per table, and its head.
  insert into snapshot
  select t, seq, r, h.snapshot_id,
         case when t = any (defined) and (defs -> t::text ? 'fields' or defs -> t::text ? 'drop_fields'
                                          or defs -> t::text ? 'create') then seq
              else ps.schema_seq end,
         -- The defaults change only with the definition: otherwise they are the previous snapshot's.
         case when t = any (defined) or ps.snapshot_id is null then (select spec_id from default_spec(r, t, latest))
              else ps.spec_id end,
         case when t = any (defined) or ps.snapshot_id is null
              then coalesce((select sort_order_id from default_sort_order(r, t, latest)), 0)
              else ps.sort_order_id end,
         jsonb_build_object('operation', op,
           'added-data-files', (select count(*) from unnest(adds) f where (f).table_id = t),
           'deleted-data-files', (select count(*) from unnest(rems) x where (x).table_id = t),
           'added-records', coalesce((select sum((f).record_count) from unnest(adds) f where (f).table_id = t), 0),
           'deleted-records', case when cardinality(rems) = 0 then 0 else coalesce((select sum(d.record_count) from data_file d
                                         where d.table_id = t
                                           and d.file_id in (select (x).file_id from unnest(rems) x where (x).table_id = t)
                                           and d.begin_seq < seq and d.end_seq >= seq
                                           and d.ref_id in (select ch.ref_id from chain(r, seq - 1) ch)), 0) end)
    from unnest(tables) t left join head h on h.ref_id = r and h.table_id = t
    left join snapshot ps on ps.table_id = t and ps.snapshot_id = h.snapshot_id;
  insert into head select r, t, seq from unnest(tables) t
    on conflict (ref_id, table_id) do update set snapshot_id = excluded.snapshot_id;

  -- CMT-10 and CMT-5: intents, idempotency, notification.
  delete from intent where path in (select (f).path from unnest(adds) f);
  if app is not null then
    update idempotency x set version = ver, commit_seq = seq where x.ref_id = r and x.app_id = app;
  end if;
  perform pg_notify('metalake', current_schema() || ':' || seq);
  return row(seq, true)::commit_result;
end $$;
