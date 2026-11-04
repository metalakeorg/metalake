-- MetaLake v0 · reference behaviour · commits (§9).
--
-- It defines the right answer, not the fast one.

-- RET-5 (0.2): expiring and a commit exclude each other on a path, so that a path is never queued while a commit
-- adds it: expiring holds it exclusively, a commit shared. The lock is the path's bucket, one of 1,024, so that a
-- batch holds a bounded number of locks.
create function path_bucket(path text) returns int language sql immutable as $$
  select hashtext(path) & 1023;
$$;

create function path_lock(path text, exclusive boolean) returns void language plpgsql as $$
begin
  if exclusive then
    perform pg_advisory_xact_lock(hashtext(current_schema() || ' paths'), path_bucket(path));
  else
    perform pg_advisory_xact_lock_shared(hashtext(current_schema() || ' paths'), path_bucket(path));
  end if;
end $$;

-- CMT-2: the snapshot of t at (r, s), or NULL.
create function snapshot_at(r int, t uuid, s bigint) returns bigint language sql stable as $$
  select x.id from chain(r, s) c
    cross join lateral (select max(snapshot_id) as id from snapshot
                         where table_id = t and ref_id = c.ref_id and snapshot_id between c.since and c.seq) x
   where x.id is not null
   order by c.level limit 1;
$$;

-- CMT-10: a writer's intent to upload `path`.
create function register_intent(path text, t uuid, r int, ttl interval) returns void language sql as $$
  insert into intent values (path, t, r, now() + ttl);
$$;

create function conflict(what text) returns void language plpgsql as $$
begin
  raise exception 'metalake conflict: %', what using errcode = '40001';
end $$;

create function invalid(what text) returns void language plpgsql as $$
begin
  raise exception 'metalake invalid: %', what using errcode = '22023';
end $$;

-- The bytea array of a JSON array of hex strings, NULL elements kept.
create function hex_array(j jsonb) returns bytea[] language sql immutable as $$
  select coalesce(array_agg(decode(x, 'hex') order by n), '{}') from jsonb_array_elements_text(j) with ordinality e(x, n);
$$;

create function bigint_array(j jsonb) returns bigint[] language sql immutable as $$
  select coalesce(array_agg(x::bigint order by n), '{}') from jsonb_array_elements_text(j) with ordinality e(x, n);
$$;

-- The reference commit (§9). It is not an API or a wire protocol: the protocol is the text of §9, and an
-- implementation exposes whatever interface it chooses. `c` is one JSON object:
--
--   {"ref": 1, "author": "...", "actor": "...", "operation": "append", "app_id": null, "app_version": null,
--    "definition": null, "message": null,
--    "tables": [{"table_id": "<uuid>", "expected_snapshot": null,
--                "create": {"namespace": ["db"], "name": "t", "location": "gs://..."},
--                "rename": {"namespace": ["db"], "name": "u"}, "drop": true,
--                "add": [{"file_id": null, "path": "...", "spec_id": 0, "partition": [], "sort_order_id": null,
--                         "record_count": 0, "file_size": 0, "footer_size": 0, "first_row_id": null,
--                         "stat_field_ids": [], "lower": ["<hex>"], "upper": ["<hex>"], "null_count": [],
--                         "nan_count": [], "value_count": null, "inline": "<hex>", "footer": "<hex>"}],
--                "remove": [<file_id>],
--                "dvs": [{"file_id": 1, "base_seq": null, "path": "...", "content_offset": 4, "content_size": 0,
--                         "cardinality": 0, "inline": "<hex>"}],
--                "fields": [{"field_id": 1, "new": true, "parent_field_id": null, "position": 1, "name": "id",
--                            "type": "long", "required": true, "initial_default": null, "write_default": null,
--                            "doc": null}],
--                "drop_fields": [<field_id>],
--                "properties": {"key": "value or null to remove"},
--                "partition_spec": [<partition field>], "sort_order": [<sort field>], "cluster_key": [<field_id>]}],
--    "namespaces": [{"namespace": ["db"], "create": true, "drop": true,
--                    "properties": {"key": "value or null to remove"}}]}
--
-- Every key but "ref", "author", "actor", "operation" and "tables" is optional; "namespaces" is revision 0.1.
-- A commit MAY carry namespaces only, and then writes no snapshot. New field ids are drawn with
-- next_id() beforehand, except in a created table. Returns the commit_seq, or the stored one if an idempotent
-- commit was already applied.
create function commit(c jsonb) returns bigint language plpgsql as $$
declare
  latest constant bigint := 9223372036854775806;
  r int := (c ->> 'ref')::int;
  app text := c ->> 'app_id';
  ver bigint := (c ->> 'app_version')::bigint;
  entries jsonb := coalesce(c -> 'tables', '[]');
  e jsonb;
  x jsonb;
  t uuid;
  stored idempotency;
  rf ref;
  seq bigint;
  cur bigint;
  old field;
  fid bigint;
  n bigint;
  problems text;
  removed_rows bigint;
  spaces jsonb := coalesce(c -> 'namespaces', '[]');
  ns text[];
  nv namespace;
  st text;
begin
  perform writable();
  -- CMT-5: idempotency.
  if app is not null then
    if ver is null or ver < 1 then
      perform invalid('app_version must be positive');
    end if;
    insert into idempotency values (r, app, 0, 0) on conflict do nothing;
    select * into stored from idempotency where ref_id = r and app_id = app for update;
    if ver <= stored.version then
      return stored.commit_seq;
    end if;
  end if;

  -- CMT-4: the ref, then the heads of the existing tables, in table_id order.
  if jsonb_array_length(spaces) > 0 or exists (select 1 from jsonb_array_elements(entries) y
              where y ? 'create' or y ? 'rename' or coalesce((y ->> 'drop')::boolean, false)) then
    select * into rf from ref where ref_id = r for update;
  else
    select * into rf from ref where ref_id = r for key share;
  end if;
  if rf.ref_id is null or rf.deleted or rf.kind <> 'branch' then
    perform invalid(format('ref %s does not take commits', r));
  end if;
  for t in select (y ->> 'table_id')::uuid from jsonb_array_elements(entries) y where not y ? 'create' order by 1 loop
    if not exists (select 1 from visible_table_names(r, latest) v where v.table_id = t) then
      perform conflict(format('table %s is not visible on ref %s', t, r));
    end if;
    insert into head values (r, t, snapshot_at(r, t, latest)) on conflict do nothing;
    perform 1 from head h where h.ref_id = r and h.table_id = t for update;
  end loop;

  -- CMT-6 and CMT-7: requirements and conflicts, against the current state under the locks.
  for e in select * from jsonb_array_elements(entries) loop
    t := (e ->> 'table_id')::uuid;
    if e ? 'create' then
      if exists (select 1 from tbl where table_id = t) then
        perform invalid(format('table id %s exists', t));
      end if;
      if table_by_name(r, array(select jsonb_array_elements_text(e -> 'create' -> 'namespace')), e -> 'create' ->> 'name', latest) is not null then
        perform conflict(format('the name %s is taken', e -> 'create'));
      end if;
      continue;
    end if;
    if e ? 'expected_snapshot' and (e ->> 'expected_snapshot')::bigint
         is distinct from (select h.snapshot_id from head h where h.ref_id = r and h.table_id = t) then
      perform conflict(format('table %s is no longer at snapshot %s', t, e ->> 'expected_snapshot'));
    end if;
    if e ? 'rename' and coalesce(table_by_name(r, array(select jsonb_array_elements_text(e -> 'rename' -> 'namespace')),
                                               e -> 'rename' ->> 'name', latest), t) <> t then
      perform conflict(format('the name %s is taken', e -> 'rename'));
    end if;
    for fid in select jsonb_array_elements_text(coalesce(e -> 'remove', '[]'))::bigint loop
      if not file_visible(r, t, latest, fid) then
        perform conflict(format('file %s is not visible', fid));
      end if;
    end loop;
    for x in select * from jsonb_array_elements(coalesce(e -> 'dvs', '[]')) loop
      -- A vector may also delete rows of a file this commit adds.
      continue when x ->> 'base_seq' is null and (x ->> 'file_id')::bigint in (
        select (y ->> 'file_id')::bigint from jsonb_array_elements(coalesce(e -> 'add', '[]')) y);
      if not file_visible(r, t, latest, (x ->> 'file_id')::bigint) then
        perform conflict(format('file %s is not visible', x ->> 'file_id'));
      end if;
      if (file_dv(r, t, latest, (x ->> 'file_id')::bigint)).begin_seq
         is distinct from (x ->> 'base_seq')::bigint then
        perform conflict(format('the deletion vector of file %s is not the one at %s', x ->> 'file_id', x ->> 'base_seq'));
      end if;
    end loop;
    if c ->> 'operation' = 'replace'
       and exists (select 1 from jsonb_object_keys(e) k where k not in ('table_id', 'expected_snapshot', 'add', 'remove')) then
      perform invalid('a replace commit only removes and adds files');
    end if;
    for x in select * from jsonb_array_elements(coalesce(e -> 'fields', '[]')) loop
      if coalesce((x ->> 'new')::boolean, false) then
        if exists (select 1 from visible_fields(r, t, latest) v
                    where v.parent_field_id is not distinct from (x ->> 'parent_field_id')::int and v.name = x ->> 'name') then
          perform conflict(format('a field named %s exists', x ->> 'name'));
        end if;
      else
        select * into old from visible_fields(r, t, latest) v where v.field_id = (x ->> 'field_id')::int;
        if old.field_id is null then
          perform conflict(format('field %s is not visible', x ->> 'field_id'));
        end if;
        if not field_type_change_allowed(t, old.field_id, old.type, x ->> 'type') then
          perform conflict(format('field %s cannot change from %s to %s', old.field_id, old.type, x ->> 'type'));
        end if;
      end if;
    end loop;
    for fid in select jsonb_array_elements_text(coalesce(e -> 'drop_fields', '[]'))::bigint loop
      if not exists (select 1 from visible_fields(r, t, latest) v where v.field_id = fid) then
        perform conflict(format('field %s is not visible', fid));
      end if;
    end loop;
  end loop;

  -- NS-1 to NS-3 (revision 0.1): the namespaces the commit changes, each once, against the current state.
  if jsonb_array_length(spaces) <> (select count(distinct y -> 'namespace') from jsonb_array_elements(spaces) y) then
    perform invalid('NS-1: a commit names a namespace twice');
  end if;
  for x in select * from jsonb_array_elements(spaces) loop
    ns := array(select jsonb_array_elements_text(x -> 'namespace'));
    if cardinality(ns) = 0 or array_position(ns, '') is not null then
      perform invalid(format('NS-1: %s is not a namespace', x -> 'namespace'));
    end if;
    nv := namespace_version(r, ns, latest);
    if coalesce((x ->> 'create')::boolean, false) then
      if nv.namespace is not null and not nv.dropped then
        perform conflict(format('namespace %s exists', ns));
      end if;
    elsif nv.namespace is null then
      perform invalid(format('NS-2: namespace %s does not exist', ns));
    elsif nv.dropped then
      perform conflict(format('namespace %s is dropped', ns));
    end if;
  end loop;

  -- CMT-3: the commit.
  seq := nextval('commit_seq_gen');
  insert into commit values (seq, r, pg_current_xact_id(), null, c ->> 'author', c ->> 'actor', c ->> 'operation',
                             app, ver, c ->> 'definition', c ->> 'message');

  -- CMT-8: the namespaces first, so that tables of the same commit may go into them.
  for x in select * from jsonb_array_elements(spaces) loop
    ns := array(select jsonb_array_elements_text(x -> 'namespace'));
    if coalesce((x ->> 'create')::boolean, false) or coalesce((x ->> 'drop')::boolean, false) then
      update namespace set end_seq = seq where namespace = ns and ref_id = r and end_seq = 9223372036854775807;
      insert into namespace values (ns, coalesce((x ->> 'drop')::boolean, false), r, seq);
    end if;
    for e in select jsonb_build_object('k', k, 'v', v) from jsonb_each(coalesce(x -> 'properties', '{}')) p(k, v) loop
      update namespace_property set end_seq = seq
       where namespace = ns and key = e ->> 'k' and ref_id = r and end_seq = 9223372036854775807;
      insert into namespace_property values (ns, e ->> 'k', e ->> 'v', jsonb_typeof(e -> 'v') = 'null', r, seq);
    end loop;
  end loop;

  -- CMT-8: the rows.
  for e in select * from jsonb_array_elements(entries) loop
    t := (e ->> 'table_id')::uuid;
    cur := (select h.snapshot_id from head h where h.ref_id = r and h.table_id = t);
    removed_rows := (select coalesce(sum((visible_file(r, t, latest, y.id)).record_count
                                         - coalesce((file_dv(r, t, latest, y.id)).cardinality, 0)), 0)
                       from (select distinct jsonb_array_elements_text(coalesce(e -> 'remove', '[]'))::bigint) y(id));

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
      update table_name set end_seq = seq where table_id = t and ref_id = r and end_seq = 9223372036854775807;
      insert into table_name
        select t, coalesce(array(select jsonb_array_elements_text(e -> 'rename' -> 'namespace')), v.namespace),
               coalesce(e -> 'rename' ->> 'name', v.name), coalesce((e ->> 'drop')::boolean, false), r, seq
          from visible_table_names(r, seq - 1) v where v.table_id = t;
    end if;

    for x in select * from jsonb_array_elements(coalesce(e -> 'add', '[]')) loop
      fid := coalesce((x ->> 'file_id')::bigint, nextval('file_id_gen'));
      insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, sort_order_id,
                             record_count, file_size, footer_size, first_row_id, stat_field_ids, lower, upper,
                             null_count, nan_count, value_count)
      values (t, fid, r, seq, x ->> 'path',
              coalesce((x ->> 'spec_id')::int, 0), coalesce(x -> 'partition', '[]'), (x ->> 'sort_order_id')::int,
              (x ->> 'record_count')::bigint, (x ->> 'file_size')::bigint, (x ->> 'footer_size')::int,
              (x ->> 'first_row_id')::bigint,
              array(select y::int from jsonb_array_elements_text(coalesce(x -> 'stat_field_ids', '[]')) y),
              hex_array(coalesce(x -> 'lower', '[]')), hex_array(coalesce(x -> 'upper', '[]')),
              bigint_array(coalesce(x -> 'null_count', '[]')), bigint_array(coalesce(x -> 'nan_count', '[]')),
              case when jsonb_typeof(x -> 'value_count') = 'array' then bigint_array(x -> 'value_count') end);
      if x ? 'inline' then
        insert into inline_file values (t, fid, decode(x ->> 'inline', 'hex'));
      end if;
      if x ? 'footer' then
        insert into file_footer values (t, fid, decode(x ->> 'footer', 'hex'));
      end if;
    end loop;

    for fid in select jsonb_array_elements_text(coalesce(e -> 'remove', '[]'))::bigint loop
      update data_file set end_seq = seq
       where table_id = t and file_id = fid and ref_id = r and end_seq = 9223372036854775807;
      if not found then
        insert into tombstone values (t, fid, r, seq);
      end if;
      update deletion_vector set end_seq = seq
       where table_id = t and file_id = fid and ref_id = r and end_seq = 9223372036854775807;
    end loop;

    for x in select * from jsonb_array_elements(coalesce(e -> 'dvs', '[]')) loop
      update deletion_vector set end_seq = seq
       where table_id = t and file_id = (x ->> 'file_id')::bigint and ref_id = r and end_seq = 9223372036854775807;
      insert into deletion_vector values (t, (x ->> 'file_id')::bigint, x ->> 'path', (x ->> 'content_offset')::bigint,
        coalesce((x ->> 'content_size')::bigint, length(decode(x ->> 'inline', 'hex'))), (x ->> 'cardinality')::bigint,
        decode(x ->> 'inline', 'hex'), r, seq);
    end loop;

    -- CMT-11: a replace keeps the live rows.
    if c ->> 'operation' = 'replace' and removed_rows <>
       coalesce((select sum((y ->> 'record_count')::bigint) from jsonb_array_elements(e -> 'add') y), 0) then
      perform invalid(format('a replace removes %s live rows and adds %s', removed_rows,
        coalesce((select sum((y ->> 'record_count')::bigint) from jsonb_array_elements(e -> 'add') y), 0)));
    end if;

    for x in select * from jsonb_array_elements(coalesce(e -> 'fields', '[]')) loop
      if coalesce((x ->> 'new')::boolean, false) and not e ? 'create' and (
           (x ->> 'field_id')::int > (select last_field_id from table_counter where table_id = t)
           or exists (select 1 from field f where f.table_id = t and f.field_id = (x ->> 'field_id')::int)) then
        perform invalid(format('field id %s was not drawn for a new field', x ->> 'field_id'));
      end if;
      update field set end_seq = seq
       where table_id = t and field_id = (x ->> 'field_id')::int and ref_id = r and end_seq = 9223372036854775807;
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
      update field set end_seq = seq
       where table_id = t and field_id = fid and ref_id = r and end_seq = 9223372036854775807;
      insert into field (table_id, field_id, parent_field_id, position, name, type, required, initial_default,
                         write_default, doc, dropped, ref_id, begin_seq)
        select t, v.field_id, v.parent_field_id, v.position, v.name, v.type, v.required, v.initial_default,
               v.write_default, v.doc, true, r, seq
          from visible_fields(r, t, seq - 1) v where v.field_id = fid;
    end loop;

    for x in select jsonb_build_object('k', k, 'v', v) from jsonb_each(coalesce(e -> 'properties', '{}')) p(k, v) loop
      update table_property set end_seq = seq
       where table_id = t and key = x ->> 'k' and ref_id = r and end_seq = 9223372036854775807;
      insert into table_property values (t, x ->> 'k', x ->> 'v', jsonb_typeof(x -> 'v') = 'null', r, seq);
    end loop;

    if e ? 'partition_spec' and not e ? 'create' then
      update partition_spec set end_seq = seq where table_id = t and ref_id = r and end_seq = 9223372036854775807;
      insert into partition_spec values (t, next_id(t, 'spec'), e -> 'partition_spec', r, seq);
    end if;
    if e ? 'sort_order' and not e ? 'create' then
      update sort_order set end_seq = seq where table_id = t and ref_id = r and end_seq = 9223372036854775807;
      insert into sort_order values (t, next_id(t, 'sort_order'), e -> 'sort_order', r, seq);
    end if;
    if e ? 'cluster_key' then
      update cluster_key set end_seq = seq where table_id = t and ref_id = r and end_seq = 9223372036854775807;
      insert into cluster_key values (t, array(select y::int from jsonb_array_elements_text(e -> 'cluster_key') y), r, seq);
    end if;

    -- RET-5: the paths it adds, held against expiring before RET-8 looks at the queue.
    perform path_lock(z.path, false) from (
      select d.path from data_file d where d.table_id = t and d.ref_id = r and d.begin_seq = seq
      union select x.path from deletion_vector x
       where x.table_id = t and x.ref_id = r and x.begin_seq = seq and x.path is not null) z
     order by z.path;

    -- CMT-9: the result is valid.
    problems := (select string_agg(p, '; ') from (
      select table_errors(r, t, latest) as p
      union all select file_errors(r, t, latest, seq)
      union all select dv_errors(r, t, latest, seq)
      union all select inline_errors(r, t, latest, seq)
      union all select partition_errors(r, t, latest, seq)
      union all select format('RET-8: %s is queued for deletion', d.path)
        from data_file d join deletion_queue q on q.path = d.path
       where d.table_id = t and d.ref_id = r and d.begin_seq = seq
      union all select format('RET-8: %s is queued for deletion', x.path)
        from deletion_vector x join deletion_queue q on q.path = x.path
       where x.table_id = t and x.ref_id = r and x.begin_seq = seq
      union all select format('RET-7: the intent of %s expired at %s', i.path, i.expires_at)
        from intent i join data_file d on d.path = i.path
       where d.table_id = t and d.ref_id = r and d.begin_seq = seq and i.expires_at < now()
      union all select format('RET-7: the intent of %s expired at %s', i.path, i.expires_at)
        from intent i join deletion_vector x on x.path = i.path
       where x.table_id = t and x.ref_id = r and x.begin_seq = seq and i.expires_at < now()
      union all select format('STA-3: file %s has no statistics for cluster key field %s', d.file_id, k)
        from data_file d, visible_cluster_key(r, t, latest) ck, unnest(ck.field_ids) k
       where d.table_id = t and d.ref_id = r and d.begin_seq = seq and not k = any (d.stat_field_ids)) z);
    if problems is not null then
      perform invalid(problems);
    end if;

    -- CMT-1: the snapshot and the head.
    insert into snapshot values (t, seq, r, cur,
      case when e ? 'fields' or e ? 'drop_fields' or e ? 'create' then seq
           else (select s.schema_seq from snapshot s where s.table_id = t and s.snapshot_id = cur) end,
      (select spec_id from default_spec(r, t, latest)),
      coalesce((select sort_order_id from default_sort_order(r, t, latest)), 0),
      jsonb_build_object('operation', c ->> 'operation',
        'added-data-files', jsonb_array_length(coalesce(e -> 'add', '[]')),
        'deleted-data-files', jsonb_array_length(coalesce(e -> 'remove', '[]')),
        'added-records', coalesce((select sum((y ->> 'record_count')::bigint) from jsonb_array_elements(e -> 'add') y), 0),
        'deleted-records', coalesce((select sum(d.record_count) from data_file d
                                      where d.table_id = t and d.file_id in (
                                        select jsonb_array_elements_text(coalesce(e -> 'remove', '[]'))::bigint)
                                        and d.begin_seq < seq and d.end_seq >= seq
                                        and d.ref_id in (select ch.ref_id from chain(r, seq - 1) ch)), 0)));
    insert into head values (r, t, seq) on conflict (ref_id, table_id) do update set snapshot_id = seq;
  end loop;

  -- NS-2 and NS-3: tables the commit created or renamed, and namespaces it changed, once everything is applied.
  select z.state, z.problem into st, problems from namespace_errors(r, latest,
    array(select (y ->> 'table_id')::uuid from jsonb_array_elements(entries) y where y ? 'create' or y ? 'rename'),
    (select coalesce(jsonb_agg(y -> 'namespace'), '[]') from jsonb_array_elements(spaces) y)) z
   order by z.state desc limit 1;
  if problems is not null then
    if st = '40001' then perform conflict(problems); else perform invalid(problems); end if;
  end if;

  -- CMT-10 and CMT-5: intents, idempotency, notification.
  delete from intent where path in (
    select y ->> 'path' from jsonb_array_elements(entries) e2, jsonb_array_elements(coalesce(e2 -> 'add', '[]')) y
    union
    select y ->> 'path' from jsonb_array_elements(entries) e2, jsonb_array_elements(coalesce(e2 -> 'dvs', '[]')) y);
  if app is not null then
    update idempotency set version = ver, commit_seq = seq where ref_id = r and app_id = app;
  end if;
  perform pg_notify('metalake', current_schema() || ':' || seq);
  return seq;
end $$;
