-- MetaLake v0 · reference behaviour · branches and merges (§12).
--
-- It defines the right answer, not the fast one.

-- BR-1 and BR-2: a branch or a tag `name` of ref `p`, seeing `p` at `at`, a snapshot `p` keeps (RET-6), or by
-- default at the last commit_seq drawn, read under the lock. Returns its ref_id.
create function create_ref(name text, p int, kind text default 'branch', at bigint default null,
    author text default 'metalake') returns int language plpgsql as $$
declare
  parent ref;
  fork bigint;
  c bigint;
  id int := nextval('ref_id_gen');
begin
  perform writable();
  select * into parent from ref x where x.ref_id = p for update;
  if parent.ref_id is null or parent.deleted then
    perform invalid(format('ref %s cannot be forked', p));
  end if;
  fork := (select last_value from commit_seq_gen);
  if at is not null then
    if at not in (select kept_snapshots(p)) then
      perform invalid(format('%s is not a snapshot ref %s keeps (RET-6)', at, p));
    end if;
    fork := at;
  end if;
  c := nextval('commit_seq_gen');
  insert into ref (ref_id, name, kind, created_seq) values (id, name, kind, c);
  insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (id, c, p, fork);
  insert into commit values (c, id, pg_current_xact_id(), null, author, author, kind, null, null, null, null);
  return id;
end $$;

-- BR-3
create function delete_ref(r int) returns void language plpgsql as $$
declare x ref;
begin
  select * into x from ref where ref_id = r for update;
  if x.ref_id is null or x.ref_id = 1 or x.protected then
    perform invalid(format('ref %s cannot be deleted', r));
  end if;
  update ref set deleted = true where ref_id = r;
end $$;

-- The versioned tables a merge carries, with the columns of their key (VIS-9).
create function versioned_tables() returns table (name text, keys text[]) language sql immutable as $$
  values ('table_name', '{table_id}'::text[]), ('table_property', '{table_id,key}'), ('field', '{table_id,field_id}'),
         ('partition_spec', '{table_id}'), ('sort_order', '{table_id}'), ('cluster_key', '{table_id}'),
         ('deletion_vector', '{table_id,file_id}');
$$;

-- The version of key `k` (a JSON object of its key columns) of versioned table `tab` visible at (r, s), as JSON
-- without its ref and validity, or NULL. Dropped versions are returned: they are content too.
create function version_at(tab text, k jsonb, r int, s bigint) returns jsonb language plpgsql stable as $$
declare v jsonb;
begin
  execute format($q$
    select to_jsonb(x) - 'ref_id' - 'begin_seq' - 'end_seq'
      from chain($1, $2) c join %I x on x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     where x.table_id = ($3 ->> 'table_id')::uuid and to_jsonb(x) @> $3
     order by c.level limit 1$q$, tab) into v using r, s, k;
  return v;
end $$;

-- MRG-5: whether a supplied vector holds both sides.
create function dv_covers(v jsonb, a jsonb, b jsonb) returns boolean language sql immutable as $$
  select (v ->> 'cardinality')::bigint >= greatest((a ->> 'cardinality')::bigint, (b ->> 'cardinality')::bigint)
     and (v ->> 'inline' is null or not exists (
            select dv_positions(decode(substring(x ->> 'inline' from 3), 'hex'))
              from (values (a), (b)) y(x) where x ->> 'inline' is not null
            except select dv_positions(decode(substring(v ->> 'inline' from 3), 'hex'))));
$$;

-- MRG-5: the vector of a file both sides changed, `mine` on the branch and `now_` on the parent, merged: their
-- union if both are inline, or the vector the caller supplied in args' "dvs", if it holds both. A conflict otherwise.
create function merged_vector(t uuid, mine jsonb, now_ jsonb, args jsonb) returns jsonb language plpgsql as $$
declare supplied jsonb;
begin
  if mine ->> 'inline' is not null and now_ ->> 'inline' is not null then
    mine := mine || jsonb_build_object('inline', '\x' || encode(dv_encode(array(
              select dv_positions(decode(substring(mine ->> 'inline' from 3), 'hex'))
              union select dv_positions(decode(substring(now_ ->> 'inline' from 3), 'hex')))), 'hex'));
    return mine || jsonb_build_object(
      'cardinality', (select count(*) from dv_positions(decode(substring(mine ->> 'inline' from 3), 'hex'))),
      'content_size', length(decode(substring(mine ->> 'inline' from 3), 'hex')));
  end if;
  supplied := (select x from jsonb_array_elements(coalesce(args -> 'dvs', '[]')) x
                where (x ->> 'table_id')::uuid = t and (x ->> 'file_id')::bigint = (mine ->> 'file_id')::bigint);
  if supplied is null then
    perform conflict(format('the vector of file %s changed on both sides and no merged vector was given', mine ->> 'file_id'));
  end if;
  supplied := supplied - 'base_seq' || jsonb_build_object('inline',
    case when supplied ? 'inline' then '\x' || (supplied ->> 'inline') end);
  if not dv_covers(supplied, mine, now_) then
    perform conflict(format('the merged vector of file %s does not hold both sides', mine ->> 'file_id'));
  end if;
  return mine || jsonb_build_object('path', supplied -> 'path', 'content_offset', supplied -> 'content_offset',
    'content_size', coalesce(supplied -> 'content_size',
                             to_jsonb(length(decode(substring(supplied ->> 'inline' from 3), 'hex')))),
    'cardinality', supplied -> 'cardinality', 'inline', supplied -> 'inline');
end $$;

-- MRG-6 (revision 0.1): whether namespace ns lies under one of the namespaces of a selection (a JSON array of arrays).
create function under_any(ns text[], selection jsonb) returns boolean language sql immutable as $$
  select exists (select 1 from jsonb_array_elements(coalesce(selection, '[]')) j
                  where cardinality(ns) >= jsonb_array_length(j)
                    and ns[1:jsonb_array_length(j)] = array(select jsonb_array_elements_text(j)));
$$;

-- MRG-1 to MRG-8: merges branch b into its parent. `args` may carry "author", "message" and "dvs": merged
-- vectors for files whose vector both sides changed, as in commit(); and, revision 0.1, "only": {"tables":
-- [<table_id>], "namespaces": [[<level>, ...]]} to merge a selection and rebase the rest, with "split": true to
-- allow commits that changed both sides of it. Returns the merge's commit_seq, or NULL
-- if b had nothing to merge.
create function merge(b int, args jsonb default '{}') returns bigint language plpgsql as $$
declare
  latest constant bigint := 9223372036854775806;
  open constant bigint := 9223372036854775807;
  rb ref;
  base ref_base;
  p int;
  f bigint;
  m bigint;
  t uuid;
  tables uuid[];
  named uuid[];
  spaces jsonb;
  ns text[];
  vt record;
  row jsonb;
  k jsonb;
  at_fork jsonb;
  now_ jsonb;
  mine jsonb;
  fid bigint;
  supplied jsonb;
  problems text;
  cur bigint;
  selected uuid[];
  rest uuid[];
  only_ jsonb := args -> 'only';
  chosen jsonb;
  parent_snap bigint;
begin
  perform writable();
  select * into base from ref_base where ref_id = b and end_seq = open;
  if base.ref_id is null then
    perform invalid(format('ref %s has no parent to merge into', b));
  end if;
  p := base.parent_ref_id;
  perform 1 from ref where ref_id = p and kind = 'branch' and not deleted for update;
  if not found then
    perform invalid(format('ref %s does not take merges', p));
  end if;
  select * into rb from ref where ref_id = b for update;
  select * into base from ref_base where ref_id = b and end_seq = open;
  if rb.kind <> 'branch' or rb.deleted or base.parent_ref_id <> p then
    perform invalid(format('ref %s cannot be merged into %s', b, p));
  end if;
  f := base.fork_seq;

  tables := array(
    select table_id from data_file where ref_id = b and end_seq = open
    union select table_id from tombstone where ref_id = b and end_seq = open
    union select table_id from deletion_vector where ref_id = b and end_seq = open
    union select table_id from field where ref_id = b and end_seq = open
    union select table_id from table_name where ref_id = b and end_seq = open
    union select table_id from table_property where ref_id = b and end_seq = open
    union select table_id from partition_spec where ref_id = b and end_seq = open
    union select table_id from sort_order where ref_id = b and end_seq = open
    union select table_id from cluster_key where ref_id = b and end_seq = open
    order by 1);
  -- Revision 0.1: the namespaces the branch changed, and the tables it named.
  spaces := (select coalesce(jsonb_agg(distinct to_jsonb(y.namespace)), '[]') from (
               select x.namespace from namespace x where x.ref_id = b and x.end_seq = open
               union select x.namespace from namespace_property x where x.ref_id = b and x.end_seq = open) y);
  named := array(select x.table_id from table_name x where x.ref_id = b and x.end_seq = open);
  if cardinality(tables) = 0 and jsonb_array_length(spaces) = 0 then
    return null;
  end if;

  -- MRG-6 (revision 0.1): the selection, the namespaces it brings, and the rest.
  if only_ is null then
    selected := tables;
    chosen := spaces;
  else
    selected := array(
      select x from unnest(tables) x
       where x::text in (select jsonb_array_elements_text(coalesce(only_ -> 'tables', '[]')))
          or exists (select 1 from visible_table_names(b, latest) v
                      where v.table_id = x and under_any(v.namespace, only_ -> 'namespaces')));
    -- The namespace objects under a selected namespace, and those of the selected tables and of their parents.
    chosen := (select coalesce(jsonb_agg(to_jsonb(x.namespace)), '[]') from namespace x
                where x.ref_id = b and x.end_seq = open
                  and (under_any(x.namespace, only_ -> 'namespaces')
                       or exists (select 1 from visible_table_names(b, latest) v
                                   where v.table_id = any (selected)
                                     and cardinality(v.namespace) >= cardinality(x.namespace)
                                     and v.namespace[1:cardinality(x.namespace)] = x.namespace)));
    if cardinality(selected) = 0 and jsonb_array_length(chosen) = 0
       and not exists (select 1 from namespace_property x where x.ref_id = b and x.end_seq = open
                          and under_any(x.namespace, only_ -> 'namespaces')) then
      perform invalid('MRG-6: the selection names nothing the branch changed');
    end if;
  end if;
  rest := array(select x from unnest(tables) x where not x = any (selected));

  -- MRG-7: a commit of the branch since its base that changed both sides of the selection.
  if cardinality(rest) > 0 and not coalesce((args ->> 'split')::boolean, false) then
    cur := (select s.snapshot_id from snapshot s where s.ref_id = b and s.snapshot_id > base.begin_seq
             group by s.snapshot_id
            having bool_or(s.table_id = any (selected)) and bool_or(s.table_id = any (rest))
             order by 1 limit 1);
    if cur is not null then
      perform conflict(format('MRG-7: commit %s changed tables on both sides of the selection', cur));
    end if;
  end if;

  -- MRG-1: p's heads of the tables b changed that p has.
  foreach t in array tables loop
    if exists (select 1 from visible_table_names(p, latest) v where v.table_id = t) then
      insert into head values (p, t, snapshot_at(p, t, latest)) on conflict do nothing;
      perform 1 from head h where h.ref_id = p and h.table_id = t for update;
    elsif exists (select 1 from visible_table_names(p, f) v where v.table_id = t) then
      perform conflict(format('table %s was dropped on ref %s', t, p));
    end if;
  end loop;

  -- MRG-8: the rest, compared as if merged, against what p changed since the fork.
  foreach t in array rest loop
    for fid in select x.file_id from tombstone x where x.table_id = t and x.ref_id = b and x.end_seq = open loop
      if not file_visible(p, t, latest, fid) then
        perform conflict(format('file %s was removed on both sides', fid));
      end if;
      if version_at('deletion_vector', jsonb_build_object('table_id', t, 'file_id', fid), p, f)
         is distinct from version_at('deletion_vector', jsonb_build_object('table_id', t, 'file_id', fid), p, latest) then
        perform conflict(format('file %s was removed on one side and its rows deleted on the other', fid));
      end if;
    end loop;
    for vt in select * from versioned_tables() where name <> 'deletion_vector' loop
      for row in execute format('select to_jsonb(x) from %I x where x.table_id = $1 and x.ref_id = $2 and x.end_seq = $3',
                                vt.name) using t, b, open loop
        k := (select jsonb_object_agg(c, row -> c) from unnest(vt.keys) c);
        if version_at(vt.name, k, p, f) is distinct from version_at(vt.name, k, p, latest)
           and version_at(vt.name, k, p, latest) is distinct from row - 'ref_id' - 'begin_seq' - 'end_seq' then
          perform conflict(format('%s %s changed on both sides', vt.name, k));
        end if;
      end loop;
    end loop;
    for fid in select x.file_id from deletion_vector x where x.table_id = t and x.ref_id = b and x.end_seq = open loop
      if file_visible(p, t, f, fid) and not file_visible(p, t, latest, fid) then
        perform conflict(format('the rows of file %s were deleted on one side and the file removed on the other', fid));
      end if;
    end loop;
  end loop;
  for row in select to_jsonb(x) from namespace x where x.ref_id = b and x.end_seq = open
                and not exists (select 1 from jsonb_array_elements(chosen) e where e = to_jsonb(x.namespace)) loop
    ns := array(select jsonb_array_elements_text(row -> 'namespace'));
    if (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq' from namespace_version(p, ns, f) v where v.namespace is not null)
       is distinct from (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq' from namespace_version(p, ns, latest) v
                          where v.namespace is not null)
       and (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq' from namespace_version(p, ns, latest) v
             where v.namespace is not null) is distinct from row - 'ref_id' - 'begin_seq' - 'end_seq' then
      perform conflict(format('namespace %s changed on both sides', ns));
    end if;
  end loop;
  for row in select to_jsonb(x) from namespace_property x where x.ref_id = b and x.end_seq = open
                and only_ is not null and not under_any(x.namespace, only_ -> 'namespaces') loop
    ns := array(select jsonb_array_elements_text(row -> 'namespace'));
    if (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq'
          from namespace_property_version(p, ns, row ->> 'key', f) v where v.namespace is not null)
       is distinct from (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq'
                           from namespace_property_version(p, ns, row ->> 'key', latest) v where v.namespace is not null)
       and (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq'
              from namespace_property_version(p, ns, row ->> 'key', latest) v where v.namespace is not null)
           is distinct from row - 'ref_id' - 'begin_seq' - 'end_seq' then
      perform conflict(format('property %s of namespace %s changed on both sides', row ->> 'key', ns));
    end if;
  end loop;

  m := nextval('commit_seq_gen');
  insert into commit values (m, p, pg_current_xact_id(), null, coalesce(args ->> 'author', 'metalake'),
                             coalesce(args ->> 'author', 'metalake'), 'merge', null, null, null, args ->> 'message');

  foreach t in array selected loop
    cur := (select h.snapshot_id from head h where h.ref_id = p and h.table_id = t);

    -- Removed files.
    for fid in select x.file_id from tombstone x where x.table_id = t and x.ref_id = b and x.end_seq = open loop
      if not file_visible(p, t, latest, fid) then
        perform conflict(format('file %s was removed on both sides', fid));
      end if;
      if version_at('deletion_vector', jsonb_build_object('table_id', t, 'file_id', fid), p, f)
         is distinct from version_at('deletion_vector', jsonb_build_object('table_id', t, 'file_id', fid), p, latest) then
        perform conflict(format('file %s was removed on one side and its rows deleted on the other', fid));
      end if;
      update data_file set end_seq = m where table_id = t and file_id = fid and ref_id = p and end_seq = open;
      if not found then
        insert into tombstone values (t, fid, p, m);
      end if;
      update deletion_vector set end_seq = m where table_id = t and file_id = fid and ref_id = p and end_seq = open;
      update tombstone set end_seq = m where table_id = t and file_id = fid and ref_id = b and end_seq = open;
    end loop;

    -- Added files.
    for row in select to_jsonb(x) from data_file x where x.table_id = t and x.ref_id = b and x.end_seq = open loop
      update data_file set end_seq = m
       where table_id = t and file_id = (row ->> 'file_id')::bigint and ref_id = b and end_seq = open;
      insert into data_file select (jsonb_populate_record(null::data_file,
        row || jsonb_build_object('ref_id', p, 'begin_seq', m, 'end_seq', open))).*;
    end loop;

    -- Versions, vectors included.
    for vt in select * from versioned_tables() loop
      for row in execute format('select to_jsonb(x) from %I x where x.table_id = $1 and x.ref_id = $2 and x.end_seq = $3',
                                vt.name) using t, b, open loop
        k := (select jsonb_object_agg(c, row -> c) from unnest(vt.keys) c);
        mine := row - 'ref_id' - 'begin_seq' - 'end_seq';
        at_fork := version_at(vt.name, k, p, f);
        now_ := version_at(vt.name, k, p, latest);
        if vt.name = 'deletion_vector' then
          if not file_visible(p, t, latest, (row ->> 'file_id')::bigint) then
            perform conflict(format('the rows of file %s were deleted on one side and the file removed on the other', row ->> 'file_id'));
          end if;
          if at_fork is distinct from now_ then
            -- MRG-5: both sides changed the vector.
            mine := merged_vector(t, mine, now_, args);
          end if;
        elsif at_fork is distinct from now_ and now_ is distinct from mine then
          perform conflict(format('%s %s changed on both sides', vt.name, k));
        end if;
        execute format('update %I x set end_seq = $1 where x.table_id = $5 and to_jsonb(x) @> $2 and x.ref_id = $3 and x.end_seq = $4', vt.name)
          using m, k, p, open, t;
        execute format('update %I x set end_seq = $1 where x.table_id = $5 and to_jsonb(x) @> $2 and x.ref_id = $3 and x.end_seq = $4', vt.name)
          using m, k, b, open, t;
        execute format('insert into %I select (jsonb_populate_record(null::%I, $1)).*', vt.name, vt.name)
          using mine || jsonb_build_object('ref_id', p, 'begin_seq', m, 'end_seq', open);
      end loop;
    end loop;

    -- MRG-3: the result is valid.
    problems := (select string_agg(x, '; ') from (
      select table_errors(p, t, latest) as x union all select dv_errors(p, t, latest, m)) z);
    if problems is not null then
      perform conflict(format('the merge leaves table %s invalid: %s', t, problems));
    end if;

    insert into snapshot values (t, m, p, cur,
      case when exists (select 1 from field x where x.table_id = t and x.ref_id = p and x.begin_seq = m) or cur is null then m
           else (select s.schema_seq from snapshot s where s.table_id = t and s.snapshot_id = cur) end,
      (select spec_id from default_spec(p, t, latest)),
      coalesce((select sort_order_id from default_sort_order(p, t, latest)), 0),
      jsonb_build_object('operation', 'merge',
        'added-data-files', (select count(*) from data_file x where x.table_id = t and x.ref_id = p and x.begin_seq = m),
        'deleted-data-files', (select count(*) from data_file x where x.table_id = t and x.ref_id = p and x.end_seq = m)
                            + (select count(*) from tombstone x where x.table_id = t and x.ref_id = p and x.begin_seq = m),
        'added-records', (select coalesce(sum(record_count), 0) from data_file x where x.table_id = t and x.ref_id = p and x.begin_seq = m),
        'deleted-records', (select coalesce(sum((visible_file(p, t, m - 1, y.file_id)).record_count), 0)
                              from (select x.file_id from data_file x where x.table_id = t and x.ref_id = p and x.end_seq = m
                                    union select x.file_id from tombstone x where x.table_id = t and x.ref_id = p and x.begin_seq = m) y)));
    insert into head values (p, t, m) on conflict (ref_id, table_id) do update set snapshot_id = m;
  end loop;

  -- Revision 0.1: namespaces and their properties, as versioned keys (VIS-9, MRG-3), compared by exact path.
  for row in select to_jsonb(x) from namespace x where x.ref_id = b and x.end_seq = open
                and exists (select 1 from jsonb_array_elements(chosen) e where e = to_jsonb(x.namespace)) loop
    ns := array(select jsonb_array_elements_text(row -> 'namespace'));
    mine := row - 'ref_id' - 'begin_seq' - 'end_seq';
    at_fork := (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq' from namespace_version(p, ns, f) v
                 where v.namespace is not null);
    now_ := (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq' from namespace_version(p, ns, latest) v
              where v.namespace is not null);
    if at_fork is distinct from now_ and now_ is distinct from mine then
      perform conflict(format('namespace %s changed on both sides', ns));
    end if;
    update namespace x set end_seq = m where x.namespace = ns and x.ref_id in (p, b) and x.end_seq = open;
    insert into namespace values (ns, (row ->> 'dropped')::boolean, p, m);
  end loop;
  for row in select to_jsonb(x) from namespace_property x where x.ref_id = b and x.end_seq = open
                and (only_ is null or under_any(x.namespace, only_ -> 'namespaces')) loop
    ns := array(select jsonb_array_elements_text(row -> 'namespace'));
    mine := row - 'ref_id' - 'begin_seq' - 'end_seq';
    at_fork := (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq'
                  from namespace_property_version(p, ns, row ->> 'key', f) v where v.namespace is not null);
    now_ := (select to_jsonb(v) - 'ref_id' - 'begin_seq' - 'end_seq'
               from namespace_property_version(p, ns, row ->> 'key', latest) v where v.namespace is not null);
    if at_fork is distinct from now_ and now_ is distinct from mine then
      perform conflict(format('property %s of namespace %s changed on both sides', row ->> 'key', ns));
    end if;
    update namespace_property x set end_seq = m
     where x.namespace = ns and x.key = row ->> 'key' and x.ref_id in (p, b) and x.end_seq = open;
    insert into namespace_property values (ns, row ->> 'key', row ->> 'value', (row ->> 'dropped')::boolean, p, m);
  end loop;
  -- NS-2 and NS-3 on the parent, for the tables the merge named and the namespaces it brought.
  select z.problem into problems
    from namespace_errors(p, latest, array(select x from unnest(named) x where x = any (selected)), chosen) z limit 1;
  if problems is not null then
    perform conflict(format('the merge breaks a namespace rule: %s', problems));
  end if;

  if exists (select 1 from visible_table_names(p, latest) group by namespace, name having count(*) > 1) then
    perform conflict('the merge gives two tables one name');
  end if;

  -- MRG-4: b becomes an empty child of p.
  update ref_base set end_seq = m where ref_id = b and end_seq = open;
  insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (b, m, p, m);

  -- MRG-8: the rest, rebased: the union of vectors both sides changed, a snapshot per table, and its validity.
  foreach t in array rest loop
    for row in select to_jsonb(x) from deletion_vector x where x.table_id = t and x.ref_id = b and x.end_seq = open loop
      k := jsonb_build_object('table_id', t, 'file_id', row -> 'file_id');
      at_fork := version_at('deletion_vector', k, p, f);
      now_ := version_at('deletion_vector', k, p, latest);
      continue when at_fork is not distinct from now_;
      mine := merged_vector(t, row - 'ref_id' - 'begin_seq' - 'end_seq', now_, args);
      update deletion_vector set end_seq = m
       where table_id = t and file_id = (row ->> 'file_id')::bigint and ref_id = b and end_seq = open;
      insert into deletion_vector select (jsonb_populate_record(null::deletion_vector,
        mine || jsonb_build_object('ref_id', b, 'begin_seq', m, 'end_seq', open))).*;
    end loop;
    cur := (select h.snapshot_id from head h where h.ref_id = b and h.table_id = t);
    parent_snap := coalesce(snapshot_at(p, t, latest), cur);
    insert into snapshot values (t, m, b, parent_snap,
      case when exists (select 1 from field x where x.table_id = t and x.ref_id = b and x.end_seq = open) then m
           else (select s.schema_seq from snapshot s where s.table_id = t and s.snapshot_id = parent_snap) end,
      (select spec_id from default_spec(b, t, latest)),
      coalesce((select sort_order_id from default_sort_order(b, t, latest)), 0),
      jsonb_build_object('operation', 'rebase'));
    problems := (select string_agg(x, '; ') from (
      select table_errors(b, t, latest) as x union all select dv_errors(b, t, latest, m)) z);
    if problems is not null then
      perform conflict(format('the rebase leaves table %s invalid on the branch: %s', t, problems));
    end if;
  end loop;
  if cardinality(rest) > 0 or jsonb_array_length(chosen) < jsonb_array_length(spaces) then
    if exists (select 1 from visible_table_names(b, latest) group by namespace, name having count(*) > 1) then
      perform conflict('the rebase gives two tables one name on the branch');
    end if;
    select z.problem into problems from namespace_errors(b, latest,
      array(select x from unnest(named) x where x = any (rest)),
      (select coalesce(jsonb_agg(e), '[]') from jsonb_array_elements(spaces) e
        where not exists (select 1 from jsonb_array_elements(chosen) c where c = e))) z limit 1;
    if problems is not null then
      perform conflict(format('the rebase breaks a namespace rule on the branch: %s', problems));
    end if;
  end if;
  update head h set snapshot_id = snapshot_at(b, h.table_id, latest) where h.ref_id = b;
  perform pg_notify('metalake', current_schema() || ':' || m);
  return m;
end $$;

