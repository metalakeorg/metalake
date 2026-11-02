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

-- MRG-1 to MRG-5: merges branch b into its parent. `args` may carry "author", "message" and "dvs": merged
-- vectors for files whose vector both sides changed, as in commit(). Returns the merge's commit_seq, or NULL
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
begin
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
  if cardinality(tables) = 0 then
    return null;
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

  m := nextval('commit_seq_gen');
  insert into commit values (m, p, pg_current_xact_id(), null, coalesce(args ->> 'author', 'metalake'),
                             coalesce(args ->> 'author', 'metalake'), 'merge', null, null, null, args ->> 'message');

  foreach t in array tables loop
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
            if mine ->> 'inline' is not null and now_ ->> 'inline' is not null then
              mine := mine || jsonb_build_object('inline', '\x' || encode(dv_encode(array(
                        select dv_positions(decode(substring(mine ->> 'inline' from 3), 'hex'))
                        union select dv_positions(decode(substring(now_ ->> 'inline' from 3), 'hex')))), 'hex'));
              mine := mine || jsonb_build_object(
                'cardinality', (select count(*) from dv_positions(decode(substring(mine ->> 'inline' from 3), 'hex'))),
                'content_size', length(decode(substring(mine ->> 'inline' from 3), 'hex')));
            else
              supplied := (select x from jsonb_array_elements(coalesce(args -> 'dvs', '[]')) x
                            where (x ->> 'table_id')::uuid = t and (x ->> 'file_id')::bigint = (row ->> 'file_id')::bigint);
              if supplied is null then
                perform conflict(format('the vector of file %s changed on both sides and no merged vector was given', row ->> 'file_id'));
              end if;
              supplied := supplied - 'base_seq' || jsonb_build_object('inline',
                case when supplied ? 'inline' then '\x' || (supplied ->> 'inline') end);
              if not dv_covers(supplied, mine, now_) then
                perform conflict(format('the merged vector of file %s does not hold both sides', row ->> 'file_id'));
              end if;
              mine := mine || jsonb_build_object('path', supplied -> 'path', 'content_offset', supplied -> 'content_offset',
                'content_size', coalesce(supplied -> 'content_size',
                                         to_jsonb(length(decode(substring(supplied ->> 'inline' from 3), 'hex')))),
                'cardinality', supplied -> 'cardinality', 'inline', supplied -> 'inline');
            end if;
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

  if exists (select 1 from visible_table_names(p, latest) group by namespace, name having count(*) > 1) then
    perform conflict('the merge gives two tables one name');
  end if;

  -- MRG-4: b becomes an empty child of p.
  update ref_base set end_seq = m where ref_id = b and end_seq = open;
  insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (b, m, p, m);
  update head h set snapshot_id = snapshot_at(b, h.table_id, latest) where h.ref_id = b;
  perform pg_notify('metalake', current_schema() || ':' || m);
  return m;
end $$;
