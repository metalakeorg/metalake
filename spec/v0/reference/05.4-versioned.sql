-- MetaLake v0 · reference behaviour · versioned objects (§5.4).
--
-- It defines the right answer, not the fast one.

-- The visible version of each key at (r, s), the one valid at the lowest level of the
-- chain, unless it is dropped (VIS-9).

create function visible_table_names(r int, s bigint) returns setof table_name language sql stable as $$
  select (v).* from (
    select distinct on (x.table_id) x as v
      from chain(r, s) c join table_name x on x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.table_id, c.level) y
   where not (v).dropped;
$$;

create function visible_properties(r int, t uuid, s bigint) returns setof table_property language sql stable as $$
  select (v).* from (
    select distinct on (x.key) x as v
      from chain(r, s) c join table_property x
        on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.key, c.level) y
   where not (v).dropped;
$$;

create function visible_fields(r int, t uuid, s bigint) returns setof field language sql stable as $$
  select (v).* from (
    select distinct on (x.field_id) x as v
      from chain(r, s) c join field x
        on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.field_id, c.level) y
   where not (v).dropped;
$$;

create function default_spec(r int, t uuid, s bigint) returns setof partition_spec language sql stable as $$
  select x.* from chain(r, s) c join partition_spec x
    on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   order by c.level limit 1;
$$;

create function default_sort_order(r int, t uuid, s bigint) returns setof sort_order language sql stable as $$
  select x.* from chain(r, s) c join sort_order x
    on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   order by c.level limit 1;
$$;

create function visible_cluster_key(r int, t uuid, s bigint) returns setof cluster_key language sql stable as $$
  select x.* from chain(r, s) c join cluster_key x
    on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   order by c.level limit 1;
$$;

-- TBL-1: the table a name denotes at (r, s), or null.
create function table_by_name(r int, ns text[], n text, s bigint) returns uuid language sql stable as $$
  select table_id from visible_table_names(r, s) where namespace = ns and name = n;
$$;

-- NS-1 (revision 0.1): the version of namespace ns at (r, s), dropped or not, or NULL.
create function namespace_version(r int, ns text[], s bigint) returns namespace language sql stable as $$
  select x.* from chain(r, s) c
    join namespace x on x.namespace = ns and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   order by c.level limit 1;
$$;

create function namespace_exists(r int, ns text[], s bigint) returns boolean language sql stable as $$
  select coalesce(not (namespace_version(r, ns, s)).dropped, false);
$$;

create function visible_namespaces(r int, s bigint) returns setof namespace language sql stable as $$
  select (v).* from (
    select distinct on (x.namespace) x as v
      from chain(r, s) c join namespace x on x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.namespace, c.level) y
   where not (v).dropped;
$$;

-- NS-4: the version of property k of namespace ns at (r, s), dropped or not, or NULL.
create function namespace_property_version(r int, ns text[], k text, s bigint) returns namespace_property
language sql stable as $$
  select x.* from chain(r, s) c
    join namespace_property x
      on x.namespace = ns and x.key = k and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
   order by c.level limit 1;
$$;

-- NS-4: the properties of namespace ns at (r, s).
create function visible_namespace_properties(r int, ns text[], s bigint) returns setof namespace_property
language sql stable as $$
  select (v).* from (
    select distinct on (x.key) x as v
      from chain(r, s) c join namespace_property x
        on x.namespace = ns and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.key, c.level) y
   where not (v).dropped;
$$;

-- NS-2 and NS-3 at (r, s), for the tables and namespaces a commit or a merge touched: one row per problem, with
-- the SQLSTATE it raises. A namespace that never existed is invalid (22023); one whose visible version is
-- dropped, or that is dropped with something under it, is a conflict (40001).
create function namespace_errors(r int, s bigint, tables uuid[], spaces jsonb)
returns table (state text, problem text) language sql stable as $$
  -- A table's namespace exists.
  select case when v.namespace is null then '22023' else '40001' end,
         format('NS-2: table %s is in namespace %s, which %s', t.table_id, t.namespace,
                case when v.namespace is null then 'does not exist' else 'is dropped' end)
    from visible_table_names(r, s) t
    left join lateral (select (namespace_version(r, t.namespace, s)).*) v on true
   where t.table_id = any (tables) and (v.namespace is null or v.dropped)
  union all
  -- An existing namespace's parent exists.
  select case when pv.namespace is null then '22023' else '40001' end,
         format('NS-2: the parent of namespace %s %s', n.ns,
                case when pv.namespace is null then 'does not exist' else 'is dropped' end)
    from (select array(select jsonb_array_elements_text(j)) as ns from jsonb_array_elements(spaces) j) n
    left join lateral (select (namespace_version(r, n.ns[1:cardinality(n.ns) - 1], s)).*) pv on true
   where cardinality(n.ns) > 1 and namespace_exists(r, n.ns, s) and (pv.namespace is null or pv.dropped)
  union all
  -- A dropped namespace has nothing under it.
  select '40001', format('NS-3: namespace %s is dropped with tables or namespaces under it', n.ns)
    from (select array(select jsonb_array_elements_text(j)) as ns from jsonb_array_elements(spaces) j) n
   where (namespace_version(r, n.ns, s)).dropped
     and (exists (select 1 from visible_table_names(r, s) t where t.namespace[1:cardinality(n.ns)] = n.ns)
          or exists (select 1 from visible_namespaces(r, s) x
                      where cardinality(x.namespace) > cardinality(n.ns) and x.namespace[1:cardinality(n.ns)] = n.ns));
$$;
