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
