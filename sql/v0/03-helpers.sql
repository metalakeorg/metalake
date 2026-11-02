-- MetaLake v0 · protocol · refusals and the lookups its functions share.

-- A refusal carries the SQLSTATE of the specification and, in DETAIL, the rule it enforces (ADR 0003 §2.2).
create function @schema@.refuse_conflict(rule text, what text) returns void language plpgsql as $$
begin
  raise exception 'metalake conflict: %', what using errcode = '40001', detail = rule;
end $$;

create function @schema@.refuse_invalid(rule text, what text) returns void language plpgsql as $$
begin
  raise exception 'metalake invalid: %', what using errcode = '22023', detail = rule;
end $$;

-- TBL-1 for one table: whether t exists at (r, s), from its name's visible version alone.
create function @schema@.table_visible(r int, t uuid, s bigint) returns boolean language sql stable as $$
  select coalesce((select not x.dropped
                     from @schema@.chain(r, s) c
                     join @schema@.table_name x
                       on x.table_id = t and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
                    order by c.level limit 1), false);
$$;

-- CMT-2: the snapshot of t at (r, s), or NULL.
create function @schema@.snapshot_at(r int, t uuid, s bigint) returns bigint language sql stable as $$
  select x.id from @schema@.chain(r, s) c
    cross join lateral (select max(snapshot_id) as id from @schema@.snapshot
                         where table_id = t and ref_id = c.ref_id and snapshot_id between c.since and c.seq) x
   where x.id is not null
   order by c.level limit 1;
$$;

-- The locks of a ref (ADR 0002 §21.4). Commits hold their ref shared; a merge, create_ref and delete_ref hold it
-- exclusively. They are advisory locks, which Postgres queues in order: an exclusive request waits for the commits
-- in flight, and commits that arrive after it wait behind it. A row lock would not: new shared row locks pass a
-- waiting exclusive one, which then waits for as long as commits keep arriving. The first key is the catalog's, so
-- that catalogs in one database do not share locks. Held until the transaction ends.
create function @schema@.lock_ref_shared(r int) returns void language sql as $$
  select pg_advisory_xact_lock_shared(hashtext('@schema@'), r);
$$;

create function @schema@.lock_ref(r int) returns void language sql as $$
  select pg_advisory_xact_lock(hashtext('@schema@'), r);
$$;

-- TBL-1: a commit that gives a table a name on ref r holds that name exclusively, instead of the whole ref.
create function @schema@.lock_name(r int, ns text[], name text) returns void language sql as $$
  select pg_advisory_xact_lock(hashtext('@schema@ names'), hashtext(r || ' ' || array_to_string(ns, '.') || ' ' || name));
$$;

-- NS-2, NS-3: a commit holds a namespace it changes exclusively, and one it puts a table or a namespace in, shared.
create function @schema@.lock_namespace(r int, ns text[], exclusive boolean) returns void language plpgsql as $$
begin
  if exclusive then
    perform pg_advisory_xact_lock(hashtext('@schema@ namespaces'), hashtext(r || ' ' || array_to_string(ns, '.')));
  else
    perform pg_advisory_xact_lock_shared(hashtext('@schema@ namespaces'), hashtext(r || ' ' || array_to_string(ns, '.')));
  end if;
end $$;
