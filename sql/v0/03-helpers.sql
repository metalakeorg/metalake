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
