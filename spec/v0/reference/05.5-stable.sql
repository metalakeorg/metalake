-- MetaLake v0 · reference behaviour · stable sequences (§5.5).
--
-- It defines the right answer, not the fast one.

-- VIS-11: a candidate stable sequence and the transaction horizon it waits for.
create function stable_point(out seq bigint, out horizon xid8) language sql volatile as $$
  select (select last_value from commit_seq_gen), pg_snapshot_xmax(pg_current_snapshot());
$$;

create function is_stable(horizon xid8) returns boolean language sql volatile as $$
  select pg_snapshot_xmin(pg_current_snapshot()) >= horizon;
$$;
