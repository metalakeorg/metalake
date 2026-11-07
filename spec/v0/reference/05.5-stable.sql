-- MetaLake v0 · reference behaviour · stable sequences (§5.5).
--
-- It defines the right answer, not the fast one.

-- VIS-11: a candidate stable sequence and the transaction horizon it waits for.
create function stable_point(out seq bigint, out horizon xid8) language plpgsql volatile as $$
begin
  -- In this order: every commit that has drawn a sequence within L already has an id, so a new id is above them all.
  -- pg_snapshot_xmax would not be: it is one past the latest id to complete, and a commit in flight can be at it.
  seq := (select last_value from commit_seq_gen);
  horizon := pg_current_xact_id();
end $$;

-- CHG-4: where a reader that follows a table starts: a time taken before the sequence, which is taken before the horizon.
create function stream_point(out at timestamptz, out seq bigint, out horizon xid8) language plpgsql volatile as $$
begin
  at := clock_timestamp();
  seq := (select last_value from commit_seq_gen);
  horizon := pg_current_xact_id();
end $$;

create function is_stable(horizon xid8) returns boolean language sql volatile as $$
  select pg_snapshot_xmin(pg_current_snapshot()) >= horizon;
$$;
