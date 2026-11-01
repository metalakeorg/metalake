-- MetaLake v0 · reference behaviour · commit time (TT-1). Loaded before §12 and §13, which use it.
--
-- It defines the right answer, not the fast one.

-- The 32-bit xid of a commit's xid8, for pg_xact_commit_timestamp.
create function xid32(x xid8) returns xid language sql immutable as $$
  select (x::text::numeric % 4294967296)::text::xid;
$$;

-- TT-1: when a commit was committed: committed_at, or its transaction's commit timestamp until it is filled.
create function committed_time(seq bigint) returns timestamptz language sql stable as $$
  select coalesce(committed_at, pg_xact_commit_timestamp(xid32(xid))) from commit where commit_seq = seq;
$$;
