-- MetaLake v0 · reference behaviour · commit time (TT-1). Loaded before §12 and §13, which use it.
--
-- It defines the right answer, not the fast one.

-- The 32-bit xid of a commit's xid8, for pg_xact_commit_timestamp.
create function xid32(x xid8) returns xid language sql immutable as $$
  select (x::text::numeric % 4294967296)::text::xid;
$$;

-- TT-1 (0.2): a visible commit without a time is a damaged catalog, refused; the commit of the current
-- transaction, not yet committed, has none yet.
create function no_time(seq bigint, x xid8) returns timestamptz language plpgsql stable as $$
begin
  if x = pg_current_xact_id_if_assigned() then
    return null;
  end if;
  raise exception 'metalake unsupported: commit % has no time (TT-1)', seq using errcode = '0A000';
end $$;

-- TT-1: when a commit was committed: committed_at, or its transaction's commit timestamp until it is filled.
create function committed_time(seq bigint) returns timestamptz language sql stable as $$
  select coalesce(committed_at, pg_xact_commit_timestamp(xid32(xid)), no_time(commit_seq, xid))
    from commit where commit_seq = seq;
$$;
