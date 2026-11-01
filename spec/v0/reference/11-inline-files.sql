-- MetaLake v0 · reference behaviour · inline files and footers (§11).
--
-- It defines the right answer, not the fast one.

-- A table property at (r, s), as an integer, or `fallback` when absent.
create function int_property(r int, t uuid, s bigint, k text, fallback bigint) returns bigint language sql stable as $$
  select coalesce((select value::bigint from visible_properties(r, t, s) where key = k), fallback);
$$;

-- Every rule of §11 that a checked file of t breaks at (r, s), one line each; with `added`, only the files ref r
-- added at commit `added` (CMT-9).
create function inline_errors(r int, t uuid, s bigint, added bigint default null) returns setof text
language sql stable as $$
  select format('INL-1: file %s has no path and no inline content, or both', f.file_id)
    from checked_files(r, t, s, added) f
   where (f.path is null) <> exists (select 1 from inline_file i where i.table_id = t and i.file_id = f.file_id)
  union all
  select format('INL-1: inline file %s has file_size %s for %s bytes, above the limit of %s',
                f.file_id, f.file_size, length(i.content), int_property(r, t, s, 'inline.max-bytes', 65536))
    from checked_files(r, t, s, added) f join inline_file i on i.table_id = t and i.file_id = f.file_id
   where f.file_size <> length(i.content) or length(i.content) > int_property(r, t, s, 'inline.max-bytes', 65536)
  union all
  select format('FTR-1: the footer of file %s has %s bytes, not its footer_size %s', f.file_id, length(x.footer), f.footer_size)
    from checked_files(r, t, s, added) f join file_footer x on x.table_id = t and x.file_id = f.file_id
   where length(x.footer) <> f.footer_size;
$$;

-- INL-2: whether t must be flushed on r: its live inline files exceed inline.flush-bytes in total, or the oldest
-- was committed more than inline.flush-age-seconds ago.
create function inline_flush_due(r int, t uuid) returns boolean language sql stable as $$
  with latest (s) as (select 9223372036854775806::bigint),
  inline as (
    select f.file_size, pg_xact_commit_timestamp(c.xid::text::xid) as at
      from latest, visible_files(r, t, latest.s) f join commit c on c.commit_seq = f.begin_seq
     where f.path is null)
  select coalesce(sum(file_size) > int_property(r, t, (select s from latest), 'inline.flush-bytes', 1048576)
               or min(at) < now() - make_interval(secs => int_property(r, t, (select s from latest), 'inline.flush-age-seconds', 3600)),
                  false)
    from inline;
$$;
