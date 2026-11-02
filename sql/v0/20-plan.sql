-- MetaLake v0 · protocol · planning (ADR 0003 §2.6).
--
-- plan is LANGUAGE sql, stable, without SET and qualified throughout, so that Postgres inlines it into the caller's
-- query: its rows stream, and its plan is cached with the caller's prepared statement. Its parameters are read by
-- position, since their names are those of columns.

-- VIS-1 to VIS-10, DV-1, STA-10: the files of table $2 a read of ref $1 at sequence $3 opens, with their vectors,
-- pruned to those that may hold a value of field $4 in [$5, $6]. Without a field, or on a field that is not
-- visible or whose bounds are not ordered values, every visible file.
create function @schema@.plan(ref_id int, table_id uuid, at bigint default 9223372036854775806,
                              field_id int default null, lo bytea default null, hi bytea default null)
returns setof @schema@.planned_file language sql stable as $$
  with recursive c (level, ref_id, seq) as (
    select 0, $1, $3
    union all
    select c.level + 1, b.parent_ref_id, b.fork_seq
      from c join @schema@.ref_base b on b.ref_id = c.ref_id and b.begin_seq <= c.seq and c.seq < b.end_seq
  ) cycle ref_id set looped using walked,
  -- The field's visible version (§6), whether its bounds prune (STA-10), and the factor that turns the days of a
  -- bound of 4 bytes into its timestamp, when it was promoted from date (STA-7). Built-ins only: the reference's
  -- functions resolve through a search_path, which an inlined function cannot set.
  fv as (
    select distinct on (x.field_id) regexp_replace(x.type, '[\[(].*$', '') as base, x.dropped
      from c join @schema@.field x
        on x.table_id = $2 and x.field_id = $4 and x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     order by x.field_id, c.level),
  ft as (
    select base not in ('geometry', 'geography', 'variant') as prunes,
           case when base in ('timestamp', 'timestamptz') then 86400000000
                when base in ('timestamp_ns', 'timestamptz_ns') then 86400000000000 end as day
      from fv where not dropped),
  q as (select coalesce((select prunes from ft), false) as prunes, coalesce((select day is not null from ft), false) as dated),
  hidden as (
    select x.file_id, min(c.level) as level
      from @schema@.tombstone x join c on x.ref_id = c.ref_id and x.begin_seq <= c.seq and c.seq < x.end_seq
     where x.table_id = $2
     group by x.file_id),
  candidate as (
    -- Files whose first statistics field is the predicate's, through the index over their bound range.
    select c.level, f.*
      from q, c join @schema@.data_file f
        on f.table_id = $2 and f.ref_id = c.ref_id and f.begin_seq <= c.seq and c.seq < f.end_seq
       and f.stat_field_ids[1] = $4
       and @schema@.bound_range(f.lower[1], f.upper[1]) && @schema@.bound_range($5, $6)
     where q.prunes and not (q.dated and (coalesce(length(f.lower[1]) = 4 or length(f.upper[1]) = 4, false)))
    union all
    -- The same files with bounds of 4 bytes under a field promoted from date, which the range does not order.
    select c.level, f.*
      from q, c join @schema@.data_file f
        on f.table_id = $2 and f.ref_id = c.ref_id and f.begin_seq <= c.seq and c.seq < f.end_seq
       and f.stat_field_ids[1] = $4
     where q.prunes and q.dated and (coalesce(length(f.lower[1]) = 4 or length(f.upper[1]) = 4, false))
    union all
    -- Files whose first statistics field is another, or that have none.
    select c.level, f.*
      from q, c join @schema@.data_file f
        on f.table_id = $2 and f.ref_id = c.ref_id and f.begin_seq <= c.seq and c.seq < f.end_seq
       and f.stat_field_ids[1] is distinct from $4
     where q.prunes
    union all
    -- No pruning: every file.
    select c.level, f.*
      from q, c join @schema@.data_file f
        on f.table_id = $2 and f.ref_id = c.ref_id and f.begin_seq <= c.seq and c.seq < f.end_seq
     where not q.prunes)
  select f.file_id, f.path, f.spec_id, f.partition, f.record_count, f.file_size, f.footer_size, f.first_row_id,
         i.content, ff.footer, d.begin_seq, d.path, d.content_offset, d.content_size, d.cardinality, d.inline
    from candidate f
    left join hidden h on h.file_id = f.file_id and h.level < f.level
    left join lateral (
      select x.begin_seq, x.path, x.content_offset, x.content_size, x.cardinality, x.inline
        from c join @schema@.deletion_vector x
          on x.table_id = $2 and x.file_id = f.file_id and x.ref_id = c.ref_id
         and x.begin_seq <= c.seq and c.seq < x.end_seq
       order by c.level limit 1) d on true
    left join @schema@.inline_file i on i.table_id = $2 and i.file_id = f.file_id
    left join @schema@.file_footer ff on ff.table_id = $2 and ff.file_id = f.file_id
   where h.file_id is null
     -- The bounds themselves, as STA-10 reads them.
     and not coalesce((
       select (($5 is not null and b.upper < $5) or ($6 is not null and b.lower > $6))
         from ft, array_position(f.stat_field_ids, $4) p,
              lateral (select case when length(f.lower[p]) = 4 and ft.day is not null
                                   then int8send((((('x' || encode(f.lower[p], 'hex'))::bit(32)::int # (-2147483648)::int)::bigint
                                                  * ft.day) # (-9223372036854775808)::bigint))
                                   else f.lower[p] end as lower,
                              case when length(f.upper[p]) = 4 and ft.day is not null
                                   then int8send((((('x' || encode(f.upper[p], 'hex'))::bit(32)::int # (-2147483648)::int)::bigint
                                                  * ft.day) # (-9223372036854775808)::bigint))
                                   else f.upper[p] end as upper) b
        where ft.prunes), false);
$$;
