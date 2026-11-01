-- MetaLake · preliminary study 1.4 + 2.5: three ways to store per-file, per-column statistics.
-- 20 columns: c1..c10 integers, c11..c20 text (min/max truncated to 16 characters, as Parquet does).
--   e1 · one row per (file, column), typed: a single index serves to prune on any column.
--   e2 · one jsonb per file: {"c1": [min, max, nulls], ...}.
--   e3 · arrays per file: mins_i[10], maxs_i[10], mins_s[10], maxs_s[10], null_counts[20].
-- Column c3 grows with the file (like a date in an append-only table); the others are unordered.

create table if not exists st_row (
  tbl int not null, file bigint not null, col smallint not null,
  min_i bigint, max_i bigint, min_s text, max_s text, null_count bigint not null);
create table if not exists st_json (tbl int not null, file bigint not null, s jsonb not null);
create table if not exists st_arr (
  tbl int not null, file bigint not null,
  mins_i bigint[], maxs_i bigint[], mins_s text[], maxs_s text[], null_counts bigint[]);

create or replace procedure seed_stats(n bigint) language sql as $$
  insert into st_row
    select 1, f, c,
           case when c > 10 then null when c = 3 then f * 10 else abs(hashint8(f * 31 + c)) % 1000000 end,
           case when c > 10 then null when c = 3 then f * 10 + 9 else abs(hashint8(f * 31 + c)) % 1000000 + 1000 end,
           case when c > 10 then left(md5(f || '-' || c), 16) end,
           case when c > 10 then left(md5(f || '-' || c), 15) || 'z' end,
           f % 7
      from generate_series(1, n) f, generate_series(1, 20) c;
  insert into st_json
    select tbl, file,
           jsonb_object_agg('c' || col, case when col <= 10 then jsonb_build_array(min_i, max_i, null_count)
                                             else jsonb_build_array(min_s, max_s, null_count) end)
      from st_row group by tbl, file;
  insert into st_arr
    select tbl, file,
           array_agg(min_i order by col) filter (where col <= 10), array_agg(max_i order by col) filter (where col <= 10),
           array_agg(min_s order by col) filter (where col > 10), array_agg(max_s order by col) filter (where col > 10),
           array_agg(null_count order by col)
      from st_row group by tbl, file;
  -- e1: one range index for all integer columns and one for the text ones.
  create index st_row_i on st_row using gist (tbl, col, int8range(min_i, max_i, '[]')) where min_i is not null;
  create index st_row_s on st_row (tbl, col, min_s, max_s) where min_s is not null;
  create index st_row_f on st_row (tbl, file);
  -- e2 and e3: one expression index per column; here only for c3 (each extra column is another index).
  create index st_json_c3 on st_json using gist (tbl, int8range((s -> 'c3' ->> 0)::bigint, (s -> 'c3' ->> 1)::bigint, '[]'));
  create index st_json_f on st_json (tbl, file);
  create index st_arr_c3 on st_arr using gist (tbl, int8range(mins_i[3], maxs_i[3], '[]'));
  create index st_arr_f on st_arr (tbl, file);
  analyze st_row; analyze st_json; analyze st_arr;
$$;
