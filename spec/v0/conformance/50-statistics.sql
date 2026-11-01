-- Statistics (§8). Sampled values are generated with a fixed seed, so every run checks the same values.

select new_table('gs://lake/t', 1) as t \gset
select setseed(0.42);
-- The encoded order of `vals` of type `t` is exactly the order of `native`, an SQL expression over `v`.
create function expect_order(t text, native text, vals text[]) returns void language plpgsql as $$
declare bad text;
begin
  execute format($q$
    select string_agg(v, ', ') from (
      select v, dense_rank() over (order by %s) as a, dense_rank() over (order by encode_bound(%L, v)) as b
        from unnest($1) v) x
     where a <> b$q$, native, t) into bad using vals;
  if bad is not null then
    raise exception '%: encoded order differs from value order at %', t, left(bad, 300);
  end if;
end $$;
create function random_string() returns text language sql volatile as $$
  select string_agg(chr(case floor(random() * 4)::int
      when 0 then 32 + floor(random() * 95)::int
      when 1 then 128 + floor(random() * 1920)::int
      when 2 then (array[2048 + floor(random() * 53248)::int, 57344 + floor(random() * 8192)::int])[1 + floor(random() * 2)::int]
      else 65536 + floor(random() * 1048576)::int end), '')
    from generate_series(1, floor(random() * 20)::int);
$$;
-- A data file of the only table with statistics, live unless `e` is given.
create function sf(id bigint, ids int[], lows bytea[], ups bytea[], r int default 1, b bigint default 1)
returns void language sql as $$
  insert into data_file (table_id, file_id, ref_id, begin_seq, path, spec_id, partition, record_count, file_size,
                         footer_size, stat_field_ids, lower, upper, null_count, nan_count)
  select table_id, id, r, b, 'gs://lake/t/' || id || '.parquet', 0, '[]', 100, 1 << 20, 4096, ids, lows, ups,
         array_fill(0::bigint, array[cardinality(ids)]), array_fill(null::bigint, array[cardinality(ids)]) from tbl;
$$;
create function pruned(f int, lo bytea, hi bytea) returns bigint[] language sql as $$
  select coalesce(array_agg(file_id order by file_id), '{}') from prune(1, (select table_id from tbl), 9, f, lo, hi);
$$;

-- case: record count, file size and footer size are always there and never negative
-- rules: STA-1
select expect_error(format('insert into data_file (table_id, file_id, ref_id, begin_seq, spec_id, partition, record_count,
  file_size) values (%L, 1, 1, 1, 0, ''[]'', 1, 1)', :'t'), '23502', 'no footer size');
select expect_error(format('insert into data_file (table_id, file_id, ref_id, begin_seq, spec_id, partition, record_count,
  file_size, footer_size) values (%L, 1, 1, 1, 0, ''[]'', -1, 1, 1)', :'t'), '23514', 'a negative record count');

-- case: statistics are parallel arrays over distinct fields, with NaN counts only for floating point
-- rules: STA-2
select fld(1, 'id', 'long', 1, 1), fld(2, 'x', 'double', 1, 1);
insert into partition_spec values (:'t', 0, '[]', 1, 1);
select expect_error(format('insert into data_file (table_id, file_id, ref_id, begin_seq, spec_id, partition, record_count,
  file_size, footer_size, stat_field_ids, lower, upper, null_count, nan_count)
  values (%L, 1, 1, 1, 0, ''[]'', 1, 1, 1, ''{1,2}'', ''{null}'', ''{null,null}'', ''{0,0}'', ''{null,null}'')', :'t'),
  '23514', 'a lower array shorter than the field ids');
select sf(1, '{1,2}', '{null,null}', '{null,null}');
update data_file set nan_count = '{null,3}';
select expect((select count(*) = 0 from file_errors(1, :'t', 9)), 'a NaN count for the double');
update data_file set nan_count = '{3,null}';
select expect((select array_agg(left(e, 5)) = '{STA-2}' from file_errors(1, :'t', 9) e), 'a NaN count for the long');
update data_file set nan_count = '{null,null}', stat_field_ids = '{1,1}';
select expect((select array_agg(left(e, 5)) = '{STA-2}' from file_errors(1, :'t', 9) e), 'a repeated field');

-- case: pruning finds a field's statistics at any position
-- rules: STA-3, STA-10
select fld(1, 'id', 'long', 1, 1), fld(2, 'name', 'string', 1, 1);
select sf(1, '{1,2}', array[encode_bound('long', '0'), encode_bound('string', 'a')], array[encode_bound('long', '9'), encode_bound('string', 'c')]);
select sf(2, '{2,1}', array[encode_bound('string', 'd'), encode_bound('long', '10')], array[encode_bound('string', 'f'), encode_bound('long', '19')]);
select sf(3, '{2}', array[encode_bound('string', 'a')], array[encode_bound('string', 'z')]);
select expect(pruned(1, encode_bound('long', '12'), encode_bound('long', '12')) = '{2,3}', 'id = 12: 2, and 3 with no bounds for id');
select expect(pruned(1, encode_bound('long', '9'), null) = '{1,2,3}', 'id >= 9');
select expect(pruned(1, null, encode_bound('long', '-1')) = '{3}', 'id < 0');
select expect(pruned(2, encode_bound('string', 'b'), encode_bound('string', 'b')) = '{1,3}', 'name = b');
select expect(pruned(2, encode_bound('string', 'g'), null) = '{3}', 'name >= g');

-- case: partition values match the file's spec, and its sort order exists
-- rules: STA-4
select fld(1, 'id', 'long', 1, 1);
insert into partition_spec values (:'t', 0, '[{"source_ids": [1], "field_id": 1000, "name": "b", "transform": "bucket[4]"}]', 1, 1);
insert into sort_order values (:'t', 1, '[]', 1, 1);
select sf(1, '{}', '{}', '{}');
update data_file set partition = '[3]', sort_order_id = 1;
select expect((select count(*) = 0 from file_errors(1, :'t', 9)), 'valid');
update data_file set partition = '[3, 4]';
select expect((select array_agg(left(e, 5)) = '{STA-4}' from file_errors(1, :'t', 9) e), 'two values for one field');
update data_file set partition = '[3]', spec_id = 7;
select expect((select array_agg(left(e, 5)) = '{STA-4}' from file_errors(1, :'t', 9) e), 'a spec the table does not have');
update data_file set spec_id = 0, sort_order_id = 2;
select expect((select array_agg(left(e, 5)) = '{STA-4}' from file_errors(1, :'t', 9) e), 'a sort order the table does not have');
update data_file set sort_order_id = 0;
select expect((select count(*) = 0 from file_errors(1, :'t', 9)), 'unsorted');

-- case: the encoding preserves the order of integers, dates, times and timestamps
-- rules: STA-5
select expect_order('boolean', 'v::boolean', '{true,false}');
select expect_order('int', 'v::int', array['-2147483648', '2147483647', '-1', '0', '1'] ||
  array(select (floor(random() * 4294967296) - 2147483648)::bigint::text from generate_series(1, 300)));
select expect_order('long', 'v::bigint', array['-9223372036854775808', '9223372036854775807', '-1', '0', '1'] ||
  array(select ((random() - 0.5) * 1.8e19)::numeric(20, 0)::text from generate_series(1, 300)));
select expect_order('date', 'v::date', array['1970-01-01', '1969-12-31', '0001-01-01', '9999-12-31'] ||
  array(select (date '1970-01-01' + (random() * 2e6 - 1e6)::int)::text from generate_series(1, 300)));
select expect_order('time', 'v::time', array['00:00:00', '23:59:59.999999', '12:00:00.000001'] ||
  array(select (time '00:00' + random() * interval '24 hours')::text from generate_series(1, 300)));
select expect_order('timestamp', 'v::timestamp', array['1970-01-01T00:00:00', '1969-12-31T23:59:59.999999', '1600-02-29T12:00:00.5'] ||
  array(select to_char(timestamp '1970-01-01' + (random() * 2e16 - 1e16) * interval '1 microsecond', 'YYYY-MM-DD"T"HH24:MI:SS.US') from generate_series(1, 300)));
select expect_order('timestamptz', 'v::timestamptz', array['2017-11-16T17:10:34-08:00', '2017-11-17T01:10:33.999999+00:00', '2017-11-17T01:10:34+00:00'] ||
  array(select to_char(timestamptz '1970-01-01 00:00Z' + (random() * 2e16 - 1e16) * interval '1 microsecond', 'YYYY-MM-DD"T"HH24:MI:SS.US') || '+00:00' from generate_series(1, 300)));
-- Nanoseconds: the native order is the timestamp to the second, then the nine fractional digits.
select expect_order('timestamp_ns', $$(regexp_replace(v, '\.[0-9]+', '')::timestamp, substring(v from '\.([0-9]+)'))$$,
  array['1970-01-01T00:00:00.000000000', '1969-12-31T23:59:59.999999999', '1970-01-01T00:00:00.000000001'] ||
  array(select to_char(timestamp '1970-01-01' + floor(random() * 1.8e10 - 9e9) * interval '1 second', 'YYYY-MM-DD"T"HH24:MI:SS')
               || '.' || lpad(floor(random() * 1e9)::text, 9, '0') from generate_series(1, 300)));
select expect_order('timestamptz_ns', $$(regexp_replace(v, '\.[0-9]+', '')::timestamptz, substring(v from '\.([0-9]+)'))$$,
  array['2017-11-16T17:10:34.000000001-08:00', '2017-11-17T01:10:34.000000000+00:00', '2017-11-17T01:10:34.000000002+00:00']);

-- case: the encoding preserves the order of floating point values and decimals at the limits of their scale
-- rules: STA-5
select expect_order('double', 'v::float8', array['Infinity', '-Infinity', '1', '-1', '1e-308', '-1e-308', '5e-324', '-5e-324',
  '1.7976931348623157e308', '-1.7976931348623157e308'] ||
  array(select ((random() - 0.5) * 10 ^ (random() * 600 - 300))::float8::text from generate_series(1, 300)));
select expect_order('float', 'v::float4', array['Infinity', '-Infinity', '3.4028235e38', '-3.4028235e38', '1e-45', '-1e-45'] ||
  array(select ((random() - 0.5) * 10 ^ (random() * 70 - 35))::float4::text from generate_series(1, 300)));
select expect(encode_bound('double', '-0') < encode_bound('double', '0'), '-0.0 precedes +0.0');
select expect(encode_bound('double', '-5e-324') < encode_bound('double', '-0')
          and encode_bound('double', '0') < encode_bound('double', '5e-324'), '±0 between the smallest subnormals');
select expect_order('decimal(38,10)', 'v::numeric', array['9999999999999999999999999999.9999999999',
  '-9999999999999999999999999999.9999999999', '0', '0.0000000001', '-0.0000000001'] ||
  array(select round(((random() - 0.5) * 10 ^ (random() * 56 - 28))::numeric, 10)::text from generate_series(1, 300)));
select expect_order('decimal(9,2)', 'v::numeric', '{-9999999.99,9999999.99,0,0.01,-0.01,1.5,1.50}');
select expect_error($$select encode_bound('decimal(9,2)', '1.505')$$, '22023', 'more decimal places than the scale');

-- case: the encoding preserves the order of strings by code point, and of uuids, fixed and binary values
-- rules: STA-5
select expect_order('string', 'v collate "C"', array['', 'a', 'ab', 'b', 'é', 'z', '￿', '𝄞', U&'\+10FFFF', 'aé', 'a𝄞'] ||
  array(select coalesce(random_string(), '') from generate_series(1, 300)));
select expect_order('uuid', 'v::uuid', array['00000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff',
  '7fffffff-ffff-ffff-ffff-ffffffffffff', '80000000-0000-0000-0000-000000000000'] ||
  array(select gen_random_uuid()::text from generate_series(1, 300)));
select expect_order('binary', 'decode(v, ''hex'')', array['', '00', '0000', 'ff', 'ff00', '7f', '80'] ||
  array(select encode(decode(string_agg(lpad(to_hex(floor(random() * 256)::int), 2, '0'), ''), 'hex'), 'hex')
          from generate_series(1, 300) g, generate_series(1, 1 + g % 20) b group by g));
select expect_order('fixed[2]', 'decode(v, ''hex'')', '{0000,0001,00ff,7fff,8000,ff00,ffff}');

-- case: known encodings, byte for byte
-- rules: STA-5
select expect(encode_bound(t, v) = b::bytea, format('%s %s -> %s, got %s', t, v, b, encode_bound(t, v)))
  from (values
    ('boolean', 'false', '\x00'), ('boolean', 'true', '\x01'),
    ('long', '0', '\x8000000000000000'), ('int', '-1', '\x7fffffffffffffff'),
    ('long', '-9223372036854775808', '\x0000000000000000'),
    ('date', '1970-01-02', '\x80000001'), ('date', '1969-12-31', '\x7fffffff'),
    ('time', '00:00:01.5', '\x800000000016e360'),
    ('timestamp', '1970-01-01T00:00:00.5', '\x800000000007a120'),
    ('timestamptz', '1970-01-01T01:00:00+01:00', '\x8000000000000000'),
    ('timestamp_ns', '1970-01-01T00:00:00.000000001', '\x8000000000000001'),
    ('timestamp_ns', '1970-01-01T00:00:00.5', '\x800000001dcd6500'),
    ('double', '1', '\xbff0000000000000'), ('double', '-1', '\x400fffffffffffff'),
    ('double', '0', '\x8000000000000000'), ('double', '-0', '\x7fffffffffffffff'),
    ('float', '1', '\xbff0000000000000'),
    ('decimal(9,2)', '1.00', '\x80000000000000000000000000000064'),
    ('decimal(9,2)', '-0.01', '\x7fffffffffffffffffffffffffffffff'),
    ('string', 'é', '\xc3a9'),
    ('uuid', 'F79C3E09-677C-4BBD-A479-3F349CB785E7', '\xf79c3e09677c4bbda4793f349cb785e7'),
    ('binary', '00ff', '\x00ff')) x(t, v, b);

-- case: NaN is never a bound
-- rules: STA-6
select expect_error($$select encode_bound('double', 'NaN')$$, '22023', 'a NaN double');
select expect_error($$select encode_bound('float', 'NaN')$$, '22023', 'a NaN float');

-- case: promoted fields keep comparable bounds, and dates read as timestamps after promotion
-- rules: STA-7
select expect(encode_bound('int', '-5') = encode_bound('long', '-5'), 'int and long');
select expect(encode_bound('float', '0.1') = encode_bound('double', (0.1::float4::float8)::text), 'float and double');
select expect(encode_bound('decimal(9,2)', '-1.5') = encode_bound('decimal(20,2)', '-1.50'), 'decimal widened');
-- File 1 was written when the field was a date; file 2 after it became a timestamp.
select fld(1, 'at', 'timestamp', 1, 1);
select sf(1, '{1}', array[encode_bound('date', '2026-01-01')], array[encode_bound('date', '2026-01-31')]);
select sf(2, '{1}', array[encode_bound('timestamp', '2026-02-01T08:00:00')], array[encode_bound('timestamp', '2026-02-02T08:00:00')]);
select expect(pruned(1, encode_bound('timestamp', '2026-01-31T00:00:00'), encode_bound('timestamp', '2026-01-31T00:00:00')) = '{1}',
  'midnight of the last day of file 1');
select expect(pruned(1, encode_bound('timestamp', '2026-01-31T00:00:01'), encode_bound('timestamp', '2026-02-01T07:00:00')) = '{}',
  'between the files');
select expect(pruned(1, null, encode_bound('timestamp', '2026-02-01T09:00:00')) = '{1,2}', 'both');
select fld(2, 'at_ns', 'timestamp_ns', 1, 1);
update data_file set stat_field_ids = '{2}' where file_id = 1;
select expect(pruned(2, encode_bound('timestamp_ns', '2026-01-15T00:00:00.000000001'), null) = '{1,2}',
  'nanoseconds: 1 by its date bounds, 2 has none for this field');
select expect(pruned(2, encode_bound('timestamp_ns', '2026-01-31T00:00:00.000000001'), null) = '{2}',
  'nanoseconds: after the last day of 1');

-- case: string and binary bounds truncate to 16 code points or bytes, and upper bounds stay upper bounds
-- rules: STA-8
select expect(truncate_lower('string', encode_bound('string', 'abcdefghijklmnopqrstuvwxyz')) = 'abcdefghijklmnop'::bytea, 'lower');
select expect(truncate_upper('string', encode_bound('string', 'abcdefghijklmnopqrstuvwxyz')) = 'abcdefghijklmnoq'::bytea, 'upper');
select expect(truncate_upper('string', encode_bound('string', 'short')) = 'short'::bytea, 'short strings are kept');
select expect(truncate_upper('string', encode_bound('string', 'abcdefghijklmnop')) = 'abcdefghijklmnop'::bytea
          and truncate_upper('binary', decode(repeat('ab', 16), 'hex')) = decode(repeat('ab', 16), 'hex'),
  'exactly 16 code points or bytes are kept');
select expect(truncate_upper('string', convert_to('abcdefghijklmno' || U&'\+10FFFF' || 'z', 'UTF8')) = 'abcdefghijklmnp'::bytea,
  'a trailing U+10FFFF is dropped');
select expect(truncate_upper('string', convert_to('abcdefghijklmno' || U&'\D7FF' || 'z', 'UTF8')) = convert_to('abcdefghijklmno' || U&'\E000', 'UTF8'),
  'the increment skips the surrogates');
select expect(truncate_upper('string', convert_to(repeat(U&'\+10FFFF', 17), 'UTF8')) is null, 'nothing to increment');
select expect(truncate_upper('binary', decode(repeat('ab', 15) || 'ffff00', 'hex')) = decode(repeat('ab', 14) || 'ac', 'hex'),
  'binary: trailing 0xff dropped, then incremented');
select expect(truncate_upper('binary', decode(repeat('ff', 17), 'hex')) is null, 'binary: nothing to increment');
-- Property: the bounds hold for random strings, and a truncated upper bound for any string with the same first
-- 16 code points.
select expect(bool_and(truncate_lower('string', convert_to(v, 'UTF8')) <= convert_to(v, 'UTF8')
              and (length(v) <= 16 or coalesce(truncate_upper('string', convert_to(v, 'UTF8')) >= convert_to(w, 'UTF8'), true))
              and length(convert_from(truncate_lower('string', convert_to(v, 'UTF8')), 'UTF8')) <= 16
              and coalesce(length(convert_from(truncate_upper('string', convert_to(v, 'UTF8')), 'UTF8')) <= 16, true)),
  'random strings')
  from (select v, left(v, 16) || coalesce(random_string(), '') || coalesce(random_string(), '') as w
          from (select coalesce(random_string(), '') || coalesce(random_string(), '') as v from generate_series(1, 500)) x) y;

-- case: geospatial bounds are box corners with ordered coordinates, and they are not pruned
-- rules: STA-9
select expect(length(encode_point(1, 2)) = 16 and length(encode_point(1, 2, 3)) = 24 and length(encode_point(1, 2, 3, 4)) = 32
  and length(encode_point(1, 2, m => 4)) = 32, 'x:y, x:y:z, x:y:z:m, and x:y:NaN:m');
select expect(substring(encode_point(1, 2, m => 4) from 17 for 8) = order_double('NaN'), 'z is NaN when only m is set');
select expect(encode_point(-180, -90) < encode_point(-179.5, 0) and encode_point(-179.5, 0) < encode_point(180, 90),
  'x orders first');
select fld(1, 'shape', 'geography', 1, 1);
-- A box that crosses the antimeridian: x from 170 to -170.
select sf(1, '{1}', array[encode_point(170, -10)], array[encode_point(-170, 10)]);
select expect(pruned(1, encode_point(175, 0), encode_point(175, 0)) = '{1}', 'never pruned');

-- case: pruning returns exactly the visible files whose bounds overlap the range
-- rules: STA-10
select fld(1, 'k', 'long', 1, 1);
create temp table truth (file_id bigint, lo bigint, hi bigint);
insert into truth select g, a, a + floor(random() * 1000)::bigint
  from generate_series(1, 300) g, lateral (select floor(random() * 100000)::bigint - 50000 + g * 0 as a) x;
select sf(file_id, '{1}', array[encode_bound('long', lo::text)], array[encode_bound('long', hi::text)]) from truth;
select sf(1000, '{}', '{}', '{}');
select sf(1001, '{1}', '{null}', '{null}');
update data_file set end_seq = 5 where file_id = 7;
create temp table probe as select floor(random() * 120000)::bigint - 60000 as a, floor(random() * 3000)::bigint as w
  from generate_series(1, 200);
select expect(bool_and(
  (select coalesce(array_agg(file_id order by file_id), '{}') from prune(1, :'t', 9, 1, encode_bound('long', a::text), encode_bound('long', (a + w)::text)))
  = (select array_agg(file_id order by file_id) from (
       select file_id from truth where hi >= a and lo <= a + w and file_id <> 7 union all select 1000 union all select 1001) y)),
  '200 random ranges against 300 random files, one of them closed, two without bounds')
  from probe;
