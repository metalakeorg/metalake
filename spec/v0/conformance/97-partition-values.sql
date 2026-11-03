-- Partition values (§15), checked against the examples of the Iceberg v3 specification.

create function cm(tables jsonb, r int default 1) returns bigint language sql as $$
  select commit(jsonb_build_object('ref', r, 'author', 'ana', 'actor', 'test', 'operation', 'append', 'tables', tables));
$$;
create function h(t text, v text) returns text language sql as $$ select encode(encode_bound(t, v), 'hex'); $$;
-- A table with 1 id long, 2 ts timestamp, 3 s string, partitioned by bucket[4](id), day(ts), truncate[2](s).
create function mk() returns uuid language plpgsql as $$
declare t uuid := gen_random_uuid();
begin
  perform cm(jsonb_build_array(jsonb_build_object('table_id', t,
    'create', '{"namespace": ["db"], "name": "t", "location": "gs://lake/t"}'::jsonb,
    'fields', '[{"field_id": 1, "new": true, "position": 1, "name": "id", "type": "long", "required": true},
                {"field_id": 2, "new": true, "position": 2, "name": "ts", "type": "timestamp"},
                {"field_id": 3, "new": true, "position": 3, "name": "s", "type": "string"}]'::jsonb,
    'partition_spec', '[{"source_ids": [1], "field_id": 1000, "name": "id_b", "transform": "bucket[4]"},
                        {"source_ids": [2], "field_id": 1001, "name": "ts_day", "transform": "day"},
                        {"source_ids": [3], "field_id": 1002, "name": "s_t", "transform": "truncate[2]"}]'::jsonb)));
  return t;
end $$;
-- A file with partition `p` and bounds (lo, hi) for id, ts and s; `nulls` are the null counts.
create function pf(id bigint, p jsonb, lo text[], hi text[], nulls bigint[] default '{0,0,0}') returns jsonb language sql as $$
  select jsonb_build_object('file_id', id, 'path', 'gs://lake/t/' || id || '.parquet', 'record_count', 10, 'file_size', 1,
    'footer_size', 1, 'partition', p, 'stat_field_ids', '[1, 2, 3]'::jsonb,
    'lower', jsonb_build_array(h('long', lo[1]), h('timestamp', lo[2]), h('string', lo[3])),
    'upper', jsonb_build_array(h('long', hi[1]), h('timestamp', hi[2]), h('string', hi[3])),
    'null_count', to_jsonb(nulls), 'nan_count', '[null, null, null]'::jsonb);
$$;
create function add(t uuid, f jsonb) returns bigint language sql as $$
  select cm(jsonb_build_array(jsonb_build_object('table_id', t, 'add', jsonb_build_array(f))));
$$;

-- case: bucket hashes reproduce Iceberg's test values, and the bucket masks the sign bit
-- rules: PAR-1
select expect(hash_value(t, v) = want, format('%s %s -> %s, got %s', t, v, want, hash_value(t, v))) from (values
  ('int', '34', 2017239379), ('long', '34', 2017239379), ('decimal(4,2)', '14.20', -500754589),
  ('date', '2017-11-16', -653330422), ('time', '22:31:08', -662762989),
  ('timestamp', '2017-11-16T22:31:08', -2047944441), ('timestamp', '2017-11-16T22:31:08.000001', -1207196810),
  ('timestamptz', '2017-11-16T14:31:08-08:00', -2047944441), ('timestamptz', '2017-11-16T14:31:08.000001-08:00', -1207196810),
  ('timestamp_ns', '2017-11-16T22:31:08', -2047944441), ('timestamp_ns', '2017-11-16T22:31:08.000001001', -1207196810),
  ('timestamptz_ns', '2017-11-16T14:31:08-08:00', -2047944441),
  ('timestamptz_ns', '2017-11-16T14:31:08.000001001-08:00', -1207196810),
  ('string', 'iceberg', 1210000089), ('uuid', 'f79c3e09-677c-4bbd-a479-3f349cb785e7', 1488055340),
  ('fixed[4]', '00010203', -188683207), ('binary', '00010203', -188683207), ('string', '34', -427558391)) x(t, v, want);
select expect(transform_value('int', 'bucket[16]', '34') = to_jsonb(2017239379 % 16), 'bucket of a positive hash');
select expect(transform_value('string', 'bucket[16]', '34') = '9', 'bucket of a negative hash: the sign bit is dropped first');

-- case: truncate, as in Iceberg's examples, by code point for strings and by byte for binary
-- rules: PAR-1
select expect(transform_value(t, tr, v) = want, format('%s %s %s -> %s, got %s', t, tr, v, want, transform_value(t, tr, v))) from (values
  ('int', 'truncate[10]', '1', '0'::jsonb), ('int', 'truncate[10]', '-1', '-10'), ('long', 'truncate[10]', '25', '20'),
  ('decimal(9,2)', 'truncate[50]', '10.65', '"10.50"'), ('decimal(9,2)', 'truncate[50]', '-0.01', '"-0.50"'),
  ('string', 'truncate[3]', 'iceberg', '"ice"'), ('string', 'truncate[2]', '𝄞éa', '"𝄞é"'),
  ('binary', 'truncate[3]', '0102030405', '"010203"')) x(t, tr, v, want);

-- case: years, months, days and hours from the epoch in UTC, rounding down; void and null give null
-- rules: PAR-1
select expect(transform_value(t, tr, v) = want, format('%s %s %s -> %s, got %s', t, tr, v, want, transform_value(t, tr, v))) from (values
  ('date', 'year', '2017-11-16', '47'::jsonb), ('date', 'month', '2017-11-16', '574'), ('date', 'day', '2017-11-16', '17486'),
  ('date', 'year', '1969-12-31', '-1'), ('date', 'month', '1969-12-31', '-1'), ('date', 'day', '1969-12-31', '-1'),
  ('timestamp', 'day', '1969-12-31T23:59:59.999999', '-1'), ('timestamp', 'hour', '1969-12-31T23:59:59.999999', '-1'),
  ('timestamp', 'hour', '1970-01-01T01:00:00', '1'),
  ('timestamptz', 'day', '2017-11-16T23:30:00-08:00', '17487'),
  ('timestamp_ns', 'hour', '1969-12-31T23:59:59.999999999', '-1'),
  ('long', 'void', '7', 'null'), ('long', 'identity', null, 'null'), ('long', 'identity', '7', '7')) x(t, tr, v, want);
-- The session's time zone does not matter: timestamps with a zone count in UTC.
set timezone = 'America/Los_Angeles';
select expect(transform_value('timestamptz', 'day', '2017-11-16T23:30:00-08:00') = '17487'
          and transform_value('timestamptz', 'hour', '1970-01-01T00:30:00+01:00') = '-1'
          and hash_value('timestamptz', '2017-11-16T14:31:08-08:00') = -2047944441, 'in another session time zone');
reset timezone;

-- case: a file holds one partition value per field, serialized as its result type
-- rules: PAR-2
select mk() as t \gset
select add(:'t', pf(1, jsonb_build_array(transform_value('long', 'bucket[4]', '7'), 17486, 'ab'),
  '{7,2017-11-16T01:00:00,abX}', '{7,2017-11-16T23:00:00,abZ}'));
select expect((select partition = jsonb_build_array(transform_value('long', 'bucket[4]', '7'), 17486, 'ab') from data_file where file_id = 1),
  'an int bucket, an int day and a string truncation');

-- case: a commit refuses partition values its statistics contradict
-- rules: PAR-3
select mk() as t \gset
select expect_error(format('select add(%L, %L::jsonb)', :'t', pf(2, jsonb_build_array(transform_value('long', 'bucket[4]', '7'), 17486, 'ab'),
  '{7,2017-11-16T01:00:00,abX}', '{7,2017-11-17T01:00:00,abZ}')), '22023', 'rows of two days');
select expect_error(format('select add(%L, %L::jsonb)', :'t', pf(3, jsonb_build_array((transform_value('long', 'bucket[4]', '7')::int + 1) % 4, 17486, 'ab'),
  '{7,2017-11-16T01:00:00,abX}', '{7,2017-11-16T02:00:00,abZ}')), '22023', 'one id, in another bucket');
select expect(add(:'t', pf(4, jsonb_build_array(0, 17486, 'ab'), '{1,2017-11-16T01:00:00,abX}', '{9,2017-11-16T02:00:00,abZ}')) > 0,
  'a range of ids: the bucket cannot be checked');
select expect(add(:'t', pf(5, jsonb_build_array(transform_value('long', 'bucket[4]', '7'), 17486, 'ab'),
  '{7,2017-11-16T01:00:00,ab}', '{7,2017-11-16T02:00:00,ac}')) > 0, 'a truncated string upper bound, ac, still covers ab');
select expect_error(format('select add(%L, %L::jsonb)', :'t', pf(6, jsonb_build_array(transform_value('long', 'bucket[4]', '7'), 17486, 'zz'),
  '{7,2017-11-16T01:00:00,abX}', '{7,2017-11-16T02:00:00,abZ}')), '22023', 'a string partition outside the bounds');
select expect_error(format('select add(%L, %L::jsonb)', :'t', pf(7, jsonb_build_array(transform_value('long', 'bucket[4]', '7'), null, 'ab'),
  '{7,2017-11-16T01:00:00,abX}', '{7,2017-11-16T02:00:00,abZ}')), '22023', 'a null day for a file with timestamps');
select expect(add(:'t', pf(8, jsonb_build_array(transform_value('long', 'bucket[4]', '7'), null, 'ab'),
  array['7', null, 'abX'], array['7', null, 'abZ'], '{0,10,0}')) > 0, 'a null day for a file whose timestamps are all null');
select expect_error(format('select add(%L, %L::jsonb)', :'t', pf(9, jsonb_build_array(transform_value('long', 'bucket[4]', '7'), 17486, 'ab'),
  '{7,2017-11-16T01:00:00,abX}', '{7,2017-11-16T02:00:00,abZ}', '{0,3,0}')), '22023', 'nulls in a file of one day');

-- case: bounds decode back to the values they encode, for every type, so transforms can read them
-- rules: PAR-3
select expect(encode_bound(t, decode_bound(t, encode_bound(t, v))) = encode_bound(t, v), format('%s %s', t, v)) from (values
  ('boolean', 'true'), ('int', '-2147483648'), ('long', '9223372036854775807'), ('date', '0001-01-01'),
  ('time', '23:59:59.999999'), ('timestamp', '1600-02-29T12:00:00.000001'), ('timestamptz', '2017-11-16T14:31:08.5-08:00'),
  ('timestamp_ns', '1969-12-31T23:59:59.999999999'), ('timestamptz_ns', '2017-11-16T14:31:08.000001001-08:00'),
  ('double', '-0'), ('double', '5e-324'), ('double', '-1.7976931348623157e308'), ('double', 'Infinity'), ('float', '3.4028235e38'),
  ('decimal(38,10)', '-9999999999999999999999999999.9999999999'), ('decimal(9,2)', '0.01'),
  ('string', '𝄞é'), ('uuid', 'f79c3e09-677c-4bbd-a479-3f349cb785e7'), ('fixed[2]', 'ff00'), ('binary', '')) x(t, v);

-- case: nanoseconds before the epoch hash as the microsecond they round down to
-- rules: PAR-1
select expect(hash_value('timestamp_ns', '1969-12-31T23:59:59.999999999') = hash_value('timestamp', '1969-12-31T23:59:59.999999'),
  '-1 nanosecond is in microsecond -1');

-- case: years and months count the proleptic calendar, which has a year 0, as Iceberg does
-- rules: PAR-1
select expect(transform_value('date', 'year', '0001-12-31 BC') = '-1970', '1 BC is year 0');
select expect(transform_value('date', 'year', '0002-01-01 BC') = '-1971', '2 BC is year -1');
select expect(transform_value('date', 'month', '0001-12-01 BC') = to_jsonb(-1970 * 12 + 11), 'December of year 0');
select expect(transform_value('date', 'year', '0001-01-01') = '-1969', 'AD 1 is year 1');
