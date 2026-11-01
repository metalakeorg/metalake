select file from st_json where tbl = 1 and int8range((s -> 'c3' ->> 0)::bigint, (s -> 'c3' ->> 1)::bigint, '[]') && int8range(:lo, :hi, '[]');
