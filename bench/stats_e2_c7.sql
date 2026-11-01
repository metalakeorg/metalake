select file from st_json where tbl = 1 and (s -> 'c7' ->> 0)::bigint <= 500100 and (s -> 'c7' ->> 1)::bigint >= 500000;
