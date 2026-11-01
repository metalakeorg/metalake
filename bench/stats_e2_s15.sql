select file from st_json where tbl = 1 and s -> 'c15' ->> 0 <= 'a3zz' and s -> 'c15' ->> 1 >= 'a3';
