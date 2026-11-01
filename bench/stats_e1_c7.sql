select file from st_row where tbl = 1 and col = 7 and min_i is not null and int8range(min_i, max_i, '[]') && int8range(500000, 500100, '[]');
