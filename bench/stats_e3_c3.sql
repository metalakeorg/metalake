select file from st_arr where tbl = 1 and int8range(mins_i[3], maxs_i[3], '[]') && int8range(:lo, :hi, '[]');
