-- Day 45: the product catalogue gains brands. The column is added to the table, not to the files written before it,
-- which have none: Hive reads it as null there.
alter table curated.productos add columns (marca string);
