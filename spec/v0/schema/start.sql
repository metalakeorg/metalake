-- MetaLake v0 · reference DDL · the rows a new catalog starts with (LAY-2, ID-2).

insert into meta values
  ('format_version', '0'),
  ('catalog_uuid', gen_random_uuid()::text),
  ('created_at', to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
insert into ref (ref_id, name, kind, created_seq) values (1, 'main', 'branch', 0);
