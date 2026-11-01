-- MetaLake v0 · reference DDL · the catalog: meta and sequences (§2, §3).

-- Loaded in file-name order into an empty schema, which becomes one catalog (LAY-1). 9223372036854775807 is OPEN
-- (LAY-4).

create table meta (
  key text primary key,
  value text not null);

create sequence commit_seq_gen as bigint start 1 no cycle;
create sequence ref_id_gen as int start 2 no cycle;
create sequence file_id_gen as bigint start 1 no cycle;
