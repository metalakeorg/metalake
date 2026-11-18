-- MetaLake v0 · reference DDL · revision 0.5: ownership (§13.4).

-- OWN-1, OWN-2: prefixes a table owns besides its location, which a commit adds and nothing removes. Not versioned:
-- whether an object may be deleted is a fact of storage, the same on every ref.
create table owned_prefix (
  table_id uuid not null references tbl,
  prefix text not null,
  adopted_seq bigint not null,
  primary key (table_id, prefix));
