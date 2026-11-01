-- MetaLake v0 · reference behaviour · identity (§3).
--
-- It defines the right answer, not the fast one.

-- ID-3: a new table identity and its counters.
create function new_table(location text, created_seq bigint) returns uuid language sql as $$
  with t as (insert into tbl values (gen_random_uuid(), location, created_seq) returning table_id)
  insert into table_counter (table_id) select table_id from t returning table_id;
$$;

-- ID-5: the next field, spec, partition field or sort order id of a table.
create function next_id(t uuid, kind text) returns int language plpgsql as $$
declare id int;
begin
  if kind not in ('field', 'spec', 'partition_field', 'sort_order') then
    raise exception 'metalake: unknown id kind %', kind;
  end if;
  update table_counter set
      last_field_id = last_field_id + (kind = 'field')::int,
      last_spec_id = last_spec_id + (kind = 'spec')::int,
      last_partition_field_id = last_partition_field_id + (kind = 'partition_field')::int,
      last_sort_order_id = last_sort_order_id + (kind = 'sort_order')::int
    where table_id = t
    returning case kind
      when 'field' then last_field_id when 'spec' then last_spec_id
      when 'partition_field' then last_partition_field_id else last_sort_order_id end
    into id;
  if id is null then
    raise exception 'metalake: unknown table %', t;
  end if;
  return id;
end $$;
