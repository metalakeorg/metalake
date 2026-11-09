-- MetaLake v0 · reference behaviour · catalog layout (§2).
--
-- It defines the right answer, not the fast one.

-- LAY-3: refuse a catalog of an unknown version.
create function open_catalog() returns uuid language plpgsql stable as $$
declare v text := (select value from meta where key = 'format_version');
begin
  if v is distinct from '0' then
    raise exception 'metalake unsupported: format_version %', coalesce(v, 'missing') using errcode = '0A000';
  end if;
  return (select value::uuid from meta where key = 'catalog_uuid');
end $$;

-- LAY-5: a writer of revision 0.3 refuses a catalog whose writer_version it does not know, and one of an earlier
-- revision until it is migrated (migrate(), §18).
create function writable() returns void language plpgsql stable as $$
declare v text := coalesce((select value from meta where key = 'writer_version'), '0');
begin
  if v is distinct from '3' then
    raise exception 'metalake unsupported: writer_version %', v using errcode = '0A000';
  end if;
end $$;
