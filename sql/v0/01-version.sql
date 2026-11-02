-- MetaLake v0 · protocol · its version, beside the format's (ADR 0003 §2.1).
--
-- Every file of this directory is rendered for one catalog: `@schema@` becomes the catalog's schema, so no object
-- depends on the caller's search_path. Installing it again over a catalog, as migrating does, changes nothing else.

insert into @schema@.meta values ('protocol_version', '0')
  on conflict (key) do update set value = excluded.value;

-- LAY-3, LAY-5: the versions a client reads on connecting, to refuse a catalog it does not know.
create function @schema@.versions(out format_version text, out writer_version text, out protocol_version text)
language sql stable as $$
  select (select value from @schema@.meta where key = 'format_version'),
         coalesce((select value from @schema@.meta where key = 'writer_version'), '0'),
         (select value from @schema@.meta where key = 'protocol_version');
$$;
