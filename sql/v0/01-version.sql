-- MetaLake v0 · protocol · its version, beside the format's (ADR 0003 §2.1).
--
-- Every file of this directory is rendered for one catalog: `@schema@` becomes the catalog's schema, so no object
-- depends on the caller's search_path.

insert into @schema@.meta values ('protocol_version', '0');
