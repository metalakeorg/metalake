# MetaLake specification

- [`metalake-v0.md`](metalake-v0.md): the normative text of version 0 (draft, written step by step).
- [`v0/schema.sql`](v0/schema.sql): the reference DDL.
- [`v0/reference.sql`](v0/reference.sql): the reference behaviour, in SQL.
- [`v0/conformance/`](v0/conformance/): the conformance cases. Each names the rules it checks.

Run the suite with `spec/v0/run.sh` (needs Docker). It prints one line per case and fails on any failing case,
any rule of the text without a passing case, and any rule a case names that the text lacks.

The design behind version 0 is [ADR 0002](../docs/adr/0002-specification-v0.md).
