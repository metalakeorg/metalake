# MetaLake specification

- [`metalake-v0.md`](metalake-v0.md): the normative text of version 0 (draft, written step by step).
- [`v0/schema/`](v0/schema/): the reference DDL, one file per section of the text, loaded in file-name order.
- [`v0/reference/`](v0/reference/): the reference behaviour, in SQL, one file per section. It defines the right
  answer, not the fast one, and its functions are not an API: the protocols are the text.
- [`v0/conformance/`](v0/conformance/): the conformance cases. Each names the rules it checks.

Run the suite with `spec/v0/run.sh` (needs Docker). The Iceberg projection (§16) is checked separately with
PyIceberg by `spec/v0/conformance/iceberg/run.sh`, which builds a pinned harness image the first time. It prints one line per case and fails on any failing case,
any rule of the text without a passing case, and any rule a case names that the text lacks.

The design behind version 0 is [ADR 0002](../docs/adr/0002-specification-v0.md).
