# MetaLake specification

The specification describes the MetaLake table format independently of any implementation: the tables
and columns in the catalog database, what each column means, the invariants every implementation must
keep, and the rules for commits, branches, merges, retention and the Iceberg projection.

It is written when the preliminary study of work package P1 closes (see
[ADR 0001](../docs/adr/0001-metalake.md) §9.1). Until then, the design and its evidence live in the ADR.
