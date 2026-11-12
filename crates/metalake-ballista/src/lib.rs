//! MetaLake on Ballista: the DataFusion adapter, distributed. Not a surface of its own: the same SQL, catalogs and
//! tables as `metalake-datafusion`, whose codecs this crate registers in each process of a Ballista cluster.
//!
//! - The client plans a statement with MetaLake's catalogs and sends its logical plan, MetaLake's tables as values.
//! - The scheduler decodes it with a catalog of its own, plans it (the protocol plans the files here), and sends each
//!   stage's physical plan, MetaLake's scans with their tasks.
//! - An executor decodes its stage with storage and no catalog, and reads.
//!
//! See ADR 0006.
