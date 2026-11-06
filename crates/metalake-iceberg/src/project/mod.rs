//! Rows into what an Iceberg client reads (§16.1). Plain functions: they take what Postgres answered and give back values
//! or bytes, so that each is tested without a server, against the reference's functions.

pub mod avro;
pub mod bounds;
pub mod manifest;
pub mod metadata;
