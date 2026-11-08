//! It builds only if the core's Arrow types are this crate's, those of Arrow 58.

use arrow_schema::SchemaRef;

pub fn schema(stream: &metalake::TableStream) -> SchemaRef {
    stream.schema().clone()
}
