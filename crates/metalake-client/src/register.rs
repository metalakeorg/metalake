//! `register`: the Parquet files under a prefix of the lake's store as a table's, in place, as every face asks for it.
//! The table is named as SQL names one (I7), on a branch, `main` unless told.

use metalake::register::{self, BATCH, Register, Report};

use crate::{Error, Lake, Result};

/// What to register, as a face takes it.
#[derive(Debug, Clone, Default)]
pub struct RegisterRequest {
    pub source: String,
    /// `table`, `schema.table` or `catalog.schema.table`.
    pub table: String,
    /// The directories' partition columns, in order, each with its type: `("dt", "date")`.
    pub partition: Vec<(String, String)>,
    pub adopt: bool,
    pub location: Option<String>,
    pub check: bool,
    /// The branch; `main` when none.
    pub on: Option<String>,
}

impl RegisterRequest {
    /// A partition column as a face spells it, `column:type`.
    pub fn partition_column(text: &str) -> Result<(String, String)> {
        let (column, ty) = text
            .split_once(':')
            .filter(|(c, t)| !c.is_empty() && !t.is_empty())
            .ok_or_else(|| Error::usage(format!("a partition column is column:type, as dt:date, not {text}")))?;
        Ok((column.to_string(), ty.to_string()))
    }
}

impl Lake {
    /// Registers the files under `request.source` as a table's. A run with refused files commits nothing and answers
    /// them in its report, as a check does.
    pub async fn register(&self, request: &RegisterRequest) -> Result<Report> {
        let on = self
            .catalog
            .branch(request.on.as_deref().unwrap_or(crate::MAIN))
            .await?;
        let path = crate::TablePath::parse_on(&request.table, on.name())?;
        let namespace: Vec<&str> = path.namespace.iter().map(String::as_str).collect();
        let partition: Vec<(&str, &str)> = request
            .partition
            .iter()
            .map(|(column, ty)| (column.as_str(), ty.as_str()))
            .collect();
        let register = Register {
            source: Some(&request.source),
            namespace: &namespace,
            name: &path.name,
            partition,
            adopt: request.adopt,
            location: request.location.clone(),
            check: request.check,
            batch: BATCH,
        };
        Ok(register::register(&on, &self.storage()?, &register).await?)
    }
}
