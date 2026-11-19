//! `register`: the Parquet files under a prefix of the lake's store as a table's, in place, as every face asks for it.
//! The table is named as SQL names one (I7), on a branch, `main` unless told.

use metalake::register::{self, BATCH, Register, RegisterSchema, Report, SchemaReport};

use crate::{Error, Lake, Result};

/// What to register, as a face takes it.
#[derive(Debug, Clone, Default)]
pub struct RegisterRequest {
    /// Where the files are: a path in the lake's store, `/user/hive/warehouse/sales`, or a URL of it. None, a table
    /// registered before registers again from where it was.
    pub source: Option<String>,
    /// `table`, `schema.table` or `catalog.schema.table`.
    pub table: String,
    /// The directories' partition columns, in order, each with its type: `("dt", "date")`. None, a new table's are
    /// found in its directories, and an existing table's are its own.
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
        Ok((column.to_string(), ty.to_lowercase()))
    }
}

/// What to register as a schema, as a face takes it: each prefix right under `source` a table of `schema`.
#[derive(Debug, Clone, Default)]
pub struct RegisterSchemaRequest {
    /// A prefix of the lake's store, a path in it or a URL of it: a Hive database's directory, say.
    pub source: String,
    /// `schema` or `catalog.schema`.
    pub schema: String,
    pub adopt: bool,
    pub check: bool,
    /// The branch; `main` when none.
    pub on: Option<String>,
}

impl Lake {
    /// Registers each prefix right under `request.source` as a table of `request.schema`. A table refused or failing
    /// leaves the others registered; the report says each one's outcome.
    pub async fn register_schema(&self, request: &RegisterSchemaRequest) -> Result<SchemaReport> {
        let on = self
            .catalog
            .branch(request.on.as_deref().unwrap_or(crate::MAIN))
            .await?;
        // A schema is named as a table's namespace is: the name of a table in it, less the table.
        let path = crate::TablePath::parse_on(&format!("{}.t", request.schema), on.name())?;
        let namespace: Vec<&str> = path.namespace.iter().map(String::as_str).collect();
        let register = RegisterSchema {
            source: &request.source,
            namespace: &namespace,
            adopt: request.adopt,
            check: request.check,
            batch: BATCH,
        };
        Ok(register::register_schema(&on, &self.storage()?, &register).await?)
    }

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
            source: request.source.as_deref(),
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
