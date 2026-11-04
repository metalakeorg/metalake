//! A `ScanTask` as a file DataFusion's Parquet source opens: its own reader, so that the catalog's footer saves the
//! request for it and an inline file needs no object, and its vector as a selection of rows, which DataFusion applies
//! together with its own pruning.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{ParquetFileReaderFactory, ParquetRowSelection};
use datafusion::error::{DataFusionError, Result};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use metalake::{ScanTask, Storage};

use crate::catalog::external;
use crate::stats;

/// Opens the task each file carries.
#[derive(Debug)]
pub(crate) struct TaskReaders {
    pub storage: Storage,
}

impl ParquetFileReaderFactory for TaskReaders {
    fn create_reader(
        &self,
        _partition: usize,
        file: PartitionedFile,
        _size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let task = file
            .extension::<Arc<ScanTask>>()
            .ok_or_else(|| DataFusionError::Internal(format!("{}: a file without its task", file.path())))?;
        Ok(Box::new(task.reader(&self.storage).map_err(external)?))
    }
}

/// The file of `task` for DataFusion, and its statistics under the table's `schema`. A vector in Puffin is one request.
/// With `positions`, the file's id is a constant column of its rows, and neither position column has statistics.
pub(crate) async fn file(
    task: ScanTask,
    schema: &SchemaRef,
    storage: &Storage,
    positions: bool,
) -> Result<(PartitionedFile, Statistics)> {
    let size = task.file_size as u64;
    let mut file = match &task.path {
        Some(url) => PartitionedFile::new(storage.path(url).map_err(external)?.to_string(), size),
        // An inline file has no object: this name is only DataFusion's, which the reader above never asks for.
        None => PartitionedFile::new(format!("inline/{}", task.file_id), size),
    };
    let metadata = match (task.metadata().map_err(external)?, &task.vector) {
        (Some(metadata), _) => Some(Arc::new(metadata)),
        (None, Some(_)) => {
            let mut reader = task.reader(storage).map_err(external)?;
            Some(reader.get_metadata(None).await?)
        }
        (None, None) => None,
    };
    if let Some(vector) = task.vector(storage).await.map_err(external)? {
        let metadata = metadata.as_ref().expect("read above for a task with a vector");
        let mut first = 0;
        let groups: Vec<(u64, u64)> = metadata
            .row_groups()
            .iter()
            .map(|group| {
                let rows = group.num_rows() as u64;
                first += rows;
                (first - rows, rows)
            })
            .collect();
        file = file.with_extension(ParquetRowSelection::new(vector.row_selection(&groups)));
    }
    let mut statistics = stats::of_file(&task, metadata.as_deref(), schema)?;
    if positions {
        file = file.with_partition_values(vec![ScalarValue::Int64(Some(task.file_id))]);
        statistics
            .column_statistics
            .extend([ColumnStatistics::new_unknown(), ColumnStatistics::new_unknown()]);
    }
    let file = file
        .with_statistics(Arc::new(statistics.clone()))
        .with_extension(Arc::new(task));
    Ok((file, statistics))
}
