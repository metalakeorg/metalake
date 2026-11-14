//! What the connector's tests share: a catalog of their own on the Postgres at `DATABASE_URL`, with one table written
//! and read through the core.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arroyo_rpc::var_str::VarStr;
use arroyo_types::TaskInfo;
use futures::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{Catalog, CatalogConfig, NewTable, Ref, Scan, Storage, TableWriter, WriteOptions};
use metalake_arroyo::config::{MetaLakeProfile, MetaLakeTable, MetaLakeTableType};

pub fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: a Postgres with track_commit_timestamp = on")
}

pub fn unique() -> String {
    format!("t{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos())
}

/// A catalog in a schema of its own, with `shop.orders (k bigint)`, its files on local disk.
pub struct Lake {
    pub catalog: Catalog,
    pub main: Ref,
    pub storage: Storage,
    pub schema: String,
}

impl Lake {
    pub async fn new() -> Self {
        let schema = unique();
        let dir = std::env::temp_dir().join("metalake-arroyo").join(&schema);
        std::fs::create_dir_all(&dir).unwrap();
        let warehouse = format!("file://{}", dir.display());
        let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse(&warehouse), None)
            .await
            .unwrap();
        let main = catalog.branch("main").await.unwrap();
        let (tx, _) = main
            .transaction()
            .create_namespace(&["shop"])
            .create_table(NewTable {
                namespace: &["shop"],
                name: "orders",
                schema: &keys(0..1).schema(),
                location: None,
                cluster_key: vec![],
                partitioning: vec![],
                properties: vec![],
            })
            .unwrap();
        tx.commit().await.unwrap();
        let storage = Storage::new(&warehouse, Vec::<(String, String)>::new()).unwrap();
        Lake {
            catalog,
            main,
            storage,
            schema,
        }
    }

    /// Commits the rows `keys`, in a file of their own.
    pub async fn append(&self, keys: Range<i64>) -> i64 {
        let t = self.main.table(&["shop"], "orders").await.unwrap();
        let options = WriteOptions {
            stored: true,
            ..Default::default()
        };
        let mut writer = TableWriter::new(&self.catalog, &t, self.storage.clone(), options).unwrap();
        writer.write(&self::keys(keys)).await.unwrap();
        let files = writer.finish().await.unwrap();
        self.main.transaction().append(files).commit().await.unwrap().seq()
    }

    /// Deletes the rows whose key `chosen` keeps, as an engine does.
    pub async fn delete(&self, chosen: impl Fn(i64) -> bool) -> i64 {
        let t = self.main.table(&["shop"], "orders").await.unwrap();
        let scan = Scan::new(&self.catalog, &t).with_positions();
        let batches: Vec<RecordBatch> = scan
            .read(scan.plan().await.unwrap(), &self.storage)
            .try_collect()
            .await
            .unwrap();
        let mut deletes = Deletes::new();
        for b in &batches {
            let keys = b.column(0).as_primitive::<Int64Type>();
            let keep = BooleanArray::from_iter(keys.iter().map(|k| Some(chosen(k.unwrap()))));
            deletes
                .add(&arrow_select::filter::filter_record_batch(b, &keep).unwrap())
                .unwrap();
        }
        self.main
            .transaction()
            .delete(&t, &self.storage, deletes)
            .commit()
            .await
            .unwrap()
            .seq()
    }

    pub fn profile(&self) -> MetaLakeProfile {
        MetaLakeProfile {
            postgres: VarStr::new(url()),
            schema: self.schema.clone(),
            auth: None,
            storage: BTreeMap::new(),
        }
    }

    /// `shop.orders` on main, as a source or a sink.
    pub fn table(&self, table_type: MetaLakeTableType) -> MetaLakeTable {
        MetaLakeTable {
            branch: "main".to_string(),
            namespace: vec!["shop".to_string()],
            table: "orders".to_string(),
            table_type,
        }
    }

    /// The keys the table holds now, read through the core, sorted.
    pub async fn keys(&self) -> Vec<i64> {
        let t = self.main.table(&["shop"], "orders").await.unwrap();
        let scan = Scan::new(&self.catalog, &t);
        let batches: Vec<RecordBatch> = scan
            .read(scan.plan().await.unwrap(), &self.storage)
            .try_collect()
            .await
            .unwrap();
        let mut keys: Vec<i64> = batches
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect();
        keys.sort_unstable();
        keys
    }
}

pub fn keys(keys: Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, true)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

pub fn task_info(job: &str, index: u32, parallelism: u32) -> TaskInfo {
    let mut info = arroyo_types::get_test_task_info();
    info.job_id = job.to_string();
    info.task_index = index;
    info.parallelism = parallelism;
    info
}
