//! C15 (ADR 0003 §5): HDFS is a store as a bucket is. Through `Storage`, an object written under `hdfs://` is read whole
//! and by its tail, is there, is listed under its prefix with the URL the catalog spells, and is gone once deleted, and
//! deleting it again is no error. Run against the HDFS that `METALAKE_TEST_HDFS` names (`hdfs://localhost:9000`), with
//! the feature `hdfs`; skipped without it.

#![cfg(feature = "hdfs")]

use bytes::Bytes;
use metalake::Storage;

fn namenode() -> Option<String> {
    let found = std::env::var("METALAKE_TEST_HDFS").ok();
    if found.is_none() {
        eprintln!("skipped: no HDFS named by METALAKE_TEST_HDFS");
    }
    found
}

#[tokio::test]
async fn c15_hdfs_store() {
    let Some(namenode) = namenode() else { return };
    let namenode = namenode.trim_end_matches('/');
    let location = format!("{namenode}/c15/{}", uuid::Uuid::new_v4());
    let storage = Storage::new(&location, Vec::<(String, String)>::new()).unwrap();
    assert_eq!(storage.base(), namenode);

    let a = format!("{location}/t/data/a.parquet");
    let b = format!("{location}/t/data/b.parquet");
    storage.put(&a, Bytes::from_static(b"0123456789")).await.unwrap();
    storage.put(&b, Bytes::from_static(b"b")).await.unwrap();

    assert_eq!(storage.get(&a).await.unwrap(), Bytes::from_static(b"0123456789"));
    assert_eq!(storage.tail(&a, 10, 4).await.unwrap(), Bytes::from_static(b"6789"));
    assert!(storage.exists(&a).await.unwrap());

    let mut listed: Vec<String> = storage
        .list(&location)
        .await
        .unwrap()
        .into_iter()
        .map(|(url, _)| url)
        .collect();
    listed.sort();
    assert_eq!(
        listed,
        vec![a.clone(), b.clone()],
        "listed as the catalog spells them, with no temporary file left"
    );

    storage.delete(&a).await.unwrap();
    assert!(!storage.exists(&a).await.unwrap());
    storage.delete(&a).await.unwrap();

    let other = format!("hdfs://elsewhere:8020{}", &a[namenode.len()..]);
    assert!(
        storage.get(&other).await.is_err(),
        "a path on another NameNode is not this store's"
    );

    storage.delete(&b).await.unwrap();
}
