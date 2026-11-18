//! C15b (ADR 0003 §3.7): a ViewFS mount table of explicit links alone, which Hadoop allows, is refused as the store is
//! made, since MetaLake's HDFS client builds one only with a `linkFallback`. The refusal says what to do, add one or name
//! the warehouse by `hdfs://`, and no fallback is made up. No cluster is reached: the mount table is read from
//! `HADOOP_CONF_DIR` as the store is made. A file of its own, since it sets that variable for its process.

#![cfg(feature = "hdfs")]

use metalake::Storage;

#[test]
fn c15b_viewfs_without_fallback() {
    let conf = std::env::temp_dir().join(format!("c15b-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&conf).unwrap();
    std::fs::write(
        conf.join("core-site.xml"),
        "<configuration>\n\
           <property><name>fs.viewfs.mounttable.cluster.link./lake</name>\
           <value>hdfs://localhost:9000/warehouse</value></property>\n\
         </configuration>\n",
    )
    .unwrap();
    // SAFETY: the only test of this process, which sets the variable before anything reads it.
    unsafe { std::env::set_var("HADOOP_CONF_DIR", &conf) };

    let refused = Storage::new("viewfs://cluster/lake", Vec::<(String, String)>::new()).unwrap_err();
    let message = refused.to_string();
    assert!(
        message.contains("fs.viewfs.mounttable.cluster.linkFallback") && message.contains("hdfs://"),
        "{message}"
    );
    std::fs::remove_dir_all(&conf).unwrap();
}
