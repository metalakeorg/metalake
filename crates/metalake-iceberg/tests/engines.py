"""Engines other than PyIceberg against the server: what `c12_engines` holds them to, printed as JSON. The rows of `main`, of
branch `b`, and of the filter that prunes, from table `db.wide`."""

import json
import sys


def duckdb(uri):
    import duckdb

    connection = duckdb.connect()
    connection.execute("INSTALL iceberg")
    connection.execute("LOAD iceberg")
    connection.execute(f"ATTACH '' AS lake (TYPE iceberg, ENDPOINT '{uri}', AUTHORIZATION_TYPE 'none')")
    keys = lambda query: [row[0] for row in connection.execute(query).fetchall()]
    return {
        "main": keys("select k from lake.db.wide order by k"),
        "pruned": keys("select k from lake.db.wide where k >= 10 order by k"),
    }


def spark_session(uri):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "rest")
        .config("spark.sql.catalog.lake.uri", uri)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def spark(uri):
    session = spark_session(uri)
    keys = lambda query: [row[0] for row in session.sql(query).collect()]
    return {
        "main": keys("select k from lake.db.wide order by k"),
        "branch": keys("select k from lake.db.wide version as of 'b' order by k"),
        "pruned": keys("select k from lake.db.wide where k >= 10 order by k"),
    }


def spark_insert(uri):
    session = spark_session(uri)
    session.sql("insert into lake.db.wide values (30), (31)")
    return {"rows": [row[0] for row in session.sql("select k from lake.db.wide order by k").collect()]}


def spark_lifecycle(uri):
    """Everything a client changes, from a catalog with no namespace of this name."""
    session = spark_session(uri)
    keys = lambda query: [row[0] for row in session.sql(query).collect()]
    session.sql("create namespace lake.flow")
    session.sql("create table lake.flow.t (k bigint, s string) using iceberg partitioned by (bucket(4, k))")
    session.sql("insert into lake.flow.t values (1, 'a'), (2, 'b'), (3, 'c')")
    said = {"inserted": keys("select k from lake.flow.t order by k")}
    session.sql("delete from lake.flow.t where k = 2")
    said["deleted"] = keys("select k from lake.flow.t order by k")
    session.sql("update lake.flow.t set k = 30 where k = 3")
    said["updated"] = keys("select k from lake.flow.t order by k")
    session.sql("""merge into lake.flow.t t using (select 1 as k, 'z' as s union all select 5, 'y') u on t.k = u.k
                   when matched then update set s = u.s when not matched then insert *""")
    said["merged"] = keys("select k from lake.flow.t order by k")
    said["merged_s"] = keys("select s from lake.flow.t where k = 1")
    session.sql("alter table lake.flow.t set tblproperties ('team' = 'data')")
    said["property"] = keys("select value from (show tblproperties lake.flow.t ('team'))")
    session.sql("create table lake.flow.c using iceberg as select k from lake.flow.t where k < 10")
    said["created_as"] = keys("select k from lake.flow.c order by k")
    session.sql("alter table lake.flow.c rename to flow.d")
    said["renamed"] = sorted(row[1] for row in session.sql("show tables in lake.flow").collect())
    session.sql("drop table lake.flow.d")
    session.sql("drop table lake.flow.t purge")
    session.sql("drop namespace lake.flow")
    said["namespaces"] = [row[0] for row in session.sql("show namespaces in lake").collect()]
    return said


if __name__ == "__main__":
    print("\n" + json.dumps({"duckdb": duckdb, "spark": spark, "spark_insert": spark_insert, "spark_lifecycle": spark_lifecycle}[sys.argv[1]](sys.argv[2])))
