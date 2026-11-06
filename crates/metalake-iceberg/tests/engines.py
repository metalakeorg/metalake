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


if __name__ == "__main__":
    print("\n" + json.dumps({"duckdb": duckdb, "spark": spark, "spark_insert": spark_insert}[sys.argv[1]](sys.argv[2])))
