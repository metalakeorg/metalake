"""Engines other than PyIceberg against the server: what `c12_engines` holds them to, printed as JSON. The rows of `main`, of
branch `b`, and of the filter that prunes, from table `db.wide`."""

import json
import os
import subprocess
import sys
import tempfile
import time


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
    said["property"] = [row[1] for row in session.sql("show tblproperties lake.flow.t ('team')").collect()]
    session.sql("create table lake.flow.c using iceberg as select k from lake.flow.t where k < 10")
    said["created_as"] = keys("select k from lake.flow.c order by k")
    session.sql("alter table lake.flow.c rename to flow.d")
    said["renamed"] = sorted(row[1] for row in session.sql("show tables in lake.flow").collect())
    session.sql("drop table lake.flow.d")
    session.sql("drop table lake.flow.t purge")
    session.sql("drop namespace lake.flow")
    said["namespaces"] = [row[0] for row in session.sql("show namespaces in lake").collect()]
    return said


def trino_container(uri):
    """Trino in its image, on the host's network and paths, with one catalog: the server under test."""
    catalog = tempfile.mkdtemp()
    with open(os.path.join(catalog, "lake.properties"), "w") as properties:
        properties.write(
            "connector.name=iceberg\n"
            "iceberg.catalog.type=rest\n"
            f"iceberg.rest-catalog.uri={uri}\n"
            "fs.hadoop.enabled=true\n"
        )
    os.chmod(catalog, 0o755)
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", "metalake-trino", "--network", "host", "--user", "root",
         "-v", f"{catalog}:/etc/trino/catalog", "-v", "/tmp:/tmp", "trinodb/trino:484"],
        check=True, stdout=subprocess.DEVNULL,
    )


def trino_lifecycle(uri):
    import trino

    trino_container(uri)
    try:
        connection = trino.dbapi.connect(host="127.0.0.1", port=8080, user="metalake", catalog="lake")
        cursor = connection.cursor()
        for _ in range(120):
            try:
                cursor.execute("select 1 from system.runtime.nodes where state = 'active'")
                if cursor.fetchall():
                    cursor.execute("show schemas from lake")
                    cursor.fetchall()
                    break
            except Exception:
                pass
            time.sleep(1)

        def run(query):
            cursor.execute(query)
            return cursor.fetchall()

        keys = lambda query: [row[0] for row in run(query)]
        said = {
            "main": keys("select k from lake.db.wide order by k"),
            "branch": keys("select k from lake.db.wide for version as of 'b' order by k"),
            "pruned": keys("select k from lake.db.wide where k >= 10 order by k"),
        }
        run("insert into lake.db.wide values (30), (31)")
        said["inserted"] = keys("select k from lake.db.wide order by k")
        try:
            run("delete from lake.db.wide where k = 2")
            said["row_delete"] = "deleted"
        except Exception as refused:
            said["row_delete"] = str(refused)
        said["after_delete"] = keys("select k from lake.db.wide order by k")
        run("create schema lake.flow")
        run("create table lake.flow.t (k bigint, s varchar)")
        run("insert into lake.flow.t values (1, 'a'), (2, 'b')")
        said["table_rows"] = keys("select k from lake.flow.t order by k")
        run("create table lake.flow.c as select k from lake.flow.t")
        said["created_snapshots"] = keys("""select operation from lake.flow."c$snapshots" order by committed_at""")
        said["created_as"] = keys("select k from lake.flow.c order by k")
        run("alter table lake.flow.c rename to lake.flow.d")
        said["renamed"] = sorted(row[0] for row in run("show tables from lake.flow"))
        run("drop table lake.flow.d")
        run("drop table lake.flow.t")
        run("drop schema lake.flow")
        said["schemas"] = keys("show schemas from lake")
        return said
    finally:
        subprocess.run(["docker", "logs", "--tail", "40", "metalake-trino"], stdout=sys.stderr, stderr=sys.stderr)
        subprocess.run(["docker", "rm", "-f", "metalake-trino"], stdout=subprocess.DEVNULL)


def flink_lifecycle(uri):
    from pyflink.table import EnvironmentSettings, TableEnvironment

    environment = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    environment.execute_sql(
        f"create catalog lake with ('type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = '{uri}')"
    ).wait()

    def keys(query):
        with environment.execute_sql(query).collect() as rows:
            return sorted(row[0] for row in rows)

    said = {
        "main": keys("select k from lake.db.wide"),
        "branch": keys("select k from lake.db.wide /*+ OPTIONS('branch' = 'b') */"),
        "pruned": keys("select k from lake.db.wide where k >= 10"),
    }
    environment.execute_sql("insert into lake.db.wide values (30), (31)").wait()
    said["inserted"] = keys("select k from lake.db.wide")
    environment.execute_sql("create database lake.flow").wait()
    environment.execute_sql("create table lake.flow.t (k bigint, s string)").wait()
    environment.execute_sql("insert into lake.flow.t values (1, 'a'), (2, 'b')").wait()
    environment.execute_sql("insert overwrite lake.flow.t values (3, 'c')").wait()
    said["overwritten"] = keys("select k from lake.flow.t")
    environment.execute_sql("alter table lake.flow.t set ('team' = 'data')").wait()
    environment.execute_sql("drop table lake.flow.t").wait()
    environment.execute_sql("drop database lake.flow").wait()
    said["databases"] = sorted(row[0] for row in environment.execute_sql("show databases in lake").collect())
    return said


if __name__ == "__main__":
    said = "\n" + json.dumps({"duckdb": duckdb, "spark": spark, "spark_insert": spark_insert, "spark_lifecycle": spark_lifecycle, "trino": trino_lifecycle, "flink": flink_lifecycle}[sys.argv[1]](sys.argv[2]))
    print(said, flush=True)
    # A JVM an engine left behind may fail the interpreter's exit, after the answer is out.
    sys.stderr.flush()
    os._exit(0)
