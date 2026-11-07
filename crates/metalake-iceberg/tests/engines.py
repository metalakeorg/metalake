"""Engines other than PyIceberg against the server: what `c12_engines` holds them to, printed as JSON. The rows of `main`, of
branch `b`, and of the filter that prunes, from table `db.wide`."""

import glob
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request


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


def spark_recovery(uri):
    """A batch run again after its commit, as when the driver dies before it records that commit. Spark then finds the
    batch in the summary of the snapshot that holds it."""
    from pyspark.sql.functions import count

    session = spark_session(uri)
    root = tempfile.mkdtemp()
    keys = lambda query: [row[0] for row in session.sql(query).collect()]

    def add(source, name, rows):
        os.makedirs(source, exist_ok=True)
        with open(f"{root}/{name}", "w") as file:
            file.writelines(json.dumps({"k": k}) + "\n" for k in rows)
        os.rename(f"{root}/{name}", f"{source}/{name}.json")

    def forget(checkpoint, batch):
        for name in (str(batch), f".{batch}.crc"):
            os.remove(f"{checkpoint}/commits/{name}")

    def run(frame, mode, checkpoint, table):
        writer = frame.writeStream.format("iceberg").outputMode(mode).trigger(availableNow=True)
        writer.option("checkpointLocation", checkpoint).toTable(table).awaitTermination()

    said = {}
    session.sql("create namespace lake.flow")
    session.sql("create table lake.flow.s (k bigint)")
    session.sql("create table lake.flow.total (n bigint)")

    source, checkpoint = f"{root}/source", f"{root}/appended"
    appended = lambda: run(session.readStream.schema("k bigint").json(source), "append", checkpoint, "lake.flow.s")
    add(source, "a", [1, 2])
    appended()
    forget(checkpoint, 0)
    appended()
    said["first_again"] = keys("select k from lake.flow.s order by k")
    add(source, "b", [3])
    appended()
    forget(checkpoint, 1)
    appended()
    said["second_again"] = keys("select k from lake.flow.s order by k")
    add(source, "c", [])
    appended()
    said["epochs"] = sorted(
        epoch for epoch in keys("select summary['spark.sql.streaming.epochId'] from lake.flow.s.snapshots") if epoch
    )

    source, checkpoint = f"{root}/counted", f"{root}/complete"
    counted = lambda: run(
        session.readStream.schema("k bigint").json(source).agg(count("*").alias("n")),
        "complete",
        checkpoint,
        "lake.flow.total",
    )
    add(source, "a", [1, 2])
    counted()
    forget(checkpoint, 0)
    counted()
    said["complete_again"] = keys("select n from lake.flow.total")
    add(source, "b", [3])
    counted()
    said["complete_next"] = keys("select n from lake.flow.total")
    return said


def spark_stream(uri):
    """A stream read from the first snapshot: it takes what a merge added, and every snapshot of a batch."""
    session = spark_session(uri)
    root = tempfile.mkdtemp()

    def stream(table):
        writer = session.readStream.table(f"lake.db.{table}").writeStream.format("memory").queryName(table)
        query = writer.outputMode("append").trigger(availableNow=True)
        query.option("checkpointLocation", f"{root}/{table}").start().awaitTermination()
        return sorted(row[0] for row in session.sql(f"select k from {table}").collect())

    said = {"merged": stream("merged"), "evolved": stream("evolved")}
    try:
        said["removed"] = stream("removed")
    except Exception as refused:
        said["removed"] = str(refused)
    return said


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


def flink_recovery(uri):
    """A job cancelled just after the commit of a checkpoint and restored from that checkpoint, whose state still holds
    what it committed. Flink then finds the checkpoint in the summary of the snapshot that holds it."""
    from pyflink.common import Configuration
    from pyflink.table import EnvironmentSettings, TableEnvironment

    root = tempfile.mkdtemp()
    last_row = 3000

    def streaming(restored=None):
        options = {
            "parallelism.default": "1",
            "restart-strategy.type": "none",
            "execution.checkpointing.interval": "5 s",
            "execution.checkpointing.externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION",
            "state.checkpoints.dir": f"file://{root}/checkpoints",
        }
        if restored:
            options["execution.savepoint.path"] = f"file://{restored}"
        configuration = Configuration()
        for key, value in options.items():
            configuration.set_string(key, value)
        settings = EnvironmentSettings.new_instance().in_streaming_mode().with_configuration(configuration).build()
        environment = TableEnvironment.create(settings)
        environment.execute_sql(
            f"create catalog lake with ('type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = '{uri}')"
        ).wait()
        environment.execute_sql(
            "create temporary table numbers (k bigint) with ('connector' = 'datagen', 'rows-per-second' = '200',"
            f" 'fields.k.kind' = 'sequence', 'fields.k.start' = '1', 'fields.k.end' = '{last_row}')"
        ).wait()
        return environment

    def current_summary():
        with urllib.request.urlopen(f"{uri}/v1/namespaces/flow/tables/s") as answer:
            metadata = json.load(answer)["metadata"]
        current = metadata.get("current-snapshot-id")
        snapshots = [s for s in metadata["snapshots"] if s["snapshot-id"] == current]
        return snapshots[0]["summary"] if snapshots else None

    batch = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    batch.execute_sql(
        f"create catalog lake with ('type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = '{uri}')"
    ).wait()
    batch.execute_sql("create database lake.flow").wait()
    batch.execute_sql("create table lake.flow.s (k bigint)").wait()

    insert = "insert into lake.flow.s select k from numbers"
    job = streaming().execute_sql(insert)
    deadline = time.time() + 120
    while current_summary() is None and time.time() < deadline:
        time.sleep(0.1)
    job.get_job_client().cancel().result()

    checkpoints = glob.glob(f"{root}/checkpoints/*/chk-*")
    newest = max(checkpoints, key=lambda path: int(path.rsplit("-", 1)[1]))
    said = {
        "restored": newest.rsplit("-", 1)[1],
        "committed": current_summary().get("flink.max-committed-checkpoint-id"),
    }
    streaming(newest).execute_sql(insert).wait()
    with batch.execute_sql("select k from lake.flow.s").collect() as rows:
        keys = [row[0] for row in rows]
    said.update({"rows": len(keys), "distinct": len(set(keys)), "last": max(keys), "expected": last_row})
    return said


def flink_incremental(uri):
    """What Flink reads between the first write and the last, which it takes from `append` snapshots alone."""
    from pyflink.table import EnvironmentSettings, TableEnvironment

    environment = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    environment.execute_sql(
        f"create catalog lake with ('type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = '{uri}')"
    ).wait()

    def since_first(table):
        with urllib.request.urlopen(f"{uri}/v1/namespaces/db/tables/{table}") as answer:
            metadata = json.load(answer)["metadata"]
        ordered = sorted(s["snapshot-id"] for s in metadata["snapshots"])
        options = f"'start-snapshot-id' = '{ordered[1]}', 'end-snapshot-id' = '{ordered[-1]}'"
        with environment.execute_sql(f"select k from lake.db.{table} /*+ OPTIONS({options}) */").collect() as rows:
            return sorted(row[0] for row in rows)

    return {"merged": since_first("merged"), "evolved": since_first("evolved")}


if __name__ == "__main__":
    said = "\n" + json.dumps({"duckdb": duckdb, "spark": spark, "spark_insert": spark_insert, "spark_lifecycle": spark_lifecycle, "spark_recovery": spark_recovery, "trino": trino_lifecycle, "flink": flink_lifecycle, "flink_recovery": flink_recovery, "spark_stream": spark_stream, "flink_incremental": flink_incremental}[sys.argv[1]](sys.argv[2]))
    print(said, flush=True)
    # A JVM an engine left behind may fail the interpreter's exit, after the answer is out.
    sys.stderr.flush()
    os._exit(0)
