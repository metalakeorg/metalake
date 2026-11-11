"""Engines connected to the lake by `lake.iceberg`: each reads a table of the lake, and a write through it is a commit of
the lake. Each engine is skipped where it is not installed: MetaLake installs none."""

import subprocess
import sys
import textwrap

import pytest

import metalake


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("engines")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql("create table t (k bigint); insert into t values (1), (2), (3)")
    return made


def test_profile(lake):
    profile = lake.iceberg.profile("duckdb", name="prod")
    assert (profile.engine, profile.name) == ("duckdb", "prod")
    assert profile.uri == lake.iceberg.uri
    assert profile.statements[-1].startswith("ATTACH '' AS prod")
    assert str(profile).endswith(f"ENDPOINT '{profile.uri}');")
    with pytest.raises(metalake.UsageError, match="trino"):
        lake.iceberg.profile("hive")


def test_pyiceberg_reads_and_writes(lake):
    pytest.importorskip("pyiceberg")
    pyarrow = pytest.importorskip("pyarrow")
    catalog = lake.iceberg.pyiceberg()
    table = catalog.load_table("default.t")
    assert sorted(table.scan().to_arrow()["k"].to_pylist()) == [1, 2, 3]

    before = lake.log(limit=1)[0].seq
    table.append(pyarrow.table({"k": pyarrow.array([4], pyarrow.int64())}))
    assert lake.log(limit=1)[0].seq > before, "a commit of the lake"
    assert lake.sql("select count(*) as n from t").to_arrow()["n"].to_pylist() == [4]


def test_polars_reads_pyiceberg_catalog(lake):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    polars = pytest.importorskip("polars")
    table = lake.iceberg.pyiceberg().load_table("default.t")
    assert polars.scan_iceberg(table).select(polars.len()).collect().item() >= 3


def test_duckdb_reads_and_writes(lake):
    duckdb = pytest.importorskip("duckdb")
    connection = lake.iceberg.duckdb()
    assert connection.sql("select count(*) from lake.default.t").fetchall()[0][0] >= 3

    before = lake.log(limit=1)[0].seq
    connection.execute("insert into lake.default.t values (10)")
    assert lake.log(limit=1)[0].seq > before, "a commit of the lake"

    # A connection of one's own, the lake attached under another name.
    own = duckdb.connect()
    assert lake.iceberg.duckdb(own, name="other") is own
    assert own.sql("select max(k) from other.default.t").fetchall()[0][0] == 10


@pytest.fixture(scope="module")
def spark_session(lake):
    pytest.importorskip("pyspark")
    session = lake.iceberg.spark()
    yield session
    session.stop()


def test_spark_reads_and_merges(lake, spark_session):
    assert spark_session.sql("select count(*) from lake.default.t").first()[0] >= 3
    before = lake.log(limit=1)[0].seq
    spark_session.sql(
        "merge into lake.default.t d using (select 1 as k union all select 20 as k) s on d.k = s.k "
        "when not matched then insert *"
    )
    assert lake.log(limit=1)[0].seq > before, "a commit of the lake"
    # Counted by MetaLake, through no library of Arrow's: the engine's environment may have none.
    assert sum(batch.num_rows for batch in lake.sql("select k from t where k = 20")) == 1


def test_spark_adopts_session(lake, spark_session):
    from pyspark.sql import SparkSession

    adopted = lake.iceberg.spark(name="other")
    assert adopted is SparkSession.getActiveSession()
    assert adopted.sql("select max(k) from other.default.t").first()[0] >= 20


def test_spark_without_iceberg_refused(directory, lake):
    pytest.importorskip("pyspark")
    # Another process: a JVM of its own, started without Iceberg.
    script = textwrap.dedent(
        f"""
        import metalake
        from pyspark.sql import SparkSession
        session = SparkSession.builder.appName("plain").getOrCreate()
        lake = metalake.connect({str(directory / "lake")!r})
        try:
            lake.iceberg.spark(session)
        except metalake.UsageError as refused:
            assert "spark.jars.packages" in str(refused), refused
            print("refused")
        finally:
            session.stop()
        """
    )
    said = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=300)
    assert "refused" in said.stdout, said.stderr[-2000:]


def flink_rows(environment, query):
    with environment.execute_sql(query).collect() as rows:
        return [tuple(row) for row in rows]


@pytest.fixture(scope="module")
def flink_environment(lake):
    pytest.importorskip("pyflink")
    # The first: PyFlink's JVM starts here, with Hadoop's client on its classpath.
    return lake.iceberg.flink()


def test_flink_reads_and_inserts(lake, flink_environment):
    before = lake.log(limit=1)[0].seq
    flink_environment.execute_sql("insert into lake.default.t values (30)").wait()
    assert lake.log(limit=1)[0].seq > before, "a commit of the lake"
    assert flink_rows(flink_environment, "select max(k) from lake.default.t")[0][0] >= 30


def test_flink_adopts_environment(lake, flink_environment):
    from pyflink.table import EnvironmentSettings, TableEnvironment

    environment = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    assert lake.iceberg.flink(environment, name="other") is environment
    assert flink_rows(environment, "select count(*) from other.default.t")[0][0] >= 3
    # Again, under a third name: the jars it was given already are taken again without harm.
    assert lake.iceberg.flink(environment, name="third") is environment
    assert flink_rows(environment, "select count(*) from third.default.t")[0][0] >= 3


def test_flink_without_hadoop_refused(directory, lake):
    pytest.importorskip("pyflink")
    # Another process: a JVM of its own, started before the lake was asked for, without Hadoop.
    script = textwrap.dedent(
        f"""
        import os
        os.environ.pop("HADOOP_CLASSPATH", None)
        import metalake
        from pyflink.table import EnvironmentSettings, TableEnvironment
        environment = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
        lake = metalake.connect({str(directory / "lake")!r})
        try:
            lake.iceberg.flink(environment)
        except metalake.UsageError as refused:
            assert "HADOOP_CLASSPATH=" in str(refused), refused
            print("refused")
        """
    )
    said = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=300)
    assert "refused" in said.stdout, said.stderr[-2000:]
