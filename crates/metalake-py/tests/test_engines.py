"""Engines connected to the lake by `lake.iceberg`: each reads a table of the lake, and a write through it is a commit of
the lake. Each engine is skipped where it is not installed: MetaLake installs none."""

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
    assert connection.sql("select count(*) from lake.default.t").fetchone()[0] >= 3

    before = lake.log(limit=1)[0].seq
    connection.execute("insert into lake.default.t values (10)")
    assert lake.log(limit=1)[0].seq > before, "a commit of the lake"

    # A connection of one's own, the lake attached under another name.
    own = duckdb.connect()
    assert lake.iceberg.duckdb(own, name="other") is own
    assert own.sql("select max(k) from other.default.t").fetchone()[0] == 10
