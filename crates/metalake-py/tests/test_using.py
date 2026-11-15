"""`USING duckdb` from Python's `lake.sql`: a statement, or the statements after a `USING` alone, run on DuckDB in this
process, as `lake.iceberg.duckdb()` connects it, the lake attached through its Iceberg server; `USING metalake` back on
the lake's own engine. What DuckDB refuses is said in its words; a missing DuckDB says what to install."""

import sys

import pytest

import metalake

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("using")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory / "lake"), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    lake = metalake.init(directory / "lake")
    lake.sql("create schema demo; create table demo.t (k int); insert into demo.t values (1), (2), (3)")
    return lake


def one(result):
    """The first row of a result, by column."""
    return result.to_arrow().to_pylist()[0]


def test_a_statement_runs_on_duckdb_over_the_lakes_tables(lake):
    assert one(lake.sql("using duckdb select version() as v"))["v"].startswith("v")
    assert one(lake.sql("using duckdb select count(*) as n, sum(k) as s from demo.t")) == {"n": 3, "s": 6}
    # Without USING, the lake's own engine, as before.
    assert "DataFusion" in one(lake.sql("select version() as v"))["v"]


def test_using_alone_chooses_the_engine_until_another(lake):
    assert one(lake.sql("using duckdb; select 1 as a; select version() as v"))["v"].startswith("v")
    assert "DataFusion" in one(lake.sql("using duckdb; select 1 as a; using metalake; select version() as v"))["v"]
    # The choice is the run's alone: the next starts on the lake's own engine.
    assert "DataFusion" in one(lake.sql("select version() as v"))["v"]


def test_what_duckdb_refuses_is_said_in_its_words(lake):
    with pytest.raises(metalake.MetaLakeError, match="duckdb: .*nope"):
        lake.sql("using duckdb select nope")


def test_an_engine_that_does_not_run_here_is_refused(lake):
    with pytest.raises(metalake.UsageError, match="no such engine; duckdb run here, and metalake.toml names none"):
        lake.sql("using spark select 1")


def test_an_engine_the_lakes_file_names_is_sent_the_statement(lake):
    """Named, an engine elsewhere is sent what `USING` gives it: here a Trino that is not there, which says so."""
    lake.add_engine("trino_test", url="http://127.0.0.1:1", user="ana")
    try:
        with pytest.raises(metalake.MetaLakeError, match="trino at http://127.0.0.1:1"):
            lake.sql("using trino_test select 1")
        with pytest.raises(metalake.UsageError, match="metalake.toml names trino_test"):
            lake.sql("using nope select 1")
    finally:
        lake.remove_engine("trino_test")


def test_a_missing_duckdb_says_what_to_install(lake, monkeypatch):
    monkeypatch.setitem(sys.modules, "duckdb", None)
    with pytest.raises(metalake.UsageError, match=r'pip install "metalake\[duckdb\]"'):
        lake.sql("using duckdb select 1")
