"""`USING duckdb` from Python's `lake.sql`: a statement, or the statements after a `USING` alone, run on DuckDB in a
process of its own, as `lake.iceberg.duckdb()` connects it, the lake attached through its Iceberg server; `USING
metalake` back on the lake's own engine. What DuckDB refuses is said in its words; a missing DuckDB says what to install."""

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


@pytest.fixture
def worker(tmp_path, monkeypatch):
    """A worker of one's own for `USING duckdb`: a module by its name and source, which the worker's Python finds. A
    worker of another module than the running one's is started in its place."""

    def worker(name, source):
        (tmp_path / f"{name}.py").write_text(source)
        monkeypatch.setenv("PYTHONPATH", str(tmp_path))
        monkeypatch.setenv("METALAKE_LOCAL_WORKER", name)

    return worker


def test_a_missing_duckdb_says_what_to_install(lake, worker):
    worker("no_duckdb", "import sys\nsys.modules['duckdb'] = None\nfrom metalake._worker import main\nmain()\n")
    with pytest.raises(metalake.UsageError, match=r'pip install "metalake\[duckdb\]"'):
        lake.sql("using duckdb select 1")


# A worker that ends as one does on an instruction its processor lacks: by that code on Windows, that signal elsewhere.
ILLEGAL_INSTRUCTION = """import os, signal, sys
sys.stdin.readline()
if os.name == "nt":
    os._exit(0xC000001D - 2**32)
os.kill(os.getpid(), signal.SIGILL)
"""


def test_an_engine_that_fails_as_native_code_fails_ends_its_process_not_this_one(lake, worker, monkeypatch):
    """DuckDB runs in a process of its own: one that dies as native code dies fails its statement, saying how it
    ended, and the next `USING duckdb` starts another."""
    worker("dies", ILLEGAL_INSTRUCTION)
    with pytest.raises(metalake.MetaLakeError, match=r"duckdb stopped: illegal instruction .*starts it again"):
        lake.sql("using duckdb select 1")
    monkeypatch.delenv("METALAKE_LOCAL_WORKER")
    assert one(lake.sql("using duckdb select count(*) as n from demo.t")) == {"n": 3}


@pytest.fixture
def on_a_branch(lake):
    """`lake` on branch `ventas`, where `demo.t` has a row more and `demo.only_there` is: back on `main` after."""
    if "ventas" not in [r.name for r in lake.refs()]:
        lake.branch("ventas")
        lake.sql(
            "use branch ventas; insert into demo.t values (40); "
            "create table demo.only_there (k int); insert into demo.only_there values (9); use branch main"
        )
    lake.on("ventas")
    yield lake
    lake.on("main")


def test_using_runs_on_the_ref_the_session_is_on(on_a_branch):
    """As the selector of Query or of a notebook sets it (I6): DuckDB reads and writes that ref, with no ref named."""
    lake = on_a_branch
    lake.on("main")
    main_rows = one(lake.sql("select count(*) as n from demo.t"))["n"]
    lake.on("ventas")
    assert one(lake.sql("using duckdb select k from demo.only_there")) == {"k": 9}
    before = one(lake.sql("using duckdb select count(*) as n from demo.t"))["n"]
    assert before == one(lake.sql("select count(*) as n from demo.t"))["n"], "DuckDB and the lake see the same ref"
    lake.sql("using duckdb; insert into demo.t values (41); using metalake")
    assert one(lake.sql("select count(*) as n from demo.t"))["n"] == before + 1
    lake.on("main")
    assert one(lake.sql("select count(*) as n from demo.t"))["n"] == main_rows, "main is untouched"


def test_using_on_a_tag_reads_it_and_writes_nothing(lake):
    lake.tag("frozen")
    lake.on("frozen")
    try:
        frozen = one(lake.sql("select count(*) as n from demo.t"))["n"]
        assert one(lake.sql("using duckdb select count(*) as n from demo.t"))["n"] == frozen
        with pytest.raises(metalake.MetaLakeError, match="tag"):
            lake.sql("using duckdb insert into demo.t values (99)")
    finally:
        lake.on("main")


def test_an_engine_the_lakes_file_names_is_refused_on_another_ref(on_a_branch):
    """Its catalog is connected where whoever runs it chose: MetaLake does not move it, and says how to."""
    lake = on_a_branch
    lake.add_engine("trino_test", url="http://127.0.0.1:1", user="ana")
    try:
        with pytest.raises(metalake.UsageError, match=r"metalake iceberg connect <engine> --ref ventas"):
            lake.sql("using trino_test select 1")
    finally:
        lake.remove_engine("trino_test")
