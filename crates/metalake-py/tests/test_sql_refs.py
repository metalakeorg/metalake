"""A ref's verbs in `lake.sql` (I5): the session's, as in Query and `metalake sql`. `USE BRANCH` moves `lake` as
`lake.on` does: `lake.ref`, what a bare name means, and the engines `lake.iceberg` connected (I6)."""

import pytest

import metalake

# The answers are read as Arrow: a `metalake` installed with no extra skips this.
pytest.importorskip("pyarrow")


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("sql_refs")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory / "lake"), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    lake = metalake.init(directory / "lake")
    lake.sql("create schema demo; create table demo.t (k bigint); insert into demo.t values (1), (2)")
    yield lake
    lake.on("main")


def rows(result):
    return result.to_arrow().to_pylist()


def count(lake, table):
    return rows(lake.sql(f"select count(*) as n from {table}"))[0]["n"]


def test_create_use_merge_drop_from_lake_sql(lake):
    created = rows(lake.sql("create branch py_dev"))
    assert created[0]["name"] == "py_dev" and created[0]["kind"] == "branch" and created[0]["parent"] == "main"
    assert any(r.name == "py_dev" for r in lake.refs()), "the faces see it at once"

    lake.sql("use branch py_dev; insert into demo.t values (3)")
    assert lake.ref == "py_dev", "USE moves lake, as lake.on does"
    assert count(lake, "demo.t") == 3

    lake.on("main")
    assert count(lake, "demo.t") == 2
    merged = rows(lake.sql("merge branch py_dev"))
    assert merged[0]["outcome"] == "merged"
    assert count(lake, "demo.t") == 3

    with pytest.raises(metalake.UsageError, match="FORCE"):
        lake.sql("drop branch py_dev")
    assert rows(lake.sql("drop branch py_dev force")) == [{"deleted": "py_dev"}]


def test_show_and_log(lake):
    assert {r["name"] for r in rows(lake.sql("show branches"))} >= {"main"}
    log = rows(lake.sql("show log main limit 2"))
    assert len(log) == 2 and {"commit_seq", "operation"} <= set(log[0])


def test_use_in_lake_sql_moves_the_engines_lake_iceberg_connected(lake):
    pytest.importorskip("duckdb")
    lake.sql("create branch if not exists py_side")
    lake.sql("use branch py_side; insert into demo.t values (40); use branch main")
    con = lake.iceberg.duckdb()
    on_main = con.sql("select count(*) from lake.demo.t").fetchall()[0][0]
    lake.sql("use branch py_side")
    assert con.sql("select count(*) from lake.demo.t").fetchall()[0][0] == on_main + 1, "the same connection moved"
    lake.sql("use branch main")
    assert con.sql("select count(*) from lake.demo.t").fetchall()[0][0] == on_main


def test_catalogs_from_lake_sql(lake):
    """A lake's catalogs (I7), the level above a schema, on the ref the session is on."""
    assert rows(lake.sql("create catalog ventas")) == [{"name": "ventas"}]
    lake.sql("create schema ventas.eu; create table ventas.eu.t (k bigint); insert into ventas.eu.t values (1)")
    assert {r["name"] for r in rows(lake.sql("show catalogs"))} >= {"default", "ventas"}
    assert count(lake, "ventas.eu.t") == 1
    with pytest.raises(metalake.UsageError, match="holds schemas"):
        lake.sql("drop catalog ventas")
    assert rows(lake.sql("create catalog if not exists ventas")) == [{"name": "ventas"}]
    lake.sql("create catalog bare")
    assert rows(lake.sql("drop catalog bare")) == [{"deleted": "bare"}]
