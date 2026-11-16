"""The engines `lake.iceberg` connects follow the ref `lake` is on (I6): `lake.on`, which a notebook's selector calls,
moves them as it moves `lake.sql`, at their next statement, with no new connection. Given `ref=`, one stays on it. A
table already loaded is a snapshot: written after a move, its commit conflicts, and a client that tries again lands it
on the ref the session is on now, never on the one it was loaded from."""

import pytest

import metalake


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("iceberg_refs")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory / "lake"), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    lake = metalake.init(directory / "lake")
    lake.sql("create schema demo; create table demo.t (k bigint); insert into demo.t values (1), (2)")
    lake.branch("ventas_ana")
    lake.sql(
        "insert into ventas_ana.demo.t values (3); "
        "create table ventas_ana.demo.only_there (k bigint); insert into ventas_ana.demo.only_there values (9)"
    )
    yield lake
    lake.on("main")


def count(lake, table):
    return lake.sql(f"select count(*) as n from {table}").to_arrow().to_pylist()[0]["n"]


def test_one_duckdb_connection_follows_lake_on(lake):
    pytest.importorskip("duckdb")
    lake.on("ventas_ana")
    con = lake.iceberg.duckdb()
    assert con.sql("select count(*) from lake.demo.t").fetchall()[0][0] == 3
    assert con.sql("select k from lake.demo.only_there").fetchall() == [(9,)]
    con.sql("insert into lake.demo.t values (4)")
    assert count(lake, "ventas_ana.demo.t") == 4
    assert count(lake, "main.demo.t") == 2, "main is untouched"

    lake.on("main")
    assert con.sql("select count(*) from lake.demo.t").fetchall()[0][0] == 2, "the same connection, now on main"
    lake.on("ventas_ana")
    assert con.sql("select count(*) from lake.demo.t").fetchall()[0][0] == 4, "and back"


def test_two_refs_pinned_in_one_duckdb_connection(lake):
    pytest.importorskip("duckdb")
    con = lake.iceberg.duckdb(name="ana", ref="ventas_ana")
    lake.iceberg.duckdb(con, name="trunk", ref="main")
    rows = con.sql(
        "select 'ana', count(*) from ana.demo.t union all select 'trunk', count(*) from trunk.demo.t order by 1"
    ).fetchall()
    assert rows[1] == ("trunk", 2)
    assert rows[0][0] == "ana" and rows[0][1] >= 3


def test_one_pyiceberg_catalog_follows_lake_on(lake):
    pytest.importorskip("pyiceberg")
    pa = pytest.importorskip("pyarrow")
    lake.on("ventas_ana")
    catalog = lake.iceberg.pyiceberg()
    assert ("demo", "only_there") in catalog.list_tables("demo")
    before = count(lake, "ventas_ana.demo.only_there")
    catalog.load_table("demo.only_there").append(pa.table({"k": pa.array([10], pa.int64())}))
    assert count(lake, "ventas_ana.demo.only_there") == before + 1
    lake.on("main")
    assert ("demo", "only_there") not in catalog.list_tables("demo"), "the same catalog, now on main"


def test_a_table_loaded_before_a_move_never_lands_on_the_ref_it_was_loaded_from(lake):
    """Its commit conflicts on the ref the session is on now; PyIceberg, as Iceberg's clients do, then loads the table
    again, through the same session, and applies the append there: never on the ref the table was loaded from."""
    pytest.importorskip("pyiceberg")
    pa = pytest.importorskip("pyarrow")
    lake.on("ventas_ana")
    t = lake.iceberg.pyiceberg().load_table("demo.t")
    main_rows, ventas_rows = count(lake, "main.demo.t"), count(lake, "ventas_ana.demo.t")
    lake.on("main")
    t.append(pa.table({"k": pa.array([77], pa.int64())}))
    assert count(lake, "ventas_ana.demo.t") == ventas_rows, "not on the ref it was loaded from"
    assert count(lake, "main.demo.t") == main_rows + 1, "on the ref the session is on"


def test_the_profile_names_the_ref(lake):
    lake.on("ventas_ana")
    assert lake.iceberg.properties("pyiceberg")["warehouse"] == "ventas_ana"
    assert any("ATTACH 'ventas_ana'" in s for s in lake.iceberg.profile("duckdb").statements)
    lake.on("main")
    assert "warehouse" not in lake.iceberg.properties("pyiceberg")
