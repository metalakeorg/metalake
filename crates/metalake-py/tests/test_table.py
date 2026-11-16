"""A table by its name, as a statement names it."""

import pytest

import metalake


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("table")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql(
        "create table t (k bigint, s varchar);"
        "create schema ventas;"
        "create table ventas.pedidos (id bigint);"
    )
    metalake._cli(["metalake", "--lake", str(directory), "branch", "create", "dev"])
    return made


def test_table_path_parse(lake):
    """`[[catalog.]schema.]table`, on the ref `lake` is on: no name says its ref (I7)."""
    assert lake.table("t").name == "default.default.t"
    assert lake.table("ventas.pedidos").name == "default.ventas.pedidos"
    assert lake.table("default.ventas.pedidos").name == "default.ventas.pedidos"
    assert lake.table("t").ref == "main"
    lake.on("dev")
    try:
        on_dev = lake.table("ventas.pedidos")
        assert (on_dev.name, on_dev.ref) == ("default.ventas.pedidos", "dev")
    finally:
        lake.on("main")


def test_table_columns(lake):
    table = lake.table("t")
    assert isinstance(table, metalake.Table)
    assert table.columns == ["k", "s"]
    assert repr(table) == 'Table("default.default.t")'


def test_table_arrow_schema(lake):
    pyarrow = pytest.importorskip("pyarrow")
    schema = pyarrow.schema(lake.table("t"))
    assert schema.field("k").type == pyarrow.int64()
    assert schema.field("s").type == pyarrow.string()


def test_missing_table_raises(lake):
    with pytest.raises(metalake.MetaLakeError, match="nowhere"):
        lake.table("nowhere")
    with pytest.raises(metalake.MetaLakeError, match="nobody"):
        lake.table("nobody.ventas.pedidos")


def test_invalid_name_raises(lake):
    with pytest.raises(metalake.UsageError):
        lake.table("a..b")
