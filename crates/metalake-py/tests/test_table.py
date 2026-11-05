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


def test_a_name_reads_as_in_a_statement(lake):
    assert lake.table("t").name == "main.default.t"
    assert lake.table("ventas.pedidos").name == "main.ventas.pedidos"
    assert lake.table("dev.ventas.pedidos").name == "dev.ventas.pedidos"


def test_a_table_says_its_columns(lake):
    table = lake.table("t")
    assert isinstance(table, metalake.Table)
    assert table.columns == ["k", "s"]
    assert repr(table) == 'Table("main.default.t")'


def test_a_schema_goes_through_arrow(lake):
    pyarrow = pytest.importorskip("pyarrow")
    schema = pyarrow.schema(lake.table("t"))
    assert schema.field("k").type == pyarrow.int64()
    assert schema.field("s").type == pyarrow.string()


def test_a_table_that_is_not_there_says_so(lake):
    with pytest.raises(metalake.MetaLakeError, match="nowhere"):
        lake.table("nowhere")
    with pytest.raises(metalake.MetaLakeError, match="no ref"):
        lake.table("nobody.ventas.pedidos")


def test_a_name_that_is_no_name_is_a_wrong_use(lake):
    with pytest.raises(metalake.UsageError):
        lake.table("a..b")
