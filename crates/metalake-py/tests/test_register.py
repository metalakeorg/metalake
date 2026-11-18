"""Registering files in place from Python: `lake.register` and `REGISTER TABLE` in `lake.sql`, a path in the lake's
store, the partition found in the directories, and again with no source."""

import pytest

import metalake

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("register")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory / "lake"), "stop"])


def hive_file(path, keys):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"k": pa.array(keys, pa.int64())}), path)


def rows(result):
    return result.to_arrow().to_pylist()


def test_register_in_sql_then_again_in_python(directory):
    lake = metalake.init(directory / "lake")
    source = directory / "hive" / "ventas"
    hive_file(source / "dt=2024-01-02" / "part-0.parquet", [1, 2])
    hive_file(source / "dt=2024-01-03" / "part-0.parquet", [3])
    # The store is the machine's files: a path in it is the URL's after `file://`.
    path = source.as_uri()[len("file://") :]

    created = rows(lake.sql(f"REGISTER TABLE tienda.ventas FROM '{path}'"))[0]
    assert created["created"] and created["registered"] == 2 and created["partition"] == "dt date", created
    assert rows(lake.sql("select count(*) as n from tienda.ventas"))[0]["n"] == 3

    hive_file(source / "dt=2024-01-04" / "part-0.parquet", [4])
    again = lake.register(table="tienda.ventas")
    assert (again.registered, again.already, again.partition) == (1, 2, [("dt", "date")]), again
    assert again.source.endswith("/hive/ventas")
    assert rows(lake.sql("select count(*) as n from tienda.ventas"))[0]["n"] == 4

    checked = rows(lake.sql("register table tienda.ventas check"))[0]
    assert checked["check"] and checked["registered"] == 0 and checked["already"] == 3


def test_register_needs_a_table(directory):
    lake = metalake.connect(directory / "lake")
    with pytest.raises(ValueError, match="needs the table"):
        lake.register("/x")
