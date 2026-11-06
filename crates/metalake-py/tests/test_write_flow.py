"""Every way of writing, one after the other, as a program does, and an interrupt through the engine."""

import _thread
import threading
import time

import pytest

import metalake

pl = pytest.importorskip("polars")


def pedidos(*records):
    return pl.DataFrame(records, schema={"id": pl.Int64, "total": pl.Float64, "nota": pl.String}, orient="row")


def rows(lake, table="pedidos"):
    return sorted(lake.sql(f"select * from {table}").to_polars().rows())


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("flow")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


def test_write_sequence(directory, capfd):
    lake = metalake.init(directory / "lake")
    lake.sql("create table pedidos (id bigint, total double, nota varchar)")
    t = lake.table("pedidos")

    data = pedidos((1, 9.5, "a"), (2, -1.0, None))
    assert t.append(data, message="carga", idempotent=("job", 1)).applied
    assert not t.append(data, message="carga", idempotent=("job", 1)).applied
    assert rows(lake) == [(1, 9.5, "a"), (2, -1.0, None)]

    changed = t.upsert(pedidos((1, 10.0, "b"), (3, 7.0, "c")), on=["id"])
    assert (changed.inserted, changed.updated) == (1, 1)

    assert t.delete("total < 0", message="limpieza").rows == 1
    assert rows(lake) == [(1, 10.0, "b"), (3, 7.0, "c")]

    before = t.overwrite(pedidos((9, 1.0, "z"))).commit
    assert rows(lake) == [(9, 1.0, "z")]
    assert rows(lake, f'"main@{before - 1}".default.pedidos') == [(1, 10.0, "b"), (3, 7.0, "c")]

    capfd.readouterr()
    metalake._cli(["metalake", "--lake", str(directory), "log", "--format", "json"])
    said = capfd.readouterr().out
    assert "carga" in said and "limpieza" in said


def test_interrupt_delete_and_upsert(directory):
    pyarrow = pytest.importorskip("pyarrow")
    lake = metalake.connect(directory / "lake")
    lake.sql("create table slow (id bigint); insert into slow values (1)")
    schema = pyarrow.schema([("id", pyarrow.int64())])

    def batches():
        for k in range(1000):
            time.sleep(0.05)
            yield pyarrow.record_batch([[k]], schema=schema)

    timer = threading.Timer(1, _thread.interrupt_main)
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        lake.table("slow").upsert(pyarrow.RecordBatchReader.from_batches(schema, batches()), on=["id"])
    timer.join()
    assert rows(lake, "slow") == [(1,)]
