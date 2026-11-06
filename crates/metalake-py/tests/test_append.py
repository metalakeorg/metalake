"""Rows into a table from anything that speaks Arrow, in whatever representation it has them."""

import _thread
import datetime
import decimal
import threading
import time

import pytest

import metalake

pl = pytest.importorskip("polars")

COLUMNS = "k bigint, s varchar, m decimal(10, 2), day date, at timestamp, big double, tags bigint"


def rows(lake, table):
    return sorted(pl.DataFrame(lake.sql(f"select * from {table}")).rows(), key=str)


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("append")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    return metalake.init(directory / "lake")


@pytest.fixture
def table(lake, request):
    name = request.node.name[:40].lower()
    lake.sql(f"create table {name} (k bigint, s varchar)")
    return name


def parquet(directory):
    return set((directory / "lake" / "data").rglob("*.parquet"))


def test_polars(lake, table):
    written = lake.table(table).append(pl.DataFrame({"k": [1, 2], "s": ["a", None]}))
    assert isinstance(written, metalake.Written)
    assert written.rows == 2 and written.applied and written.commit
    assert rows(lake, table) == [(1, "a"), (2, None)]


def test_pyarrow(lake, table):
    pyarrow = pytest.importorskip("pyarrow")
    data = pyarrow.table({"k": [1], "s": ["a"]})
    lake.table(table).append(data)
    lake.table(table).append(data.to_batches()[0])
    lake.table(table).append(data.to_reader())
    assert rows(lake, table) == [(1, "a")] * 3


def test_pandas(lake, table):
    pandas = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    lake.table(table).append(pandas.DataFrame({"k": [1, 2], "s": ["a", "b"]}))
    assert rows(lake, table) == [(1, "a"), (2, "b")]


def test_pandas_nullable_integers(lake, table):
    pandas = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    data = pandas.DataFrame({"k": pandas.array([1, None], dtype="Int64"), "s": ["a", "b"]})
    lake.table(table).append(data)
    assert rows(lake, table) == [(1, "a"), (None, "b")]


def test_pandas_integers_with_nulls_raises(lake, table, directory):
    pandas = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    before = parquet(directory)
    with pytest.raises(metalake.UsageError, match="Int64"):
        lake.table(table).append(pandas.DataFrame({"k": [1, None], "s": ["a", "b"]}))
    assert parquet(directory) == before


def test_append_sql_result(lake, table):
    lake.table(table).append(pl.DataFrame({"k": [1], "s": ["a"]}))
    lake.table(table).append(lake.sql(f"select k + 1 as k, s from {table}"))
    assert rows(lake, table) == [(1, "a"), (2, "a")]


def test_polars_all_types(lake):
    lake.sql(f"create table kinds ({COLUMNS})")
    data = pl.DataFrame(
        {
            "k": [1, None],
            "s": ["a", None],
            "m": [decimal.Decimal("3.25"), None],
            "day": [datetime.date(2026, 11, 5), None],
            "at": [datetime.datetime(2026, 11, 5, 10, 30), None],
            "big": [1.5, None],
            "tags": [7, None],
        },
        schema_overrides={"m": pl.Decimal(10, 2), "tags": pl.Int32},
    )
    lake.table("kinds").append(data)
    assert pl.DataFrame(lake.sql("select * from kinds order by k")).row(0) == data.row(0)


def test_columns_by_name(lake, table):
    lake.table(table).append(pl.DataFrame({"s": ["a"], "k": [1]}))
    assert rows(lake, table) == [(1, "a")]


def test_missing_or_extra_column_raises(lake, table, directory):
    before = parquet(directory)
    with pytest.raises(metalake.UsageError, match="missing"):
        lake.table(table).append(pl.DataFrame({"k": [1]}))
    with pytest.raises(metalake.UsageError, match="extra"):
        lake.table(table).append(pl.DataFrame({"k": [1], "s": ["a"], "z": [1]}))
    with pytest.raises(metalake.UsageError, match="double"):
        lake.table(table).append(pl.DataFrame({"k": [1.5], "s": ["a"]}))
    assert parquet(directory) == before
    assert rows(lake, table) == []


def test_overflow_raises(lake):
    lake.sql("create table small (k int, m decimal(4, 2))")
    with pytest.raises(metalake.MetaLakeError):
        lake.table("small").append(pl.DataFrame({"k": [2**40], "m": [decimal.Decimal("1.00")]}))
    with pytest.raises(metalake.MetaLakeError):
        lake.table("small").append(
            pl.DataFrame({"k": [1], "m": [decimal.Decimal("123456.00")]}, schema_overrides={"m": pl.Decimal(10, 2)})
        )
    assert rows(lake, "small") == []


def test_empty_input_skips_commit(lake, table):
    written = lake.table(table).append(pl.DataFrame(schema={"k": pl.Int64, "s": pl.String}))
    assert (written.commit, written.rows, written.applied) == (None, 0, False)


def test_many_rows(lake):
    lake.sql("create table many (k bigint)")
    written = lake.table("many").append(pl.DataFrame({"k": range(1_000_000)}))
    assert written.rows == 1_000_000
    assert pl.DataFrame(lake.sql("select sum(k) as s from many"))["s"][0] == 999_999 * 1_000_000 // 2


def test_idempotency_key(lake, table):
    data = pl.DataFrame({"k": [1], "s": ["a"]})
    first = lake.table(table).append(data, idempotent=("job", 7))
    again = lake.table(table).append(data, idempotent=("job", 7))
    assert first.applied and not again.applied and again.commit == first.commit
    assert lake.table(table).append(data, idempotent=("job", 8)).applied
    assert rows(lake, table) == [(1, "a")] * 2


def test_commit_author_and_message(lake, table, directory, capfd):
    lake.table(table).append(pl.DataFrame({"k": [1], "s": ["a"]}), message="the load", author="ana")
    metalake._cli(["metalake", "--lake", str(directory), "log", "--format", "json", "--limit", "1"])
    said = capfd.readouterr().out
    assert "the load" in said and "ana" in said


def test_tag_raises(lake, table, directory):
    metalake._cli(["metalake", "--lake", str(directory), "tag", "create", f"{table}_v1"])
    with pytest.raises(metalake.MetaLakeError):
        lake.table(f"{table}_v1.default.{table}").append(pl.DataFrame({"k": [1], "s": ["a"]}))


def test_branch_isolation(lake, table, directory):
    metalake._cli(["metalake", "--lake", str(directory), "branch", "create", f"{table}_dev"])
    lake.table(f"{table}_dev.default.{table}").append(pl.DataFrame({"k": [1], "s": ["a"]}))
    assert rows(lake, table) == []
    assert rows(lake, f'"{table}_dev".default.{table}') == [(1, "a")]


def test_python_stream(lake, table):
    pyarrow = pytest.importorskip("pyarrow")
    schema = pyarrow.schema([("k", pyarrow.int64()), ("s", pyarrow.string())])

    def batches():
        for k in range(50):
            yield pyarrow.record_batch([[k], ["a"]], schema=schema)

    written = lake.table(table).append(pyarrow.RecordBatchReader.from_batches(schema, batches()))
    assert written.rows == 50


def test_stream_failure_skips_commit(lake, table):
    pyarrow = pytest.importorskip("pyarrow")
    schema = pyarrow.schema([("k", pyarrow.int64()), ("s", pyarrow.string())])

    def batches():
        yield pyarrow.record_batch([[1], ["a"]], schema=schema)
        raise ValueError("the source broke")

    with pytest.raises(metalake.MetaLakeError, match="the source broke"):
        lake.table(table).append(pyarrow.RecordBatchReader.from_batches(schema, batches()))
    assert rows(lake, table) == []


def test_interrupt_skips_commit(lake, table):
    pyarrow = pytest.importorskip("pyarrow")
    schema = pyarrow.schema([("k", pyarrow.int64()), ("s", pyarrow.string())])

    def batches():
        for k in range(1000):
            time.sleep(0.05)
            yield pyarrow.record_batch([[k], ["a"]], schema=schema)

    timer = threading.Timer(1, _thread.interrupt_main)
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        lake.table(table).append(pyarrow.RecordBatchReader.from_batches(schema, batches()))
    timer.join()
    assert rows(lake, table) == []


def test_non_arrow_input_raises(lake, table):
    with pytest.raises((TypeError, ValueError)):
        lake.table(table).append({"k": [1]})
