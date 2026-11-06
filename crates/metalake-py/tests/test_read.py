"""Reading through Arrow, on a lake of its own. With METALAKE_TEST_BARE set, nothing here may need pyarrow or arro3."""

import _thread
import importlib.util
import os
import threading
import time

import pytest

import metalake

pl = pytest.importorskip("polars")

LONG = "select count(*) from generate_series(1, 400000) a, generate_series(1, 400000) b"
ROWS = 2_000_000


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    directory = tmp_path_factory.mktemp("read")
    here = os.getcwd()
    os.chdir(directory)
    try:
        made = metalake.init("./lake")
    finally:
        os.chdir(here)
    made.sql("create table t (k bigint, name varchar); insert into t values (1, 'a'), (2, 'b'), (3, null); select 1")
    yield made
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.mark.skipif("METALAKE_TEST_BARE" not in os.environ, reason="only in a bare environment")
def test_no_optional_dependencies():
    assert importlib.util.find_spec("pyarrow") is None
    assert importlib.util.find_spec("arro3") is None


def test_polars_rows_and_types(lake):
    frame = pl.DataFrame(lake.sql("select k, name from t order by k"))
    assert frame.schema == {"k": pl.Int64, "name": pl.String}
    assert frame["k"].to_list() == [1, 2, 3]
    assert frame["name"].to_list() == ["a", "b", None]


def test_polars_large_result(lake):
    frame = pl.DataFrame(lake.sql(f"select value as k from generate_series(1, {ROWS})"))
    assert frame.shape == (ROWS, 1)
    assert frame["k"].sum() == ROWS * (ROWS + 1) // 2


def test_columns_without_reading(lake):
    assert lake.sql("select k, name from t").columns == ["k", "name"]


def test_batches_complete(lake):
    batches = list(lake.sql(f"select value from generate_series(1, {ROWS})"))
    assert sum(b.num_rows for b in batches) == ROWS
    assert isinstance(batches[0], metalake.Batch)
    assert batches[0].columns == ["value"] and batches[0].num_columns == 1
    assert "rows=" in repr(batches[0])
    assert sum(pl.DataFrame(b)["value"].sum() for b in batches) == ROWS * (ROWS + 1) // 2


def test_stream_failure_in_iteration(lake):
    with pytest.raises(metalake.MetaLakeError, match="(?i)divide"):
        list(lake.sql("select 1 / (k - k) from t"))


def test_stream_failure_in_polars(lake):
    # An old Polars panics on it, which is no `Exception`; `to_polars` gives back ours whatever the version.
    with pytest.raises(BaseException, match="(?i)divide"):
        pl.DataFrame(lake.sql("select 1 / (k - k) from t"))


def test_invalid_statement_raises_immediately(lake):
    with pytest.raises(metalake.MetaLakeError, match="nowhere"):
        lake.sql("select * from nowhere")
    with pytest.raises(metalake.UsageError):
        lake.sql("   ")


def interrupted(read):
    """How long `read` takes to end when an interrupt comes after a second."""
    timer = threading.Timer(1, _thread.interrupt_main)
    timer.start()
    start = time.perf_counter()
    with pytest.raises(KeyboardInterrupt):
        read()
    timer.join()
    return time.perf_counter() - start


def test_interrupt_iteration(lake):
    assert interrupted(lambda: list(lake.sql(LONG))) < 5
    assert pl.DataFrame(lake.sql("select k from t")).height == 3


def test_interrupt_polars(lake, interrupt_polars):
    # Polars says it in its own words, which depend on its version, and Polars 1 panics on a stream that fails:
    # `to_polars` is what gives the interrupt back.
    start = time.perf_counter()
    with interrupt_polars(), pytest.raises(BaseException) as raised:
        pl.DataFrame(lake.sql(LONG))
    assert time.perf_counter() - start < 5
    assert type(raised.value).__name__ in ("ComputeError", "KeyboardInterrupt", "PanicException")
    assert pl.DataFrame(lake.sql("select k from t")).height == 3


def test_partial_read_preserves_lake(lake):
    for _ in range(20):
        result = iter(lake.sql(f"select value from generate_series(1, {ROWS})"))
        assert next(result).num_rows > 0
        del result
    assert pl.DataFrame(lake.sql("select k from t")).height == 3


def test_pyarrow_result(lake):
    pyarrow = pytest.importorskip("pyarrow")
    table = pyarrow.table(lake.sql("select k, name from t order by k"))
    assert table.column("k").to_pylist() == [1, 2, 3]
    assert table.num_columns == 2


def test_second_thread_on_iterator_raises(lake):
    result = iter(lake.sql(LONG))
    errors = []
    started = threading.Event()

    def first():
        started.set()
        try:
            next(result)
        except metalake.MetaLakeError as e:
            errors.append(("first", e))
        except BaseException:
            pass

    worker = threading.Thread(target=first, daemon=True)
    worker.start()
    started.wait()
    time.sleep(0.3)
    with pytest.raises(metalake.MetaLakeError, match="another thread"):
        next(result)
    del result
    worker.join(timeout=0.5)
