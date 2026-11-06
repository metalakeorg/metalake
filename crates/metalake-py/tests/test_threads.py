"""One `Lake` shared by the threads of a program. A hang is the failure to fear, so every wait has a limit."""

import threading
import time

import pytest

import metalake

pl = pytest.importorskip("polars")

LIMIT = 90
LONG = "select count(*) from generate_series(1, 150000) a, generate_series(1, 150000) b"


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("threads")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql("create table t (k bigint); insert into t values (1), (2), (3)")
    return made


def together(*calls):
    """Runs each call in a thread of its own, from one moment, and answers what each answered."""
    answers = [None] * len(calls)
    failures = []
    start = threading.Barrier(len(calls))

    def run(at, call):
        try:
            start.wait(timeout=LIMIT)
            answers[at] = call()
        except BaseException as e:
            failures.append(e)

    threads = [threading.Thread(target=run, args=(at, call), daemon=True) for at, call in enumerate(calls)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + LIMIT
    for thread in threads:
        thread.join(timeout=max(0, deadline - time.monotonic()))
    assert not any(thread.is_alive() for thread in threads), "a thread hung"
    assert not failures, failures[:3]
    return answers


def keys(result):
    return sorted(row[0] for batch in result for row in pl.DataFrame(batch).rows())


def test_concurrent_read_one_result(lake):
    result = lake.sql("select k from t")
    assert together(*[lambda: keys(result)] * 16) == [[1, 2, 3]] * 16


def test_readers_exceed_pool(lake):
    assert together(*[lambda: keys(lake.sql("select k from t"))] * 64) == [[1, 2, 3]] * 64


def test_concurrent_readers_and_writers(lake):
    names = [f"mixed_{n}" for n in range(8)]

    def write(name):
        lake.sql(f"create table {name} (k bigint); insert into {name} values (7)")
        lake.table(name).append(pl.DataFrame({"k": [8]}))
        return keys(lake.sql(f"select k from {name}"))

    reads = [lambda: keys(lake.sql("select k from t"))] * 8
    writes = [lambda n=name: write(n) for name in names]
    answers = together(*reads, *writes)
    assert answers[:8] == [[1, 2, 3]] * 8
    assert answers[8:] == [[7, 8]] * 8


def test_concurrent_appends_applied(lake):
    lake.sql("create table appended (k bigint)")
    table = lake.table("appended")
    together(*[lambda k=k: table.append(pl.DataFrame({"k": [k]})) for k in range(32)])
    assert keys(lake.sql("select k from appended")) == list(range(32))


def test_deletes_at_pool_limit(lake):
    # Each would hold a connection and wait for a second, if reading what to delete kept one.
    for round in range(6):
        name = f"deleted_{round}"
        lake.sql(f"create table {name} (k bigint)")
        lake.table(name).append(pl.DataFrame({"k": range(16)}))

        def delete(k):
            while True:
                try:
                    return lake.table(name).delete(f"k = {k}")
                except metalake.ConflictError:
                    pass

        together(*[lambda k=k: delete(k) for k in range(16)])
        assert keys(lake.sql(f"select k from {name}")) == []


def test_updates_at_pool_limit(lake):
    names = [f"updated_{n}" for n in range(16)]
    for name in names:
        lake.sql(f"create table {name} (k bigint); insert into {name} values (1)")
    together(*[lambda name=name: lake.sql(f"update {name} set k = 2") for name in names])
    assert all(keys(lake.sql(f"select k from {name}")) == [2] for name in names)


def test_open_read_does_not_block(lake):
    held = iter(lake.sql("select value from generate_series(1, 2000000)"))
    assert next(held).num_rows > 0
    assert together(*[lambda: keys(lake.sql("select k from t"))] * 8) == [[1, 2, 3]] * 8
    del held


def test_gil_released_during_call(lake):
    ticks = []
    stop = threading.Event()

    def tick():
        while not stop.is_set():
            ticks.append(1)
            time.sleep(0.01)

    thread = threading.Thread(target=tick, daemon=True)
    thread.start()
    result = lake.sql(LONG)
    start = time.perf_counter()
    list(result)
    took = time.perf_counter() - start
    stop.set()
    thread.join(timeout=LIMIT)
    assert took > 0.2, "the statement was meant to take a while"
    assert len(ticks) > took * 10


def test_iterator_single_thread_result_shared(lake):
    result = lake.sql(LONG)
    reading = iter(result)
    errors = []

    def first():
        try:
            next(reading)
        except metalake.MetaLakeError as e:
            errors.append(e)

    thread = threading.Thread(target=first, daemon=True)
    thread.start()
    time.sleep(0.3)
    with pytest.raises(metalake.MetaLakeError, match="another thread"):
        next(reading)
    second = iter(lake.sql("select k from t"))
    assert next(second).num_rows == 3
