"""A process copied by `fork`: failing here would be hanging, so every wait has a limit."""

import multiprocessing
import os
import pickle
import sys

import pytest

import metalake

needs_fork = pytest.mark.skipif(not hasattr(os, "fork"), reason="this system has no fork")
not_on_macos = pytest.mark.skipif(sys.platform == "darwin", reason="macOS refuses a forked child")
LIMIT = 60

# What a child inherits. A fork copies these, which is the point.
LAKE = None
TABLE = None
RESULT = None


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("fork")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    global LAKE, TABLE, RESULT
    LAKE = metalake.init(directory / "lake")
    LAKE.sql("create table t (k bigint); insert into t values (1), (2)")
    TABLE = LAKE.table("t")
    RESULT = LAKE.sql("select k from t")
    return LAKE


def in_children(function, *calls):
    """Runs `function` with each of `calls` in a process of its own, and answers what each said."""
    with multiprocessing.get_context("fork").Pool(len(calls)) as pool:
        running = [pool.apply_async(function, call) for call in calls]
        return [each.get(timeout=LIMIT) for each in running]


def refused(call):
    try:
        call()
    except metalake.UsageError as e:
        return str(e)
    return "it answered"


def use_lake():
    return refused(LAKE.status)


def use_lake_sql():
    return refused(lambda: LAKE.sql("select 1"))


def use_table():
    return refused(lambda: TABLE.columns)


def use_table_write():
    return refused(lambda: TABLE.delete("true"))


def use_result():
    return refused(lambda: list(RESULT))


def use_result_stream():
    return refused(RESULT.__arrow_c_stream__)


def connect_again(directory):
    own = metalake.connect(directory)
    return [batch.num_rows for batch in own.sql("select k from t")]


def write(directory, k):
    metalake.connect(directory).sql(f"insert into written values ({k})")
    return k


def connect_refused(directory):
    return refused(lambda: metalake.connect(directory))


def leave():
    return os.getpid()


@needs_fork
@pytest.mark.parametrize(
    "use", [use_lake, use_lake_sql, use_table, use_table_write, use_result, use_result_stream]
)
def test_what_a_child_inherits_says_so_and_does_not_hang(lake, use):
    said = in_children(use, ())[0]
    assert "another process" in said and "connect again" in said


@needs_fork
@not_on_macos
def test_a_child_connects_on_its_own(lake, directory):
    assert sum(in_children(connect_again, (directory,))[0]) == 2


@needs_fork
@not_on_macos
def test_children_write_each_with_its_own(lake, directory):
    lake.sql("create table written (k bigint)")
    assert sorted(in_children(write, *[(directory, k) for k in range(4)])) == [0, 1, 2, 3]
    assert sum(batch.num_rows for batch in lake.sql("select k from written")) == 4


@pytest.mark.skipif(sys.platform != "darwin", reason="only macOS refuses a forked child")
def test_on_macos_a_forked_child_is_told_to_spawn(lake, directory):
    assert "spawn" in in_children(connect_refused, (directory,))[0]


def test_a_spawned_child_connects_on_its_own(lake, directory):
    with multiprocessing.get_context("spawn").Pool(1) as pool:
        assert sum(pool.apply_async(connect_again, (directory,)).get(timeout=LIMIT)) == 2


@needs_fork
def test_a_child_ends_with_what_it_inherited(lake):
    assert in_children(leave, ())[0] != os.getpid()


@needs_fork
def test_the_parent_goes_on(lake):
    in_children(use_lake, ())
    assert lake.status().schema == "metalake"
    assert sum(batch.num_rows for batch in lake.sql("select k from t")) == 2


def test_a_lake_does_not_travel_by_pickle(lake):
    with pytest.raises(TypeError):
        pickle.dumps(lake)
    with pytest.raises(TypeError):
        pickle.dumps(lake.table("t"))


def test_this_process_is_not_refused(lake):
    assert lake.status().schema == "metalake"
    assert sum(batch.num_rows for batch in lake.sql("select k from t")) == 2
