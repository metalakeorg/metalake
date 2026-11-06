"""Replacing what a table holds."""

import pytest

import metalake

pl = pytest.importorskip("polars")


def rows(lake, table):
    return sorted(pl.DataFrame(lake.sql(f"select * from {table}")).rows(), key=str)


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("overwrite")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    return metalake.init(directory / "lake")


@pytest.fixture
def table(lake, request):
    name = request.node.name[:40].lower()
    lake.sql(f"create table {name} (k bigint); insert into {name} values (1), (2)")
    return name


def frame(*keys):
    return pl.DataFrame({"k": list(keys)}, schema={"k": pl.Int64})


def test_overwrite(lake, table):
    written = lake.table(table).overwrite(frame(9))
    assert written.rows == 1 and written.applied
    assert rows(lake, table) == [(9,)]


def test_overwrite_after_delete(lake, table):
    lake.sql(f"delete from {table} where k = 1")
    lake.table(table).overwrite(frame(9))
    assert rows(lake, table) == [(9,)]


def test_overwrite_empty_input_commits(lake, table):
    written = lake.table(table).overwrite(frame())
    assert written.commit and written.applied and written.rows == 0
    assert rows(lake, table) == []


def test_time_travel_preserved(lake, table):
    before = lake.sql(f"select k from {table}")
    point = lake.table(table).append(frame(3)).commit
    lake.table(table).overwrite(frame(9))
    assert rows(lake, table) == [(9,)]
    assert sorted(pl.DataFrame(before).rows()) == [(1,), (2,)]
    assert rows(lake, f'"main@{point}".default.{table}') == [(1,), (2,), (3,)]


def test_branch_isolation(lake, table, directory):
    metalake._cli(["metalake", "--lake", str(directory), "branch", "create", f"{table}_dev"])
    lake.table(f"{table}_dev.default.{table}").overwrite(frame(9))
    assert rows(lake, table) == [(1,), (2,)]
    assert rows(lake, f'"{table}_dev".default.{table}') == [(9,)]


def test_missing_column_raises(lake, table, directory):
    with pytest.raises(metalake.UsageError, match="missing"):
        lake.table(table).overwrite(pl.DataFrame({"z": [1]}))
    assert rows(lake, table) == [(1,), (2,)]


def test_concurrent_append_conflicts(lake, table, directory):
    pyarrow = pytest.importorskip("pyarrow")
    schema = pyarrow.schema([("k", pyarrow.int64())])
    other = metalake.connect(directory / "lake")

    def batches():
        yield pyarrow.record_batch([[9]], schema=schema)
        other.table(table).append(frame(3))

    with pytest.raises(metalake.ConflictError) as raised:
        lake.table(table).overwrite(pyarrow.RecordBatchReader.from_batches(schema, batches()))
    assert raised.value.rule
    assert rows(lake, table) == [(1,), (2,), (3,)]


def test_idempotency_key(lake, table):
    first = lake.table(table).overwrite(frame(9), idempotent=("load", 1))
    lake.table(table).append(frame(10))
    again = lake.table(table).overwrite(frame(9), idempotent=("load", 1))
    assert first.applied and not again.applied and again.commit == first.commit
    assert rows(lake, table) == [(10,), (9,)]


def test_sql_overwrite(lake, table):
    lake.sql(f"insert overwrite {table} values (5)")
    assert rows(lake, table) == [(5,)]
