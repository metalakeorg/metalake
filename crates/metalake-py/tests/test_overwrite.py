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


def test_it_replaces_every_row(lake, table):
    written = lake.table(table).overwrite(frame(9))
    assert written.rows == 1 and written.applied
    assert rows(lake, table) == [(9,)]


def test_it_replaces_rows_a_delete_left_too(lake, table):
    lake.sql(f"delete from {table} where k = 1")
    lake.table(table).overwrite(frame(9))
    assert rows(lake, table) == [(9,)]


def test_no_rows_leave_it_empty_and_commit(lake, table):
    written = lake.table(table).overwrite(frame())
    assert written.commit and written.applied and written.rows == 0
    assert rows(lake, table) == []


def test_what_it_held_stays_at_earlier_points(lake, table):
    before = lake.sql(f"select k from {table}")
    point = lake.table(table).append(frame(3)).commit
    lake.table(table).overwrite(frame(9))
    assert rows(lake, table) == [(9,)]
    assert sorted(pl.DataFrame(before).rows()) == [(1,), (2,)]
    assert rows(lake, f'"main@{point}".default.{table}') == [(1,), (2,), (3,)]


def test_a_branch_leaves_its_parent_alone(lake, table, directory):
    metalake._cli(["metalake", "--lake", str(directory), "branch", "create", f"{table}_dev"])
    lake.table(f"{table}_dev.default.{table}").overwrite(frame(9))
    assert rows(lake, table) == [(1,), (2,)]
    assert rows(lake, f'"{table}_dev".default.{table}') == [(9,)]


def test_columns_are_held_to_as_in_append(lake, table, directory):
    with pytest.raises(metalake.UsageError, match="missing"):
        lake.table(table).overwrite(pl.DataFrame({"z": [1]}))
    assert rows(lake, table) == [(1,), (2,)]


def test_a_write_that_lands_meanwhile_is_not_lost(lake, table, directory):
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


def test_repeating_with_a_key_applies_once(lake, table):
    first = lake.table(table).overwrite(frame(9), idempotent=("load", 1))
    lake.table(table).append(frame(10))
    again = lake.table(table).overwrite(frame(9), idempotent=("load", 1))
    assert first.applied and not again.applied and again.commit == first.commit
    assert rows(lake, table) == [(10,), (9,)]


def test_an_overwrite_by_sql_still_replaces(lake, table):
    lake.sql(f"insert overwrite {table} values (5)")
    assert rows(lake, table) == [(5,)]
