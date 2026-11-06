"""What `sql` means: a read waits and stays where it was, anything else has happened."""

import _thread
import threading

import pytest

import metalake

pl = pytest.importorskip("polars")


def rows(result):
    return sorted(pl.DataFrame(result).rows())


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("sql")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql(
        "create table t (k bigint, v bigint); create table u (k bigint, w bigint);"
        "insert into t values (1, 10), (2, 20), (3, 30);"
        "insert into u values (1, 100), (2, 200)"
    )
    return made


def count(lake, table):
    return pl.DataFrame(lake.sql(f"select count(*) as n from {table}"))["n"][0]


@pytest.fixture
def table(lake, request):
    name = request.node.name[:40].lower()
    lake.sql(f"create table {name} (k bigint); insert into {name} values (1)")
    return name


def test_read_stable_point(lake, table):
    result = lake.sql(f"select k from {table}")
    lake.sql(f"insert into {table} values (2)")
    assert rows(result) == [(1,)]
    assert rows(lake.sql(f"select k from {table}")) == [(1,), (2,)]


def test_two_tables_stable_point(lake, table):
    lake.sql(f"create table {table}_b (k bigint); insert into {table}_b values (1)")
    result = lake.sql(f"select a.k from {table} a join {table}_b b on a.k = b.k")
    lake.sql(f"insert into {table} values (5); insert into {table}_b values (5)")
    assert rows(result) == [(1,)]


def test_write_without_read(lake, table):
    lake.sql(f"insert into {table} values (2)")
    lake.sql(f"update {table} set k = 3 where k = 2")
    lake.sql(f"delete from {table} where k = 1")
    assert rows(lake.sql(f"select k from {table}")) == [(3,)]


def test_write_row_count(lake, table):
    result = lake.sql(f"insert into {table} values (2), (3)")
    assert pl.DataFrame(result)["count"][0] == 2


def test_statements_commit_individually(lake, table):
    with pytest.raises(metalake.MetaLakeError, match="nowhere"):
        lake.sql(f"insert into {table} values (2); select * from nowhere; insert into {table} values (3)")
    assert rows(lake.sql(f"select k from {table}")) == [(1,), (2,)]


@pytest.mark.parametrize(
    "read",
    [
        "select k, v from t",
        "select k, v from t order by v desc",
        "select count(*) as n, sum(v) as s from t",
        "select k % 2 as g, sum(v) as s from t group by g",
        "select t.k, w from t join u on t.k = u.k",
        "select t.k, w from t cross join u",
        "select k from t where v > (select avg(v) from t)",
        "select k, sum(v) over (order by k) as s from t",
        "select k from t union all select k from u",
        "select distinct k from u",
        "with r as (select k from t) select * from r where k > 1",
        "select k from t order by k limit 2",
    ],
)
def test_result_repeatable(lake, read):
    result = lake.sql(read)
    first = rows(result)
    assert first, read
    assert rows(result) == first
    assert sorted(row for batch in result for row in pl.DataFrame(batch).rows()) == first
    assert rows(result) == first


def test_result_stable_after_rewrite(lake, table):
    result = lake.sql(f"select k from {table}")
    assert rows(result) == [(1,)]
    lake.sql(f"insert overwrite {table} values (9)")
    assert rows(result) == [(1,)]


def test_new_table_in_information_schema(lake):
    lake.sql("create table listed (k bigint)")
    names = pl.DataFrame(lake.sql("select table_name from information_schema.tables"))["table_name"]
    assert "listed" in names.to_list()


def test_refresh_external_commits(lake, directory):
    other = metalake.connect(directory / "lake")
    other.sql("create table by_another (k bigint)")
    query = "select table_name from information_schema.tables"
    assert "by_another" not in pl.DataFrame(lake.sql(query))["table_name"].to_list()
    lake.refresh()
    assert "by_another" in pl.DataFrame(lake.sql(query))["table_name"].to_list()
    assert count(lake, "by_another") == 0


def test_external_table_resolved_by_name(lake, directory):
    metalake.connect(directory / "lake").sql("create table named_by_another (k bigint)")
    assert count(lake, "named_by_another") == 0


def test_ref_and_sequence_names(lake, table, directory):
    metalake._cli(["metalake", "--lake", str(directory), "branch", "create", f"{table}_dev"])
    lake.sql(f'insert into "{table}_dev".default.{table} values (2)')
    assert rows(lake.sql(f"select k from {table}")) == [(1,)]
    assert rows(lake.sql(f'select k from "{table}_dev".default.{table}')) == [(1,), (2,)]


def test_concurrent_writers_conflict(lake, table):
    lake.sql(f"insert into {table} values (2)")
    refused = []

    def write():
        for _ in range(12):
            try:
                lake.sql(f"update {table} set k = k + 10 where k > 0")
            except metalake.ConflictError as e:
                refused.append(e)

    writers = [threading.Thread(target=write) for _ in range(4)]
    for writer in writers:
        writer.start()
    for writer in writers:
        writer.join()
    assert refused, "writers on one row were meant to conflict"
    assert all(e.rule for e in refused), [str(e) for e in refused][:3]


def test_interrupt_rolls_back(lake, table):
    timer = threading.Timer(1, _thread.interrupt_main)
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        lake.sql(
            f"insert into {table} select a.value from generate_series(1, 400000) a, generate_series(1, 400000) b"
        )
    timer.join()
    assert rows(lake.sql(f"select k from {table}")) == [(1,)]
