"""One row per key: replacing those a table has and adding the others."""

import re
import threading

import pytest

import metalake

pl = pytest.importorskip("polars")


def rows(lake, table):
    return sorted(pl.DataFrame(lake.sql(f"select * from {table}")).rows(), key=str)


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("upsert")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    return metalake.init(directory / "lake")


@pytest.fixture
def table(lake, request):
    name = re.sub(r"\W", "_", request.node.name)[:48].lower()
    lake.sql(f"create table {name} (id bigint, total bigint, note varchar)")
    lake.sql(f"insert into {name} values (1, 10, 'a'), (2, 20, 'b')")
    return name


def frame(*records):
    return pl.DataFrame(records, schema={"id": pl.Int64, "total": pl.Int64, "note": pl.String}, orient="row")


def test_upsert(lake, table):
    written = lake.table(table).upsert(frame((2, 21, "new"), (3, 30, None)), on=["id"])
    assert (written.rows, written.inserted, written.updated, written.applied) == (2, 1, 1, True)
    assert written.commit
    assert rows(lake, table) == [(1, 10, "a"), (2, 21, "new"), (3, 30, None)]


def test_insert_only(lake, table):
    written = lake.table(table).upsert(frame((3, 30, "c")), on=["id"])
    assert (written.inserted, written.updated) == (1, 0)


def test_update_only(lake, table):
    written = lake.table(table).upsert(frame((1, 11, "x")), on=["id"])
    assert (written.inserted, written.updated) == (0, 1)
    assert rows(lake, table) == [(1, 11, "x"), (2, 20, "b")]


def test_composite_key(lake):
    lake.sql("create table pairs (a bigint, b bigint, v bigint); insert into pairs values (1, 1, 10), (1, 2, 20)")
    data = pl.DataFrame({"a": [1, 2], "b": [2, 2], "v": [21, 30]})
    written = lake.table("pairs").upsert(data, on=["a", "b"])
    assert (written.inserted, written.updated) == (1, 1)
    assert rows(lake, "pairs") == [(1, 1, 10), (1, 2, 21), (2, 2, 30)]


def test_key_only_table(lake):
    lake.sql("create table keys (k bigint); insert into keys values (1)")
    written = lake.table("keys").upsert(pl.DataFrame({"k": [1, 2]}), on=["k"])
    assert written.inserted == 1 and rows(lake, "keys") == [(1,), (2,)]


def test_duplicate_key_raises(lake, table):
    before = rows(lake, table)
    with pytest.raises(metalake.UsageError, match="more than once"):
        lake.table(table).upsert(frame((7, 1, "a"), (7, 2, "b")), on=["id"])
    with pytest.raises(metalake.UsageError, match="more than once"):
        lake.table(table).upsert(frame((1, 1, "a"), (1, 2, "b")), on=["id"])
    assert rows(lake, table) == before


def test_null_key_raises(lake, table):
    before = rows(lake, table)
    with pytest.raises(metalake.UsageError, match="nulls"):
        lake.table(table).upsert(frame((None, 1, "a")), on=["id"])
    assert rows(lake, table) == before


def test_invalid_key_raises(lake, table):
    with pytest.raises(metalake.UsageError, match="key"):
        lake.table(table).upsert(frame((1, 1, "a")), on=[])
    with pytest.raises(metalake.UsageError, match="nope"):
        lake.table(table).upsert(frame((1, 1, "a")), on=["nope"])
    with pytest.raises(TypeError):
        lake.table(table).upsert(frame((1, 1, "a")))


def test_missing_column_raises(lake, table):
    with pytest.raises(metalake.UsageError, match="missing"):
        lake.table(table).upsert(pl.DataFrame({"id": [1]}), on=["id"])


def test_empty_input_skips_commit(lake, table):
    written = lake.table(table).upsert(frame(), on=["id"])
    assert (written.commit, written.rows, written.applied) == (None, 0, False)


def test_idempotency_key(lake, table):
    first = lake.table(table).upsert(frame((3, 30, "c")), on=["id"], idempotent=("sync", 1))
    again = lake.table(table).upsert(frame((3, 31, "d")), on=["id"], idempotent=("sync", 1))
    assert first.applied and not again.applied and again.commit == first.commit
    assert (again.inserted, again.updated) == (0, 0) or again.rows == 0
    assert rows(lake, table) == [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")]


def test_commit_author_and_message(lake, table, directory, capfd):
    lake.table(table).upsert(frame((1, 11, "x")), on=["id"], message="the sync", author="ana")
    capfd.readouterr()
    metalake._cli(["metalake", "--lake", str(directory), "log", "--format", "json", "--limit", "1"])
    said = capfd.readouterr().out
    assert "the sync" in said and "ana" in said


def test_branch_isolation(lake, table, directory):
    metalake._cli(["metalake", "--lake", str(directory), "branch", "create", f"{table}_dev"])
    lake.table(f"{table}_dev.default.{table}").upsert(frame((1, 11, "x")), on=["id"])
    assert rows(lake, table) == [(1, 10, "a"), (2, 20, "b")]
    assert (1, 11, "x") in rows(lake, f'"{table}_dev".default.{table}')


def test_quoted_identifiers(lake):
    lake.sql('create table odd ("Id" bigint, "a b" bigint); insert into odd values (1, 10)')
    written = lake.table("odd").upsert(pl.DataFrame({"Id": [1, 2], "a b": [11, 20]}), on=["Id"])
    assert (written.inserted, written.updated) == (1, 1)
    assert rows(lake, "odd") == [(1, 11), (2, 20)]


def test_source_removed_on_failure(lake, table):
    with pytest.raises(metalake.UsageError):
        lake.table(table).upsert(frame((7, 1, "a"), (7, 2, "b")), on=["id"])
    names = pl.DataFrame(lake.sql("select table_catalog from information_schema.tables"))["table_catalog"].to_list()
    assert ".source" not in names


def test_concurrent_upserts_unique_keys(lake, table):
    def write(start):
        for k in range(start, start + 3):
            while True:
                try:
                    lake.table(table).upsert(frame((k, 0, "w")), on=["id"])
                    break
                except metalake.ConflictError:
                    continue

    threads = [threading.Thread(target=write, args=(10 * n,)) for n in range(1, 5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    ids = [row[0] for row in rows(lake, table)]
    assert len(ids) == len(set(ids)) == 14


def test_many_rows(lake):
    lake.sql("create table many (id bigint, v bigint)")
    lake.table("many").append(pl.DataFrame({"id": range(100_000), "v": [0] * 100_000}))
    data = pl.DataFrame({"id": range(50_000, 150_000), "v": [1] * 100_000})
    written = lake.table("many").upsert(data, on=["id"])
    assert (written.inserted, written.updated) == (50_000, 50_000)
    total = pl.DataFrame(lake.sql("select count(*) as n, sum(v) as s from many")).row(0)
    assert total == (150_000, 100_000)
