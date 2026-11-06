"""Deleting by a filter, through the engine, with what a commit says."""

import re
import threading

import pytest

import metalake

pl = pytest.importorskip("polars")


def rows(lake, table):
    return sorted(pl.DataFrame(lake.sql(f"select k from {table}")).rows())


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("delete")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    return metalake.init(directory / "lake")


@pytest.fixture
def table(lake, request):
    name = re.sub(r"\W", "_", request.node.name)[:48].lower()
    lake.sql(f"create table {name} (k bigint); insert into {name} values (1), (2), (3)")
    return name


def log(directory, capfd):
    capfd.readouterr()
    metalake._cli(["metalake", "--lake", str(directory), "log", "--format", "json", "--limit", "30"])
    return capfd.readouterr().out


def test_delete_by_predicate(lake, table):
    written = lake.table(table).delete("k > 1")
    assert written.rows == 2 and written.applied and written.commit
    assert rows(lake, table) == [(1,)]


def test_branch_isolation(lake, table, directory):
    metalake._cli(["metalake", "--lake", str(directory), "branch", "create", f"{table}_dev"])
    lake.table(f"{table}_dev.default.{table}").delete("k = 1")
    assert rows(lake, table) == [(1,), (2,), (3,)]
    assert rows(lake, f'"{table}_dev".default.{table}') == [(2,), (3,)]


def test_commit_author_and_message(lake, table, directory, capfd):
    lake.table(table).delete("k = 1", message="the cleaning", author="ana")
    said = log(directory, capfd)
    assert "the cleaning" in said and "ana" in said


def test_no_match_skips_commit(lake, table):
    written = lake.table(table).delete("k > 100")
    assert (written.commit, written.rows, written.applied) == (None, 0, False)


def test_predicate_true_deletes_all_rows(lake, table):
    assert lake.table(table).delete("true").rows == 3
    assert rows(lake, table) == []


def test_predicate_required(lake, table):
    with pytest.raises(TypeError):
        lake.table(table).delete()


def test_idempotency_key(lake, table):
    first = lake.table(table).delete("k = 1", idempotent=("clean", 1))
    lake.sql(f"insert into {table} values (1)")
    again = lake.table(table).delete("k = 1", idempotent=("clean", 1))
    assert first.applied and not again.applied and again.commit == first.commit
    assert rows(lake, table) == [(1,), (2,), (3,)]


@pytest.mark.parametrize(
    "filter",
    ["1 = 1; drop table other", "1 = 1; insert into other values (9)", "k = 1; delete from other where true"],
)
def test_predicate_injection_raises(lake, table, filter):
    if "other" not in pl.DataFrame(lake.sql("select table_name from information_schema.tables"))["table_name"].to_list():
        lake.sql("create table other (k bigint); insert into other values (1)")
    before = rows(lake, "other")
    with pytest.raises(metalake.UsageError):
        lake.table(table).delete(filter)
    assert rows(lake, table) == [(1,), (2,), (3,)]
    assert rows(lake, "other") == before


def test_invalid_predicate_raises(lake, table):
    with pytest.raises(metalake.UsageError):
        lake.table(table).delete("k >")
    with pytest.raises(metalake.MetaLakeError, match="nope"):
        lake.table(table).delete("nope = 1")


def test_tag_raises(lake, table, directory):
    metalake._cli(["metalake", "--lake", str(directory), "tag", "create", f"{table}_v1"])
    with pytest.raises(metalake.MetaLakeError):
        lake.table(f"{table}_v1.default.{table}").delete("true")


def test_thread_commit_messages(lake, directory, capfd):
    tables = [f"mixed_{i}" for i in range(6)]
    for name in tables:
        lake.sql(f"create table {name} (k bigint); insert into {name} values (1)")

    def delete(name):
        lake.table(name).delete("true", message=f"message of {name}")

    threads = [threading.Thread(target=delete, args=(name,)) for name in tables]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    said = log(directory, capfd)
    assert all(f"message of {name}" in said for name in tables)


def test_concurrent_deletes_dv4_retry(lake):
    lake.sql("create table meanwhile (k bigint)")
    lake.table("meanwhile").append(pl.DataFrame({"k": range(40)}))

    def delete(k):
        while True:
            try:
                return lake.table("meanwhile").delete(f"k = {k}")
            except metalake.ConflictError as e:
                assert e.rule == "DV-4"

    threads = [threading.Thread(target=delete, args=(k,)) for k in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(rows(lake, "meanwhile")) == 32


def test_statement_without_details(lake, table):
    lake.sql(f"delete from {table} where k = 1")
    assert rows(lake, table) == [(2,), (3,)]
