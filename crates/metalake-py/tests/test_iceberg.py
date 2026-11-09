"""When Iceberg clients find each table's metadata written: on load, as every table has, or on commit."""

import pytest

import metalake


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("iceberg")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql("create table a (k int); create table b (k int)")
    return made


def test_on_commit_one_commit_for_all(lake):
    commit = lake.iceberg.on_commit(["default.a", "default.b"])
    assert isinstance(commit, int)
    assert lake.log(limit=1)[0].seq == commit
    assert lake.iceberg.on_commit(["default.a"]) is None, "nothing changed"
    status = lake.iceberg.status()
    assert [t.table for t in status] == ["default.a", "default.b"]
    assert all(t.written is None and t.behind > 0 for t in status), status


def test_on_commit_missing_table_raises(lake):
    with pytest.raises(metalake.MetaLakeError, match="default.nowhere"):
        lake.iceberg.on_commit(["default.b", "default.nowhere"], on="main")


def test_on_load_on_a_branch(lake):
    lake.iceberg.on_commit(["default.a"])
    lake.branch("dev")
    assert [t.table for t in lake.iceberg.status(on="dev")] == ["default.a", "default.b"], "inherited"
    assert isinstance(lake.iceberg.on_load(["default.a"], on="dev"), int)
    assert [t.table for t in lake.iceberg.status(on="dev")] == ["default.b"]
    assert "default.a" in [t.table for t in lake.iceberg.status()], "main keeps it"
