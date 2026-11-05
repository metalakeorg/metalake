"""What a lake says of itself."""

import pytest

import metalake

pl = pytest.importorskip("polars")


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("look")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql("create table t (k bigint)")
    made.table("t").append(pl.DataFrame({"k": [1]}), message="the load", author="ana")
    made.table("t").delete("k = 1", message="the cleaning", author="ben")
    metalake._cli(["metalake", "--lake", str(directory), "branch", "create", "dev"])
    metalake._cli(["metalake", "--lake", str(directory), "tag", "create", "v1"])
    return made


def test_status_says_what_the_lake_is(lake, directory):
    status = lake.status()
    assert isinstance(status, metalake.Status)
    assert status.local and status.local.endswith("lake") and status.postgres is None
    assert status.schema == "metalake"
    assert status.warehouse.startswith("file:")
    assert status.format_version and status.writer_version
    assert set(status.refs) == {"main", "dev", "v1"}


def test_status_prints_as_the_command_does(lake, directory, capfd):
    capfd.readouterr()
    metalake._cli(["metalake", "--lake", str(directory), "status"])
    said = capfd.readouterr().out
    assert str(lake.status()).split() == said.split()


def test_refs_says_what_each_is(lake):
    refs = {r.name: r for r in lake.refs()}
    assert isinstance(refs["main"], metalake.Ref)
    assert set(refs) == {"main", "dev", "v1"}
    assert refs["main"].kind == "branch" and refs["main"].parent is None
    assert refs["dev"].kind == "branch" and refs["dev"].parent == "main"
    assert refs["v1"].kind == "tag"
    assert refs["dev"].fork_seq is not None and refs["dev"].created_seq > 0
    assert refs["dev"].protected is False
    assert '"dev"' in repr(refs["dev"])


def test_log_says_what_happened_latest_first(lake):
    commits = lake.log()
    assert all(isinstance(c, metalake.Commit) for c in commits)
    assert [c.seq for c in commits] == sorted((c.seq for c in commits), reverse=True)
    latest, before = commits[0], commits[1]
    assert (latest.message, latest.author) == ("the cleaning", "ben")
    assert (before.message, before.author) == ("the load", "ana")
    assert latest.operation and "the cleaning" in repr(latest)


def test_a_commit_is_a_point_to_read_at(lake):
    point = lake.log()[1].seq
    assert pl.DataFrame(lake.sql(f'select k from "main@{point}".default.t')).height == 1


def test_log_has_a_limit(lake):
    assert len(lake.log(limit=1)) == 1
    assert len(lake.log("main", limit=2)) == 2


def test_log_of_a_table_says_its_snapshots(lake):
    snapshots = lake.log(table="default.t")
    assert all(isinstance(s, metalake.Snapshot) for s in snapshots)
    assert len(snapshots) >= 3
    assert snapshots[0].parent == snapshots[1].id
    assert snapshots[0].operation


def test_a_ref_that_is_not_there_says_so(lake):
    with pytest.raises(metalake.MetaLakeError, match="no ref nobody"):
        lake.log("nobody")
    with pytest.raises(metalake.UsageError):
        lake.log(table="t")
