"""The life of a ref. A branch or a tag covers the whole lake and copies nothing."""

import pytest

import metalake

pl = pytest.importorskip("polars")


def keys(lake, table):
    return sorted(row[0] for row in pl.DataFrame(lake.sql(f"select k from {table}")).rows())


def listed(lake):
    query = "select table_catalog from information_schema.tables"
    return set(pl.DataFrame(lake.sql(query))["table_catalog"].to_list())


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("refs")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql("create table t (k bigint); insert into t values (1)")
    return made


def test_a_branch_starts_at_main_and_goes_its_own_way(lake):
    made = lake.branch("own")
    assert isinstance(made, metalake.Ref)
    assert (made.name, made.kind, made.parent) == ("own", "branch", "main")
    lake.table("own.default.t").append(pl.DataFrame({"k": [2]}))
    assert keys(lake, '"own".default.t') == [1, 2]
    assert keys(lake, "t") == [1]


def test_a_branch_can_start_at_a_ref_or_a_point(lake):
    before = lake.log()[0].seq
    lake.table("t").append(pl.DataFrame({"k": [3]}))
    lake.branch("from_point", start=f"main@{before}")
    assert 3 not in keys(lake, '"from_point".default.t')
    lake.branch("from_ref", start="from_point")
    assert {r.name: r.parent for r in lake.refs()}["from_ref"] == "from_point"


def test_a_start_at_a_time_says_what_to_do(lake):
    with pytest.raises(metalake.UsageError, match="sequence"):
        lake.branch("at_time", start="main@2026-11-05")


def test_a_tag_does_not_take_writes(lake):
    made = lake.tag("frozen")
    assert made.kind == "tag"
    with pytest.raises(metalake.MetaLakeError):
        lake.table("frozen.default.t").append(pl.DataFrame({"k": [9]}))
    assert 9 not in keys(lake, '"frozen".default.t')


def test_a_new_ref_shows_in_the_schema_at_once(lake):
    lake.branch("shown")
    assert "shown" in listed(lake)


def test_a_deleted_branch_goes_and_its_name_is_not_used_again(lake):
    lake.branch("gone")
    lake.delete_branch("gone")
    assert "gone" not in {r.name for r in lake.refs()}
    assert "gone" not in listed(lake)
    with pytest.raises(metalake.MetaLakeError):
        lake.branch("gone")


def test_a_deleted_tag_goes(lake):
    lake.tag("gone_tag")
    lake.delete_tag("gone_tag")
    assert "gone_tag" not in {r.name for r in lake.refs()}


def test_each_kind_is_deleted_by_its_own_verb(lake):
    lake.tag("a_tag")
    lake.branch("a_branch")
    with pytest.raises(metalake.MetaLakeError):
        lake.delete_branch("a_tag")
    with pytest.raises(metalake.MetaLakeError):
        lake.delete_tag("a_branch")
    assert {"a_tag", "a_branch"} <= {r.name for r in lake.refs()}


def test_a_name_taken_or_a_start_that_is_not_there_is_refused(lake):
    with pytest.raises(metalake.MetaLakeError):
        lake.branch("main")
    with pytest.raises(metalake.MetaLakeError, match="no ref nobody"):
        lake.branch("orphan", start="nobody")
