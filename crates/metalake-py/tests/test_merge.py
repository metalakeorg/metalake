"""Merging a branch into its parent. Postgres decides, and a refusal changes nothing."""

import re

import pytest

import metalake

pl = pytest.importorskip("polars")


def rows(lake, table):
    return sorted(pl.DataFrame(lake.sql(f"select * from {table}")).rows())


def frame(*keys):
    return pl.DataFrame({"k": list(keys)}, schema={"k": pl.Int64})


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("merge")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    return metalake.init(directory / "lake")


@pytest.fixture
def case(lake, request):
    """A table with a row, and a branch of its own."""
    name = re.sub(r"\W", "_", request.node.name)[:44].lower()
    lake.sql(f"create table {name} (k bigint); insert into {name} values (1)")
    lake.branch(f"{name}_dev")
    return name, f"{name}_dev"


def test_it_merges(lake, case):
    table, branch = case
    lake.table(f"{branch}.default.{table}").append(frame(2))
    merged = lake.merge(branch, message="the merge", author="ana")
    assert isinstance(merged, metalake.Merged)
    assert merged.outcome == "merged" and merged.commit
    assert merged.merged == [f"default.{table}"]
    assert rows(lake, table) == [(1,), (2,)]
    latest = lake.log()[0]
    assert (latest.message, latest.author, latest.seq) == ("the merge", "ana", merged.commit)


def test_a_check_says_and_changes_nothing(lake, case):
    table, branch = case
    lake.table(f"{branch}.default.{table}").append(frame(2))
    checked = lake.merge(branch, check=True)
    assert checked.outcome == "would merge" and checked.commit is None
    assert checked.merged == [f"default.{table}"]
    assert rows(lake, table) == [(1,)]


def test_nothing_to_merge_says_so(lake, case):
    _, branch = case
    merged = lake.merge(branch)
    assert merged.outcome == "nothing" and merged.commit is None


def test_a_conflict_is_a_conflict_with_its_rule_and_changes_nothing(lake, case):
    table, branch = case
    lake.table(f"{branch}.default.{table}").delete("k = 1")
    lake.table(table).overwrite(frame(7))
    for check in (True, False):
        with pytest.raises(metalake.ConflictError) as raised:
            lake.merge(branch, check=check)
        assert raised.value.rule, check
    assert rows(lake, table) == [(7,)]
    assert rows(lake, f'"{branch}".default.{table}') == []


def test_only_the_tables_it_names(lake, case):
    table, branch = case
    other = f"{table}_b"
    lake.sql(f'create table {other} (k bigint); create table "{branch}".default.{other}_c (k bigint)')
    lake.branch(f"{branch}_two")
    two = f"{branch}_two"
    lake.table(f"{two}.default.{table}").append(frame(2))
    lake.table(f"{two}.default.{other}").append(frame(3))
    merged = lake.merge(two, tables=[f"default.{table}"])
    assert merged.merged == [f"default.{table}"]
    assert rows(lake, table) == [(1,), (2,)]
    assert rows(lake, other) == []


def test_a_table_that_is_no_name_is_a_wrong_use(lake, case):
    _, branch = case
    with pytest.raises(metalake.UsageError):
        lake.merge(branch, tables=["no_namespace"])


def test_a_commit_across_a_selection_needs_split(lake, case):
    table, branch = case
    other = f"{table}_b"
    lake.sql(f"create table {other} (k bigint)")
    lake.branch(f"{branch}_mid")
    middle = f"{branch}_mid"
    lake.branch(f"{branch}_sub", start=middle)
    below = f"{branch}_sub"
    lake.table(f"{below}.default.{table}").append(frame(2))
    lake.table(f"{below}.default.{other}").append(frame(3))
    lake.merge(below)

    with pytest.raises(metalake.ConflictError):
        lake.merge(middle, tables=[f"default.{table}"])
    assert rows(lake, table) == [(1,)]

    merged = lake.merge(middle, tables=[f"default.{table}"], split=True)
    assert merged.outcome == "merged"
    assert rows(lake, table) == [(1,), (2,)]
    assert rows(lake, other) == []


def test_deletes_on_both_sides_join(lake):
    lake.sql("create table both_sides (k bigint)")
    lake.table("both_sides").append(pl.DataFrame({"k": range(100_000)}))
    lake.branch("both_dev")
    lake.table("both_dev.default.both_sides").delete("k < 5000")
    lake.table("both_sides").delete("k >= 95000")
    assert lake.merge("both_dev").outcome == "merged"
    assert pl.DataFrame(lake.sql("select count(*) as n from both_sides"))["n"][0] == 90_000


def test_a_new_table_shows_after_a_merge(lake, case):
    table, branch = case
    lake.sql(f'create table "{branch}".default.{table}_new (k bigint)')
    lake.merge(branch)
    names = pl.DataFrame(lake.sql("select table_name from information_schema.tables"))["table_name"]
    assert f"{table}_new" in names.to_list()


def test_a_branch_that_is_not_there_says_so(lake):
    with pytest.raises(metalake.MetaLakeError, match="nobody"):
        lake.merge("nobody")


def test_a_branch_with_no_parent_says_so(lake):
    with pytest.raises(metalake.MetaLakeError):
        lake.merge("main")
