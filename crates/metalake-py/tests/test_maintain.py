"""The core's tasks, run once."""

import pytest

import metalake

pl = pytest.importorskip("polars")


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("maintain")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql("create table small (k bigint)")
    return made


def stored(directory):
    return set((directory / "lake" / "data").rglob("*.parquet"))


def big(start=0):
    return pl.DataFrame({"k": range(start, start + 100_000)})


def test_task_order(lake):
    done = lake.maintain()
    assert all(isinstance(d, metalake.Done) for d in done)
    tasks = [d.task for d in done]
    assert list(dict.fromkeys(tasks)) == ["times", "flush", "compact", "footers", "expire", "sweep", "orphans"]
    flush = next(d for d in done if d.task == "flush")
    assert flush.on == "main" and flush.table == "default.small"
    assert "flush" in repr(flush)


def test_strays_requires_table(lake):
    with pytest.raises(metalake.UsageError, match="name the table"):
        lake.maintain("strays")
    done = lake.maintain("strays", table="default.small")
    assert [(d.task, d.table, d.result) for d in done] == [("strays", "default.small", 0)]
    assert "strays" not in [d.task for d in lake.maintain()]


def test_one_task(lake):
    assert [d.task for d in lake.maintain("times")] == ["times"]


def test_unknown_task_raises(lake):
    with pytest.raises(metalake.UsageError, match="compact"):
        lake.maintain("clean")


def test_compact_ref_and_table_filter(lake):
    lake.sql("create table other (k bigint)")
    done = lake.maintain("compact", table="default.small")
    assert {d.table for d in done} == {"default.small"}
    lake.branch("narrow")
    assert {d.on for d in lake.maintain("compact", on="narrow")} == {"narrow"}


def test_compact_table_on_some_branches(lake):
    lake.branch("before_it")
    lake.sql("create table later (k bigint)")
    assert {d.on for d in lake.maintain("compact", table="default.later")} == {"main"}
    with pytest.raises(metalake.MetaLakeError, match="later"):
        lake.maintain("compact", table="default.later", on="before_it")
    with pytest.raises(metalake.MetaLakeError, match="no branch has the table default.nowhere"):
        lake.maintain("compact", table="default.nowhere")


def test_filter_limited_to_rewrites(lake):
    with pytest.raises(metalake.UsageError, match="flush, compact, footers and strays"):
        lake.maintain("expire", table="default.small")
    with pytest.raises(metalake.UsageError, match="flush, compact, footers and strays"):
        lake.maintain(on="main")


def test_missing_branch_raises(lake):
    with pytest.raises(metalake.MetaLakeError, match="no branch nobody"):
        lake.maintain("compact", on="nobody")


def test_compaction_preserves_rows(lake, directory):
    lake.sql("create table joined (k bigint)")
    t = lake.table("joined")
    t.append(big())
    t.append(big(100_000))
    done = lake.maintain("compact", table="default.joined")
    assert done[0].result, "it wrote a commit"
    assert lake.log()[0].operation == "replace"
    frame = lake.sql("select count(*) as n, sum(k) as s from joined").to_polars()
    assert frame.row(0) == (200_000, 199_999 * 200_000 // 2)


def test_maintain_is_idempotent(lake):
    first = lake.maintain()
    assert lake.maintain()
    assert first


def test_failure_reports_completed_tasks(tmp_path):
    lake = metalake.init(tmp_path / "lake")
    try:
        lake.sql("create table t (k bigint)")
        lake.table("t").append(big())
        lake.table("t").delete("k = 1")
        for file in (tmp_path / "lake" / "data").rglob("*.parquet"):
            file.unlink()
        with pytest.raises(metalake.MetaLakeError) as raised:
            lake.maintain()
        assert [d.task for d in raised.value.done][:2] == ["times", "flush"]
        assert "expire" not in [d.task for d in raised.value.done]
    finally:
        metalake._cli(["metalake", "--lake", str(tmp_path), "stop"])


def test_failure_without_completed_tasks(lake):
    with pytest.raises(metalake.MetaLakeError) as raised:
        lake.sql("select * from nowhere")
    assert raised.value.done == []
