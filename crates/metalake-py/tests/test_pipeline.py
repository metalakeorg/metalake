"""A continuous pipeline from Python: what is refused before anything runs, in the methods and in SQL. A pipeline that
runs needs Arroyo; that is the end-to-end workflow's, through the client every face shares."""

import pytest

import metalake


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("pipeline")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    lake = metalake.init(directory / "lake")
    lake.sql("create schema shop")
    lake.sql("create table shop.orders (customer varchar, amount double)")
    lake.sql("create table shop.totals (customer varchar, amount double)")
    return lake


QUERY = "INSERT INTO shop.totals SELECT customer, sum(amount) FROM shop.orders GROUP BY customer"


@pytest.mark.parametrize(
    ("name", "query", "says"),
    [
        ("Totals", QUERY, "not a pipeline's name"),
        ("totals", "SELECT * FROM shop.orders", "INSERT INTO"),
        ("totals", "INSERT INTO shop.orders SELECT * FROM shop.orders", "feed itself"),
        ("totals", "INSERT INTO shop.missing SELECT * FROM shop.orders", "missing"),
    ],
)
def test_refused_before_it_runs(lake, name, query, says):
    with pytest.raises(metalake.MetaLakeError, match=says):
        lake.create_pipeline(name, query)


def test_a_written_table_needs_emptying(lake):
    lake.sql("insert into shop.totals values ('ana', 1)")
    with pytest.raises(metalake.UsageError, match="empty_target"):
        lake.create_pipeline("totals", QUERY)
    with pytest.raises(metalake.UsageError, match="empty_target"):
        lake.sql(f"create pipeline totals as {QUERY}")
    with pytest.raises(metalake.UsageError, match="parallelism"):
        lake.create_pipeline("totals", QUERY, parallelism=0)


def test_none_yet(lake):
    assert lake.pipelines() == []
    with pytest.raises(metalake.UsageError, match="no pipeline"):
        lake.pipeline("totals")
    shown = lake.sql("show pipelines")
    assert shown.columns[:4] == ["name", "state", "reads", "writes"]
    assert sum(batch.num_rows for batch in shown) == 0
    lake.sql("drop pipeline if exists totals")
    with pytest.raises(metalake.UsageError, match="no pipeline"):
        lake.sql("alter pipeline totals suspend")
