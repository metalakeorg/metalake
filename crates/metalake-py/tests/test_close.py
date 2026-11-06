"""Closing a `Lake`, and how a result shows itself."""

import pytest

import metalake

pl = pytest.importorskip("polars")


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("close")
    made = metalake.init(directory / "lake")
    made.sql("create table t (k bigint); insert into t values (1)")
    made.close()
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


def test_with_gives_the_lake_and_closes_it(directory):
    with metalake.connect(directory) as lake:
        assert isinstance(lake, metalake.Lake)
        assert lake.status().schema == "metalake"
    with pytest.raises(metalake.UsageError, match="closed Lake"):
        lake.status()


def test_a_failure_inside_still_closes_and_still_rises(directory):
    with pytest.raises(ZeroDivisionError):
        with metalake.connect(directory) as lake:
            1 / 0
    with pytest.raises(metalake.UsageError, match="closed Lake"):
        lake.refs()


def test_what_a_lake_gave_out_goes_with_it(directory):
    lake = metalake.connect(directory)
    table = lake.table("t")
    result = lake.sql("select k from t")
    reading = iter(lake.sql("select k from t"))
    lake.close()
    with pytest.raises(metalake.UsageError, match="closed Lake"):
        table.append(pl.DataFrame({"k": [2]}))
    with pytest.raises(metalake.UsageError, match="closed Lake"):
        table.columns
    with pytest.raises(metalake.UsageError, match="closed Lake"):
        result.to_polars()
    with pytest.raises(metalake.UsageError, match="closed Lake"):
        iter(result)
    with pytest.raises(metalake.UsageError, match="closed Lake"):
        next(reading)


def test_closing_twice_does_nothing(directory):
    lake = metalake.connect(directory)
    lake.close()
    lake.close()


def test_a_closed_lake_cannot_be_entered(directory):
    lake = metalake.connect(directory)
    lake.close()
    with pytest.raises(metalake.UsageError, match="closed Lake"):
        with lake:
            pass


def test_closing_leaves_the_server_up_and_others_on_it(directory):
    other = metalake.connect(directory)
    with metalake.connect(directory):
        pass
    assert other.status().schema == "metalake"
    assert (directory / "lake" / "postgres" / "postmaster.pid").exists()
    other.close()


def test_closing_gives_the_connections_back(directory):
    # More than a server takes: kept open, these would run it out.
    kept = []
    for _ in range(150):
        with metalake.connect(directory) as lake:
            lake.status()
        kept.append(lake)
    assert len(kept) == 150


def test_a_result_shows_its_columns_and_does_not_run(directory):
    with metalake.connect(directory) as lake:
        long = "select a.value as x from generate_series(1, 400000) a, generate_series(1, 400000) b"
        assert repr(lake.sql(long)) == 'Result(columns=["x"])'
        assert repr(lake.sql("select k from t")) == 'Result(columns=["k"])'
