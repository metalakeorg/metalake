"""What a lake keeps: a policy declared once, which `maintain` acts on."""

import datetime

import pytest

import metalake

pl = pytest.importorskip("polars")

ROWS = 200_000


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("retain")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.branch("kept")
    return made


def stored(directory):
    return set((directory / "lake" / "data").rglob("*.parquet"))


def maintain(directory):
    assert metalake._cli(["metalake", "--lake", str(directory), "maintain"]) == 0


def big(start):
    return pl.DataFrame({"k": range(start, start + ROWS)})


def test_a_ref_says_what_it_keeps(lake):
    assert lake.retain("kept", max_age="30d", min_snapshots=10) is None
    lake.retain("kept", max_age=datetime.timedelta(days=30))
    lake.retain("kept", min_seq=1)


def test_a_ref_must_say_something(lake):
    with pytest.raises(metalake.UsageError, match="what to keep"):
        lake.retain("kept")


def test_an_age_that_is_no_age_is_a_wrong_use(lake):
    with pytest.raises(metalake.UsageError, match="a number and"):
        lake.retain("kept", max_age="soon")
    with pytest.raises(metalake.UsageError, match="a number and"):
        lake.grace("é")
    with pytest.raises(TypeError):
        lake.retain("kept", max_age=3)


def test_a_ref_that_is_not_there_says_so(lake):
    with pytest.raises(metalake.MetaLakeError, match="no ref nobody"):
        lake.retain("nobody", max_age="1d")


def test_with_no_policy_nothing_goes(lake, directory):
    lake.sql("create table nothing_goes (k bigint)")
    t = lake.table("nothing_goes")
    t.append(big(0))
    t.overwrite(big(ROWS))
    before = stored(directory)
    lake.grace("0s")
    maintain(directory)
    assert stored(directory) >= before


def test_what_a_policy_lets_go_goes_from_the_store(lake, directory):
    lake.sql("create table let_go (k bigint)")
    t = lake.table("let_go")
    t.append(big(0))
    old = stored(directory)
    point = t.overwrite(big(ROWS)).commit
    new = stored(directory) - old
    assert new

    lake.grace("0s")
    lake.retain("main", min_seq=point, min_snapshots=1)
    lake.retain("kept", min_seq=point, min_snapshots=1)
    maintain(directory)

    after = stored(directory)
    assert new <= after, "what is kept stays"
    assert len(after) < len(old | new), "what was let go is gone"
    assert pl.DataFrame(lake.sql("select count(*) as n from let_go"))["n"][0] == ROWS
