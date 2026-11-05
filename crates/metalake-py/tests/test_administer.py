"""What whoever administers a lake does, with no `Lake` to do it on."""

import os
import uuid

import pytest

import metalake

pl = pytest.importorskip("polars")


@pytest.fixture
def made(tmp_path):
    lake = metalake.init(tmp_path / "lake")
    lake.sql("create table t (k bigint); insert into t values (1)")
    yield tmp_path, lake
    metalake._cli(["metalake", "--lake", str(tmp_path), "stop"])


def count(lake):
    return pl.DataFrame(lake.sql("select count(*) as n from t"))["n"][0]


def test_a_catalog_up_to_date_is_left_as_it_is(made):
    directory, lake = made
    before = lake.log()[0].seq
    migrated = metalake.migrate(directory / "lake")
    assert isinstance(migrated, metalake.Lake)
    assert migrated.log()[0].seq == before
    assert count(migrated) == 1


def test_a_lake_is_named_as_connect_names_it(made, monkeypatch):
    directory, _ = made
    assert isinstance(metalake.migrate(directory), metalake.Lake)
    monkeypatch.delenv("METALAKE_POSTGRES", raising=False)
    monkeypatch.chdir(directory)
    assert isinstance(metalake.migrate(), metalake.Lake)


def test_stop_stops_and_connect_brings_it_back(made):
    directory, lake = made
    stopped = metalake.stop(directory / "lake")
    assert str(stopped).endswith("lake")
    assert (directory / "lake" / "postgres").is_dir()
    assert not (directory / "lake" / "postgres" / "postmaster.pid").exists()
    assert count(metalake.connect(directory)) == 1


def test_stop_is_safe_to_repeat(made):
    directory, _ = made
    metalake.stop(directory)
    metalake.stop(directory)


def test_a_lake_open_when_its_server_stops_says_what_to_do(made):
    directory, lake = made
    metalake.stop(directory)
    with pytest.raises(metalake.MetaLakeError, match="`connect` again"):
        count(lake)
    assert count(metalake.connect(directory)) == 1


def test_nothing_there_says_so(tmp_path):
    with pytest.raises(metalake.MetaLakeError, match="neither"):
        metalake.stop(tmp_path)
    with pytest.raises(metalake.MetaLakeError, match="neither"):
        metalake.migrate(tmp_path)


@pytest.mark.skipif("DATABASE_URL" not in os.environ, reason="needs a Postgres")
def test_a_postgres_that_exists_has_no_server_to_stop(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    url = os.environ["DATABASE_URL"]
    schema = f"py_{uuid.uuid4().hex[:8]}"
    metalake.init(url, tmp_path.as_uri(), schema=schema)
    assert isinstance(metalake.migrate(url, schema=schema), metalake.Lake)
    with pytest.raises(metalake.MetaLakeError, match="no Postgres of its own"):
        metalake.stop(url)


def test_a_postgres_with_no_server_of_its_own_says_so():
    with pytest.raises(metalake.MetaLakeError, match="no Postgres of its own"):
        metalake.stop("postgresql://metalake@127.0.0.1:1/none")
