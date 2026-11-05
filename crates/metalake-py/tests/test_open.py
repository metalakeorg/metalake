"""Opening a lake, as a program does."""

import os
import uuid

import pytest

import metalake


@pytest.fixture(scope="module")
def made(tmp_path_factory):
    """A lake of its own, made once, as `metalake.init("./lake")` makes it: the file here, the lake in `lake`."""
    directory = tmp_path_factory.mktemp("made")
    here = os.getcwd()
    os.chdir(directory)
    try:
        metalake.init("./lake")
    finally:
        os.chdir(here)
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


def test_a_lake_is_named_by_the_directory_of_its_file(made):
    assert "lake" in repr(metalake.connect(made))


def test_a_lake_is_named_by_its_own_directory(made):
    assert isinstance(metalake.connect(made / "lake"), metalake.Lake)


def test_naming_none_finds_the_file_from_below(made, monkeypatch):
    monkeypatch.delenv("METALAKE_POSTGRES", raising=False)
    below = made / "a" / "b"
    below.mkdir(parents=True)
    monkeypatch.chdir(below)
    assert isinstance(metalake.connect(), metalake.Lake)


def test_a_lake_goes_on_after_the_directory_changes(made, monkeypatch):
    monkeypatch.chdir(made)
    lake = metalake.connect("./lake")
    monkeypatch.chdir(made.parent)
    assert "lake" in repr(lake)
    assert isinstance(metalake.connect(made / "lake"), metalake.Lake)


def test_nothing_there_says_so(tmp_path):
    with pytest.raises(metalake.MetaLakeError, match="neither") as raised:
        metalake.connect(tmp_path)
    assert raised.value.retryable is False and raised.value.rule is None


def test_no_postgres_there_is_a_failure():
    with pytest.raises(metalake.MetaLakeError):
        metalake.connect("postgresql://metalake@127.0.0.1:1/none")


def test_a_password_does_not_go_in_a_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(metalake.UsageError, match="PGPASSWORD") as raised:
        metalake.init("postgresql://u:secret@host/db", "s3://bucket/x")
    assert isinstance(raised.value, ValueError)
    assert not (tmp_path / "metalake.toml").exists()


def test_the_file_goes_beside_the_lake_wherever_the_program_runs(tmp_path, monkeypatch):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    target = tmp_path / "there" / "lake"
    try:
        metalake.init(target)
        assert (target.parent / "metalake.toml").is_file()
        assert not (elsewhere / "metalake.toml").exists()
        assert isinstance(metalake.connect(target.parent), metalake.Lake)
        assert isinstance(metalake.connect(target), metalake.Lake)
    finally:
        metalake._cli(["metalake", "--lake", str(target.parent), "stop"])


def test_a_lake_is_not_made_twice(made, monkeypatch):
    monkeypatch.chdir(made)
    with pytest.raises(metalake.MetaLakeError, match="already exists"):
        metalake.init("./lake")
    assert isinstance(metalake.connect(made), metalake.Lake)


@pytest.mark.skipif("DATABASE_URL" not in os.environ, reason="needs a Postgres")
def test_a_postgres_that_exists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    url = os.environ["DATABASE_URL"]
    schema = f"py_{uuid.uuid4().hex[:8]}"
    metalake.init(url, tmp_path.as_uri(), schema=schema)
    lake = metalake.connect(url, schema=schema)
    assert "password" not in repr(lake)


def test_a_failure_of_each_kind_is_its_own_class():
    assert issubclass(metalake.ConflictError, metalake.MetaLakeError)
    assert issubclass(metalake.UnknownOutcomeError, metalake.MetaLakeError)
    assert issubclass(metalake.UsageError, ValueError)
