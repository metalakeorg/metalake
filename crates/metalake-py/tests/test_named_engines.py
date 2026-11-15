"""Engines a lake names in its metalake.toml, from Python: named with nothing running, listed, found by name, forgotten;
what could never be asked refused before it is written; a secret never written, only its variable."""

import pytest

import metalake


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("engines")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory / "lake"), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    return metalake.init(directory / "lake")


def test_named_listed_found_and_forgotten(lake, directory):
    added = lake.add_engine(
        "trino_prod", url="https://trino.example:8443", user="ana", token_env="TRINO_TOKEN"
    )
    assert (added.name, added.type, added.url, added.user) == ("trino_prod", "trino", "https://trino.example:8443", "ana")
    assert added.auth == "token from $TRINO_TOKEN"
    assert added.catalog == "lake"

    written = (directory / "lake" / "metalake.toml").read_text()
    assert "[engines.trino_prod]" in written
    assert 'token_env = "TRINO_TOKEN"' in written

    assert [e.name for e in lake.engines()] == ["trino_prod"]
    assert lake.engine("trino_prod").url == "https://trino.example:8443"

    with pytest.raises(metalake.UsageError, match="--replace"):
        lake.add_engine("trino_prod", url="https://other:8443", user="ana")
    assert lake.add_engine("trino_prod", url="https://other:8443", user="ana", replace=True).url == "https://other:8443"

    lake.remove_engine("trino_prod")
    assert lake.engines() == []
    with pytest.raises(metalake.UsageError, match="no engine"):
        lake.engine("trino_prod")


@pytest.mark.parametrize(
    ("name", "kwargs", "says"),
    [
        ("Trino-Prod", {"url": "https://t:8443", "user": "ana"}, "lowercase"),
        ("t", {"url": "ftp://t", "user": "ana"}, "http or https"),
        ("t", {"url": "https://t:8443"}, "name the user"),
        ("t", {"url": "https://t:8443", "user": "ana", "type": "oracle"}, "the types are"),
    ],
)
def test_refused_before_it_is_written(lake, name, kwargs, says):
    with pytest.raises(metalake.UsageError, match=says):
        lake.add_engine(name, **kwargs)
    assert lake.engines() == []
