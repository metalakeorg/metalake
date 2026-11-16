"""The life of a ref. A branch or a tag covers the whole lake and copies nothing."""

import pytest

import metalake

pl = pytest.importorskip("polars")


def keys(lake, table):
    return sorted(row[0] for row in pl.DataFrame(lake.sql(f"select k from {table}")).rows())


def listed(lake):
    """The catalogs the session's ref holds: the lake's, not its refs (I7)."""
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


def test_create_branch(lake, on_ref):
    made = lake.branch("own")
    assert isinstance(made, metalake.Ref)
    assert (made.name, made.kind, made.parent) == ("own", "branch", "main")
    with on_ref(lake, "own"):
        lake.table("t").append(pl.DataFrame({"k": [2]}))
        assert keys(lake, "t") == [1, 2]
    assert keys(lake, "t") == [1]


def test_create_branch_from_ref_or_sequence(lake, on_ref):
    before = lake.log()[0].seq
    lake.table("t").append(pl.DataFrame({"k": [3]}))
    lake.branch("from_point", start=f"main@{before}")
    with on_ref(lake, "from_point"):
        assert 3 not in keys(lake, "t")
    lake.branch("from_ref", start="from_point")
    assert {r.name: r.parent for r in lake.refs()}["from_ref"] == "from_point"


def test_create_branch_from_time_raises(lake):
    with pytest.raises(metalake.UsageError, match="sequence"):
        lake.branch("at_time", start="main@2026-11-05")


def test_tag_raises(lake, on_ref):
    made = lake.tag("frozen")
    assert made.kind == "tag"
    with on_ref(lake, "frozen"):
        with pytest.raises(metalake.MetaLakeError):
            lake.table("t").append(pl.DataFrame({"k": [9]}))
        assert 9 not in keys(lake, "t")


def test_new_ref_in_information_schema(lake, on_ref):
    """A new ref holds the lake's catalogs at once; it is not one of them (I7)."""
    lake.branch("shown")
    assert "shown" not in listed(lake)
    with on_ref(lake, "shown"):
        assert "default" in listed(lake)


def test_delete_branch_name_not_reused(lake):
    lake.branch("gone")
    lake.delete_branch("gone")
    assert "gone" not in {r.name for r in lake.refs()}
    with pytest.raises(metalake.MetaLakeError):
        lake.branch("gone")


def test_delete_tag(lake):
    lake.tag("gone_tag")
    lake.delete_tag("gone_tag")
    assert "gone_tag" not in {r.name for r in lake.refs()}


def test_delete_ref_kind_mismatch_raises(lake):
    lake.tag("a_tag")
    lake.branch("a_branch")
    with pytest.raises(metalake.MetaLakeError):
        lake.delete_branch("a_tag")
    with pytest.raises(metalake.MetaLakeError):
        lake.delete_tag("a_branch")
    assert {"a_tag", "a_branch"} <= {r.name for r in lake.refs()}


def test_create_branch_invalid_name_or_start(lake):
    with pytest.raises(metalake.MetaLakeError):
        lake.branch("main")
    with pytest.raises(metalake.MetaLakeError, match="no ref nobody"):
        lake.branch("orphan", start="nobody")


def test_on_ref_names_without_ref(lake):
    """`lake.on`: every name reads and writes on it, as no name says its ref (I7); a table says its ref by `ref`."""
    # The module's lake is shared: `main` holds what the tests before this one wrote.
    main = keys(lake, "t")
    lake.branch("worked")
    assert lake.ref == "main"
    lake.on("worked")
    assert lake.ref == "worked"
    assert 'ref="worked"' in repr(lake)
    lake.table("t").append(pl.DataFrame({"k": [7]}))
    lake.sql("insert into t values (8)")
    assert keys(lake, "t") == sorted([*main, 7, 8])
    assert (lake.table("t").name, lake.table("t").ref) == ("default.default.t", "worked")
    # `main.default.t` is the catalog `main` now, not the ref: there is none.
    with pytest.raises(metalake.MetaLakeError):
        keys(lake, "main.default.t")
    assert lake.log()[0].seq > lake.log("main")[0].seq
    # `table_changes` names its table on the session's ref too: the engine does not follow a branch's changes yet, and
    # says so of `worked`, the session's, rather than reading `main`'s.
    with pytest.raises(metalake.MetaLakeError, match="following worked"):
        lake.sql("select k from table_changes('t', 0)")
    lake.on("main")
    assert keys(lake, "t") == main
    assert (lake.table("t").name, lake.table("t").ref) == ("default.default.t", "main")


def test_on_tag_reads_as_tagged(lake):
    lake.tag("then")
    lake.sql("insert into t values (9)")
    lake.on("then")
    assert 9 not in keys(lake, "t")
    with pytest.raises(metalake.MetaLakeError):
        lake.sql("insert into t values (10)")
    lake.on("main")


def test_on_missing_ref_refused(lake):
    with pytest.raises(metalake.UsageError, match="no ref nowhere"):
        lake.on("nowhere")
    assert lake.ref == "main"
