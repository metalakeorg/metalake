"""The contract of failure: one exception for one exit code, and what each says."""

import inspect
import pickle

import pytest

import metalake

pl = pytest.importorskip("polars")


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("errors")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql("create table t (k bigint); insert into t values (1)")
    made.branch("dev")
    made.table("dev.default.t").delete("k = 1")
    made.table("t").overwrite(pl.DataFrame({"k": [7]}))
    return made


def command(directory, *args):
    return metalake._cli(["metalake", "--lake", str(directory), *args])


@pytest.mark.parametrize(
    ("args", "code", "call", "exception"),
    [
        (["merge", "dev", "--check"], 3, lambda lake: lake.merge("dev", check=True), metalake.ConflictError),
        (["merge", "dev"], 3, lambda lake: lake.merge("dev"), metalake.ConflictError),
        (["maintain", "expire", "--table", "default.t"], 2, lambda lake: lake.maintain("expire", table="default.t"), metalake.UsageError),
        (["log", "nobody"], 1, lambda lake: lake.log("nobody"), metalake.MetaLakeError),
        (["sql", "select * from nowhere"], 1, lambda lake: lake.sql("select * from nowhere"), metalake.MetaLakeError),
    ],
)
def test_exit_code_matches_exception(lake, directory, args, code, call, exception):
    assert command(directory, *args) == code
    with pytest.raises(metalake.MetaLakeError) as raised:
        call(lake)
    assert type(raised.value) is exception


def test_error_hierarchy():
    for kind in (metalake.UsageError, metalake.ConflictError, metalake.UnknownOutcomeError):
        assert issubclass(kind, metalake.MetaLakeError)
    assert issubclass(metalake.UsageError, ValueError)


def test_conflict_error_rule(lake):
    with pytest.raises(metalake.ConflictError) as raised:
        lake.merge("dev")
    assert raised.value.rule
    assert str(raised.value)


def test_unknown_outcome_error(tmp_path):
    # A connection lost in a commit cannot be arranged here. The pool closed under a commit takes the same way out.
    pyarrow = pytest.importorskip("pyarrow")
    lake = metalake.init(tmp_path / "lake")
    try:
        lake.sql("create table t (k bigint)")
        schema = pyarrow.schema([("k", pyarrow.int64())])

        def batches():
            yield pyarrow.record_batch([[1]], schema=schema)
            lake.close()

        with pytest.raises(metalake.UnknownOutcomeError) as raised:
            lake.table("t").append(pyarrow.RecordBatchReader.from_batches(schema, batches()))
        assert raised.value.retryable is False
    finally:
        metalake._cli(["metalake", "--lake", str(tmp_path), "stop"])


@pytest.mark.parametrize(
    "error",
    [
        metalake.MetaLakeError("it failed", "CMT-6", "40001", True),
        metalake.ConflictError("it conflicts", "DV-4", "40001", False),
        metalake.UsageError("wrong"),
    ],
)
def test_error_pickle_round_trip(error):
    again = pickle.loads(pickle.dumps(error))
    assert type(again) is type(error)
    assert (str(again), again.rule, again.code, again.retryable) == (
        str(error),
        error.rule,
        error.code,
        error.retryable,
    )


def public(owner):
    for name, member in inspect.getmembers(owner):
        if not name.startswith("_") and callable(member):
            yield f"{getattr(owner, '__name__', owner)}.{name}", member


@pytest.mark.parametrize("owner", [metalake.Lake, metalake.Table, metalake.Result])
def test_methods_documented(owner):
    undocumented = [name for name, member in public(owner) if not (member.__doc__ or "").strip()]
    assert not undocumented


def test_functions_documented():
    for function in (metalake.connect, metalake.init, metalake.migrate, metalake.stop):
        assert (function.__doc__ or "").strip(), function.__name__


def test_public_api_exports():
    assert all(hasattr(metalake, name) for name in metalake.__all__)
