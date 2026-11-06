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
def test_an_exit_code_and_an_exception_say_one_thing(lake, directory, args, code, call, exception):
    assert command(directory, *args) == code
    with pytest.raises(metalake.MetaLakeError) as raised:
        call(lake)
    assert type(raised.value) is exception


def test_every_failure_is_a_metalake_error():
    for kind in (metalake.UsageError, metalake.ConflictError, metalake.UnknownOutcomeError):
        assert issubclass(kind, metalake.MetaLakeError)
    assert issubclass(metalake.UsageError, ValueError)


def test_a_conflict_says_its_rule_and_changes_nothing(lake):
    with pytest.raises(metalake.ConflictError) as raised:
        lake.merge("dev")
    assert raised.value.rule
    assert str(raised.value)


def test_a_write_whose_fate_is_not_known_says_so(tmp_path):
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
def test_an_error_crosses_a_process_whole(error):
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
def test_every_method_says_what_it_does(owner):
    undocumented = [name for name, member in public(owner) if not (member.__doc__ or "").strip()]
    assert not undocumented


def test_every_function_says_what_it_does():
    for function in (metalake.connect, metalake.init, metalake.migrate, metalake.stop):
        assert (function.__doc__ or "").strip(), function.__name__


def test_the_public_names_are_there():
    assert all(hasattr(metalake, name) for name in metalake.__all__)
