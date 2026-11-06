"""A result as the DataFrame of each library, and what is raised when that goes wrong."""

import _thread
import datetime
import decimal
import sys
import threading

import pytest

import metalake

LONG = "select count(*) from generate_series(1, 400000) a, generate_series(1, 400000) b"
COLUMNS = "b boolean, i int, l bigint, f float, d double, m decimal(10, 2), s varchar, day date, at timestamp"
FIRST = "(true, 1, 2, 1.5, 2.5, 3.25, 'a', date '2026-11-05', timestamp '2026-11-05 10:30:00')"
NULLS = "(null, null, null, null, null, null, null, null, null)"


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("convert")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    made = metalake.init(directory / "lake")
    made.sql(
        f"create table kinds ({COLUMNS}); create table empty ({COLUMNS});"
        f"insert into kinds values {FIRST}, {NULLS}"
    )
    return made


def test_polars_keeps_every_kind(lake):
    pl = pytest.importorskip("polars")
    frame = lake.sql("select * from kinds order by l").to_polars()
    assert isinstance(frame, pl.DataFrame)
    assert frame.schema["b"] == pl.Boolean and frame.schema["i"] == pl.Int32 and frame.schema["l"] == pl.Int64
    assert frame.schema["f"] == pl.Float32 and frame.schema["d"] == pl.Float64
    assert frame.schema["m"] == pl.Decimal(10, 2)
    assert frame.schema["s"] == pl.String and frame.schema["day"] == pl.Date
    assert isinstance(frame.schema["at"], pl.Datetime)
    assert frame.row(0) == (
        True, 1, 2, 1.5, 2.5, decimal.Decimal("3.25"), "a",
        datetime.date(2026, 11, 5), datetime.datetime(2026, 11, 5, 10, 30),
    )
    assert frame.row(1) == (None,) * 9


def test_arrow_keeps_every_kind(lake):
    pyarrow = pytest.importorskip("pyarrow")
    table = lake.sql("select * from kinds order by l").to_arrow()
    assert isinstance(table, pyarrow.Table)
    assert table.schema.field("i").type == pyarrow.int32()
    assert table.schema.field("m").type == pyarrow.decimal128(10, 2)
    assert table.schema.field("day").type == pyarrow.date32()
    assert table.column("s").to_pylist() == ["a", None]
    assert table.column("m").to_pylist() == [decimal.Decimal("3.25"), None]


def test_pandas_has_the_rows(lake):
    pandas = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    frame = lake.sql("select l, s from kinds order by l").to_pandas()
    assert isinstance(frame, pandas.DataFrame)
    assert list(frame.columns) == ["l", "s"]
    assert frame["l"].iloc[0] == 2 and frame["s"].iloc[0] == "a"
    assert pandas.isna(frame["l"].iloc[1]) and pandas.isna(frame["s"].iloc[1])


def test_an_empty_result_keeps_its_columns(lake):
    pl = pytest.importorskip("polars")
    frame = lake.sql("select * from empty").to_polars()
    assert frame.shape == (0, 9) and frame.schema["l"] == pl.Int64
    pyarrow = pytest.importorskip("pyarrow")
    table = lake.sql("select * from empty").to_arrow()
    assert table.num_rows == 0 and table.schema.field("l").type == pyarrow.int64()


@pytest.mark.parametrize("method", ["to_polars", "to_arrow", "to_pandas"])
def test_a_failure_in_the_stream_is_ours(lake, method):
    pytest.importorskip({"to_polars": "polars", "to_arrow": "pyarrow", "to_pandas": "pandas"}[method])
    pytest.importorskip("pyarrow" if method != "to_polars" else "polars")
    with pytest.raises(metalake.MetaLakeError, match="(?i)divide"):
        getattr(lake.sql("select 1 / (l - l) from kinds where l is not null"), method)()


def test_an_interrupt_is_an_interrupt_in_polars(lake, interrupt_polars):
    with interrupt_polars(), pytest.raises(KeyboardInterrupt):
        lake.sql(LONG).to_polars()


def test_an_interrupt_is_an_interrupt_in_arrow(lake):
    pytest.importorskip("pyarrow")
    timer = threading.Timer(1, _thread.interrupt_main)
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        lake.sql(LONG).to_arrow()
    timer.join()


@pytest.mark.parametrize(
    ("method", "missing", "extra"),
    [
        ("to_polars", "polars", "polars"),
        ("to_arrow", "pyarrow", "pyarrow"),
        ("to_pandas", "pandas", "pandas"),
        ("to_pandas", "pyarrow", "pandas"),
    ],
)
def test_a_library_that_is_missing_says_what_to_install(lake, monkeypatch, method, missing, extra):
    if missing != "pandas":
        pytest.importorskip("pandas" if method == "to_pandas" else missing)
    monkeypatch.setitem(sys.modules, missing, None)
    with pytest.raises(ImportError, match=rf"pip install metalake\[{extra}\]"):
        getattr(lake.sql("select 1"), method)()


def test_a_polars_that_would_answer_an_empty_frame_is_refused(lake, monkeypatch):
    pl = pytest.importorskip("polars")
    monkeypatch.setattr(pl, "__version__", "1.21.0")
    with pytest.raises(ImportError, match="1.22"):
        lake.sql("select 1").to_polars()


def test_importing_loads_no_library():
    code = "import sys, metalake; print([m for m in ('polars', 'pyarrow', 'pandas') if m in sys.modules])"
    import subprocess

    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert done.stdout.strip() == "[]"


def test_a_file_gone_says_so(lake, directory):
    pl = pytest.importorskip("polars")
    lake.sql("create table big (k bigint); insert into big select value from generate_series(1, 200000)")
    result = lake.sql("select k from big")
    assert result.to_polars().height == 200000
    files = list((directory / "lake" / "data").rglob("*.parquet"))
    assert files
    for file in files:
        file.unlink()
    with pytest.raises(metalake.MetaLakeError, match="(?i)not found|no such|cannot find") as raised:
        result.to_polars()
    assert raised.value.args[0]
    assert pl is not None
