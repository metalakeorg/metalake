"""A table's changes and a stream into it, from Python: a range of commits as a result, a follower that goes on after
the position a reader kept, and a writer that commits on its cadence and, with a key, takes each version once."""

import _thread
import datetime
import threading
import time

import pytest

import metalake

pl = pytest.importorskip("polars")

# A commit reaches a follower once it is a second old (CHG-1).
SETTLE = 1.2


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("streaming")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    lake = metalake.init(directory / "lake")
    lake.sql("create schema shop")
    return lake


@pytest.fixture
def table(lake, request):
    name = f"shop.{request.node.name.lower()}"
    lake.sql(f"create table {name} (k bigint not null, s varchar)")
    return lake.table(name)


def append(table, keys):
    """Appends the keys, and answers the commit."""
    rows = pl.DataFrame({"k": keys, "s": [str(k) for k in keys]})
    return table.append(rows).commit


def changes(result):
    """Each row: (change, key, commit), sorted."""
    frame = pl.DataFrame(result)
    return sorted(zip(frame["_change_type"], frame["k"], frame["_commit_seq"]))


def test_changes_range_chains(table):
    one = append(table, [1, 2])
    two = append(table, [3])
    three = table.delete("k = 1").commit
    time.sleep(SETTLE)

    assert changes(table.changes(one, two)) == [("insert", 3, two)]
    assert changes(table.changes(two)) == [("delete", 1, three)]
    chained = sorted(changes(table.changes(one, two)) + changes(table.changes(two, three)))
    assert changes(table.changes(one, three)) == chained
    result = table.changes(one)
    assert changes(result) == changes(result), "a result read twice gives the same rows"
    with pytest.raises(metalake.UsageError):
        table.changes(two, one)


def test_follow_goes_on_after_position(table):
    append(table, [1, 2])
    follower = table.follow()
    read = next(follower)
    assert sorted(pl.DataFrame(read.rows)["k"]) == [1, 2]
    assert read.position is not None and follower.position == read.position

    later = append(table, [3])
    step = next(follower)
    assert changes(step.rows) == [("insert", 3, later)]

    # A reader that kept a position goes on after it, with nothing before it.
    again = append(table, [4])
    resumed = table.follow(after=step.position)
    assert changes(next(resumed).rows) == [("insert", 4, again)]

    latest = table.follow("latest")
    first = next(latest)
    assert len(pl.DataFrame(first.rows)) == 0, "from now: nothing that was before"


def test_follow_since_time(table):
    append(table, [1])
    time.sleep(SETTLE)
    since = datetime.datetime.now(datetime.timezone.utc)
    later = append(table, [2])
    follower = table.follow(since=since)
    rows = []
    while not rows:
        rows = changes(next(follower).rows)
    assert rows == [("insert", 2, later)]


def test_follow_refusals(table):
    with pytest.raises(metalake.UsageError):
        table.follow("everything")
    with pytest.raises(metalake.UsageError):
        table.follow(after=1, since=datetime.datetime.now(datetime.timezone.utc))
    with pytest.raises(metalake.UsageError):
        table.follow(interval=0)


def test_follow_interrupted_keeps_position(table):
    append(table, [1])
    follower = table.follow(interval=0.05)
    changes(next(follower).rows)
    kept = follower.position

    # Ctrl-C while it waits for a commit that does not come ends the wait, and the reader is where it was.
    timer = threading.Timer(0.3, _thread.interrupt_main)
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        next(follower)
    timer.join()
    assert follower.position == kept
    later = append(table, [2])
    assert changes(next(follower).rows) == [("insert", 2, later)]


def batch(keys):
    return pl.DataFrame({"k": keys, "s": [str(k) for k in keys]})


def test_writer_with_key_goes_on(lake, table):
    with table.writer(app_id="producer", commit_every=3600) as writer:
        assert writer.version is None
        for v in (1, 2, 3):
            writer.write(batch([v]), version=v)
    assert lake.key_version("producer") == 3

    # A producer that comes back asks where it was; what it gives again of before is skipped.
    with table.writer(app_id="producer", commit_every=3600) as writer:
        assert writer.version == 3
        assert writer.write(batch([3]), version=3) == []
        writer.write(batch([4]), version=4)
    assert sorted(pl.DataFrame(lake.sql(f"select k from {table.name}"))["k"]) == [1, 2, 3, 4]


def test_writer_exception_drops_what_it_holds(lake, table):
    with pytest.raises(RuntimeError):
        with table.writer(app_id="halfway", commit_every=3600) as writer:
            writer.write(batch([1]), version=1)
            raise RuntimeError("the producer fell over within version 1")
    assert lake.key_version("halfway") is None, "a version cut short is not committed"
    with pytest.raises(metalake.UsageError):
        writer.write(batch([2]), version=2)


def test_writer_upsert_and_cadence(lake, table):
    with table.writer(upsert_on=["k"], commit_every=3600) as writer:
        writer.write(batch([1, 2]))
        writer.write(pl.DataFrame({"k": [1], "s": ["one"]}))
    rows = sorted(pl.DataFrame(lake.sql(f"select k, s from {table.name}")).rows())
    assert rows == [(1, "one"), (2, "2")]

    writer = table.writer(max_bytes=1)
    assert len(writer.write(batch([3]))) == 1, "what it held reached its bytes"
    assert writer.close() is None, "nothing was left"
    assert writer.close() is None


def test_writer_reader_batch_by_batch(table):
    pa = pytest.importorskip("pyarrow")
    schema = pa.schema([("k", pa.int64()), ("s", pa.string())])
    batches = [pa.record_batch([[k], [str(k)]], schema=schema) for k in (1, 2, 3)]
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    # Each batch reaches the writer as it is read, and each commit it made is said.
    committed = table.writer(max_bytes=1).write(reader)
    assert [w.rows for w in committed] == [1, 1, 1]


def test_follow_step_read_once_counts_when_read(table):
    append(table, [1])
    follower = table.follow()
    first = next(follower)
    assert follower.position is None, "a step counts once its rows are read"
    assert sorted(pl.DataFrame(first.rows)["k"]) == [1]
    assert follower.position == first.position
    with pytest.raises(metalake.MetaLakeError):
        pl.DataFrame(first.rows)

    # A step left unread is given again.
    later = append(table, [2])
    unread = next(follower)
    assert unread.position == later
    again = next(follower)
    assert changes(again.rows) == [("insert", 2, later)]
    assert follower.position == later
