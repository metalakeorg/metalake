"""Credentials vended for a table's files on S3 (REST-11): an engine with no key of its own reads and writes a lake on an
object store with the credential its server mints, which expires and opens that table's files alone. Needs a store with
a token service, as MinIO is, named by METALAKE_TEST_S3_ENDPOINT and METALAKE_TEST_S3_BUCKET, the server's own
credential in AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, and AWS_ENDPOINT, AWS_ALLOW_HTTP, AWS_REGION for it."""

import os

import pytest

import metalake

ENDPOINT = os.environ.get("METALAKE_TEST_S3_ENDPOINT")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="no S3 store with a token service: METALAKE_TEST_S3_ENDPOINT")


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    directory = tmp_path_factory.mktemp("vended")
    yield directory
    metalake._cli(["metalake", "--lake", str(directory), "stop"])


@pytest.fixture(scope="module")
def lake(directory):
    warehouse = f"s3://{os.environ['METALAKE_TEST_S3_BUCKET']}/{directory.name}"
    made = metalake.init(directory / "lake", warehouse)
    made.sql(
        "create table t (k bigint); insert into t values (1), (2), (3); create table u (k bigint); insert into u values (9)"
    )
    return made


def vended(table):
    """What the table's FileIO was given, as PyIceberg applies a load's storage-credentials."""
    return table.io.properties


def test_pyiceberg_reads_and_writes_vended(lake):
    pytest.importorskip("pyiceberg")
    pyarrow = pytest.importorskip("pyarrow")
    table = lake.iceberg.pyiceberg().load_table("default.t")
    given = vended(table)
    assert given.get("s3.session-token"), "a credential that expires"
    assert given["s3.access-key-id"] != os.environ["AWS_ACCESS_KEY_ID"], "not the server's own"
    assert int(given["s3.session-token-expires-at-ms"]) > 0
    assert sorted(table.scan().to_arrow()["k"].to_pylist()) == [1, 2, 3]

    before = lake.log(limit=1)[0].seq
    table.append(pyarrow.table({"k": pyarrow.array([4], pyarrow.int64())}))
    assert lake.log(limit=1)[0].seq > before, "a commit of the lake"
    assert sum(batch.num_rows for batch in lake.sql("select k from t")) == 4


def test_vended_narrowed_to_table(lake):
    pytest.importorskip("pyiceberg")
    fs = pytest.importorskip("pyarrow.fs")
    catalog = lake.iceberg.pyiceberg()
    given = vended(catalog.load_table("default.t"))
    store = fs.S3FileSystem(
        access_key=given["s3.access-key-id"],
        secret_key=given["s3.secret-access-key"],
        session_token=given["s3.session-token"],
        endpoint_override=given["s3.endpoint"],
        region=given["s3.region"],
    )
    path = lambda location: location.split("://", 1)[1]  # noqa: E731
    own = path(catalog.load_table("default.t").metadata.location)
    other = path(catalog.load_table("default.u").metadata.location)
    files = store.get_file_info(fs.FileSelector(own, recursive=True))
    assert any(f.path.endswith(".parquet") for f in files), "its own table's files"
    with pytest.raises(OSError):
        store.get_file_info(fs.FileSelector(other, recursive=True))


def test_vended_refused_without_token(lake):
    catalog_module = pytest.importorskip("pyiceberg.catalog")
    # The lake's server, reached with no token: asking for credentials, as PyIceberg does, is refused.
    catalog = catalog_module.load_catalog("bare", type="rest", uri=lake.iceberg.uri)
    with pytest.raises(Exception, match="token"):
        catalog.load_table("default.t")
