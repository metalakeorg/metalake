"""A real client against the server: what `c12_client` and `c12_read` hold it to, printed as JSON."""

import json
import sys
import urllib.request

import pyarrow
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


class LocalIO(PyArrowFileIO):
    """A local URL as a path, which on Windows PyArrow's own parsing does not survive."""

    @staticmethod
    def parse_location(location, properties=None):
        path = location.removeprefix("file://")
        return "file", "", path[1:] if path[2:3] == ":" else path


def connect(uri):
    return load_catalog("metalake", **{"type": "rest", "uri": uri, "py-io-impl": "client.LocalIO"})


def namespaces(uri):
    catalog = connect(uri)
    said = {"top": sorted(catalog.list_namespaces()), "under_db": sorted(catalog.list_namespaces("db"))}
    try:
        catalog.list_namespaces("nowhere")
        said["nowhere"] = "answered"
    except NoSuchNamespaceError as refused:
        said["nowhere"] = str(refused)
    return said


def read(uri):
    catalog = connect(uri)
    table = catalog.load_table("db.plain")
    manifests = table.current_snapshot().manifests(table.io)
    entries = [entry for manifest in manifests for entry in manifest.fetch_manifest_entry(table.io)]
    said = {
        "entries": sorted((entry.status.name, entry.sequence_number) for entry in entries),
        "current": table.current_snapshot().snapshot_id,
        "tables": sorted(catalog.list_tables("db")),
        "rows": sorted(row["k"] for row in table.scan().to_arrow().to_pylist()),
        "planned": len(list(table.scan().plan_files())),
        "pruned": len(list(table.scan(row_filter="k >= 100").plan_files())),
        "inline": table.refresh().scan().to_arrow().num_rows,
        "small": catalog.load_table("db.small").scan().to_arrow().num_rows,
        "history": len(table.metadata.snapshots),
        "metadata": json.load(urllib.request.urlopen(f"{uri}/v1/namespaces/db/tables/plain"))["metadata"],
    }
    try:
        catalog.load_table("db.nowhere")
        said["nowhere"] = "answered"
    except NoSuchTableError as refused:
        said["nowhere"] = str(refused)
    return said


def append(uri):
    table = connect(uri).load_table("db.plain")
    before = table.current_snapshot().snapshot_id
    table.append(pyarrow.Table.from_pylist([{"k": 7}, {"k": 8}], schema=table.schema().as_arrow()))
    current = table.current_snapshot()
    reread = connect(uri).load_table("db.plain")
    return {
        "before": before,
        "after": current.snapshot_id,
        "reread": reread.current_snapshot().snapshot_id,
        "rows": sorted(row["k"] for row in reread.scan().to_arrow().to_pylist()),
    }


def rows_of(table, keys):
    return pyarrow.Table.from_pylist([{"k": k} for k in keys], schema=table.schema().as_arrow())


def retry(uri):
    """PyIceberg rebases by itself on a 409, under the snapshot id it proposed."""
    catalog = connect(uri)
    session, post = catalog._session, catalog._session.post
    sent = []

    def recording(url, data=None, **kwargs):
        response = post(url, data=data, **kwargs)
        sent.append((url, data, kwargs.get("headers"), response.status_code))
        return response

    def resend(body=None):
        url, data, headers, _ = sent[-1]
        return post(url, data=body or data, headers=headers).status_code

    def proposed(data):
        return json.loads(data)["updates"][0]["snapshot"]["snapshot-id"]

    head = lambda: catalog.load_table("db.plain").current_snapshot().snapshot_id
    first, second = catalog.load_table("db.plain"), catalog.load_table("db.plain")
    first.append(rows_of(first, [10]))
    session.post = recording
    second.append(rows_of(second, [11]))
    session.post = post
    statuses = [status for _, _, _, status in sent]
    same_id = proposed(sent[0][1]) == proposed(sent[-1][1])

    at = head()
    repeated = resend()
    unchanged = head() == at

    later = catalog.load_table("db.plain")
    later.append(rows_of(later, [12]))
    moved = head()
    stale = resend()
    stale_unchanged = head() == moved

    # An unseen id on an old base: a refusal, which leaves no key, so that it refuses again.
    other = json.loads(sent[-1][1])
    other["updates"][0]["snapshot"]["snapshot-id"] += 1
    other["updates"][1]["snapshot-id"] += 1
    other = json.dumps(other)
    return {
        "statuses": statuses,
        "same_id": same_id,
        "repeated": repeated,
        "unchanged": unchanged,
        "stale": stale,
        "stale_unchanged": stale_unchanged,
        "conflicts": [resend(other), resend(other)],
        "rows": sorted(row["k"] for row in catalog.load_table("db.plain").scan().to_arrow().to_pylist()),
    }


def lifecycle(uri):
    """Everything a client changes, from an empty catalog."""
    catalog = connect(uri)
    said = {}
    keys = lambda table: sorted(row["k"] for row in table.scan().to_arrow().to_pylist())

    catalog.create_namespace("flow", {"owner": "a"})
    said["namespace_exists"] = catalog.namespace_exists("flow")
    said["namespace_missing"] = catalog.namespace_exists("nowhere")
    summary = catalog.update_namespace_properties("flow", {"owner"}, {"team": "b"})
    said["namespace_summary"] = [sorted(summary.removed), sorted(summary.updated), sorted(summary.missing)]
    said["namespace_properties"] = catalog.load_namespace_properties("flow")
    try:
        catalog.create_namespace("flow")
        said["namespace_again"] = "created"
    except NamespaceAlreadyExistsError:
        said["namespace_again"] = "refused"

    schema = Schema(NestedField(1, "k", LongType(), required=False), NestedField(2, "s", StringType(), required=False))
    table = catalog.create_table("flow.t", schema, properties={"a": "1"})
    said["table_exists"] = catalog.table_exists("flow.t")
    try:
        catalog.create_table("flow.t", schema)
        said["table_again"] = "created"
    except TableAlreadyExistsError:
        said["table_again"] = "refused"

    data = lambda rows: pyarrow.Table.from_pylist(
        [{"k": k, "s": str(k)} for k in rows], schema=table.schema().as_arrow()
    )
    table.append(data([1, 2, 3]))
    said["appended"] = keys(table)
    table.overwrite(data([7, 8]), overwrite_filter="k = 2")
    said["overwritten"] = keys(catalog.load_table("flow.t"))
    table = catalog.load_table("flow.t")
    table.delete("k = 3")
    said["deleted"] = keys(catalog.load_table("flow.t"))
    table = catalog.load_table("flow.t")
    table.overwrite(data([9]))
    said["replaced"] = keys(catalog.load_table("flow.t"))

    with catalog.load_table("flow.t").transaction() as transaction:
        transaction.set_properties(b="2")
        transaction.remove_properties("a")
    said["properties"] = {
        k: v for k, v in catalog.load_table("flow.t").properties.items() if not k.startswith("write.")
    }

    staged = catalog.create_table_transaction("flow.staged", schema)
    staged.append(pyarrow.Table.from_pylist([{"k": 5, "s": "5"}], schema=schema.as_arrow()))
    staged.commit_transaction()
    said["staged"] = keys(catalog.load_table("flow.staged"))

    catalog.rename_table("flow.staged", "flow.moved")
    said["renamed"] = sorted(name for _, name in catalog.list_tables("flow"))
    said["moved"] = keys(catalog.load_table("flow.moved"))

    body = {"table-changes": [
        {"identifier": {"namespace": ["flow"], "name": name}, "requirements": [],
         "updates": [{"action": "set-properties", "updates": {"both": "yes"}}]}
        for name in ("t", "moved")
    ]}
    request = urllib.request.Request(f"{uri}/v1/transactions/commit", json.dumps(body).encode(), method="POST")
    said["transaction"] = urllib.request.urlopen(request).status
    said["both"] = [catalog.load_table(f"flow.{n}").properties.get("both") for n in ("t", "moved")]

    try:
        catalog.drop_namespace("flow")
        said["not_empty"] = "dropped"
    except NamespaceNotEmptyError:
        said["not_empty"] = "refused"
    catalog.drop_table("flow.moved")
    catalog.purge_table("flow.t")
    said["dropped"] = catalog.table_exists("flow.t")
    catalog.drop_namespace("flow")
    said["namespace_dropped"] = catalog.namespace_exists("flow")
    return said


def wide(uri):
    catalog = connect(uri)
    table = catalog.load_table("db.wide")
    keys = lambda scan: sorted(row["k"] for row in scan.to_arrow().to_pylist())
    return {
        "main": keys(table.scan()),
        "branch": keys(table.scan(snapshot_id=table.metadata.refs["b"].snapshot_id)),
        "pruned": len(list(table.scan(row_filter="k >= 10").plan_files())),
        "files": len(list(table.scan().plan_files())),
        "deletes": sorted(len(task.delete_files) for task in table.scan().plan_files()),
        "schemas": len(table.metadata.schemas),
        "columns": [field.name for field in table.schema().fields],
        "metadata": json.load(urllib.request.urlopen(f"{uri}/v1/namespaces/db/tables/wide"))["metadata"],
    }


if __name__ == "__main__":
    print(json.dumps({"namespaces": namespaces, "read": read, "wide": wide, "append": append, "retry": retry, "lifecycle": lifecycle}[sys.argv[1]](sys.argv[2])))
