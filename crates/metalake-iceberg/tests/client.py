"""A real client against the server: what `c12_client` and `c12_read` hold it to, printed as JSON."""

import json
import sys
import urllib.request

import pyarrow
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.io.pyarrow import PyArrowFileIO


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
    print(json.dumps({"namespaces": namespaces, "read": read, "wide": wide, "append": append, "retry": retry}[sys.argv[1]](sys.argv[2])))
