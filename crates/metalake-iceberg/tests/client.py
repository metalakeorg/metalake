"""A real client against the server: what `c12_client` and `c12_read` hold it to, printed as JSON."""

import json
import sys
import urllib.request

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
    print(json.dumps({"namespaces": namespaces, "read": read, "wide": wide}[sys.argv[1]](sys.argv[2])))
