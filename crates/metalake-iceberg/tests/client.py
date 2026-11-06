"""A real client against the server: what `c12_client` holds it to, printed as JSON."""

import json
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError

catalog = load_catalog("metalake", type="rest", uri=sys.argv[1])
said = {
    "top": sorted(catalog.list_namespaces()),
    "under_db": sorted(catalog.list_namespaces("db")),
}
try:
    catalog.list_namespaces("nowhere")
    said["nowhere"] = "answered"
except NoSuchNamespaceError as refused:
    said["nowhere"] = str(refused)
print(json.dumps(said))
