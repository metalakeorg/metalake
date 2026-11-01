"""MetaLake preliminary study 2.7: Iceberg writers committing into MetaLake.

A minimal Iceberg REST catalog (only what PyIceberg needs: config, loadTable, updateTable) in front of the o1
schema. `loadTable` serves the table as an Iceberg projection built from Postgres (one manifest, one snapshot per
head). `updateTable` (the commit) takes the client's `requirements` and `updates`, reads only the manifests the
client added in its new snapshot, and turns their ADDED / DELETED entries into rows in ONE transaction under the
head lock: `assert-ref-snapshot-id` becomes the head check (409 CommitFailedException on mismatch), added files
become `o1_file` rows (statistics from the manifest's lower/upper bounds), deleted files close their validity.

Then a real PyIceberg client appends, tries a stale append, and deletes; after every step the files Postgres sees
must be exactly the files PyIceberg plans.

  python iceberg_ingest.py <dsn>
"""

import hashlib
import json
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.conversions import from_bytes, to_bytes
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
    read_manifest_list,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField

DSN = sys.argv[1]
LOCATION = "file:///tmp/wh/t"
INF = 9223372036854775807
SCHEMA = Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "k", LongType(), required=False), schema_id=0)
TABLE_UUID = str(uuid.uuid4())
io = PyArrowFileIO()
timings = []
rejected = []  # the 409s the server answered
projected = {}  # snapshot id -> (manifest list, parent): the recent history the projection keeps serving


def r(*parts):
    print("result\t" + "\t".join(str(p) for p in parts), flush=True)


def visible_paths(cur):
    s = cur.execute("select snap from head where ref = 1 and tbl = 1").fetchone()[0]
    rows = cur.execute(
        "select f.path, f.row_count, f.min_k, f.max_k, f.begin_seq from o1_file f"
        " where f.tbl = 1 and f.id in (select visible(1, 1, %s)) order by f.id", (s,)).fetchall()
    return s, rows


def project(cur):
    """The Iceberg projection of main: one manifest with every visible file, one snapshot (the head)."""
    s, rows = visible_paths(cur)
    key = hashlib.sha256("|".join(x[0] for x in rows).encode()).hexdigest()[:16]
    mpath = f"{LOCATION}/metadata/m-{key}.avro"
    with write_manifest(2, UNPARTITIONED_PARTITION_SPEC, SCHEMA, io.new_output(mpath), s, "zstandard") as w:
        for path, n, lo, hi, seq in rows:
            df = DataFile.from_args(
                content=DataFileContent.DATA, file_path=path, file_format=FileFormat.PARQUET, partition=Record(),
                record_count=n, file_size_in_bytes=max(n, 1) * 16,
                lower_bounds={1: to_bytes(LongType(), lo)}, upper_bounds={1: to_bytes(LongType(), hi)})
            df.spec_id = 0
            w.add_entry(ManifestEntry.from_args(status=ManifestEntryStatus.EXISTING, snapshot_id=seq,
                                                sequence_number=seq, file_sequence_number=seq, data_file=df))
    mf = w.to_manifest_file()
    lpath = f"{LOCATION}/metadata/snap-{s}-{key}.avro"
    with write_manifest_list(2, io.new_output(lpath), s, None, s, "zstandard") as lw:
        lw.add_manifests([mf])
    if s not in projected:
        parent = cur.execute("select parent from snapshot where tbl = 1 and id = %s", (s,)).fetchone()
        projected[s] = (lpath, parent[0] if parent and parent[0] in projected else None)
    md = {
        "format-version": 2, "table-uuid": TABLE_UUID, "location": LOCATION, "last-sequence-number": s,
        "last-updated-ms": int(time.time() * 1000), "last-column-id": 2,
        "schemas": [json.loads(SCHEMA.model_dump_json())], "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}], "default-spec-id": 0, "last-partition-id": 999,
        "sort-orders": [{"order-id": 0, "fields": []}], "default-sort-order-id": 0, "properties": {},
        "current-snapshot-id": s,
        # The head plus the ancestors already projected: a client that rebases after a 409 looks its base up here.
        "snapshots": [{"snapshot-id": i, "sequence-number": i, "timestamp-ms": int(time.time() * 1000),
                       "manifest-list": lp, "summary": {"operation": "append"}, "schema-id": 0,
                       **({"parent-snapshot-id": pa_} if pa_ else {})}
                      for i, (lp, pa_) in sorted(projected.items())],
        "snapshot-log": [], "metadata-log": [], "refs": {"main": {"snapshot-id": s, "type": "branch"}},
    }
    loc = f"{LOCATION}/metadata/v{s}-{key}.metadata.json"
    with io.new_output(loc).create(overwrite=True) as f:
        f.write(json.dumps(md).encode())
    return {"metadata-location": loc, "metadata": md, "config": {}}


def commit(cur, body):
    """updateTable → one transaction under the head lock. Returns (status, response)."""
    a = time.time()
    expected = next((q["snapshot-id"] for q in body.get("requirements", [])
                     if q["type"] == "assert-ref-snapshot-id" and q["ref"] == "main"), None)
    added, removed = [], []
    for u in body.get("updates", []):
        if u["action"] != "add-snapshot":
            continue
        snap = u["snapshot"]
        for mf in read_manifest_list(io.new_input(snap["manifest-list"])):
            if mf.added_snapshot_id != snap["snapshot-id"]:
                continue  # manifests carried over from the parent: nothing new in them
            for e in mf.fetch_manifest_entry(io, discard_deleted=False):
                d = e.data_file
                if e.status == ManifestEntryStatus.ADDED:
                    added.append((d.file_path, d.record_count, from_bytes(LongType(), d.lower_bounds[1]),
                                  from_bytes(LongType(), d.upper_bounds[1])))
                elif e.status == ManifestEntryStatus.DELETED:
                    removed.append(d.file_path)
    head = cur.execute("select snap from head where ref = 1 and tbl = 1 for update").fetchone()[0]
    if expected is not None and expected != head:
        cur.connection.rollback()
        rejected.append(expected)
        return 409, {"error": {"message": f"branch main is at {head}, not {expected}",
                               "type": "CommitFailedException", "code": 409}}
    s = cur.execute("select nextval('seq')").fetchone()[0]
    cur.execute("insert into snapshot values (1, 1, %s, %s)", (s, head))
    for path, n, lo, hi in added:
        cur.execute("insert into o1_file (tbl, id, ref, begin_seq, row_count, min_k, max_k, path)"
                    " values (1, nextval('fid'), 1, %s, %s, %s, %s, %s)", (s, n, lo, hi, path))
    if removed:
        cur.execute("update o1_file set end_seq = %s where tbl = 1 and ref = 1 and path = any(%s) and end_seq = %s",
                    (s, removed, INF))
        if cur.rowcount != len(removed):
            cur.connection.rollback()
            return 409, {"error": {"message": "a removed file is no longer live", "type": "CommitFailedException",
                                   "code": 409}}
    cur.execute("update head set snap = %s where ref = 1 and tbl = 1", (s,))
    cur.connection.commit()
    timings.append(((time.time() - a) * 1000, len(added), len(removed)))
    return 200, project(cur)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, status, obj):
        b = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path.startswith("/v1/config"):
            return self.send(200, {"defaults": {}, "overrides": {}})
        if self.path.startswith("/v1/namespaces/default/tables/t"):
            with psycopg.connect(DSN) as con, con.cursor() as cur:
                return self.send(200, project(cur))
        self.send(404, {"error": {"message": self.path, "type": "NoSuchTableException", "code": 404}})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path.startswith("/v1/namespaces/default/tables/t"):
            with psycopg.connect(DSN) as con, con.cursor() as cur:
                return self.send(*commit(cur, body))
        self.send(404, {"error": {"message": self.path, "type": "NotFound", "code": 404}})


srv = ThreadingHTTPServer(("127.0.0.1", 8181), Handler)
threading.Thread(target=srv.serve_forever, daemon=True).start()
catalog = load_catalog("metalake", type="rest", uri="http://127.0.0.1:8181")
failures = 0


def check(step):
    global failures
    with psycopg.connect(DSN) as con, con.cursor() as cur:
        _, rows = visible_paths(cur)
    pg = {x[0] for x in rows}
    planned = {t.file.file_path for t in catalog.load_table("default.t").scan().plan_files()}
    ok = pg == planned
    failures += not ok
    r(step, f"{len(pg)} files in Postgres", "same as PyIceberg" if ok else f"DIFFERENT ({len(planned)} planned)")
    return rows


rows0 = check("start")
stale = catalog.load_table("default.t")

# 1 · append: a real Parquet written by PyIceberg, committed through updateTable.
t = catalog.load_table("default.t")
t.append(pa.table({"id": pa.array(range(9_000_000, 9_001_000), pa.int64()), "k": pa.array([7] * 1000, pa.int64())}))
rows = check("append 1000 rows")
new = [x for x in rows if x[2] == 9_000_000]
ok = len(new) == 1 and new[0][1] == 1000 and new[0][3] == 9_000_999
failures += not ok
r("append statistics", f"row_count={new[0][1] if new else None} min={new[0][2] if new else None} max={new[0][3] if new else None}",
  "from the manifest bounds" if ok else "WRONG")

# 2 · a stale writer (loaded before the append): its first attempt gets 409; PyIceberg then rebases the append
#     onto the new head and retries, which is correct for a blind append. Both files must end up live.
rejected_before = len(rejected)
try:
    stale.append(pa.table({"id": pa.array([5_000_000], pa.int64()), "k": pa.array([1], pa.int64())}))
    r("stale append", f"{len(rejected) - rejected_before} × 409 from the server, then rebased by the client and committed")
except CommitFailedException as e:
    r("stale append", "409 CommitFailedException, not retried", str(e)[:80])
rows = check("after the stale append")
both = any(x[2] == 9_000_000 for x in rows) and any(x[2] == 5_000_000 for x in rows)
failures += not both
r("stale append result", "both appends live" if both else "an append is MISSING")

# 3 · delete: every row of the appended file matches, so PyIceberg drops the whole file (DELETED entry).
catalog.load_table("default.t").delete(delete_filter=GreaterThanOrEqual("id", 9_000_000))
rows = check("delete id >= 9000000")
gone = not any(x[2] == 9_000_000 for x in rows)
failures += not gone
r("delete", "file closed in Postgres" if gone else "file STILL LIVE")
r("commit handling", " · ".join(f"{ms:.1f} ms (+{a} −{d})" for ms, a, d in timings),
  "reading the new manifests + one transaction, without re-projection")
r("correctness", f"failures={failures}")
srv.shutdown()
