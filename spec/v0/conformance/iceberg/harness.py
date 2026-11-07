"""MetaLake v0 · the Iceberg harness (§16).

Builds a catalog with the reference, projects its tables to Iceberg (metadata, manifest lists and manifests, all
computed by the reference's iceberg_* functions; this file only writes them as Avro), serves them through a minimal
Iceberg REST catalog, and checks every ICE rule with PyIceberg. Run by run.sh, inside the harness image.
"""

import json
import re
import struct
import sys
import threading
import traceback
import urllib.parse
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.conversions import from_bytes
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NamespaceNotEmptyError
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    read_manifest_list,
    write_manifest_list,
)
from pyiceberg.table import StaticTable
from pyiceberg.table.metadata import TableMetadataUtil
from pyiceberg.typedef import Record
from pyiceberg.types import DateType, DoubleType, IntegerType, LongType

SPEC = Path(__file__).resolve().parents[3]
DSN = "host=/tmp dbname=postgres user=postgres options=-csearch_path=cat"
LATEST = 9223372036854775806
io = PyArrowFileIO()
covered, failed = set(), []


def check(rules, ok, what):
    print(("ok    " if ok else "FAIL  ") + f"{' '.join(rules)}  {what}", flush=True)
    if ok:
        covered.update(rules)
    else:
        failed.append((rules, what))


def local(uri):
    return uri.removeprefix("file://")


# ── the catalog ───────────────────────────────────────────────────────────────────────────────────────────────────

def setup(cur):
    cur.execute("create extension if not exists dblink")
    cur.execute("drop schema if exists cat cascade")
    cur.execute("create schema cat")
    cur.execute("set search_path = cat")
    for f in sorted((SPEC / "v0" / "schema").glob("*.sql")) + sorted((SPEC / "v0" / "reference").glob("*.sql")):
        cur.execute(f.read_text())
    # Revision 0.1 (NS-2): the tables of the harness live in namespace db.
    cur.execute("insert into namespace values ('{db}', false, 1, 0)")


def sql(cur, q, *args):
    return cur.execute(q, args).fetchone()[0]



def commit(cur, tables, ref=1, op="append"):
    return sql(cur, "select commit(%s::jsonb)", json.dumps(
        {"ref": ref, "author": "h", "actor": "harness", "operation": op, "tables": tables}))


def rows_of(i):
    """The ten rows of data file i: ids i*100 + 11k, one date, labels s<i>a to s<i>j, x from -1.5i to 2.25i."""
    return {"id": [i * 100 + 11 * k for k in range(10)], "d": [date(2026, 1, i)] * 10,
            "s": [f"s{i}{chr(97 + k)}" for k in range(10)], "x": [-1.5 * i + k * (3.75 * i / 9) for k in range(10)]}


ARROW = pa.schema([pa.field("id", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "1"}),
                   pa.field("d", pa.date32(), metadata={"PARQUET:field_id": "2"}),
                   pa.field("s", pa.string(), metadata={"PARQUET:field_id": "3"}),
                   pa.field("x", pa.float64(), metadata={"PARQUET:field_id": "4"})])


def parquet_bytes(i):
    sink = pa.BufferOutputStream()
    pq.write_table(pa.table(rows_of(i), schema=ARROW), sink)
    return sink.getvalue().to_pybytes()


def data_file_json(cur, i, spec_id=0, partition=None, inline=False):
    """Data file i, written for real, with exact statistics on id (long), d (date), s (string), x (double)."""
    content = parquet_bytes(i)
    r = rows_of(i)
    bounds = [("long", min(r["id"]), max(r["id"])), ("date", r["d"][0], r["d"][0]),
              ("string", min(r["s"]), max(r["s"])), ("double", repr(min(r["x"])), repr(max(r["x"])))]
    f = {"file_id": i, "record_count": 10, "file_size": len(content), "footer_size": struct.unpack("<i", content[-8:-4])[0],
         "spec_id": spec_id, "partition": partition or [], "stat_field_ids": [1, 2, 3, 4],
         "lower": [sql(cur, "select encode(encode_bound(%s, %s), 'hex')", t, str(lo)) for t, lo, _ in bounds],
         "upper": [sql(cur, "select encode(encode_bound(%s, %s), 'hex')", t, str(hi)) for t, _, hi in bounds],
         "null_count": [0, 0, 0, 0], "nan_count": [None, None, None, 0]}
    if inline:
        f["inline"] = content.hex()
    else:
        f["path"] = f"file:///tmp/wh/t/data/{i}.parquet"
        Path(local(f["path"])).parent.mkdir(parents=True, exist_ok=True)
        Path(local(f["path"])).write_bytes(content)
    return f


def puffin(blob):
    """A Puffin file holding one deletion-vector-v1 blob at offset 4."""
    footer = json.dumps({"blobs": [{"type": "deletion-vector-v1", "fields": [], "snapshot-id": -1, "sequence-number": -1,
                                    "offset": 4, "length": len(blob)}]}).encode()
    return b"PFA1" + blob + b"PFA1" + footer + struct.pack("<i", len(footer)) + b"\x00\x00\x00\x00" + b"PFA1"


DELETED = {2: [1, 3], 4: [0, 9]}  # the positions the scenario's vectors delete, by data file


def scenario(cur):
    """main with removals, a schema change, a second spec, an inline file and a Puffin vector; a branch with an inline
    vector, a tag, a merge."""
    t = sql(cur, "select gen_random_uuid()::text")
    commit(cur, [{"table_id": t, "create": {"namespace": ["db"], "name": "t", "location": "file:///tmp/wh/t"},
                  "fields": [{"field_id": 1, "new": True, "position": 1, "name": "id", "type": "long", "required": True},
                             {"field_id": 2, "new": True, "position": 2, "name": "d", "type": "date"},
                             {"field_id": 3, "new": True, "position": 3, "name": "s", "type": "string"},
                             {"field_id": 4, "new": True, "position": 4, "name": "x", "type": "double"}]}])
    commit(cur, [{"table_id": t, "add": [data_file_json(cur, i) for i in (1, 2, 3, 4)]}])
    commit(cur, [{"table_id": t, "add": [data_file_json(cur, 5)], "remove": [1]}], op="overwrite")
    commit(cur, [{"table_id": t, "fields": [{"field_id": 3, "position": 3, "name": "label", "type": "string"}]}])
    pfield = sql(cur, "select next_id(%s, 'partition_field')", t)
    commit(cur, [{"table_id": t, "partition_spec": [{"source_ids": [2], "field_id": pfield, "name": "d_day", "transform": "day"}]}])
    day = lambda i: sql(cur, "select transform_value('date', 'day', %s)", date(2026, 1, i).isoformat())
    commit(cur, [{"table_id": t, "add": [data_file_json(cur, 8, 1, [day(8)])]}])
    commit(cur, [{"table_id": t, "add": [data_file_json(cur, 7, 1, [day(7)], inline=True)]}])
    blob = bytes.fromhex(sql(cur, "select encode(dv_encode(%s), 'hex')", DELETED[4]))
    Path("/tmp/wh/t/data/dv-a.puffin").write_bytes(puffin(blob))
    commit(cur, [{"table_id": t, "dvs": [{"file_id": 4, "base_seq": None, "path": "file:///tmp/wh/t/data/dv-a.puffin",
                                          "content_offset": 4, "content_size": len(blob), "cardinality": 2}]}], op="delete")
    sql(cur, "select create_ref('v1', 1, 'tag')")
    sql(cur, "select delete_ref(create_ref('gone', 1))")
    b = sql(cur, "select create_ref('b', 1)")
    dv = sql(cur, "select encode(dv_encode(%s), 'hex')", DELETED[2])
    commit(cur, [{"table_id": t, "add": [data_file_json(cur, 6)], "remove": [3],
                  "dvs": [{"file_id": 2, "base_seq": None, "cardinality": 2, "inline": dv}]}], ref=b, op="overwrite")
    c = sql(cur, "select create_ref('c', 1)")
    commit(cur, [{"table_id": t, "add": [data_file_json(cur, 9)]}], ref=c)
    sql(cur, "select merge(%s)", c)
    cur.connection.commit()
    return t


# ── the projection ───────────────────────────────────────────────────────────────────────────────────────────────

class DeleteManifestWriterV2(ManifestWriterV2):
    """PyIceberg 0.12 writes v2 data manifests only; a delete manifest differs in its declared content."""

    def content(self):
        return ManifestContent.DELETES

    @property
    def _meta(self):
        return {**super()._meta, "content": "deletes"}


def py_value(typ, v):
    """A partition value from its JSON single-value form to what PyIceberg writes."""
    if v is None:
        return None
    if isinstance(typ, DateType):
        # PyIceberg types the result of day() as a date; both are days from 1970-01-01.
        return v if isinstance(v, int) else (date.fromisoformat(v) - date(1970, 1, 1)).days
    if isinstance(typ, (IntegerType, LongType)):
        return int(v)
    if isinstance(typ, DoubleType):
        return float(v)
    return v


def materialize_deletes(cur, r):
    """ICE-4: the position delete file of a vector, from its positions, read inline or from its Puffin file."""
    blob = bytes.fromhex(r["inline"]) if r["inline"] else \
        Path(local(r["puffin"])).read_bytes()[r["content_offset"]:r["content_offset"] + r["content_size"]]
    positions = sql(cur, "select array(select dv_positions(%s) order by 1)", blob)
    table = pa.table({"file_path": [r["referenced_data_file"]] * len(positions), "pos": positions},
                     schema=pa.schema([pa.field("file_path", pa.string(), nullable=False, metadata={"PARQUET:field_id": "2147483546"}),
                                       pa.field("pos", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "2147483545"})]))
    Path(local(r["path"])).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, local(r["path"]))
    return Path(local(r["path"])).stat().st_size


def project(cur, t, tag="v"):
    md = sql(cur, "select iceberg_metadata(%s)", t)
    loc = md["location"]
    for s in md["snapshots"]:
        s["manifest-list"] = f"{loc}/metadata/snap-{s['snapshot-id']}-{tag}.avro"
    meta = TableMetadataUtil.parse_obj(md)
    for s in md["snapshots"]:
        sid = s["snapshot-id"]
        schema = meta.schema_by_id(s["schema-id"])
        files = [r[0] for r in cur.execute("select iceberg_files(%s, %s)", (t, sid))]
        dvs = [r[0] for r in cur.execute("select iceberg_dvs(%s, %s)", (t, sid))]
        manifests = []
        for deletes, rows in ((False, files), (True, dvs)):
            for spec_id in sorted({r["spec_id"] for r in rows}):
                spec = meta.specs()[spec_id]
                ptype = spec.partition_type(schema)
                here = [r for r in rows if r["spec_id"] == spec_id]
                groups = [here] if deletes else [[r for r in here if r["status"] == status] for status in ("ADDED", "EXISTING")]
                for part, group in enumerate(g for g in groups if g):
                    path = f"{loc}/metadata/m-{sid}-{'deletes' if deletes else 'data'}-{spec_id}-{part}-{tag}.avro"
                    sequences = [r["data_sequence_number"] for r in group]
                    writer = (DeleteManifestWriterV2 if deletes else ManifestWriterV2)(spec, schema, io.new_output(path), max(sequences), "null")
                    entries = []
                    for r in group:
                            partition = Record(*[py_value(fld.field_type, v) for fld, v in zip(ptype.fields, r["partition"])])
                            if deletes:
                                df = DataFile.from_args(
                                    content=DataFileContent.POSITION_DELETES, file_path=r["path"], file_format=FileFormat.PARQUET,
                                    partition=partition, record_count=r["cardinality"], file_size_in_bytes=materialize_deletes(cur, r),
                                    referenced_data_file=r["referenced_data_file"])
                            else:
                                if r.get("inline"):
                                    Path(local(r["path"])).parent.mkdir(parents=True, exist_ok=True)
                                    Path(local(r["path"])).write_bytes(bytes.fromhex(r["inline"]))
                                df = DataFile.from_args(
                                    content=DataFileContent.DATA, file_path=r["path"], file_format=FileFormat.PARQUET,
                                    partition=partition, record_count=r["record_count"], file_size_in_bytes=r["file_size"],
                                    null_value_counts={int(k): v for k, v in (r["null_count"] or {}).items()},
                                    nan_value_counts={int(k): v for k, v in (r["nan_count"] or {}).items()},
                                    lower_bounds={int(k): bytes.fromhex(v) for k, v in (r["lower"] or {}).items()},
                                    upper_bounds={int(k): bytes.fromhex(v) for k, v in (r["upper"] or {}).items()},
                                    sort_order_id=r["sort_order_id"])
                            df.spec_id = spec_id
                            entries.append(ManifestEntry.from_args(
                                status=ManifestEntryStatus.ADDED if r["status"] == "ADDED" else ManifestEntryStatus.EXISTING,
                                snapshot_id=sid if r["status"] == "ADDED" else r["data_sequence_number"],
                                sequence_number=r["data_sequence_number"], file_sequence_number=r["data_sequence_number"],
                                data_file=df))
                    # Entries are built first: an error inside the writer's block would be masked by its empty-file check.
                    with writer as w:
                        for e in entries:
                            w.add_entry(e)
                    manifest = w.to_manifest_file()
                    manifest.sequence_number, manifest.min_sequence_number = sid, min(sequences)
                    manifests.append(manifest)
        with write_manifest_list(2, io.new_output(s["manifest-list"]), sid, s.get("parent-snapshot-id"), sid, "null") as lw:
            lw.add_manifests(manifests)
    path = f"{loc}/metadata/{tag}-{md['last-sequence-number']}.metadata.json"
    with io.new_output(path).create(overwrite=True) as out:
        out.write(json.dumps(md).encode())
    return md, path


# ── outbound checks ──────────────────────────────────────────────────────────────────────────────────────────────

def visible(cur, ref, t, s):
    return {r[0] for r in cur.execute("select served_path(%s, f) from visible_files(%s, %s, %s) f", (t, ref, t, s))}


def live_ids(cur, ref, t, s):
    """The ids a reader must see at (ref, s): every visible file's rows, without the positions its vector deletes."""
    ids = set()
    for fid, vec in cur.execute(
            "select f.file_id, d.file_id is not null from visible_files(%s, %s, %s) f"
            " left join visible_dvs(%s, %s, %s) d on d.file_id = f.file_id", (ref, t, s, ref, t, s)).fetchall():
        rows = rows_of(fid)["id"]
        ids |= {v for k, v in enumerate(rows) if not (vec and k in DELETED[fid])}
    return ids


def outbound(cur, t):
    md, path = project(cur, t)
    table = StaticTable.from_metadata(path)
    check(["ICE-1"], table.metadata.format_version == 2, "format version 2")
    refs = {r[0]: (r[1], r[2]) for r in cur.execute(
        "select name, kind, snapshot_at(ref_id, %s, %s) from ref where not deleted", (t, LATEST))}
    check(["ICE-1"], {n: (r.snapshot_id, r.snapshot_ref_type.value) for n, r in table.metadata.refs.items()}
          == {n: (sid, kind) for n, (kind, sid) in refs.items()}, "every live ref, of its kind, at its current snapshot")
    seqs = {r[0] for r in cur.execute("select distinct schema_seq from snapshot where table_id = %s", (t,))}
    check(["ICE-1"], {s.schema_id for s in table.metadata.schemas} == seqs, "one schema per schema_seq")
    check(["ICE-1"], [f.name for f in table.schema().fields] == ["id", "d", "label", "x"], "main's current schema")
    check(["ICE-1"], {s.spec_id for s in table.metadata.partition_specs} == {0, 1} and table.metadata.default_spec_id == 1,
          "both partition specs, the second the default")
    ops = {s.snapshot_id: s.summary.operation.value for s in table.metadata.snapshots}
    merge_seq = sql(cur, "select max(commit_seq) from commit where operation = 'merge'")
    merged = sql(cur, "select summary from snapshot where snapshot_id = %s and table_id = %s", merge_seq, t)
    only_added = merged["deleted-data-files"] == 0 and merged["changed-vectors"] == 0
    check(["ICE-1"], ops.get(merge_seq) == ("append" if only_added else "overwrite"), "a merge is what it did to Iceberg")

    owners = dict(cur.execute("select snapshot_id, ref_id from snapshot where table_id = %s", (t,)).fetchall())
    ok = all({task.file.file_path for task in table.scan(snapshot_id=sid).plan_files()} == visible(cur, owner, t, sid)
             for sid, owner in owners.items())
    check(["ICE-2"], ok, f"every one of {len(owners)} snapshots plans exactly the files visible at it")
    ok = all({task.file.file_path for task in table.scan(snapshot_id=table.metadata.refs[n].snapshot_id).plan_files()}
             == visible(cur, sql(cur, "select ref_id from ref where name = %s", n), t, LATEST) for n in refs)
    check(["ICE-2"], ok, "every ref plans exactly its current files")
    last = md["snapshots"][-1]
    entries = [e for mf in read_manifest_list(io.new_input(last["manifest-list"]))
               for e in mf.fetch_manifest_entry(io, discard_deleted=False) if e.data_file.content == DataFileContent.DATA]
    rows = dict(cur.execute("select served_path(%s, f), f.begin_seq from visible_files(%s, %s, %s) f",
                            (t, owners[last["snapshot-id"]], t, last["snapshot-id"])).fetchall())
    check(["ICE-2"], all(e.sequence_number == rows[e.data_file.file_path] for e in entries)
          and {e.data_file.file_path for e in entries if e.status == ManifestEntryStatus.ADDED}
          == {p for p, s in rows.items() if s == last["snapshot-id"]}, "data sequence numbers and ADDED entries")
    data = [m for m in read_manifest_list(io.new_input(last["manifest-list"])) if m.content == ManifestContent.DATA]
    check(["ICE-2"], all(not (m.added_files_count and m.existing_files_count) for m in data)
          and all(bool(m.added_files_count) == (m.added_snapshot_id == last["snapshot-id"]) for m in data),
          "a data manifest holds one status, and only the one of added files names the snapshot")

    main = table.metadata.current_snapshot_id
    filters = [("id", 250, 420, "long"), ("x", -3.0, -2.0, "double"), ("label", "s2b", "s4b", "string"),
               ("d", date(2026, 1, 3), date(2026, 1, 5), "date")]
    fid = {"id": 1, "d": 2, "label": 3, "x": 4}
    ok = True
    for name, lo, hi, typ in filters:
        expr = And(GreaterThanOrEqual(name, lo), LessThanOrEqual(name, hi))
        seen = {task.file.file_path for task in table.scan(snapshot_id=main, row_filter=expr).plan_files()}
        want = {r[0] for r in cur.execute(
            "select served_path(%s, f) from prune(1, %s, %s, %s, encode_bound(%s, %s), encode_bound(%s, %s)) f",
            (t, t, main, fid[name], typ, str(lo), typ, str(hi)))}
        if seen != want:
            ok = False
            print(f"      {name}: PyIceberg {sorted(seen)} reference {sorted(want)}")
    check(["ICE-3"], ok, "pruned scans on long, double, string and date plan exactly the reference's prune()")
    check(["ICE-3"], sql(cur, """select bool_and(iceberg_bound_in(ty.type, iceberg_bound(ty.type, x.b)) = current_bound(ty.type, x.b))
                                   from data_file f, unnest(f.stat_field_ids, f.lower || f.upper) x(id, b)
                                   join visible_fields(1, %s, %s) ty on ty.field_id = x.id
                                  where f.table_id = %s and x.b is not null""", t, LATEST, t),
          "every bound converts to Iceberg's serialization and back unchanged")
    got = {r["path"]: (from_bytes(LongType(), bytes.fromhex(r["lower"]["1"])), from_bytes(DoubleType(), bytes.fromhex(r["upper"]["4"])),
                       from_bytes(DateType(), bytes.fromhex(r["lower"]["2"])))
           for r in (x[0] for x in cur.execute("select iceberg_files(%s, %s)", (t, main)))}
    want = {re.search(r"/(\d+)\.parquet$", p).group(1): v for p, v in got.items()}
    check(["ICE-3"], all(want[str(i)] == (min(rows_of(i)["id"]), max(rows_of(i)["x"]), (date(2026, 1, i) - date(1970, 1, 1)).days)
                         for i in (2, 4, 5, 7, 8, 9)), "PyIceberg reads long, double and date bounds as the values in the files")

    ok = True
    for name in ("main", "b", "v1"):
        snap = table.metadata.refs[name].snapshot_id
        ref = sql(cur, "select ref_id from ref where name = %s", name)
        seen = set(table.scan(snapshot_id=snap).to_arrow()["id"].to_pylist())
        ok &= seen == live_ids(cur, ref, t, LATEST)
    check(["ICE-4"], ok, "reading main, b and the tag returns every live row and none of the deleted ones")
    b = table.metadata.refs["b"].snapshot_id
    dels = [e.data_file for mf in read_manifest_list(io.new_input(next(s["manifest-list"] for s in md["snapshots"] if s["snapshot-id"] == b)))
            if mf.content == ManifestContent.DELETES for e in mf.fetch_manifest_entry(io)]
    want = {(r[0]["path"], r[0]["referenced_data_file"], r[0]["cardinality"]) for r in cur.execute("select iceberg_dvs(%s, %s)", (t, b))}
    # PyIceberg 0.12 does not read referenced_data_file back: the data file is checked in the delete file itself.
    got = {(d.file_path, set(pq.read_table(local(d.file_path))["file_path"].to_pylist()).pop(), d.record_count) for d in dels}
    check(["ICE-4"], got == want and len(want) == 2 and all(d.content == DataFileContent.POSITION_DELETES for d in dels),
          "b's two vectors, one inline and one in a Puffin file, are position delete files of their data files")

    inline = "file:///tmp/wh/t/metalake-inline/7.parquet"
    check(["ICE-5"], inline in {task.file.file_path for task in table.scan().plan_files()}
          and Path(local(inline)).read_bytes() == bytes.fromhex(sql(cur, "select encode(content, 'hex') from inline_file where file_id = 7"))
          and set(rows_of(7)["id"]) <= set(table.scan().to_arrow()["id"].to_pylist()),
          "the inline file is served at its materialized path, with its exact bytes, and read")


# ── inbound: a REST catalog in front of the reference ───────────────────────────────────────────────────────────

class Rest:
    rejected = 0

    def __init__(self, t):
        self.t = t
        self.con = psycopg.connect(DSN)
        self.n = 0

    def load(self):
        with self.con.cursor() as cur:
            self.n += 1
            md, path = project(cur, self.t, tag=f"r{self.n}")
            self.con.commit()
            return {"metadata-location": path, "metadata": md, "config": {}}

    def update(self, body):
        cur = self.con.cursor()
        try:
            allowed = {"add-snapshot", "set-snapshot-ref"}
            if any(u["action"] not in allowed for u in body.get("updates", [])):
                return 400, {"error": {"message": "only add-snapshot and set-snapshot-ref in v0", "type": "BadRequestException", "code": 400}}
            ref = next((u["ref-name"] for u in body["updates"] if u["action"] == "set-snapshot-ref"), "main")
            expected = next((q.get("snapshot-id") for q in body.get("requirements", [])
                             if q["type"] == "assert-ref-snapshot-id" and q["ref"] == ref), None)
            added, removed, op = [], [], "append"
            for u in body["updates"]:
                if u["action"] != "add-snapshot":
                    continue
                snap = u["snapshot"]
                op = snap.get("summary", {}).get("operation", "append")
                for mf in read_manifest_list(io.new_input(snap["manifest-list"])):
                    if mf.added_snapshot_id != snap["snapshot-id"]:
                        continue
                    for e in mf.fetch_manifest_entry(io, discard_deleted=False):
                        d = e.data_file
                        if e.status == ManifestEntryStatus.ADDED:
                            tail = Path(local(d.file_path)).read_bytes()[-8:]
                            added.append({"path": d.file_path, "record_count": d.record_count,
                                          "file_size": d.file_size_in_bytes, "footer_size": struct.unpack("<i", tail[:4])[0],
                                          "partition": [], "spec_id": d.spec_id,
                                          "lower": {str(k): v.hex() for k, v in (d.lower_bounds or {}).items()},
                                          "upper": {str(k): v.hex() for k, v in (d.upper_bounds or {}).items()},
                                          "null_count": {str(k): v for k, v in (d.null_value_counts or {}).items()},
                                          "nan_count": {str(k): v for k, v in (d.nan_value_counts or {}).items()}})
                        elif e.status == ManifestEntryStatus.DELETED:
                            removed.append(d.file_path)
            cur.execute("select iceberg_commit(%s, %s, %s, %s, %s::jsonb, %s)",
                        (self.t, ref, expected, op, json.dumps(added), removed))
            self.con.commit()
            return 200, self.load()
        except psycopg.errors.SerializationFailure as e:
            self.con.rollback()
            Rest.rejected += 1
            return 409, {"error": {"message": str(e), "type": "CommitFailedException", "code": 409}}
        except psycopg.Error as e:
            self.con.rollback()
            return 400, {"error": {"message": str(e), "type": "BadRequestException", "code": 400}}


class Namespaces:
    """ICE-7: the namespace endpoints of the REST catalog, over the reference's iceberg_namespaces, iceberg_namespace
    and commit, on main."""

    def __init__(self):
        self.con = psycopg.connect(DSN)

    @staticmethod
    def path(segment):
        return urllib.parse.unquote(segment).split("\x1f")

    def query(self, q, *args):
        with self.con.cursor() as cur:
            row = cur.execute(q, args).fetchone()
        self.con.commit()
        return row[0] if row else None

    def commit(self, spaces):
        """The SQLSTATE of the commit's refusal, or None."""
        c = {"ref": 1, "author": "rest", "actor": "rest", "operation": "update", "namespaces": spaces, "tables": []}
        try:
            with self.con.cursor() as cur:
                cur.execute("select commit(%s::jsonb)", (json.dumps(c),))
            self.con.commit()
            return None
        except psycopg.Error as e:
            self.con.rollback()
            return e.sqlstate

    def get(self, url):
        parts = urllib.parse.urlparse(url)
        segments = parts.path.split("/")[3:]
        if not segments:
            parent = urllib.parse.parse_qs(parts.query).get("parent", [""])[0]
            listed = self.query("select coalesce(jsonb_agg(x), '[]') from iceberg_namespaces(1, %s) x",
                                parent.split("\x1f") if parent else [])
            return 200, {"namespaces": listed}
        found = self.query("select iceberg_namespace(1, %s)", self.path(segments[0]))
        if found is None:
            return 404, {"error": {"message": url, "type": "NoSuchNamespaceException", "code": 404}}
        return 200, found

    def post(self, url, body):
        segments = urllib.parse.urlparse(url).path.split("/")[3:]
        if not segments:
            state = self.commit([{"namespace": body["namespace"], "create": True,
                                  "properties": body.get("properties", {})}])
            if state == "40001":
                return 409, {"error": {"message": "exists", "type": "AlreadyExistsException", "code": 409}}
            return 200, {"namespace": body["namespace"], "properties": body.get("properties", {})}
        ns = self.path(segments[0])
        props = {**{k: None for k in body.get("removals", [])}, **body.get("updates", {})}
        if self.commit([{"namespace": ns, "properties": props}]):
            return 404, {"error": {"message": url, "type": "NoSuchNamespaceException", "code": 404}}
        return 200, {"updated": list(body.get("updates", {})), "removed": body.get("removals", []), "missing": []}

    def delete(self, url):
        ns = self.path(urllib.parse.urlparse(url).path.split("/")[3])
        state = self.commit([{"namespace": ns, "drop": True}])
        if state == "40001":
            return 409, {"error": {"message": "not empty", "type": "NamespaceNotEmptyException", "code": 409}}
        if state:
            return 404, {"error": {"message": url, "type": "NoSuchNamespaceException", "code": 404}}
        return 204, None


def serve(rest):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def send(self, status, obj):
            b = b"" if obj is None else json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self):
            if self.path.startswith("/v1/config"):
                return self.send(200, {"defaults": {}, "overrides": {}})
            if self.path.startswith("/v1/namespaces/db/tables/w"):
                return self.send(200, rest.load())
            if self.path.startswith("/v1/namespaces") and "/tables" not in self.path:
                return self.send(*spaces.get(self.path))
            self.send(404, {"error": {"message": self.path, "type": "NoSuchTableException", "code": 404}})

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path.startswith("/v1/namespaces") and "/tables" not in self.path:
                return self.send(*spaces.post(self.path, body))
            self.send(*rest.update(body))

        def do_DELETE(self):
            self.send(*spaces.delete(self.path))

    spaces = Namespaces()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def inbound(cur):
    w = sql(cur, "select gen_random_uuid()::text")
    commit(cur, [{"table_id": w, "create": {"namespace": ["db"], "name": "w", "location": "file:///tmp/wh/w"},
                  "fields": [{"field_id": 1, "new": True, "position": 1, "name": "id", "type": "long", "required": True},
                             {"field_id": 2, "new": True, "position": 2, "name": "s", "type": "string"}]}])
    sql(cur, "select create_ref('wb', 1)")
    cur.connection.commit()
    rest = Rest(w)
    server = serve(rest)
    catalog = load_catalog("mt", type="rest", uri=f"http://127.0.0.1:{server.server_port}")
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("s", pa.string())])
    files = lambda ref="main": {r[0]: (r[1], r[2], r[3]) for r in cur.execute(
        "select f.path, f.record_count, decode_bound('long', f.lower[1]), decode_bound('long', f.upper[1])"
        " from visible_files((select ref_id from ref where name = %s), %s, %s) f", (ref, w, LATEST))}

    table = catalog.load_table("db.w")
    table.append(pa.table({"id": [5, 9, 7], "s": ["a", "b", "c"]}, schema=schema))
    cur.connection.commit()
    got = files()
    check(["ICE-6"], len(got) == 1 and list(got.values())[0] == (3, "5", "9"),
          "an append is one MetaLake commit, with its record count and bounds converted back")
    check(["ICE-6"], catalog.load_table("db.w").metadata.current_snapshot_id == sql(cur, "select max(commit_seq) from commit"),
          "the snapshot carries MetaLake's id")

    stale = catalog.load_table("db.w")
    catalog.load_table("db.w").append(pa.table({"id": [20], "s": ["x"]}, schema=schema))
    stale.append(pa.table({"id": [30], "s": ["y"]}, schema=schema))
    cur.connection.commit()
    check(["ICE-6"], Rest.rejected >= 1 and len(files()) == 3,
          f"a stale append is refused with 409, rebased by PyIceberg and committed ({Rest.rejected} refused)")

    t2 = catalog.load_table("db.w")
    t2.delete(delete_filter=GreaterThanOrEqual("id", 30))
    cur.connection.commit()
    check(["ICE-6"], sorted(v[1] for v in files().values()) == ["20", "5"], "a delete of a whole file removes it")

    try:
        catalog.load_table("db.w").update_schema().add_column("z", LongType()).commit()
        refused = False
    except Exception:
        refused = True
    check(["ICE-6"], refused and [f[0] for f in cur.execute("select name from visible_fields(1, %s, %s)", (w, LATEST))] == ["id", "s"],
          "a schema update is refused in v0")
    try:
        catalog.load_table("db.w").append(pa.table({"id": [40], "s": ["z"]}, schema=schema), branch="wb")
        cur.connection.commit()
        check(["ICE-6"], len(files("wb")) == 1 and len(files()) == 2, "an append to a branch lands on that ref only")
    except TypeError:
        print("note  ICE-6  PyIceberg 0.12 cannot append to a branch")
    namespaces(catalog)
    server.shutdown()


def namespaces(catalog):
    """ICE-7, through PyIceberg's namespace calls against the minimal server."""
    catalog.create_namespace("ns7", {"comment": "seven"})
    catalog.create_namespace(("ns7", "child"))
    check(["ICE-7"], set(catalog.list_namespaces()) == {("db",), ("ns7",)}
          and catalog.list_namespaces("ns7") == [("ns7", "child")], "namespaces listed by level")
    check(["ICE-7"], catalog.load_namespace_properties("ns7") == {"comment": "seven"}, "a namespace's properties")
    catalog.update_namespace_properties("ns7", removals={"comment"}, updates={"owner": "x"})
    check(["ICE-7"], catalog.load_namespace_properties("ns7") == {"owner": "x"}, "properties updated and removed")
    try:
        catalog.create_namespace("ns7")
        exists = False
    except NamespaceAlreadyExistsError:
        exists = True
    check(["ICE-7"], exists, "a namespace created twice is refused")
    try:
        catalog.drop_namespace("ns7")
        not_empty = False
    except NamespaceNotEmptyError:
        not_empty = True
    catalog.drop_namespace(("ns7", "child"))
    catalog.drop_namespace("ns7")
    check(["ICE-7"], not_empty and set(catalog.list_namespaces()) == {("db",)},
          "a namespace that is not empty is not dropped (409); empty, it is")


def main():
    with psycopg.connect("host=/tmp dbname=postgres user=postgres", autocommit=True) as con:
        setup(con.cursor())
    with psycopg.connect(DSN) as con:
        cur = con.cursor()
        t = scenario(cur)
        for step in (lambda: outbound(cur, t), lambda: inbound(cur)):
            try:
                step()
            except Exception:
                traceback.print_exc()
                failed.append((["?"], "harness error"))
    text = (SPEC / "metalake-v0.md").read_text()
    rules = set(re.findall(r"\*\*(ICE-\d+)\.\*\*", text))
    print(f"\n{len(failed)} failed; {len(rules)} ICE rules in the text, {len(rules & covered)} covered")
    missing = sorted(rules - covered)
    if missing:
        print("ICE rules without a passing check: " + " ".join(missing))
    sys.exit(1 if failed or missing else 0)


if __name__ == "__main__":
    main()
