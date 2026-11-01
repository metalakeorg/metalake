"""MetaLake · preliminary study 1.6: project a MetaLake table to Iceberg v2, and check it with PyIceberg.

From the Postgres rows (o1: validity range per ref with inheritance) it writes what an Iceberg client that reads files
needs (level c of the projection): a `metadata.json` with the schema, the head snapshots of each ref and their `refs`;
one manifest list per snapshot; and the manifests. Manifests are named by their content (the ids of their files):
what a branch inherits untouched reuses the manifests of `main`, and only what changes is written.

Then PyIceberg reads that `metadata.json` without a catalog (StaticTable) and plans each ref, full and pruned: its
files must be exactly the ones Postgres sees (`visible()`).

  python projection.py <dsn> <output> [files-per-manifest]
"""

import hashlib
import json
import sys
import time
import uuid

import psycopg
from pyiceberg.conversions import to_bytes
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField

DSN, OUTPUT = sys.argv[1], sys.argv[2].rstrip("/")
PER_MANIFEST = int(sys.argv[3]) if len(sys.argv) > 3 else 10000
SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "k", LongType(), required=False),
    schema_id=0,
)
io = PyArrowFileIO()


def r(*parts):
    print("result\t" + "\t".join(str(p) for p in parts), flush=True)


def live(cur, ref, s):
    cur.execute(
        "select f.id, f.path, f.row_count, f.min_k, f.max_k, f.begin_seq from o1_file f"
        " where f.tbl = 1 and f.id in (select visible(%s, 1, %s)) order by f.id",
        (ref, s),
    )
    return cur.fetchall()


written = {}  # content hash -> ManifestFile (reusable across snapshots and refs)
bytes_written = 0


def manifest(rows, snap):
    """The manifest of a block of files, written once per content."""
    global bytes_written
    key = hashlib.sha256(",".join(str(f[0]) for f in rows).encode()).hexdigest()[:20]
    if key in written:
        return written[key], False
    path = f"{OUTPUT}/metadata/m-{key}.avro"
    with write_manifest(2, UNPARTITIONED_PARTITION_SPEC, SCHEMA, io.new_output(path), snap, "zstandard") as w:
        for fid, file_path, row_count, min_k, max_k, begin_seq in rows:
            df = DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=file_path,
                file_format=FileFormat.PARQUET,
                partition=Record(),
                record_count=row_count,
                file_size_in_bytes=row_count * 16,
                lower_bounds={1: to_bytes(LongType(), min_k)},
                upper_bounds={1: to_bytes(LongType(), max_k)},
            )
            df.spec_id = 0
            w.add_entry(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.EXISTING,
                    snapshot_id=begin_seq,
                    sequence_number=begin_seq,
                    file_sequence_number=begin_seq,
                    data_file=df,
                )
            )
    mf = w.to_manifest_file()
    mf.sequence_number = min(f[5] for f in rows)
    mf.min_sequence_number = mf.sequence_number
    written[key] = mf
    bytes_written += mf.manifest_length
    return mf, True


def project(cur, ref, s, snap):
    """The manifest list of snapshot `snap`, head of `ref`: the live files in blocks of PER_MANIFEST by id."""
    rows = live(cur, ref, s)
    blocks = {}
    for f in rows:
        # main's files, by id range; each branch's files (high ids), in their own blocks.
        blocks.setdefault(f[0] // PER_MANIFEST, []).append(f)
    new = 0
    manifests = []
    for _, b in sorted(blocks.items()):
        mf, is_new = manifest(b, snap)
        manifests.append(mf)
        new += is_new
    manifest_list = f"{OUTPUT}/metadata/snap-{snap}.avro"
    with write_manifest_list(2, io.new_output(manifest_list), snap, None, snap, "zstandard") as w:
        w.add_manifests(manifests)
    return manifest_list, len(rows), len(manifests), new, {f[1] for f in rows}


with psycopg.connect(DSN) as con, con.cursor() as cur:
    s = cur.execute("select last_value from seq").fetchone()[0]
    refs = cur.execute("select r.id, r.name, c.snap from ref r join head c on c.ref = r.id and c.tbl = 1 order by r.id").fetchall()
    snapshots, refs_json, expected = [], {}, {}
    t0 = time.time()
    for ref, name, snap in refs:
        a = time.time()
        manifest_list, n, m, new, paths = project(cur, ref, s, snap)
        expected[name] = (snap, paths)
        snapshots.append({
            "snapshot-id": snap, "sequence-number": snap, "timestamp-ms": int(time.time() * 1000),
            "manifest-list": manifest_list, "summary": {"operation": "append"}, "schema-id": 0,
        })
        refs_json[name] = {"snapshot-id": snap, "type": "branch"}
        r("project", f"ref={name}", f"{n} files", f"{m} manifests ({new} new, {m - new} reused)",
          f"{(time.time() - a) * 1000:.0f} ms")
    main = refs_json["main"]["snapshot-id"]
    metadata = {
        "format-version": 2, "table-uuid": str(uuid.uuid4()), "location": OUTPUT,
        "last-sequence-number": max(x["sequence-number"] for x in snapshots),
        "last-updated-ms": int(time.time() * 1000), "last-column-id": 2,
        "schemas": [json.loads(SCHEMA.model_dump_json())], "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}], "default-spec-id": 0, "last-partition-id": 999,
        "sort-orders": [{"order-id": 0, "fields": []}], "default-sort-order-id": 0,
        "properties": {"metalake.projection": "true"}, "current-snapshot-id": main,
        "snapshots": snapshots, "snapshot-log": [], "metadata-log": [], "refs": refs_json,
    }
    metadata_path = f"{OUTPUT}/metadata/v1.metadata.json"
    with io.new_output(metadata_path).create(overwrite=True) as f:
        f.write(json.dumps(metadata).encode())
    r("total", f"{(time.time() - t0) * 1000:.0f} ms", f"{len(written)} distinct manifests",
      f"{bytes_written / 1024 / 1024:.1f} MiB of manifests")

    # ── PyIceberg: read the projection and plan each ref ─────────────────────────────────────────────────────────
    table = StaticTable.from_metadata(metadata_path)
    failures = 0
    for name, (snap, paths) in expected.items():
        a = time.time()
        seen = {t.file.file_path for t in table.scan(snapshot_id=snap).plan_files()}
        ms = (time.time() - a) * 1000
        ok = seen == paths
        failures += not ok
        r("pyiceberg", f"ref={name}", "same" if ok else f"DIFFERENT ({len(seen)} vs {len(paths)})",
          f"plan_files {ms:.0f} ms")
    # Pruned: the same key window as in Postgres.
    n = cur.execute("select count(*) from o1_file where tbl = 1").fetchone()[0]
    lo, hi = n // 4 * 10, n // 4 * 10 + n // 100
    for ref, name, snap in refs:
        cur.execute(
            "select f.path from o1_file f where f.tbl = 1 and f.id in (select visible(%s, 1, %s))"
            " and f.max_k >= %s and f.min_k <= %s", (ref, s, lo, hi))
        paths = {x[0] for x in cur.fetchall()}
        a = time.time()
        seen = {t.file.file_path for t in table.scan(
            snapshot_id=snap, row_filter=And(GreaterThanOrEqual("id", lo), LessThanOrEqual("id", hi))).plan_files()}
        ms = (time.time() - a) * 1000
        ok = seen == paths
        failures += not ok
        r("pyiceberg pruned", f"ref={name}", f"{len(seen)} files", "same" if ok else f"DIFFERENT ({len(paths)} in Postgres)",
          f"plan_files {ms:.0f} ms")
    r("correctness", f"failures={failures}")

    # ── Re-project after a small commit on main (4 new files and 1 removed): what gets rewritten ─────────────────
    con.commit()
    new_id = cur.execute("select coalesce(max(id), 0) + 1 from o1_file where tbl = 1 and id < 40000000000").fetchone()[0]
    removed = cur.execute("select min(id) from o1_file where tbl = 1 and ref = 1 and id in (select visible(1, 1, %s))", (s,)).fetchone()[0]
    cur.execute("select op_add(1, 1, %s::bigint[])", ([new_id + i for i in range(4)],))
    cur.execute("select op_remove(1, 1, %s::bigint[])", ([removed],))
    con.commit()
    s2 = cur.execute("select last_value from seq").fetchone()[0]
    a = time.time()
    _, n, m, new, _ = project(cur, 1, s2, s2)
    r("re-project main after a commit", f"{n} files", f"{m} manifests ({new} rewritten, {m - new} reused)",
      f"{(time.time() - a) * 1000:.0f} ms")
