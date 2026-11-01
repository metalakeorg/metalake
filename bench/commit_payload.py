"""MetaLake · preliminary study 2.3: the commit payload comes from the footer of the Parquet file the writer just wrote.

The writer already has the footer in memory when it closes the file (ArrowWriter/ParquetSink): rows, statistics per
row group and column, and its size. From there, without re-reading anything, comes everything the commit registers:
rows, bytes, the footer size, and min/max/nulls per column aggregated from the row groups (shape e3).

  python commit_payload.py
"""

import io
import json
import time

import pyarrow as pa
import pyarrow.parquet as pq

n = 1_000_000
t = pa.table({
    "id": pa.array(range(n), pa.int64()),
    "k": pa.array([i % 97 for i in range(n)], pa.int64()),
    "s": pa.array([f"v{i % 1000:04d}" for i in range(n)]),
})
buf = io.BytesIO()
a = time.time()
pq.write_table(t, buf, row_group_size=128 * 1024)
write_ms = (time.time() - a) * 1000
data = buf.getvalue()

a = time.time()
md = pq.ParquetFile(io.BytesIO(data)).metadata  # the writer already has it in memory: re-read here only to show it
footer = int.from_bytes(data[-8:-4], "little") + 8
cols = {}
for g in range(md.num_row_groups):
    rg = md.row_group(g)
    for c in range(rg.num_columns):
        cc = rg.column(c)
        st = cc.statistics
        e = cols.setdefault(cc.path_in_schema, {"min": None, "max": None, "nulls": 0})
        e["min"] = st.min if e["min"] is None else min(e["min"], st.min)
        e["max"] = st.max if e["max"] is None else max(e["max"], st.max)
        e["nulls"] += st.null_count
payload = {
    "path": "gs://lake/t/data/00001.parquet",
    "row_count": md.num_rows,
    "bytes": len(data),
    "footer": footer,
    "row_groups": md.num_row_groups,
    "columns": cols,
}
aggregate_ms = (time.time() - a) * 1000
print("result\twrite 10^6 rows", f"{write_ms:.0f} ms", f"{len(data) / 1024 / 1024:.1f} MiB", sep="\t")
print("result\tpayload from the footer", f"{aggregate_ms:.1f} ms", f"footer={footer} B", f"row_groups={md.num_row_groups}", sep="\t")
print("result\tpayload", json.dumps(payload, default=str), sep="\t")
