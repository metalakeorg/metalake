# The Iceberg harness: the pinned Postgres of the SQL suite, plus Python with a pinned PyIceberg.
FROM postgres:17@sha256:3b0cda93be7c8dba2ed484d19393d93845dc3dafb88973e248ba8e6d99d784e1
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /venv \
 && /venv/bin/pip install --no-cache-dir "pyiceberg[pyarrow]==0.12.0" "pyarrow==21.0.0" "psycopg[binary]==3.2.10"
