"""The process a local engine runs in for `USING duckdb`: started by `metalake` with `python -m metalake._worker`, it
runs each statement it is sent with `_engines.run_local`, the call a notebook makes, and answers its rows as Arrow.

An engine's library is native code: if it fails as native code fails (an instruction this processor lacks, a bad
pointer), only this process ends, and the one that started it says so and starts another.

Each request is one line of JSON on stdin, `{"engine", "setup", "sql"}`; each answer, one line of JSON on stdout,
`{"ok": true}` followed by the rows as an Arrow IPC stream, or `{"error": "missing" | "failed", "message"}`. The
worker ends when its stdin closes, as when the process that started it ends.
"""

import json
import os
import sys


def _channel():
    """The stdout the answers go to, alone: what the engine prints (DuckDB's progress bar) goes to stderr instead."""
    channel = os.fdopen(os.dup(sys.stdout.fileno()), "wb")
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout = sys.stderr
    return channel


def _rows(rows):
    """The rows as Arrow, all of them read before any is sent, so that a failure while reading is said as one."""
    import pyarrow

    reader = pyarrow.RecordBatchReader.from_stream(rows)
    return reader.schema, list(reader)


def _answer(channel, header, rows=None):
    channel.write(json.dumps(header).encode() + b"\n")
    if rows is not None:
        import pyarrow

        schema, batches = rows
        with pyarrow.ipc.new_stream(channel, schema) as stream:
            for batch in batches:
                stream.write_batch(batch)
    channel.flush()


def main():
    channel = _channel()
    from . import _engines

    for line in sys.stdin.buffer:
        if not line.strip():
            continue
        request = json.loads(line)
        try:
            rows = _rows(_engines.run_local(request["engine"], request["setup"], request["sql"]))
        except ImportError as missing:
            _answer(channel, {"error": "missing", "message": str(missing)})
        except Exception as failed:
            _answer(channel, {"error": "failed", "message": str(failed)})
        else:
            _answer(channel, {"ok": True}, rows)


if __name__ == "__main__":
    main()
