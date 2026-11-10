"""MetaLake: an open lakehouse table format whose metadata lives in Postgres."""

import os
import signal
import sys
from pathlib import Path

# Postgres travels inside the package, so it is found however the package was installed.
_POSTGRES = Path(__file__).parent / "postgres" / "bin"
if _POSTGRES.is_dir():
    os.environ.setdefault("METALAKE_POSTGRES_BIN", str(_POSTGRES))

from ._metalake import (  # noqa: E402
    Batch,
    Commit,
    Done,
    Follower,
    Iceberg,
    IcebergTable,
    Lake,
    Merged,
    Ref,
    Result,
    Snapshot,
    Step,
    Status,
    Table,
    Writer,
    Written,
    __version__,
    connect,
    init,
    migrate,
    stop,
)
from ._metalake import cli as _cli  # noqa: E402
from .errors import ConflictError, MetaLakeError, UnknownOutcomeError, UsageError  # noqa: E402

__all__ = [
    "Batch",
    "Commit",
    "ConflictError",
    "Done",
    "Follower",
    "Iceberg",
    "IcebergTable",
    "Lake",
    "Merged",
    "MetaLakeError",
    "Ref",
    "Result",
    "Snapshot",
    "Step",
    "Status",
    "Table",
    "UnknownOutcomeError",
    "UsageError",
    "Writer",
    "Written",
    "__version__",
    "connect",
    "init",
    "migrate",
    "stop",
]


def _main():
    """The `metalake` command."""
    # Python would only note an interrupt, and nothing looks at that note while Rust runs the command.
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    sys.exit(_cli(["metalake", *sys.argv[1:]]))
