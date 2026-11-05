"""MetaLake: an open lakehouse table format whose metadata lives in Postgres."""

import os
import signal
import sys
from pathlib import Path

# Postgres travels inside the package, so it is found however the package was installed.
_POSTGRES = Path(__file__).parent / "postgres" / "bin"
if _POSTGRES.is_dir():
    os.environ.setdefault("METALAKE_POSTGRES_BIN", str(_POSTGRES))

from ._metalake import __version__, cli as _cli  # noqa: E402

__all__ = ["__version__"]


def _main():
    """The `metalake` command."""
    # Python would only note an interrupt, and nothing looks at that note while Rust runs the command.
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    sys.exit(_cli(["metalake", *sys.argv[1:]]))
