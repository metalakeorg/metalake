#!/usr/bin/env python3
"""Prepares the Postgres a wheel carries: fetches one target's binaries, checks them, and keeps what a lake runs on.

    python packaging/postgres.py x86_64-pc-windows-msvc

The result goes to the wheel's data directory, which `crates/metalake-cli/pyproject.toml` names. pip installs it under
`share/metalake/postgres` of the environment, where `metalake` finds it from its own path.
"""

import hashlib
import shutil
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

VERSION = "17.11.1"
RELEASES = "https://github.com/theseus-rs/postgresql-binaries/releases/download"
OUT = Path(__file__).parent.parent / "crates/metalake-cli/metalake.data/data/share/metalake/postgres"
# The tools a lake runs. Everything else in `bin` is not, apart from what they link.
TOOLS = ("postgres", "initdb", "pg_ctl")
# Nothing a running server opens.
DROPPED = ("include", "lib/pgxs", "lib/pkgconfig", "lib/cmake", "lib/bitcode")
# Static libraries and debug symbols; the JIT, which a lake turns off and which is most of the size where it is built;
# and an embedded Python, for functions written in it.
DROPPED_FILES = ("*.a", "*.lib", "*.pdb", "libLLVM*", "llvmjit*", "plpython*", "python*", "Python", "libpython*")


def fetch(url):
    with urllib.request.urlopen(url, timeout=300) as response:
        return response.read()


def main(target):
    name = f"postgresql-{VERSION}-{target}.tar.gz"
    archive = fetch(f"{RELEASES}/{VERSION}/{name}")
    expected = fetch(f"{RELEASES}/{VERSION}/{name}.sha256").decode().split()[0]
    actual = hashlib.sha256(archive).hexdigest()
    if actual != expected:
        sys.exit(f"{name}: sha256 {actual}, expected {expected}")

    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / name
        path.write_bytes(archive)
        with tarfile.open(path) as tar:
            tar.extractall(temporary, filter="tar")
        root = next(p for p in Path(temporary).iterdir() if p.is_dir())
        if OUT.exists():
            shutil.rmtree(OUT)
        shutil.copytree(root, OUT, symlinks=False)

    for directory in DROPPED:
        shutil.rmtree(OUT / directory, ignore_errors=True)
    for path in (OUT / "bin").iterdir():
        if path.is_dir():
            shutil.rmtree(path)
        elif path.suffix not in (".dll", ".so", ".dylib") and path.stem not in TOOLS:
            path.unlink()
    for pattern in DROPPED_FILES:
        for path in OUT.rglob(pattern):
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()
    size = sum(p.stat().st_size for p in OUT.rglob("*") if p.is_file())
    print(f"{OUT}: {size // 1048576} MB")


if __name__ == "__main__":
    main(sys.argv[1])
