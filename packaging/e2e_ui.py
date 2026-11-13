"""What a person gets from `metalake ui` once MetaLake is installed, run in a lake's directory: the application, not the
page that says how to build it; its API, to the run's token alone, saying what the lake is; and the licenses of what the
frontend bundles. Then it stops (ADR 0007, milestones 5 and 6).

    python packaging/e2e_ui.py                        # the `metalake` on PATH, as a wheel installed it
    python packaging/e2e_ui.py target/debug/metalake  # a binary built from the checkout
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import urllib.error
import urllib.request

COMMAND = os.path.abspath(sys.argv[1]) if len(sys.argv) > 1 else shutil.which("metalake")
TOKEN = "e2e0wheel0e2e0wheel0e2e0wheel0e2e0wheel0"


def get(url: str, token: str | None = None) -> tuple[int, bytes]:
    request = urllib.request.Request(url)
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=10) as answer:
            return answer.status, answer.read()
    except urllib.error.HTTPError as refused:
        return refused.code, refused.read()


ui = subprocess.Popen(
    [COMMAND, "ui", "--no-browser"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    env={**os.environ, "METALAKE_UI_TOKEN": TOKEN},
)
try:
    # Its first line is the URL, the token in its fragment, once it listens.
    url = ui.stdout.readline().strip()
    assert url.startswith("http://127.0.0.1:") and url.endswith(f"/#token={TOKEN}"), url
    base = url.split("#", 1)[0]

    status, page = get(base)
    assert status == 200, f"{status}: the frontend was not embedded\n{page.decode(errors='replace')[:400]}"
    assert b'<div id="root">' in page, page[:400]

    assert get(base + "api/v1/version")[0] == 401, "the API answers without the token"
    status, version = get(base + "api/v1/version", TOKEN)
    assert status == 200 and json.loads(version) == {"api": 1}, (status, version)
    status, lake = get(base + "api/v1/status", TOKEN)
    assert status == 200 and any(r["name"] == "main" for r in json.loads(lake)["refs"]), (status, lake)

    status, licenses = get(base + "licenses.txt")
    assert status == 200 and b"Name: react" in licenses and b"License: MIT" in licenses, licenses[:400]
finally:
    # A signal reaches a process only where signals exist; elsewhere it is ended, which leaves nothing: it writes no file.
    if sys.platform == "win32":
        ui.terminate()
    else:
        ui.send_signal(signal.SIGINT)
    code = ui.wait(timeout=20)
if sys.platform != "win32":
    assert code == 0, f"exit {code}: {ui.stderr.read()}"

print("ui checks ok")
