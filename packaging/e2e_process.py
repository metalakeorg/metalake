"""What a shell and a program expect of the `metalake` command, on a lake made in the current directory."""

import shutil
import signal
import subprocess
import sys
import time

LONG = "select count(*) from generate_series(1, 400000) a, generate_series(1, 400000) b"
COMMAND = shutil.which("metalake")

# A server started by a command must not hold the pipe of whoever reads that command's output.
subprocess.run([COMMAND, "stop"], check=True, capture_output=True, timeout=60)
done = subprocess.run([COMMAND, "status"], check=True, capture_output=True, text=True, timeout=120)
assert "warehouse" in done.stdout, done.stdout

# An interrupt ends a command at work. A signal reaches a process only where signals exist.
if sys.platform != "win32":
    running = subprocess.Popen([COMMAND, "sql", LONG], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)
    assert running.poll() is None, "the statement is meant to outlast the wait"
    running.send_signal(signal.SIGINT)
    running.wait(timeout=20)

print("process checks ok")
