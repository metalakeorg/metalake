"""The two faces offer the same: every verb of the command line, and every option of one, exists in Python.

This is read from the command itself, so a verb or a flag added there and forgotten here fails by its name. That they do
the same is `metalake-client`'s to hold. Python's own verbs, `append` and the others, have no command, on purpose.
"""

import inspect
import re

import pytest

import metalake

Lake = metalake.Lake

# What a command is in Python, with the name each flag has there where it differs. A command with verbs of its own is
# listed by them.
FACES = {
    ("init",): (metalake.init, {}),
    ("status",): (Lake.status, {}),
    ("sql",): (Lake.sql, {}),
    ("refs",): (Lake.refs, {}),
    ("branch", "list"): (Lake.refs, {}),
    ("branch", "create"): (Lake.branch, {"from": "start"}),
    ("branch", "delete"): (Lake.delete_branch, {}),
    ("tag", "list"): (Lake.refs, {}),
    ("tag", "create"): (Lake.tag, {"from": "start"}),
    ("tag", "delete"): (Lake.delete_tag, {}),
    ("merge",): (Lake.merge, {"table": "tables"}),
    ("log",): (Lake.log, {}),
    ("migrate",): (metalake.migrate, {}),
    ("retain",): (Lake.retain, {}),
    ("maintain",): (Lake.maintain, {"ref": "on"}),
    ("stop",): (metalake.stop, {}),
    ("iceberg", "on-commit"): (metalake.Iceberg.on_commit, {"ref": "on"}),
    ("iceberg", "on-load"): (metalake.Iceberg.on_load, {"ref": "on"}),
    ("iceberg", "status"): (metalake.Iceberg.status, {"ref": "on"}),
    ("changes",): (metalake.Table.changes, {}),
    ("follow",): (metalake.Table.follow, {"latest": "start"}),
    ("ingest",): (metalake.Table.writer, {}),
    ("pipeline", "create"): (Lake.create_pipeline, {}),
    ("pipeline", "list"): (Lake.pipelines, {}),
    ("pipeline", "describe"): (Lake.pipeline, {}),
    ("pipeline", "suspend"): (metalake.Pipeline.suspend, {}),
    ("pipeline", "resume"): (metalake.Pipeline.resume, {}),
    ("pipeline", "drop"): (metalake.Pipeline.drop, {}),
    ("iceberg", "stop"): (metalake.Iceberg.stop, {}),
    ("iceberg", "connect"): (metalake.Iceberg.properties, {}),
}

# What belongs to a terminal, or to the process of a command, and has no meaning in a program.
TERMINAL = {"help", "version", "lake", "format", "yes"}
# A command that is a process in the foreground, which a program asks for otherwise: `lake.iceberg.uri` starts the
# lake's server.
FOREGROUND = {("iceberg", "serve")}
# A flag of a command that Python says some other way, by what says it there.
ELSEWHERE = {
    ("retain", "grace"): Lake.grace,
    ("sql", "file"): Lake.sql,
    # A program keeps the position it reached itself, and gives it back to `follow(after=)`.
    ("follow", "position-file"): metalake.Table.follow,
    # Text lines are the terminal's: a program gives Arrow, each write with its version.
    ("ingest", "input"): metalake.Writer.write,
    ("ingest", "version-column"): metalake.Writer.write,
    ("ingest", "batch-lines"): metalake.Writer.write,
    # A program has its query in hand: from a file, it reads it itself.
    ("pipeline", "file"): Lake.create_pipeline,
}


def said(capfd, *args):
    capfd.readouterr()
    assert metalake._cli(["metalake", *args, "--help"]) == 0
    return capfd.readouterr().out


def verbs(text):
    listed = text.split("Commands:")[1].split("\n\n")[0] if "Commands:" in text else ""
    names = [line.split()[0] for line in listed.splitlines() if line.strip()]
    return [name for name in names if name != "help"]


def flags(text):
    return set(re.findall(r"--([a-z][a-z-]*)", text)) - TERMINAL


def commands(capfd):
    for verb in verbs(said(capfd)):
        below = verbs(said(capfd, verb))
        if below:
            yield from ((verb, name) for name in below)
        else:
            yield (verb,)


def test_cli_verbs_in_python(capfd):
    assert set(commands(capfd)) - FOREGROUND == set(FACES)


def test_cli_verb_discovery(capfd):
    found = set(commands(capfd))
    assert ("merge",) in found and ("branch", "create") in found and len(found) >= 12


@pytest.mark.parametrize("command", FACES, ids=" ".join)
def test_cli_options_in_python(capfd, command):
    face, renamed = FACES[command]
    parameters = set(inspect.signature(face).parameters)
    for flag in flags(said(capfd, *command)):
        if (command[0], flag) in ELSEWHERE:
            continue
        name = renamed.get(flag, flag.replace("-", "_"))
        assert name in parameters, f"{' '.join(command)} --{flag} has no `{name}` in Python"


def test_cli_aliases_callable():
    assert all(callable(face) for face in ELSEWHERE.values())
