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
}

# What belongs to a terminal, or to the process of a command, and has no meaning in a program.
TERMINAL = {"help", "version", "lake", "format", "yes"}
# A flag of a command that Python says some other way, by what says it there.
ELSEWHERE = {
    ("retain", "grace"): Lake.grace,
    ("sql", "file"): Lake.sql,
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


def test_every_verb_of_the_command_line_is_in_python(capfd):
    assert set(commands(capfd)) == set(FACES)


def test_it_reads_the_real_verbs(capfd):
    found = set(commands(capfd))
    assert ("merge",) in found and ("branch", "create") in found and len(found) >= 12


@pytest.mark.parametrize("command", FACES, ids=" ".join)
def test_every_option_of_a_verb_is_in_python(capfd, command):
    face, renamed = FACES[command]
    parameters = set(inspect.signature(face).parameters)
    for flag in flags(said(capfd, *command)):
        if (command[0], flag) in ELSEWHERE:
            continue
        name = renamed.get(flag, flag.replace("-", "_"))
        assert name in parameters, f"{' '.join(command)} --{flag} has no `{name}` in Python"


def test_what_is_said_elsewhere_is_there():
    assert all(callable(face) for face in ELSEWHERE.values())
