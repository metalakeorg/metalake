"""What the REST Compatibility Kit's run said, from its JUnit reports, held to the failures MetaLake declares
(expected.txt): how many tests passed, failed and were skipped, as Markdown for the job's summary, and an exit code that
gates the run. It fails on a failure not declared, on a declared failure that passed, and on a run with no tests."""

import glob
import os
import sys
import xml.etree.ElementTree as ET


def outcome(case):
    if case.find("failure") is not None or case.find("error") is not None:
        return "failed"
    if case.find("skipped") is not None:
        return "skipped"
    return "passed"


def outcomes(reports):
    """Each test of the kit's class, by its name as JUnit reports it, and how it ended."""
    found = {}
    for path in glob.glob(f"{reports}/*.xml"):
        for case in ET.parse(path).getroot().iter("testcase"):
            found[case.get("name")] = outcome(case)
    return found


def declared(path):
    """The tests MetaLake fails on purpose, each with its rule."""
    rules = {}
    with open(path, encoding="utf-8") as listed:
        for line in listed:
            if line.strip() and not line.startswith("#"):
                test, rule = line.split()
                rules[test] = rule
    return rules


def held(found, rules):
    """What breaks the declaration: failures not declared, and declared failures that did not fail."""
    undeclared = sorted(t for t, o in found.items() if o == "failed" and t not in rules)
    stale = sorted(t for t in rules if found.get(t) != "failed")
    return undeclared, stale


def main(reports, expected):
    found = outcomes(reports)
    rules = declared(expected)
    undeclared, stale = held(found, rules)
    counts = {kind: sum(1 for o in found.values() if o == kind) for kind in ("passed", "failed", "skipped")}
    print("## REST Compatibility Kit\n")
    print(", ".join(f"{count} {kind}" for kind, count in counts.items()) + f"; {len(rules)} failures declared\n")
    for test in undeclared:
        print(f"- failed, not declared: `{test}`")
    for test in stale:
        print(f"- declared ({rules[test]}), but {found.get(test, 'not run')}: `{test}`")
    if not found:
        print("- no test ran")
    return 0 if found and not undeclared and not stale else 1


if __name__ == "__main__":
    reports = sys.argv[1] if len(sys.argv) > 1 else "reports"
    expected = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(__file__), "expected.txt")
    sys.exit(main(reports, expected))
