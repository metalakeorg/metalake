"""What the REST Compatibility Kit's run said, from its JUnit reports: how many tests passed, failed and were skipped,
and the name of each that did not pass, as Markdown for the job's summary."""

import glob
import sys
import xml.etree.ElementTree as ET


def outcome(case):
    if case.find("failure") is not None or case.find("error") is not None:
        return "failed"
    if case.find("skipped") is not None:
        return "skipped"
    return "passed"


def main(reports):
    rows = []
    for path in glob.glob(f"{reports}/*.xml"):
        for case in ET.parse(path).getroot().iter("testcase"):
            rows.append((outcome(case), f"{case.get('classname')}.{case.get('name')}"))
    counts = {kind: sum(1 for row in rows if row[0] == kind) for kind in ("passed", "failed", "skipped")}
    print("## REST Compatibility Kit\n")
    print(", ".join(f"{count} {kind}" for kind, count in counts.items()) + "\n")
    for kind, name in sorted(rows):
        if kind != "passed":
            print(f"- {kind}: `{name}`")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "reports")
