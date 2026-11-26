#!/usr/bin/env python3
"""Report the reference spool's state as the JSON snapshot the drill reads.

The drill never guesses what your pipeline's durable state looks like: it runs an
inspector you write and reads the snapshot. This is the reference inspector for
examples/spool/pipeline.py. It is read-only and safe to run against a live
pipeline; run it by hand too, it is the fastest way to see what a boundary means.

Shape (docs/contract.md):

    {
      "cursor":  {"<stream>": <lines consumed>},
      "pending": [{"digest", "records", "staged", "whole"}],   # owed to the lake
      "staged":  [{"digest", "bytes", "whole", "pending"}],     # published bundles
      "staging": [{"path", "bytes"}]                            # writes in flight
    }
"""

import hashlib
import json
import os
import sys

SPOOL = os.environ["DRILL_SPOOL_DIR"]


def digest_of(body):
    return "sha256:" + hashlib.sha256(body).hexdigest()


def cursor():
    try:
        with open(os.path.join(SPOOL, "cursor.json"), "rb") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def journal():
    """The receipt journal, or None when a line will not parse. Missing is empty;
    corrupt is None. Conflating the two would let a torn journal read as nothing
    pending, which is the one lie a crash drill must never tell."""
    try:
        with open(os.path.join(SPOOL, "receipts.log"), "rb") as f:
            lines = f.read().split(b"\n")
    except FileNotFoundError:
        return []
    records = []
    for line in lines:
        if not line:
            continue
        try:
            records.append(json.loads(line.decode()))
        except ValueError:
            # A half-written journal line is exactly the corruption the drill is
            # looking for. Never swallow it into "nothing pending".
            print("unparseable receipt line: %r" % line, file=sys.stderr)
            return None
    return records


def pending(records):
    owed = {}
    for record in records:
        if record["op"] == "enqueue":
            owed[record["digest"]] = record.get("records", 0)
        elif record["op"] == "ack":
            owed.pop(record["digest"], None)
    return owed


def staged_files():
    root = os.path.join(SPOOL, "staged")
    try:
        names = os.listdir(root)
    except FileNotFoundError:
        return [], []
    published, in_flight = [], []
    for name in sorted(names):
        path = os.path.join(root, name)
        try:
            with open(path, "rb") as f:
                body = f.read()
        except FileNotFoundError:
            continue
        if ".tmp." in name:
            in_flight.append({"path": name, "bytes": len(body)})
            continue
        digest = "sha256:" + name
        published.append({"digest": digest, "bytes": len(body), "whole": digest_of(body) == digest})
    return published, in_flight


def main():
    records = journal()
    if records is None:
        print("the receipt journal has an unparseable line", file=sys.stderr)
        return 1
    owed = pending(records)
    published, in_flight = staged_files()
    by_digest = {entry["digest"]: entry for entry in published}
    snapshot = {
        "cursor": cursor(),
        "pending": [
            {
                "digest": digest,
                "records": count,
                "staged": digest in by_digest,
                "whole": bool(by_digest.get(digest, {}).get("whole")),
            }
            for digest, count in owed.items()
        ],
        "staged": [dict(entry, pending=entry["digest"] in owed) for entry in published],
        "staging": in_flight,
    }
    json.dump(snapshot, sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
