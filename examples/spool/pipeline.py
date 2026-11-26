#!/usr/bin/env python3
"""The reference pipeline under test: watch a directory, spool to disk, ship to the lake.

This is deliberately small (stdlib only, no compression, no retry policy beyond a
backoff) and deliberately real: every durable step is ordered so that a SIGKILL at
any instant leaves either the old state or the new state, never a receipt without
its bytes. The drill kills it; see docs/contract.md for the knobs it reads and
docs/checklist.md for the boundaries it is killed at.

Durable state (all under DRILL_SPOOL_DIR):

    cursor.json     {"<stream>": <lines consumed>}   atomic replace
    receipts.log    one JSON object per line, append-only, fsynced per append
                    {"op":"enqueue","digest":..,"records":k}  "the lake is owed this bundle"
                    {"op":"ack","digest":..}                  "the lake has it"
    staged/<hex>    the bundle body, named by the sha256 of its own bytes

The order that makes the whole thing survivable: stage bytes (temp file, fsync,
rename, dir fsync) -> commit the receipt -> deliver -> commit the ack. A crash
before the receipt costs the batch, which is stated loss (the cursor already
covers those lines). A crash after it costs nothing.
"""

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request

CHUNK = 64 * 1024
CURSOR = "cursor.json"
RECEIPTS = "receipts.log"
STAGED = "staged"


def sha(body):
    return "sha256:" + hashlib.sha256(body).hexdigest()


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def frame(stream, generation, offset, raw):
    """Identity (stream, generation, offset), a tab, the record's own bytes, a newline.

    The identity is what makes a replay harmless: the same identity carrying the
    same bytes is the same record, whichever bundle window it arrives in, so a
    merged export collapses it. The trailing newline is
    what makes a bundle self-delimiting, and therefore what makes 'torn' a
    checkable property: a bundle either ends on a frame boundary or it is not a
    bundle. Multi-line records are out of scope here; a real pipeline must travel
    them whole rather than splitting them across frames."""
    return b"%s\t%d\t%d\t%s\n" % (stream.encode(), generation, offset, raw)


def read_cursor(spool):
    path = os.path.join(spool, CURSOR)
    try:
        with open(path, "rb") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_cursor(spool, cursor):
    tmp = os.path.join(spool, CURSOR + ".tmp")
    with open(tmp, "wb") as f:
        f.write(json.dumps(cursor, sort_keys=True).encode())
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, os.path.join(spool, CURSOR))
    fsync_dir(spool)


def read_receipts(spool):
    """The whole journal, in order. A line that will not parse fails loudly: a
    silently skipped receipt is the bug this pipeline exists to not have."""
    path = os.path.join(spool, RECEIPTS)
    try:
        with open(path, "rb") as f:
            lines = f.read().split(b"\n")
    except FileNotFoundError:
        return []
    out = []
    for line in lines:
        if not line:
            continue
        out.append(json.loads(line.decode()))
    return out


def commit_receipt(spool, record):
    """One write(2) of one line, fsynced. A single append cannot tear into a
    half line under SIGKILL, which is why the journal is line-delimited JSON."""
    line = (json.dumps(record, sort_keys=True) + "\n").encode()
    fd = os.open(os.path.join(spool, RECEIPTS), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)


def pending(receipts):
    """Digests the lake is owed, oldest first, with how many records each holds."""
    owed = {}
    for record in receipts:
        if record["op"] == "enqueue":
            owed[record["digest"]] = record["records"]
        elif record["op"] == "ack":
            owed.pop(record["digest"], None)
    return owed


def staged_path(spool, digest):
    return os.path.join(spool, STAGED, digest.split(":", 1)[1])


def stage(spool, digest, body, slow_write_ms, publish_direct):
    """Publish the bundle's bytes before any receipt names them.

    `publish_direct` is the sabotage: write straight to the content-addressed
    name instead of a temp file plus rename, so a kill mid-write leaves a torn
    bundle at a path that claims to be whole. `slow_write_ms` widens the window
    so the drill can land its kill inside the write."""
    final = staged_path(spool, digest)
    if os.path.exists(final):
        return
    os.makedirs(os.path.dirname(final), mode=0o700, exist_ok=True)
    target = final if publish_direct else final + ".tmp.%d" % os.getpid()
    with open(target, "wb") as f:
        for start in range(0, len(body), CHUNK):
            f.write(body[start : start + CHUNK])
            f.flush()
            if slow_write_ms:
                time.sleep(slow_write_ms / 1000.0)
        os.fsync(f.fileno())
    if not publish_direct:
        os.replace(target, final)
    fsync_dir(os.path.dirname(final))


def post(lake, digest, body):
    """Deliver one bundle. True only on a 2xx whose digest is our own digest: an
    ack for something else leaves the receipt pending."""
    request = urllib.request.Request(
        lake + "/bundles",
        data=body,
        method="POST",
        headers={"Content-Type": "application/octet-stream", "X-Digest": digest},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status != 200:
                return False
            ack = json.loads(response.read() or b"{}")
    except (urllib.error.URLError, OSError, ValueError):
        return False
    return ack.get("digest") == digest


class Backoff:
    """Transient failures retry forever: a crash drill that gives up on a dropped
    upload would make the mid-delivery boundary untestable."""

    def __init__(self):
        self.next_try = 0.0
        self.wait = 0.1

    def failed(self):
        self.next_try = time.monotonic() + self.wait
        self.wait = min(self.wait * 2, 1.0)

    def ok(self):
        self.wait = 0.1

    def ready(self):
        return time.monotonic() >= self.next_try


def deliver(spool, lake, receipts, faults, backoff, ack_early):
    """Ship the oldest pending receipt, then the next, until one fails."""
    for digest in pending(receipts):
        if not backoff.ready():
            return
        path = staged_path(spool, digest)
        try:
            with open(path, "rb") as f:
                body = f.read()
        except FileNotFoundError:
            # A receipt with no bytes cannot be delivered. Say so and stop: this is
            # the failure the row-first sabotage produces, and the drill's job is to
            # see it, not to hide it behind a retry loop.
            print("receipt %s has no staged bytes" % digest, file=sys.stderr, flush=True)
            return
        if ack_early:
            # Sabotage: the ack is committed on the intention to deliver, not on the
            # lake's answer. Every dropped upload becomes silent loss.
            commit_receipt(spool, {"op": "ack", "digest": digest})
            receipts.append({"op": "ack", "digest": digest})
            os.unlink(path)
            post(lake, digest, body)
            continue
        if not post(lake, digest, body):
            backoff.failed()
            return
        backoff.ok()
        hold = faults.get("ack-hold")
        if hold:
            time.sleep(hold / 1000.0)
        # The ack is the last durable step, and it comes before the bytes are
        # dropped: a kill between them replays a bundle the lake already has,
        # which the lake absorbs by digest.
        commit_receipt(spool, {"op": "ack", "digest": digest})
        receipts.append({"op": "ack", "digest": digest})
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def read_new_lines(watch, consumed):
    """Whole lines past the cursor. A trailing partial line is held, never
    consumed: half a record is not a record.

    Reads the file whole on every poll, which is fine at drill sizes: a real tailer
    keeps a byte offset and reads forward, but that is a performance decision, not
    a durability one, and it is not what this example is demonstrating."""
    path = os.path.join(watch, "session.jsonl")
    try:
        with open(path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return []
    end = data.rfind(b"\n")
    if end < 0:
        return []
    lines = data[: end + 1].split(b"\n")[:-1]
    return lines[consumed:]


def parse_faults(text):
    faults = {}
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, value = item.partition(":")
        faults[name] = int(value or 0)
    return faults


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--row-first", action="store_true", help="sabotage: commit the receipt before staging the bytes")
    parser.add_argument("--publish-direct", action="store_true", help="sabotage: write the bundle at its published name instead of a temp file plus rename")
    parser.add_argument("--ack-early", action="store_true", help="sabotage: commit the ack before the lake answers")
    parser.add_argument("--forget-outbox", action="store_true", help="sabotage: start with an empty receipt journal")
    args = parser.parse_args()

    watch = os.environ["DRILL_WATCH_DIR"]
    spool = os.environ["DRILL_SPOOL_DIR"]
    lake = os.environ["DRILL_LAKE_URL"].rstrip("/")
    hold_ms = int(os.environ.get("DRILL_BATCH_HOLD_MS", "20"))
    poll_ms = int(os.environ.get("DRILL_POLL_MS", "10"))
    max_records = int(os.environ.get("DRILL_BATCH_MAX_RECORDS", "100000"))
    faults = parse_faults(os.environ.get("DRILL_FAULT", ""))
    slow_write_ms = faults.get("slow-write", 0)

    os.makedirs(os.path.join(spool, STAGED), mode=0o700, exist_ok=True)
    # A crashed run leaves its own temp file behind; nothing may publish it.
    for name in os.listdir(os.path.join(spool, STAGED)):
        if ".tmp." in name:
            os.unlink(os.path.join(spool, STAGED, name))

    stream, generation = "s:session", 0
    cursor = read_cursor(spool)
    consumed = int(cursor.get(stream, 0))
    if args.forget_outbox:
        # Sabotage: the restart does not remember what the outbox held.
        open(os.path.join(spool, RECEIPTS), "wb").close()
    receipts = read_receipts(spool)
    batch = []
    batch_started = None
    backoff = Backoff()

    while True:
        lines = read_new_lines(watch, consumed)
        if lines:
            # The cursor is saved on read return, before the batch is durable: that
            # ordering is what makes post-emit-pre-batch a loss window instead of a
            # duplicate window. The drill asserts the loss, it does not deny it.
            batch.extend(frame(stream, generation, consumed + index, raw) for index, raw in enumerate(lines))
            consumed += len(lines)
            cursor[stream] = consumed
            save_cursor(spool, cursor)
            if batch_started is None:
                batch_started = time.monotonic()

        flush = bool(batch) and (
            len(batch) >= max_records or (time.monotonic() - batch_started) * 1000 >= hold_ms
        )
        if flush:
            body = b"".join(batch)
            digest = sha(body)
            if args.row_first:
                commit_receipt(spool, {"op": "enqueue", "digest": digest, "records": len(batch)})
                receipts.append({"op": "enqueue", "digest": digest, "records": len(batch)})
                stage(spool, digest, body, slow_write_ms, args.publish_direct)
            else:
                stage(spool, digest, body, slow_write_ms, args.publish_direct)
                commit_receipt(spool, {"op": "enqueue", "digest": digest, "records": len(batch)})
                receipts.append({"op": "enqueue", "digest": digest, "records": len(batch)})
            batch = []
            batch_started = None

        deliver(spool, lake, receipts, faults, backoff, args.ack_early)
        time.sleep(poll_ms / 1000.0)


if __name__ == "__main__":
    main()
