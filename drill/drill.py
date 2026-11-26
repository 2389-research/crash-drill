#!/usr/bin/env python3
"""The crash drill: SIGKILL a pipeline at a named durability boundary, restart it,
and verify the spool against the contract that boundary claims.

Nothing here knows your language. The pipeline is a command (--pipeline), its
spool state is a JSON snapshot from a command you write (--inspect), and the lake
is a scripted ack server this process runs. The boundaries are config
(--boundaries), not code, so adding a boundary is a JSON row, not a patch.

The rule the whole thing rests on: a kill counts only when the harness has
already *observed* the window being open. A drill that kills at an arbitrary
moment and asserts "no loss" is a test that cannot fail.
"""

import argparse
import hashlib
import http.server
import json
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

DEADLINE = 40.0
QUIET = 0.4
POLL = 0.01
LOG_TAIL = 25


# ---------------------------------------------------------------------------------
# The lake: a scripted ack server. Modes are a *fault*, not a pipeline change: the
# pipeline under test always speaks its real protocol to it.
# ---------------------------------------------------------------------------------


class Lake:
    """Stores bundles by digest and acks them. `drop` accepts the request and closes
    the connection without reading the body, which is the mid-upload crash window.
    `dead` is not a mode: see dead_url()."""

    def __init__(self):
        self.mode = "ack"
        self.stored = {}
        self.uploads = 0
        self.dropped = 0
        self.lock = threading.Lock()
        self.httpd = None
        self.url = None

    def start(self, mode):
        self.mode = mode
        lake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                if lake.mode == "drop":
                    with lake.lock:
                        lake.dropped += 1
                    # Return without reading the body: the socket closes while the
                    # pipeline is still writing, so its upload fails where it stands.
                    self.close_connection = True
                    return
                body = self.rfile.read(length)
                digest = self.headers.get("X-Digest") or ""
                if "sha256:" + hashlib.sha256(body).hexdigest() != digest:
                    self.send_response(400)
                    self.send_header("Content-Length", "0")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    return
                with lake.lock:
                    lake.stored.setdefault(digest, body)
                    lake.uploads += 1
                payload = json.dumps({"digest": digest, "stored": True}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(payload)
                self.close_connection = True

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:%d" % self.httpd.server_address[1]
        return self

    def stop(self):
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None

    def digests(self):
        with self.lock:
            return sorted(self.stored)

    def export(self):
        """Every stored bundle merged: identity -> raw record, and the conflicts.

        A record replayed in a different bundle window is the same record and must
        carry the same bytes. Identity with different bytes is a disagreement, and
        the drill fails on it: that is what an envelope bug looks like from here.
        """
        merged = {}
        conflicts = []
        with self.lock:
            bodies = list(self.stored.values())
        for body in bodies:
            for stream, generation, offset, raw in parse_bundle(body):
                key = (stream, generation, offset)
                if key in merged and merged[key] != raw:
                    conflicts.append("%s/%d/%d" % key)
                merged[key] = raw
        return merged, conflicts

    def offsets(self):
        merged, _ = self.export()
        return sorted(offset for _stream, _generation, offset in merged)


def parse_bundle(body):
    """The reference frame: `<stream>\\t<generation>\\t<offset>\\t<raw record>`.

    A bundle that does not end on a frame boundary is torn, and saying so here is
    half the point of the drill: a torn bundle must never reach the lake.
    """
    if body and not body.endswith(b"\n"):
        raise ValueError("bundle does not end on a frame boundary (%d bytes)" % len(body))
    frames = []
    for chunk in body.split(b"\n")[:-1]:
        parts = chunk.split(b"\t", 3)
        if len(parts) != 4:
            raise ValueError("frame is not <stream>\\t<gen>\\t<offset>\\t<raw>: %r" % chunk[:80])
        frames.append((parts[0].decode(), int(parts[1]), int(parts[2]), parts[3]))
    return frames


def dead_url():
    """A port nothing listens on: the pipeline's delivery is refused, so a committed
    receipt stays pending with nothing delivered -- the pre-send window, held open."""
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return "http://127.0.0.1:%d" % port


# ---------------------------------------------------------------------------------
# The spool, as the pipeline's own inspector reports it.
# ---------------------------------------------------------------------------------


def snapshot(inspect_cmd, env):
    """One read of the spool. A failed read is a drill error, never an empty spool:
    swallowing a transient read error into "nothing pending" is the flake that
    makes a crash test lie."""
    result = subprocess.run(shlex.split(inspect_cmd), env=env, capture_output=True, timeout=30)
    if result.returncode != 0:
        raise DrillError(
            "the inspector failed (%d): %s" % (result.returncode, result.stderr.decode(errors="replace").strip())
        )
    try:
        snap = json.loads(result.stdout or b"{}")
    except ValueError as error:
        raise DrillError("the inspector printed no JSON: %s (%r)" % (error, result.stdout[:200]))
    for key in ("cursor", "pending", "staged", "staging"):
        snap.setdefault(key, {} if key == "cursor" else [])
    return snap


def cursor_total(snap):
    return sum(int(value) for value in snap["cursor"].values())


# ---------------------------------------------------------------------------------
# The windows: when is the pipeline *inside* the boundary? One named predicate each.
# ---------------------------------------------------------------------------------


def window_post_emit_pre_batch(snap, lake, row):
    """Read and cursor-covered, held in memory, nothing durable yet."""
    total = row["append"]["from"] + row["append"]["count"]
    return (
        cursor_total(snap) >= total
        and not snap["pending"]
        and not snap["staged"]
        and not snap["staging"]
        and not lake.digests()
    )


def window_post_batch_pre_send(snap, lake, row):
    """Bytes staged and the receipt committed, nothing delivered."""
    return bool(snap["pending"]) and not lake.digests()


def window_post_send_pre_ack(snap, lake, row):
    """An upload was made and the lake refused it or never answered."""
    return lake.dropped >= 1 and bool(snap["pending"]) and not lake.digests()


def window_post_ack_pre_cursor(snap, lake, row):
    """The lake stored the digest and the receipt is still pending: the ack landed,
    the pipeline has not yet committed it. Needs a fault hook to hold open."""
    stored = set(lake.digests())
    return any(entry["digest"] in stored for entry in snap["pending"])


def window_staged_write_in_progress(snap, lake, row):
    """A bundle's bytes are half written: a temp file with bytes in it, or -- for a
    pipeline that publishes before it finishes -- a file that does not hash to its
    own name."""
    return any(entry["bytes"] > 0 for entry in snap["staging"]) or any(not entry["whole"] for entry in snap["staged"])


WINDOWS = {
    "post_emit_pre_batch": window_post_emit_pre_batch,
    "post_batch_pre_send": window_post_batch_pre_send,
    "post_send_pre_ack": window_post_send_pre_ack,
    "post_ack_pre_cursor": window_post_ack_pre_cursor,
    "staged_write_in_progress": window_staged_write_in_progress,
}


# ---------------------------------------------------------------------------------
# Assertions: what must hold at the kill, and what must hold once it has restarted.
# ---------------------------------------------------------------------------------


def problems_at_kill(snap):
    """The spool's own invariants, asserted the instant the pipeline dies.

    A receipt always has its bytes, and its bytes are whole. Both are ordering
    claims, and both are cheap to check from outside, which is why the drill does
    not need to trust the pipeline's word for it.
    """
    found = []
    for entry in snap["pending"]:
        if not entry.get("staged"):
            found.append("receipt %s is owed but has no staged bytes" % entry["digest"])
        elif not entry.get("whole"):
            found.append("receipt %s names bytes that do not hash to it" % entry["digest"])
    for entry in snap["staged"]:
        if not entry.get("whole"):
            found.append("bundle %s is published but does not hash to its own name (torn)" % entry["digest"])
    return found


def problems_at_settle(snap, lake, row):
    """No loss, no duplication, nothing left owed."""
    found = []
    try:
        merged, conflicts = lake.export()
    except ValueError as error:
        return ["the lake holds a bundle that does not parse: %s" % error]
    if conflicts:
        found.append("the same identity arrived with different bytes (envelope disagreement): %s" % ", ".join(conflicts))
    wanted = list(range(row["expect"]["offsets"][0], row["expect"]["offsets"][1]))
    got = sorted(offset for _stream, _generation, offset in merged)
    if got != wanted:
        found.append("the lake's merged export holds offsets %s, wanted %s" % (compact(got), compact(wanted)))
    if snap["pending"]:
        found.append("%d receipt(s) still owed after the restart: %s" % (len(snap["pending"]), [e["digest"] for e in snap["pending"]]))
    if "bundles" in row["expect"]:
        distinct = len(lake.digests())
        if distinct != row["expect"]["bundles"]:
            found.append("the lake stored %d distinct bundles, wanted %d" % (distinct, row["expect"]["bundles"]))
    if "uploads_min" in row["expect"]:
        if lake.uploads < row["expect"]["uploads_min"]:
            found.append(
                "the lake acked %d uploads, wanted at least %d: the replay the window predicts never happened"
                % (lake.uploads, row["expect"]["uploads_min"])
            )
    return found


def compact(offsets):
    """0..3,7 as a range string, so a failure names the hole."""
    if not offsets:
        return "[]"
    runs = []
    start = previous = offsets[0]
    for offset in offsets[1:]:
        if offset == previous + 1:
            previous = offset
            continue
        runs.append((start, previous))
        start = previous = offset
    runs.append((start, previous))
    return "[" + ", ".join(str(a) if a == b else "%d..%d" % (a, b) for a, b in runs) + "]"


# ---------------------------------------------------------------------------------
# The driver: one boundary, one kill, one restart, one verdict.
# ---------------------------------------------------------------------------------


class DrillError(Exception):
    pass


class Child:
    """The pipeline process. SIGKILL, never SIGTERM: a caught signal lets the
    pipeline tidy up, and the window being tested is the untidy one."""

    def __init__(self, argv, env, log_path):
        self.log_path = log_path
        log = open(log_path, "wb")
        self.proc = subprocess.Popen(argv, env=env, stdout=log, stderr=log, start_new_session=True)
        log.close()

    def alive(self):
        return self.proc.poll() is None

    def sigkill(self):
        if self.proc.poll() is None:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
        status = self.proc.wait()
        if status != -signal.SIGKILL:
            raise DrillError(
                "the pipeline was not killed by SIGKILL (exit %s). A pipeline that died on its own "
                "makes the boundary the drill claims to hit a fiction.\n%s" % (status, self.log_tail())
            )

    def log_tail(self):
        try:
            with open(self.log_path, "rb") as f:
                text = f.read().decode(errors="replace")
        except OSError:
            return ""
        return "\n".join(text.splitlines()[-LOG_TAIL:])


def wait_until(what, child, reachable, deadline=DEADLINE):
    """A deadline decides, a sleep never does. Fails loudly, with the child's log,
    if the window never opens or the pipeline dies first."""
    end = time.monotonic() + deadline
    while True:
        if reachable():
            return
        if not child.alive():
            raise DrillError(
                "the pipeline exited before %s, so the drill never reached the window.\n%s" % (what, child.log_tail())
            )
        if time.monotonic() > end:
            raise DrillError(
                "%s was not reached within %.0fs. Either the boundary row's knobs do not open the "
                "window, or the pipeline never gets there.\n%s" % (what, deadline, child.log_tail())
            )
        time.sleep(POLL)


def append_records(watch, spec):
    """Append whole lines the drill owns, so the expected identity set is known
    without asking the pipeline what it thinks it read."""
    filler = ""
    if spec.get("pad"):
        # Barely compressible filler: the upload is real bytes on the wire, so a
        # lake that closes early really does break the client's write.
        state = 0x2545F4914F6CDD1D
        letters = []
        for _ in range(spec["pad"]):
            state = (state ^ (state << 13)) & 0xFFFFFFFFFFFFFFFF
            state ^= state >> 7
            state = (state ^ (state << 17)) & 0xFFFFFFFFFFFFFFFF
            letters.append(chr(ord("a") + state % 26))
        filler = "".join(letters)
    lines = []
    for index in range(spec["from"], spec["from"] + spec["count"]):
        record = {"ts": 1700000000000 + index, "n": index}
        if filler:
            record["d"] = filler
        lines.append(json.dumps(record) + "\n")
    with open(os.path.join(watch, "session.jsonl"), "a") as f:
        f.write("".join(lines))


def lake_state(lake):
    return {"bundles": len(lake.digests()), "uploads": lake.uploads, "dropped": lake.dropped, "offsets": lake.offsets()}


def run_boundary(row, args, root):
    """Spawn, enter the window, kill, read the spool, restart, verify."""
    watch = os.path.join(root, "watch")
    spool = os.path.join(root, "spool")
    os.makedirs(watch)
    os.makedirs(spool)
    log = os.path.join(root, "pipeline.log")

    lake = Lake()
    if row.get("lake", "ack") == "dead":
        url = dead_url()
    else:
        lake.start(row.get("lake", "ack"))
        url = lake.url

    env = dict(os.environ)
    env.update({"DRILL_WATCH_DIR": watch, "DRILL_SPOOL_DIR": spool, "DRILL_LAKE_URL": url})
    env.update({key: str(value) for key, value in row.get("env", {}).items()})
    argv = shlex.split(args.pipeline)

    append_records(watch, row["append"])
    child = Child(argv, env, log)
    try:
        reached = lambda: WINDOWS[row["window"]](snapshot(args.inspect, env), lake, row)
        wait_until(row["window"], child, reached, args.deadline)
        at_kill = snapshot(args.inspect, env)
        child.sigkill()
        lake_at_kill = lake_state(lake)

        found = problems_at_kill(at_kill)
        if found:
            return verdict(row, False, found, at_kill, lake_at_kill, lake_state(lake), "at the kill")

        # Restart against the same spool. Anything appended now is the drill's
        # proof that the restarted pipeline is working, not stalled.
        restart_env = dict(env)
        restart_env.update({key: str(value) for key, value in row.get("restart_env", {}).items()})
        if row.get("append_after"):
            append_records(watch, row["append_after"])
        if row.get("lake", "ack") != "dead":
            lake.mode = "ack"
        else:
            lake.stop()
            lake = Lake().start("ack")
            restart_env["DRILL_LAKE_URL"] = lake.url
        restarted = Child(argv, restart_env, log)

        settled = settle(restarted, lambda: snapshot(args.inspect, restart_env), lake, args.deadline)
        after = snapshot(args.inspect, restart_env)
        found = problems_at_settle(after, lake, row) if settled else ["the restarted pipeline never settled"]
        restarted.sigkill()
        if not settled:
            found.append("pipeline log:\n%s" % restarted.log_tail())
        return verdict(row, not found, found, at_kill, lake_at_kill, lake_state(lake), "after the restart", after)
    finally:
        lake.stop()


def settle(child, read, lake, seconds):
    """Wait until nothing is owed and the lake has stopped growing. The quiet period
    is what makes "no loss" mean no loss, instead of "no loss yet"."""
    end = time.monotonic() + seconds
    last = (len(lake.digests()), lake.uploads)
    quiet_since = None
    while time.monotonic() < end:
        if not child.alive():
            return False
        snap = read()
        now = (len(lake.digests()), lake.uploads)
        idle = not snap["pending"] and not snap["staging"] and now == last
        if idle:
            quiet_since = quiet_since or time.monotonic()
            if time.monotonic() - quiet_since >= QUIET:
                return True
        else:
            quiet_since = None
            last = now
        time.sleep(POLL)
    return False


def verdict(row, passed, found, at_kill, lake_at_kill, lake_at_end, where, after=None):
    """One boundary's result, plus the observations that justify it."""
    return {
        "name": row["name"],
        "boundary": row["boundary"],
        "claim": row["claim"],
        "passed": passed,
        "where": where,
        "problems": found,
        "at_kill": {
            "cursor": at_kill["cursor"],
            "pending": [{"digest": e["digest"][:19], "records": e["records"], "staged": e["staged"], "whole": e["whole"]} for e in at_kill["pending"]],
            "staged": [{"digest": e["digest"][:19], "whole": e["whole"]} for e in at_kill["staged"]],
            "staging": at_kill["staging"],
            "lake": lake_at_kill,
        },
        "lake": lake_at_end,
        "after": after and {"pending": len(after["pending"]), "staging": len(after["staging"])},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pipeline", required=True, help="the pipeline command the drill runs")
    parser.add_argument("--inspect", required=True, help="the command that prints the spool snapshot as JSON")
    parser.add_argument("--boundaries", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "boundaries.json"))
    parser.add_argument("--only", help="run one boundary row by name")
    parser.add_argument("--keep", action="store_true", help="keep the drill's temp dirs for inspection")
    parser.add_argument("--json", action="store_true", help="print the verdicts as JSON")
    parser.add_argument("--deadline", type=float, default=DEADLINE, help="seconds any single wait may take before it is a failure")
    args = parser.parse_args()

    with open(args.boundaries) as f:
        rows = json.load(f)["boundaries"]
    if args.only:
        # A row name selects that row; a boundary name selects every kill point of
        # that boundary, because a boundary can be entered at more than one instant.
        rows = [row for row in rows if row["name"] == args.only] or [
            row for row in rows if row["boundary"] == args.only
        ]
        if not rows:
            parser.error("no boundary named or mapped to %r" % args.only)

    results = []
    for row in rows:
        root = tempfile.mkdtemp(prefix="drill-%s-" % row["name"].replace("@", "-"))
        started = time.monotonic()
        try:
            result = run_boundary(row, args, root)
        except DrillError as error:
            result = {
                "name": row["name"],
                "boundary": row["boundary"],
                "claim": row["claim"],
                "passed": False,
                "where": "reaching the window",
                "problems": [str(error)],
                "at_kill": None,
                "lake": None,
                "after": None,
            }
        result["seconds"] = round(time.monotonic() - started, 1)
        results.append(result)
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)
        else:
            result["root"] = root
        if not args.json:
            report(result)

    if args.json:
        json.dump(results, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        failed = [r for r in results if not r["passed"]]
        print()
        print("%s: %d boundary probe(s), %d passed, %d failed" % (os.path.basename(args.pipeline), len(results), len(results) - len(failed), len(failed)))
    return 0 if all(r["passed"] for r in results) else 1


def report(result):
    mark = "PASS" if result["passed"] else "FAIL"
    print("[%s] %s  (%s, %.1fs)" % (mark, result["name"], result["boundary"], result["seconds"]))
    print("       claim: %s" % result["claim"])
    if result["at_kill"]:
        at = result["at_kill"]
        print("       at the kill: cursor %s, %d owed %s, %d published, %d mid-write" % (
            at["cursor"],
            len(at["pending"]),
            [e["digest"] for e in at["pending"]],
            len(at["staged"]),
            len(at["staging"]),
        ))
        print("       lake then: %d bundle(s), %d ack(s), %d dropped, export offsets %s" % (
            at["lake"]["bundles"],
            at["lake"]["uploads"],
            at["lake"]["dropped"],
            compact(at["lake"]["offsets"]),
        ))
    if result["lake"]:
        print("       lake after: %d bundle(s), %d ack(s), %d dropped, export offsets %s" % (
            result["lake"]["bundles"],
            result["lake"]["uploads"],
            result["lake"]["dropped"],
            compact(result["lake"]["offsets"]),
        ))
    for problem in result["problems"]:
        print("       %s: %s" % (result["where"], problem))


if __name__ == "__main__":
    sys.exit(main())
