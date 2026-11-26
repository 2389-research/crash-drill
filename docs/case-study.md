# Case study: a collector that survived four kills

This template is an extraction of a drill that was written for a production
local-first collector: a daemon tails AI-agent session logs, spools them to disk,
and ships content-addressed bundles to a lake. The collector is Rust; the spool is
one SQLite file (cursors plus a receipt table, WAL, `synchronous=FULL`) and a
content-addressed staging tree (`<spool>/ab/cd/<hex>`). The lake dedupes by bundle
digest and stores raw bytes.

The numbers below are from that drill. The specific compression, the wire format,
the retry classes and the identity scheme are deliberately **not** in this template:
they are that product's business. What travelled is the shape, which is what you
need.

## What the drill was allowed to change

Nothing in the collector. The four boundaries were reached by turning the
collector's own configuration keys — batcher hold time, poll interval, first-walk
byte pacing — and by choosing what the destination did (ack, drop, refuse). The
drill never asked the collector to behave differently from production, and it never
had a "test mode". That constraint is why the results mean something: a pipeline
with a test-only durability path proves something about the test-only path.

The one exception, and it is worth being explicit about: **timing hooks are
acceptable, correctness hooks are not.** The template's `ack-hold` and `slow-write`
faults delay a step that already exists; they do not skip it, reorder it, or stub
it out. Anything else quietly moves the pipeline out from under the drill.

## The four kills, as measured

| Boundary | Held open by | Observed before the kill | Result after the restart |
|---|---|---|---|
| `post-emit-pre-batch` | batcher hold 60 s, poll 50 ms | cursor covered all 5 records, receipt table empty, staging empty | the 5 stay lost; only the 3 appended after the kill arrived (offsets 5,6,7); zero merge notices |
| `post-batch-pre-send` | destination refuses (nothing listening) | 1 receipt / 4 records, staged digest present and whole, nothing delivered | backlog shipped once, byte-identical, all 4 offsets, zero notices |
| `post-send-pre-ack` | destination accepts, then closes after the headers | ~1 MiB upload interrupted mid-body, receipt still pending with bytes | the interrupted bytes ship once, all 40 offsets, zero notices |
| `post-ack-pre-cursor` | (see below) paced first walk at 4 KiB/s | kill landed inside the walk, no cursor persisted | re-read from the top, every one of 200 records exactly once, zero notices |

Two details are what make those rows trustworthy rather than decorative:

- **The window was observed, not assumed.** Every row asserts positive evidence
  before killing: the receipt is non-empty, the drop counter moved, the cursor is
  still short of the file. A kill at an unobserved moment is a coin flip.
- **The zero-loss rows count records, not successes.** The assertion is the merged
  export's record offsets, not "the upload returned 200".

## The durability patterns

**`F_FULLFSYNC` on darwin, `fsync` plus a directory `fsync` on linux.** On macOS a
plain `fsync` returns once the data reaches the drive, and the drive's own write
cache is not flushed, so the write is not durable until
`fcntl(fd, F_FULLFSYNC, 0)` answers. Filesystems that cannot honour it (some network
mounts) answer `ENOTSUP` or `EINVAL`, and there a plain `fsync` is the strongest
guarantee available — a fallback, not a silent downgrade. On linux, the bytes and
the *directory entry* are two separate durability problems: after renaming a staged
file into place, the rename itself is only durable once the containing directory is
`fsync`ed. Both live in one place so nothing has to remember the rule.

**Bytes before receipt.** Staging is: write the compressed bundle to a temp file,
`fsync` it, close it, rename it onto its content-addressed name, `fsync` the
directory — and only then insert the receipt that names it. The invariant this buys
is the one the drill checks at every kill: *a pending receipt always has whole
bytes*. A kill leaves either no file at the content path or a complete bundle,
never a torn one and never a receipt pointing at nothing.

**Crash between blob and receipt self-heals on retry.** If the process dies after the
bytes are published and before the receipt commits, nothing is pending, the batch is
lost (boundary 1), and the leftover file is unreferenced — the next run re-derives
the *same* digest for the same records, finds the staged file already present, and
skips the write. Content addressing is what makes that a no-op instead of a
duplicate: the publisher is idempotent by construction.

**The receipt and the ack are one table, and the ack is verified.** A 2xx whose
digest is not our own digest is not an ack: the receipt stays pending. An ack for
something else is exactly how a lake-side dedupe bug turns into a client-side
data-loss bug, and it costs one comparison to refuse.

**The spool is locked, not trusted.** A running collector holds an exclusive OS file
lock on the spool, so a second process cannot write beside it, and the OS releases
the lock when the process dies — the crash case is the common case, so the lock has
to survive it for free.

## What the extraction surfaced

Four things about the drill's shape only became obvious when it was rewritten for
another language.

1. **Three boundaries are time-observable; the fourth is not.**
   `post-ack-pre-cursor` is microseconds wide: ack arrives, receipt is deleted, next.
   You cannot catch it by polling. The Rust drill therefore killed inside a *paced
   first walk* instead, with a different (equally real) outcome: the crash leaves no
   cursor at all, the restart re-reads the file from the top, and records ship twice
   in *different bundle windows*. That is harmless only because identity and
   envelope repeat identically — the drill asserts *zero merge notices* to prove it.
   The template states boundary 4 as its own row and reaches it with an `ack-hold`
   timing hook, which is the more portable answer; the paced-walk kill is the answer
   when you refuse to add a hook.

2. **"Zero loss" is a per-boundary claim, not a global one.** The first kill in the
   Rust drill asserts that five records are *gone*, by offset. Writing down the loss
   is what makes the other three rows mean anything. A drill where every row asserts
   "nothing lost" is a drill that has not thought about where its cursor is written.

3. **`SIGKILL` cannot test `fsync`, and the drill never claimed to.** The page cache
   outlives the process, so a pipeline that never flushes still passes every kill.
   The Rust suite is honest about this: the flush is pinned by a unit test that
   asserts the source calls `libc::F_FULLFSYNC` (a call-site pin, not a behaviour
   test), because proving the flush needs a device-level crash. The kills prove
   *ordering* and *idempotency*, which is what actually breaks.

4. **The harness had its own crash bugs.** Two are worth inheriting as warnings.
   First, the killed children had to be serialised: run four kill-and-restart drills
   concurrently on a loaded 4-vCPU CI runner and the spool reads start racing, which
   looks like a product bug and is not. Second, and worse, a transient read error
   was being swallowed into "the receipt table is empty" — a busy spool read as
   nothing pending, and a boundary passed for the wrong reason. Reads that fail must
   fail the drill. An inspector that cannot tell "empty" from "could not read" is
   the same bug with a friendlier name, which is why the template's inspector treats
   an unparseable journal as an error rather than as no receipts.
