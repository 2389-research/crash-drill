# How to prove your spool survives SIGKILL

Your pipeline reads something, writes it to a local spool, and ships it onward. You
believe a crash costs nothing. This is the checklist that turns the belief into a
result you can show someone.

The method in one paragraph: **pick the instants between your durable steps, hold the
process still inside one, `SIGKILL` it, restart it against the same spool, and assert
that the state you promised is the state you got.** Each of those instants is a
*boundary*, and each boundary has a different promise. Do not assert the same
promise at all four — two of them cannot be zero-loss, and a drill that pretends
otherwise is either lying or not entering the window.

---

## 0. Before the boundaries: three things you need first

- [ ] **A way to kill a real process.** A thread cannot be `SIGKILL`ed, an in-process
      "abort" is a catchable exception, and a caught signal lets the pipeline tidy
      up. The window you are testing is the untidy one. Re-exec the pipeline as a
      child and `kill -9` it (`SIGKILL`, not `SIGTERM`).
- [ ] **A way to see the spool from outside.** You need to read durable state
      without asking the pipeline what it thinks: a receipt journal, a staging
      directory, a cursor file, whatever it has. The drill reads it through an
      *inspector* command that prints JSON (`docs/contract.md`).
- [ ] **A way to observe entry into the window.** This is the whole trick. A drill
      that kills at an arbitrary moment and asserts "no loss" passes because the
      window was never entered. Every kill below is preceded by an *observable
      predicate* that proves the pipeline is inside the boundary right now.

Two corollaries worth internalising now:

- **A deadline decides, a sleep never does.** `sleep(2)` then kill is not a
  boundary, it is a coin flip with a comment. Poll the predicate, kill on true,
  fail on timeout.
- **A refused pipeline is not a result.** Assert that a signal killed the process.
  A pipeline that exited 1 before your kill makes the boundary you claim to have
  hit a fiction.

---

## The four boundaries

| # | Boundary | What the kill lands in | The honest promise |
|---|----------|------------------------|--------------------|
| 1 | `post-emit-pre-batch` | read and cursor-covered, held in memory, nothing durable | **stated loss**, no duplicate work |
| 2 | `post-batch-pre-send` | bytes staged, receipt committed, nothing delivered | **zero loss**, bytes identical |
| 3 | `post-send-pre-ack` | an upload in flight (or dropped) with no ack | **zero loss**, exactly once |
| 4 | `post-ack-pre-cursor` | ack received, the ack not yet committed | **zero loss**, replay absorbed |

Below, each boundary is a checklist item: what to observe, where to kill, what to
assert, and what a pass actually buys you.

### 1. `post-emit-pre-batch` — the loss window you must admit

- [ ] Observe: the source offset you track has advanced past records that are
      **not** in any durable state (no receipt, no staged bytes) — they exist only
      in the process's memory.
- [ ] Kill there. Restart. Append fresh records.
- [ ] Assert: the records from the window are **gone**, the records appended after
      the kill **arrive**, and nothing arrives twice.

Why the promise is "loss" and not "zero loss": a cursor that advances on read is
ahead of the batch by construction. Naming this boundary as loss is what makes the
other three trustworthy — and it is also a design lever. Advance the cursor only
after the receipt commits and this boundary becomes zero-loss, at the price of
re-reading on every crash. Either choice is defensible; silently claiming zero loss
is not.

### 2. `post-batch-pre-send` — the ordering window

- [ ] Observe: the receipt exists, its bytes are published, and the destination
      holds nothing (point the pipeline at a port nothing listens on; refusal is a
      transient failure, so the receipt stays pending and the window stays open).
- [ ] Kill there. Restart against a live destination.
- [ ] Assert: **every record read before the kill arrives, once**, and the bundle
      that ships has the digest the spool computed — the bytes were not rebuilt
      from anything.

This is where the load-bearing ordering lives: **bytes before receipt**. Publish
the bundle (write, `fsync`, rename, `fsync` the directory), *then* commit the
receipt that names it. The receipt is the promise, so the promise must never exist
before the thing it promises.

### 2b. `post-batch-pre-send@mid-write` — the write itself

Same boundary, a different instant: half the bundle is on disk and no receipt
exists yet.

- [ ] Observe: a bundle's bytes are partially written. If the pipeline writes to a
      temp file and renames, this is a temp file with bytes in it. If it writes
      straight at the published name, it is a published file that **does not hash
      to its own name** — which is already a failure, see `docs/sabotage.md`.
- [ ] Kill there. Restart.
- [ ] Assert: no published bundle fails its own digest, no receipt is missing its
      bytes, the restart clears the orphaned temporary, and only the records
      appended after the restart arrive.

A torn bundle at a published path is the most expensive bug in this list: the spool
now lies about itself, and it lies *silently* until something reads the bytes.

### 3. `post-send-pre-ack` — the in-flight upload

- [ ] Make the destination accept the request and drop the connection without
      answering (read the headers, close, store nothing). That is the upload
      failing mid-body, which is what a real network does.
- [ ] Observe: an upload was dropped, the receipt is still pending, nothing stored.
      Kill during the retry backoff.
- [ ] Assert: the receipt survived the kill **with its bytes**, the restart retries,
      and the destination ends up holding the records **exactly once**.

A dropped upload cannot ack. If your pipeline treats "sent" as "stored", boundary 3
is where you find out, and only if the drill actually interrupts the delivery.

### 4. `post-ack-pre-cursor` — the ack you did not commit

- [ ] Observe: the destination stored the digest and the receipt naming it is
      **still pending** — the ack arrived, the pipeline did not record it.
- [ ] Kill there. Restart.
- [ ] Assert: the pipeline replays the bundle, the destination absorbs the replay
      **by digest**, and the merged export still holds every record exactly once.

This window is microseconds wide in a correct pipeline, so you cannot catch it by
timing. That is not a reason to skip it — it is the reason your pipeline needs a
documented fault hook (`ack-hold:<ms>`: commit the ack only after this delay) or a
layout in which the ack commit is a visible durable step. Skipping boundary 4 is the
commonest way a spool that "survives crashes" turns out to duplicate data.

**Replay is not a bug; double-counting is.** Records carry identity that survives
re-framing. A replay that is byte-identical is the same record arriving twice and it
must collapse — at read/export time, by identity, not by dropping bytes at write time.

---

## Two limits to state out loud

1. **`SIGKILL` does not test `fsync`.** The page cache belongs to the kernel, not the
   process. Kill the process and unflushed writes are still there. A pipeline that
   never calls `fsync` survives every drill above. What the drill tests is
   *ordering and idempotency*, which is what a crash actually violates in practice;
   `fsync` correctness needs a device-level or VM-level crash, and it needs its own
   test.
2. **Nothing before the receipt is durable, and no drill can change that.** If a
   record was read and held in memory when the process died, it is gone. The drill's
   job is to make that loss *bounded, named and visible*, not to make it zero.

---

## Run it

```
make drill      # the intact reference pipeline: all boundaries must pass
make sabotage   # the same drill against four deliberately broken spools: each must fail
```

`drill/boundaries.json` is the list. Adding a boundary, or a new instant inside one,
is a JSON row — the harness is generic, your pipeline does not change.
