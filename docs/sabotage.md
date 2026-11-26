# A test that cannot fail is not a test

Every crash drill passes the first time you run it. That is the problem.

There are exactly two ways a spool drill passes without proving anything:

1. **It never entered the window.** It killed at a moment when the pipeline happened
   to be idle, or between two durable steps it was already safe inside. The
   assertion held because nothing was at risk.
2. **The assertion is compatible with the bug.** "The export is not empty", "the
   process restarted", "no exception was raised" — all of these are true of a spool
   that loses half its records.

Both failures are invisible in a green test run. The only way to tell a drill from a
decoration is to **break the thing on purpose and require the drill to notice**.

## The discipline

For every boundary:

1. Run the drill against the intact pipeline. It must pass.
2. Break the pipeline in one specific, realistic way.
3. Run the **same drill, same boundary row, same knobs** against the broken
   pipeline. It must fail.
4. Require the failure to name the specific problem. A drill that fails with a
   timeout after 40 seconds while the bug was "the receipt has no bytes" is a drill
   whose diagnostics will not save you at 2am.
5. If step 3 passes, the drill is what is broken. Fix the drill.

Step 1 comes first so the sabotage cannot be "the drill fails at everything", and
step 3 keeps the same config so the sabotage cannot be "the drill was run
differently".

## The four sabotages this repo ships

`make sabotage` runs all of them (`scripts/sabotage.sh`). Each is a real bug with a
real production consequence; none of them is detectable by a smoke test.

| Sabotage | The bug | The drill's verdict |
|----------|---------|---------------------|
| `--row-first` | commit the receipt before staging the bytes | **fails at the kill**: `receipt … is owed but has no staged bytes` |
| `--publish-direct` | write the bundle at its published name instead of a temp file plus rename | **fails at the kill**: `bundle … is published but does not hash to its own name (torn)` |
| `--forget-outbox` | the restart starts with an empty receipt journal | **fails after the restart**: `the lake's merged export holds offsets [], wanted [0..39]` |
| `--ack-early` | commit the ack on the intention to send, not on the answer | **fails**: `post_send_pre_ack was not reached within 8s` |

Two of those failures happen **at the kill** (a spool invariant is already violated
when the process dies), one happens **after the restart** (the surviving state is
wrong), and one is a **refusal** — the pipeline destroyed the evidence that the
window ever existed, and the drill will not pretend it verified anything.

That last case is the most useful lesson in this repo. `--ack-early` makes the
pipeline invisible to boundary 3: it acks the receipt, drops the bytes, and there is
nothing pending to observe, so the drill cannot enter the window and does not reach
its assertions. A weaker harness would have shrugged and reported success; the
correct behaviour is to fail loudly, because *a verified nothing is not a verified
something*. When your drill reports "window not reached", read it as "this boundary
was not tested today" and go find out why.

## Sabotage list to grow

These are worth adding to your own copy, in this order:

- **Cursor first.** Advance the cursor before the receipt commits. It should turn
  boundary 1's stated loss into silent loss on a boundary that promised zero loss.
- **Fresh digest per attempt.** Make the bundle digest depend on the attempt
  counter. Every retry becomes a new bundle; boundary 3 and boundary 4 both start
  double-counting.
- **In-place rewrite.** Truncate and rewrite a bundle instead of staging a new one.
  Boundary 2b stops being invisible and starts being fatal.
- **Ack the whole queue.** Ack every pending receipt when one succeeds. Boundary 3
  loses everything except the first bundle.

If any of those passes, the drill is the bug.
