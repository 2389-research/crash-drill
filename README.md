# crash-drill

Prove your local-first pipeline survives `SIGKILL` — at the boundaries where it
actually has to, with assertions that can fail.

You have a pipeline that reads something, spools it durably, and ships it onward.
Between each pair of durable steps there is an instant where a crash costs data,
duplicates data, or costs nothing at all. Most crash tests never land in one of
those instants, which is why they always pass.

This repo is a small, language-agnostic harness that does land in them: spawn the
pipeline as a real process, hold it still inside a named boundary, `SIGKILL` it,
restart it against the same spool, and assert the promise that boundary makes. Plus
the writeup on how to do it yourself, and a reference pipeline (stdlib Python) so you
can see the whole thing run in about six seconds.

```
make drill
```

```
[PASS] post-emit-pre-batch  (post-emit-pre-batch, 0.7s)
       at the kill: cursor {'s:session': 5}, 0 owed [], 0 published, 0 mid-write
       lake after: 1 bundle(s), 1 ack(s), 0 dropped, export offsets [5..7]
[PASS] post-batch-pre-send  (post-batch-pre-send, 3.1s)
       at the kill: cursor {'s:session': 40}, 1 owed ['sha256:636904fddb4c'], 1 published, 0 mid-write
       lake after: 1 bundle(s), 1 ack(s), 0 dropped, export offsets [0..39]
[PASS] post-batch-pre-send@mid-write  (post-batch-pre-send, 0.6s)
…                                       (one line of claim + evidence each)
[PASS] post-send-pre-ack  (post-send-pre-ack, 0.6s)
[PASS] post-ack-pre-cursor  (post-ack-pre-cursor, 0.6s)

pipeline.py: 5 boundary probe(s), 5 passed, 0 failed
```

(abridged: each row prints its claim and the state it observed at the kill.)

And then:

```
make sabotage
```

which runs the same drill against four deliberately broken spools and requires it to
fail, for the stated reason, each time. **A crash drill that has only ever passed is
a drill that cannot fail.** `docs/sabotage.md` is the argument.

## The four boundaries

| Boundary | The kill lands in | The honest promise |
|---|---|---|
| `post-emit-pre-batch` | read and cursor-covered, held in memory | **stated loss**, no duplicate work |
| `post-batch-pre-send` | bytes staged, receipt committed, nothing sent | **zero loss** |
| `post-send-pre-ack` | upload in flight or dropped, no ack | **zero loss**, exactly once |
| `post-ack-pre-cursor` | ack received, ack not yet committed | **zero loss**, replay absorbed |

`docs/checklist.md` is the full inventory as a checklist, with what to observe, where
to kill, what to assert, and the two limits you must state out loud.

## Use it on your pipeline

The harness knows three commands and a handful of environment variables: run your
pipeline (`--pipeline`), print your spool's state as JSON (`--inspect`), and the
scripts the boundaries describe (`--boundaries`). Your language does not matter — only
that the pipeline is a process you can kill and the spool can be read from outside.
`docs/contract.md` is the interface.

```
python3 drill/drill.py \
    --pipeline "your-pipeline --watch \$DRILL_WATCH_DIR" \
    --inspect  "your-inspector" \
    --boundaries drill/boundaries.json
```

Adding a boundary, or a second instant inside one, is a JSON row in
`drill/boundaries.json`. The harness is generic; your pipeline does not change. The
reference inspector (`examples/spool/inspect.py`) is the smallest complete
example of the state the drill needs to see.

## The reference pipeline

`examples/spool/pipeline.py` is a real (if tiny) local-first pipeline: it tails a file,
frames records with a stable identity, batches them, stages each bundle through
temp-file/`fsync`/rename/`diff`-fsync, commits a receipt, delivers, and commits the
ack. Three fsync discipline points, one append-only journal, no dependencies. It is
also the sabotage target: `--row-first`, `--publish-direct`, `--forget-outbox` and
`--ack-early` are the bugs the drill must catch.

## What a `SIGKILL` cannot tell you

The page cache outlives the process. A pipeline that never calls `fsync` passes every
kill in this repo. What these kills test is **ordering and idempotency** — the two
things a crash actually violates in practice. Proof that a flush reached the platter
needs a device-level or VM-level crash, and it needs its own test;
`docs/case-study.md` says how the source project pinned it instead.

## Layout

```
drill/drill.py          the harness: scripted lake, windows, kills, verdicts
drill/boundaries.json   the boundary list = the specification
docs/checklist.md       how to prove your spool survives SIGKILL (the four boundaries)
docs/sabotage.md        a test that cannot fail is not a test
docs/case-study.md      the production collector this was extracted from
docs/contract.md        the harness ↔ pipeline interface
examples/spool/         the reference pipeline, inspector, and one-command runner
scripts/sabotage.sh     the proof that the drill can fail
```

## License

MIT.
