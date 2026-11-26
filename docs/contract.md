# The harness ↔ pipeline contract

The drill knows nothing about your language, your spool, or your wire format. It
knows three commands and five environment variables. That is the whole interface.

## 1. The pipeline is a command

```
DRILL_WATCH_DIR=… DRILL_SPOOL_DIR=… DRILL_LAKE_URL=… DRILL_BATCH_HOLD_MS=… DRILL_FAULT=… \
    <your pipeline>
```

The drill runs it as a child process in its own process group and kills the group
with `SIGKILL`. It must:

- run until killed (a pipeline that exits on its own fails the drill: the boundary
  it claims to have hit did not happen);
- re-read `DRILL_SPOOL_DIR` on start and continue from the durable state it finds
  there — that is the entire premise of the drill;
- deliver bundles to `DRILL_LAKE_URL` over whatever protocol you like.

| Variable | Meaning |
|---|---|
| `DRILL_WATCH_DIR` | a directory the drill appends whole lines to (`session.jsonl`) |
| `DRILL_SPOOL_DIR` | the durable state under test |
| `DRILL_LAKE_URL` | the destination, a scripted server the drill runs |
| `DRILL_BATCH_HOLD_MS` | how long a batch may sit in memory before it must be flushed |
| `DRILL_BATCH_MAX_RECORDS` | how many records force a flush |
| `DRILL_POLL_MS` | poll interval, if you have one |
| `DRILL_FAULT` | comma-separated timing faults, `name:value` |

The first four are ordinary configuration — a real deployment sets them too.
`DRILL_FAULT` is the drill's one concession: `slow-write:<ms>` delays each chunk
while staging bytes, `ack-hold:<ms>` delays the commit of an ack that has already
arrived. Both delay an existing step; neither may skip, reorder or stub anything. A
fault hook that changes correctness is not a hook, it is a second implementation.

## 2. The lake speaks a minimal protocol

`POST <DRILL_LAKE_URL>/bundles` with the framed bundle as the body and the bundle's
digest in the `X-Digest: sha256:<64 hex>` header. A 200 with
`{"digest": "sha256:…", "stored": true}` is an ack, and it must carry **your**
digest — an ack for something else is not an ack, and the drill will treat a
receipt that disappears after a mismatched ack as loss.

The drill runs the lake in three modes, chosen per boundary row:

- `ack` — stores the bundle by digest, answers 200. Storing by digest is how a
  replay becomes a no-op; a lake that appends on every upload will look like a
  duplicate-everything bug at boundary 4.
- `drop` — reads the headers, closes the connection without reading the body, stores
  nothing. This is the mid-upload failure the pre-ack boundary needs.
- `dead` — no listener at all. Delivery is refused, so a committed receipt stays
  pending with nothing delivered, holding the pre-send window open indefinitely.

## 3. The inspector is a command that prints JSON

```
DRILL_SPOOL_DIR=… <your inspector>
```

It runs while the pipeline is live and must be read-only. It prints one JSON object:

```json
{
  "cursor":  {"s:session": 5},
  "pending": [{"digest": "sha256:…", "records": 5, "staged": true, "whole": true}],
  "staged":  [{"digest": "sha256:…", "bytes": 512, "whole": true, "pending": true}],
  "staging": [{"path": "…tmp.4711", "bytes": 4096}]
}
```

- `cursor` — for each stream, how many records the pipeline considers consumed.
- `pending` — the receipts the destination is still owed. `staged` says whether the
  bytes exist, `whole` whether they are complete and hash to what the receipt names.
- `staged` — published bundles. `whole` is the published-file invariant.
- `staging` — writes in flight (temp files). Bytes > 0 means a write is happening now.

Exit non-zero to fail the drill. Print `"pending": []` when the journal is
unreadable and you have turned a crash into a lie: an inspector that cannot tell
"nothing is owed" from "I could not read it" makes every assertion downstream
vacuous.

## 4. A boundary row is config

`drill/boundaries.json`, one object per row:

| Field | Meaning |
|---|---|
| `name` | the row's name, unique |
| `boundary` | which of the four boundaries this row is a kill point inside |
| `window` | the predicate that proves the pipeline is inside it (see `WINDOWS` in `drill/drill.py`) |
| `lake` | `ack`, `drop` or `dead` |
| `claim` | what this row promises, in one sentence, printed with the verdict |
| `append` / `append_after` | records the drill appends before the kill / after it |
| `env` / `restart_env` | the knobs for the first run / the restart |
| `expect.offsets` | the half-open record range the merged export must hold after the restart |
| `expect.bundles` | how many distinct bundles the lake must hold |
| `expect.uploads_min` | how many acks the lake must have given (proves a replay happened) |

The windows are predicates over `(snapshot, lake, row)` and live in `drill/drill.py`
because they are the harness's *reading* of your state, not your state. A new
instant inside an existing boundary is a new window plus a row.
