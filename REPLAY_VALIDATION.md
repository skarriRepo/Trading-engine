# Candle replay after operational fixes

Run `python replay_candles.py <capture1.json> <capture2.json> --out replay.json`.
The script uses completed two-minute bars and the current runtime entry path.
It does not synthesize option bids, fills, or P&L. Missing/changed option quote
behavior must be tested on option quote and broker order records.

On the two September 28 captures supplied with the review:

| Capture | In-session distinct TAKE candidates | TAKE after 15:30 ET |
|---|---:|---:|
| Earlier capture | 38 | 0 |
| Later capture | 22 | 0 (previous engine: 11) |

The captures have 4,737 overlapping symbol/minute candles and 887 conflicting
rows. They are replayed separately. No authoritative replacement for the
conflicting rows was supplied, so this package does not silently combine them.

Focused entry/exit/state/runtime/order/audit checks: 87 tests, one skipped in
this environment (external dependencies unavailable; network-facing tests use
local stubs). The operational changes have not been exercised against a
credentialed Tradier session.
