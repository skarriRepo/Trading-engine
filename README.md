# Trading Engine — streaming-first rebuild

## Keep credentials across updates

Save your existing credentials once in
`%USERPROFILE%\.trading_engine\credentials.env` on Windows (or
`~/.trading_engine/credentials.env` on macOS/Linux). The file uses ordinary
`.env` lines, for example:

```dotenv
TRADIER_ACCESS_TOKEN=your_sandbox_token
TRADIER_LIVE_DATA_TOKEN=your_production_data_token
TRADIER_ACCOUNT_ID=your_sandbox_account_id
UW_API_KEY=your_uw_token
```

If you already keep these in another local file, set a persistent user
environment variable once in PowerShell, then open a new terminal:

```powershell
[Environment]::SetEnvironmentVariable('ENGINE_CREDENTIALS_FILE', 'C:\path\to\my-private.env', 'User')
```

You can still keep non-secret settings such as `MAX_ORDER_DEBIT=6000` in the
checkout's `.env`. Credential precedence is **process environment → private
file → checkout `.env`**. Only the four credential keys above are read from
the private file; non-secret settings there are ignored. If
`ENGINE_CREDENTIALS_FILE` is set but the file is missing, startup fails rather
than using a stale account from the checkout. Keep the private file outside
the repository and do not commit it. The sandbox token is used for sandbox
orders; the production data token supplies live quotes for all decisions.

Sandbox entry orders that remain unfilled past the configured timeout are
canceled. A symbol becomes eligible for a later signal only after Tradier
confirms the cancellation had zero executions, no position is held for that
symbol, and no other order is working. This check also reconciles eligible
cancellation blocks from an earlier run at startup. Rejected orders, partial
fills, unknown submissions, and exit-order failures remain blocked for review.

A from-scratch, streaming-first redesign of the options trading engine,
built to replace a polling-based system whose complexity had outgrown itself.
Every module below was built and tested against real historical data where
real data was available; where it wasn't, that's stated explicitly rather
than assumed away.

## Architecture

```
Streaming inputs (UW WebSocket, price/bar feed)
        |
        v
  SymbolStateStore  (symbol_state.py)
  one SymbolStream per symbol, per-feed freshness (FRESH/STALE/NOT_READY),
  no decision logic
        |
        v
  entry_pipeline.py           exit_pipeline.py
  PSAR trigger -> confluence   fresh PSAR recompute -> exit ladder
  check -> approximate target  (reads PositionState + SymbolSnapshot)
  -> TAKE / SKIP               -> HOLD / PROFIT_LOCK / EXIT_PENDING
        |                              |
        +-------------+  +-------------+
                      v  v
              runtime.py (TradingRuntime)
        orchestrates ticks -> entry or exit evaluation,
        opens/closes positions, drives the dashboard
                      |
                      v
              dashboard_store.py -> dashboard_app.py
              Scan / Active / Closed views, served over HTTP
```

`contract_selection.py` sits alongside the entry path: given an option
chain, it picks a real contract (DTE ordering + ATM strike + liquidity
filter) and implements `TradingRuntime.option_entry_price_provider`.

## Modules

| File | What it does |
|---|---|
| `trading_engine/symbol_state.py` | Streaming state store. `ingest_tick`/`ingest_bar`/`ingest_net_flow`/`ingest_interval_flow`/`ingest_market_tide`/`ingest_gex` write; `snapshot()` reads an immutable, freshness-gated `SymbolSnapshot`. No network I/O, no decision logic. |
| `trading_engine/entry_pipeline.py` | PSAR flip trigger with candle momentum and UW directional flow recorded as observations, plus optional assessment of actual fresh gamma levels. Neither momentum nor UW availability blocks entry. No structure rule or fabricated ATR target is used for entry. |
| `trading_engine/contract_selection.py` | Selects the option contract using current underlying price from a production chain prefetched on a bounded background worker. A missing or expired snapshot refuses the one-shot entry. |
| `trading_engine/exit_pipeline.py` | `PositionState`, `evaluate_exit` — the exit ladder. Recomputes PSAR fresh every call; never reads a cached direction field. |

### Reversal marker exit candidate

The engine computes the chart's bullish/bearish 9-count momentum phase from
**completed underlying bars**. A perfected red `P` on a bar after entry exits
a CALL; a perfected green `P` exits a PUT. A fresh positive live option bid
is required to submit the sandbox sell. Plain markers, old markers and
intrabar markers cannot trigger it. The exit reason is
`OPPOSITE_PERFECT_REVERSAL`; emergency and EOD exits have higher priority.

This rule is enabled by default. Set `REVERSAL_PHASE_EXIT=false` in `.env`
to disable it. `POSITION_OBSERVATION` records
`reversal_phase_side`, `reversal_phase_perfected`,
`reversal_phase_opposes_position`, `reversal_bid_retreat`, and
`reversal_perfect_exit_candidate`. The available candle replay does not
contain contemporaneous option bids, so it cannot estimate the P&L impact.
| `trading_engine/contract_selection.py` | `select_contract`, `make_option_entry_price_provider` — turns an option chain into a real, ask-priced entry. |
| `trading_engine/tradier_client.py` | Tradier REST client — quotes, time-and-sales, expirations, option chains. Sandbox token from `TRADIER_ACCESS_TOKEN`; a separate production market-data client can use `TRADIER_LIVE_DATA_TOKEN`. |
| `trading_engine/history_warmup.py` | One-time intraday 1-minute time-and-sales fetch; aggregates complete contiguous 2-minute OHLCV bars for PSAR and momentum without replaying historical entries. |
| `trading_engine/tradier_stream.py` | Tradier WebSocket streaming client — live equity ticks and quotes. Production only — see below. `parse_stream_message()` is a pure, fully-tested function; connection/reconnection handling needs a live smoke test. |
| `trading_engine/tradier_poll.py` | Tradier REST-polling fallback for sandbox — sandbox has no streaming access at all (Tradier's own documented limitation). Delayed, mechanical-testing-only; mirrors `TradierStreamClient`'s interface exactly so `main.py`'s wiring doesn't change based on which is active. |
| `trading_engine/uw_stream.py` | Unusual Whales WebSocket streaming client — net flow, interval flow, market tide, GEX. Channel names are evidenced from this session's own reference-system exploration; raw per-message field parsing is the least-verified piece of this bundle — read its module docstring before trusting it. |
| `trading_engine/tradier_orders.py` | Tradier option order preview, submit, and account order/position reads. Form-encoded requests with no automatic retry. |
| `trading_engine/sandbox_execution.py` | Sandbox-only journaled order lifecycle; confirms full broker fills before changing positions. |
| `main.py` | Real entry point — loads tokens from `.env`, wires Tradier, order placement (optional), and (optionally) UW into `TradingRuntime` and the dashboard, runs both. |
| `trading_engine/dashboard_store.py` | Thread-safe store feeding the dashboard. Decoupled from how it's fed (live runtime or replay). |
| `trading_engine/runtime.py` | `TradingRuntime` — the orchestration engine. Transport-agnostic: `on_underlying_tick`/`on_option_quote`/`on_net_flow`/etc. are called by a real stream client or a replay driver identically. |
| `dashboard_app.py`, `dashboard.html` | FastAPI operator dashboard: Overview, Trades, Diagnostics; feed and broker alerts, confirmed-fill P&L, and decision reasons. |

## Key evidenced design decisions

These aren't defaults chosen for convenience — each was tested against real
data or a real, documented failure before being set this way. Where
something is a reasoned-but-unvalidated choice, it's flagged as such below
and in the code, not presented with false confidence.

- **PSAR defaults to `(start=0.03, increment=0.02, maximum=0.20)`**, not the
  more common `(0.02, 0.02, 0.20)`. Tested against real 2-minute-resampled
  candle data across a 46-symbol universe; this setting scored best on
  forward-return quality (52.4% favorable) of every combination tried,
  including several deliberately quieter-looking ones that tested worse.

- **UW confluence requires ≥4 of 5 overlapping readings agreeing with zero
  disagreement**, not a simple majority. A straight port of the old
  majority-vote logic was replayed against a real trading day and agreed
  with a separately-validated legacy computation on only 27.8% of the cases
  where it fired, including direct directional contradictions on the same
  symbol at the same moment. The stricter rule here defaults to "no clear
  read" rather than guessing whenever agreement isn't overwhelming.

- **Per-feed freshness, never a single global "is this fresh" flag.** One
  live feed must never mask another stale one sitting behind it — this
  exact bug was found and is covered by a direct regression test
  (`test_one_fresh_feed_does_not_mask_another_stale_feed`).

- **Giveback and stall exits adapt to live option quotes.** A peak bid clears
  two observed bid/ask spreads beyond the live entry ask to arm the trail.
  The exit trail is the larger of two current spreads or one quarter of the premium
  earned at the peak; halfway to the trail is a warning. An unarmed trade
  stalls after a completed bar without a new bid high when its bid remains
  within a spread of entry and directional momentum stops expanding. These
  multipliers are configurable hypotheses, not historical performance claims.
  A valid fresh bid and ask are required for these exits.

- **`OPPOSITE_PSAR_BARE_PROVEN` is a real but explicitly unvalidated exit
  trigger**, kept separate from the validated `OPPOSITE_PSAR_CONFIRMED_BY_
  PRICE` rather than folded into it. It only engages once a position has
  cleared the observed spread threshold, on the reasoning that a bare,
  unconfirmed flip is a different risk on a proven position than on a fresh
  one. This is *reasoned*, not backtested — old log formats couldn't support
  replaying it — so it's disableable in one config flag
  (`require_price_confirmation_after_proven=True`) and logged under its own
  name specifically so real data can validate or kill it independently.

- **DTE preference in contract selection is an ordering, not a gate.** A
  hard 2%-premium-ratio block was tried earlier, went live, and was measured
  at a 12.5% win rate on its "allowed" tier at scale — it needed the
  shadow-then-validate discipline everything else here got, and didn't
  receive it before shipping. `select_contract()` tries the nearest
  expiration first but falls through to the next one rather than blocking
  outright; DTE and premium-as-%-of-underlying are recorded on every
  selection so this can finally be analyzed properly before any blocking
  rule is reconsidered.

- **Exit evaluation runs on underlying prices and fresh option bids.** An
  independent clock checks EOD. Broker status polls at a budgeted interval
  of at least 15 seconds while unresolved.

## What's real and tested vs. what's an honest gap

**Tested, including against real historical data:**
- The full pipeline (state → entry → exit → dashboard) was replayed against
  46 real symbols' worth of real intraday candle data end-to-end with zero
  crashes; every closed trade showed a real, recognized exit reason.
- Unit/integration tests across the modules, run together as one suite
  (`pytest tests/`).

**Built and unit-tested, but NOT live-tested — a real, meaningful
distinction, not a hedge:**
- **`tradier_client.py` / `tradier_stream.py`** — a REST client (quotes,
  expirations, option chains) and a WebSocket streaming client, built
  against Tradier's public, documented API and the same endpoint shapes
  this session's own reference codebase already used successfully. No real
  Tradier credentials exist in this environment, so every test here mocks
  the network layer and verifies parsing against realistic fixture JSON
  matching Tradier's documented schema — not against an actual live
  response. **Smoke-test against `TRADIER_ENV=sandbox` before trusting this
  with anything real**, and expect to fix small schema surprises a real
  connection can reveal that a fixture can't.
- **`uw_stream.py`** — weaker evidence than Tradier's, and worth
  understanding precisely why: the channel names (`interval_flow`,
  `market_tide`, `flow-alerts`, `net_flow:<TICKER>`, `gex`) are evidenced —
  they come from this session's own exploration of a working reference UW
  integration, validated against real, logged shadow events across multiple
  real trading sessions. The raw per-message JSON field names each parse
  function assumes are NOT evidenced the same way — this session saw that
  reference system's already-parsed internal fields, not UW's wire payload
  directly. Log the raw, unparsed message next to the parsed result on your
  first live connection and diff them against what `uw_stream.py`'s parse
  functions assume; expect to fix real field-name mismatches there.

**Dynamic option-quote subscription: done.** `TradingRuntime` now tracks
each position's own OCC contract symbol (`PositionState.occ_symbol`,
captured from `OptionEntryResult` at entry) and maintains a reverse
`occ_symbol -> underlying` map so `on_option_quote()` can route a
contract-level quote to the right position. `main.py` wires
`on_trade_opened`/`on_trade_closed` to live contract subscription management.
After a confirmed close, the contract stays subscribed for
`POST_EXIT_OBSERVE_SEC` (default 600 seconds). `POST_EXIT_OPTION_QUOTE`
records valid live bids and asks without placing orders. The separate
bid-only hypothesis (emergency stop, spread-aware giveback, EOD) emits
`BID_EXIT_SHADOW_TRIGGER` once while the trade is open and
`BID_EXIT_SHADOW_AT_ACTIVE_EXIT` when the active rules sell. After the
broker confirms the exit, `POST_EXIT_BID_SHADOW_TRIGGER` records the first
subsequent bid-only trigger if one occurs within the observation window.
`POST_EXIT_SHADOW_END` includes quote coverage and the first subsequent
trigger; a null trigger means unknown beyond the observation window. This
shadow never submits an order and never changes the active exit ladder.
The subscription is
removed when the window ends unless a new position uses the same contract.
An engine restart ends that observation window; missing quotes remain missing.

## Running it

## End-of-day audit

### Profit-trail tuning (October 1 live-quote replay)

The active profit giveback exit arms only after peak **live option bid** exceeds
the captured **live entry ask** by the greater of eight times the spread at
the bid high and 8% of the live entry ask. Once armed, its trail is the
greater of four times that peak spread and 25% of the earned premium.
A later widening spread cannot loosen the trail. The -30% live-bid emergency
stop and 15:50 ET close remain immediate; stall and opposing-PSAR rules still
operate independently. All decisions continue to use live quotes, never
sandbox fills. `POSITION_OBSERVATION.arm_required_gain` and `active_trail`
record the actual thresholds on each observed quote.

On the October 1 capture through 11:23 ET, replaying accepted option quotes
and the available ten-minute post-exit quotes delayed the NVDA CALL profit
trigger from 10:32:30 ($1.95 live bid) to 10:41:45 ($2.13), and the NVDA PUT
profit trigger from 10:50:51 ($1.68) to 10:53:39 ($1.86). These are
counterfactual live bids, not guaranteed broker fills. After the observation
window ends, further outcomes cannot be inferred from this data.

## Entry window and dead option feed

Entry candidates are evaluated only on weekdays from 09:30 up to 15:30 ET.
At the cutoff, new entries stop before option-chain lookup;
the broker independently enforces the same window. Existing positions continue
to receive exits, and the watchdog prioritizes the 15:50 EOD close.

For sandbox orders, the latest option quote must be within
`OPTION_QUOTE_TIMEOUT_SEC` (default 20 seconds) of both its market timestamp
and local receipt. Option-premium stop, giveback, bare-proven flip, and stall
checks are held while it is stale; price-confirmed opposite PSAR, UW thesis
break, and EOD exits remain available. The worker reports a stale option feed,
shows `OPTION_FEED_STALE` in Orders, pauses new entries, and retries a Tradier
REST quote every 15 seconds. REST data must still have a fresh market timestamp
to be accepted. A missing option feed does **not** authorize a blind market
sell; the position remains open until price-based or EOD logic exits or an
operator acts. Pending/blocked broker orders are never retried as new orders.

The EOD report preserves the last observed bid/ask when a broker exit is
reconciled, alongside the trigger snapshot and confirmed fill. That quote is
observed at reconciliation, not an exchange fill-time quote.

To check candle decisions without manufacturing option premiums, run
`python replay_candles.py <capture1.json> <capture2.json> --out replay.json`.
Run conflicting captures separately; the script reports overlapping candles
that disagree between the last two paths. A candle-only replay cannot
measure option P&L or validate the premium-dependent exit ladder.

Each run appends structured ET-dated events to `logs/engine_YYYY-MM-DD.jsonl`.
The Diagnostics view and `EXIT_DECISION` record changes in decision state;
`POSITION_OBSERVATION` separately captures every accepted held-option quote,
each newly completed held bar, and periodic underlying checks even when the
decision remains HOLD. It includes the completed candle, PSAR, momentum,
freshness, live bid/ask, spread, peak, trail, giveback and all UW horizon
readings. `UW_POSITION_SAMPLE` records each UW feed event while holding,
with its derived directional read and quote context. `uw_shadow_exit` is a
counterfactual observation for EOD review and cannot place an order.
Connection, option subscription, rejected quote, malformed message and UW
feed-health transitions are logged as separate events, so a gap in quotes or
flow can be traced back to its feed. Logging is active only while the engine
runs; a missing feed cannot be reconstructed later from these records.
Tradier option-contract trade messages are discarded from the underlying
tick path; only their quote messages can mark open options. The subscription
explicitly requests trade, timesale and quote events. Routine `summary`
messages are ignored without an error diagnostic. A UW connection failure
records the error class and HTTP status when available, without credentials.
The order journal retries temporary Windows access denials on atomic replace
before failing closed; a failed journal write never authorizes an order POST.
The event stream includes warmup bar counts, feed stale/recovery and connection
changes, entry decisions and unavailable option asks, exit decisions with bid
and peak gain, order intent/submission/status/cancel/rejection, and confirmed
entry/exit fills. It records selected contract and quoted ask, and records
broker fills separately from quotes. Secrets and raw API payloads are excluded.
For each open contract it also logs **every received option quote** (bid, ask,
exchange timestamp, receive timestamp, age and whether the strategy accepted
it). The exit order journal retains the bid, ask, peak bid and quote age seen
at the trigger. `broker.trade_observations` in the EOD report separates the
peak-to-trigger decline from the trigger-bid-to-confirmed-fill difference,
includes first/peak/last observed quotes and flags missing history. Confirmed
fill time is the broker reconciliation time, not the exchange execution time;
even complete local quote history cannot prove which part of a bid-to-fill
gap came from spread, price movement, or sandbox simulation. The report lists
first opposing UW, first shadow exit, the profit-lock warning, actual exit
context and a per-trade checklist of missing evidence. Quotes arriving
before a position is confirmed open or while the engine is stopped are absent.

From 09:30 through 16:59 ET on weekdays, the broker worker refreshes
`logs/eod_YYYY-MM-DD.json` every five minutes; shutdown writes a report as
well. Before 16:05 ET its `report_status` is `INTRADAY_SNAPSHOT`, and the
report records its generation time and the last event included. To regenerate
it offline or review a different date:

```bash
python eod_report.py --date 2026-09-29
```

The report aggregates deduplicated signal reasons and actions, event counts,
confirmed closed trades, exit reasons, live entry ask to exit bid quote changes
with unknown outcomes counted separately, and
the pending/open/blocked broker journal state at report time. Keep the
`logs/` directory and `sandbox_orders.json` together for EOD review. The
JSONL file is append-only, while the EOD JSON is regenerated from the log
and journal. If the process is stopped before market close, its report is
partial and can be regenerated after restart. Existing trades from earlier
versions have P&L in the journal but no retroactive signal or option quote trail.

```bash
pip install -r requirements.txt

# Run the full test suite (no credentials needed --
# everything network-facing is mocked)
pytest tests/ -q

# Dashboard only, no live data:
uvicorn dashboard_app:app --reload --port 8000

# The real thing -- needs a Tradier token:
cp .env.example .env
# edit .env: set TRADIER_ACCESS_TOKEN (sandbox token first), leave
# TRADIER_ENV=sandbox
python main.py
# open http://localhost:8000 -- watch the Scan tab for real ticks arriving
```

## Sandbox vs. production — a real, hard Tradier limitation, not a bug here

Confirmed directly from Tradier's own FAQ and documentation: **sandbox/paper
accounts have no streaming market data access at all.** ("Presently, we do
not offer a delayed streaming endpoint for paper trading." / "Streaming
data is available for the live account connection only.") A sandbox token
hitting `tradier_stream.py`'s WebSocket will always get a 401 — this is
Tradier working as documented, not a wrong token or a code problem.

`main.py` handles the separated credentials based on `TRADIER_ENV`:
- **`TRADIER_ENV=sandbox`** → requires `TRADIER_LIVE_DATA_TOKEN` and uses
  the production market-data endpoint for historical bars, underlying ticks,
  option chains, live option bids/asks, and quote recovery. The sandbox
  token/account is used only for order preview, placement, fill reconciliation,
  and execution accounting. Startup fails when the live token is absent.
- **`TRADIER_ENV=production`** → uses `tradier_stream.py`, the real
  WebSocket, requires a live, funded Tradier brokerage account with
  real-time market data entitlement.

The exit ladder uses the live entry ask and live option bid. Exposure and
daily-loss entry limits use live quote references; sandbox fills remain separate
in the broker journal for order reconciliation and execution accounting. The
dashboard's quote result is not actual broker P&L. Existing
open journal records made before this separation lack a live entry ask and
require reconciliation before automated exits can resume. Orders remain
sandbox-only.

## Wiring in a real live connection

**Tradier: done, needs a live smoke test.** `main.py` already loads
`TRADIER_ACCESS_TOKEN` from the environment (never hardcode it — see
`.env.example`), builds a real `TradierRestClient` and the appropriate
transport (see above), and wires both into a `TradingRuntime` and the
dashboard. Run it against `TRADIER_ENV=sandbox` first and confirm quotes
show up on the Scan tab before ever pointing it at production.

**UW: documented wire protocol, pending live validation.** `main.py` builds a `UWStreamClient`
automatically when `UW_API_KEY` is set in `.env`, wired into
`TradingRuntime.on_net_flow` / `on_interval_flow` / `on_market_tide` / `on_gex`.
The client joins those channels individually and parses `[channel, payload]`
frames. Net-flow windows reflect changes in session-cumulative net premiums,
with a fresh baseline after reconnect. The GEX aggregate contributes only a
positive/negative sign; it does not provide a wall or verified price target.
The scan shows the last PSAR decision and its time until another flip; its
net-premium windows can change direction as new options flow arrives.
If `UW_API_KEY` is blank, `main.py` runs with Tradier alone —
every UW confluence read shows `NOT_READY`, which `entry_pipeline.py`
already treats as "no opinion," not an error.

**Dynamic option-quote subscriptions: done.** See "What's real and tested"
above.

**Sandbox order flow (opt in):** set `TRADIER_ENV=sandbox`, a sandbox
`TRADIER_ACCESS_TOKEN`, `TRADIER_ACCOUNT_ID`, and a separate
`TRADIER_LIVE_DATA_TOKEN` (or explicitly enable delayed mechanical testing).
The engine checks existing
positions and orders, selects an OCC contract after a qualifying signal,
previews a capped limit/day `buy_to_open`, journals the intent, and
submits once. It polls the order until Tradier reports a complete fill with
an executed quantity and average fill price, and verifies the broker
position before displaying an open trade. On an exit trigger it submits a
`sell_to_close` and keeps the trade active until the confirmed fill and
broker position reduction. New entries are limited to weekdays 09:30–15:30
ET. No order is inferred from a price trigger alone.

The default entry limit is at most 5% over the selected ask. A single order
may debit at most $300, total open/pending debit at most $600, and new entries
pause once the daily live quote loss measure reaches -$500 for the ET day. Change these
in `.env` to match the sandbox account. An unfilled entry limit requests
cancellation once after 45 seconds; it stays pending until the broker confirms
the terminal state. Exits use market/day orders for urgency. Quantities must
match the journal exactly at startup, entry confirmation, and close.

The `SANDBOX_ORDER_JOURNAL` file must survive process restarts. An ambiguous
POST response stays pending and can be found by its order tag; the engine
never blindly resends it. Rejected, canceled, or unverified partially filled
orders require manual inspection. A broker position not covered by the
journal prevents startup. Do not delete the journal while the account has
active orders or positions.

On restart, a manually closed sandbox position is reconciled only when Tradier
returns one unique, fully filled `sell_to_close` receipt for the exact contract
and quantity after the journaled entry, with the broker position at zero.
The journal records `MANUAL_BROKER_CLOSE` and the broker fill. Since the manual
close has no live exit quote at its trigger, live quote P&L remains unknown.
The daily-loss guard assumes a zero exit bid and charges the full quote-derived
entry limit against the daily loss cap. Older records without the entry limit
use the live entry ask with the maximum permitted 20% entry slippage; missing
or invalid live entry quotes still block new entries. Broker sandbox fill P&L
is displayed for reconciliation but never used for this guard. A transient Tradier
`Unexpected server error` on an exit preview is logged and bypassed for that
exit only; the actual close submission is still journaled and sent once.
Ambiguous broker receipts continue to stop startup for manual inspection.
A missing underlying feed pauses new entries;
fresh option bids can still trigger option-price exits, and a separate clock
checks the 15:50 ET exit. Diagnostics records each completed PSAR flip decision;
Orders shows pending and blocked
submissions. Sandbox delayed data should only be
used to exercise order plumbing, not to assess strategy performance.

At startup the engine requests up to 90 minutes of current-session 1-minute
time-and-sales for each watched symbol and seeds complete 2-minute candles.
The startup log reports the count. Candle momentum needs at least 12 completed
bars to be classified, but missing momentum history does not block a PSAR flip.
A rejected flip or unavailable live option quote is not retried.
No order is placed from a flip that
occurred in the preloaded history. The Signals tab shows later filters and
reasons (momentum, UW, target) even if no trade qualifies. Entry has no structure evaluation or field.
When there is no fresh directional gamma target, the signal says `NO_VERIFIED_TARGET`
and still evaluates the other entry conditions; it makes no reward-to-risk claim.
If a fresh directional gamma level is too close for the configured minimum
underlying reward-to-risk ratio, it is skipped as `TARGET_RR_TOO_LOW` rather
than replaced with an invented ATR projection. The audit records the actual
level, underlying stop and ratio when present. These are underlying price
estimates, not option exit orders or guaranteed option reward.

Option chains are prefetched from the production market-data endpoint before
the flip, refreshed at most once per symbol every 90 seconds, and refused if
older than 150 seconds. Strike selection uses the underlying price at the
actual flip. The selected OCC contract is then checked against a new live
production bid and ask (both exchange timestamps within 10 seconds) before
the sandbox order is submitted. A cold or stale cache logs
`CHAIN_PREFETCH_UNAVAILABLE` and skips that one-shot flip. This removes
multi-chain REST lookup from the streaming callback, but fresh quote lookup
and sandbox preview/submission still run synchronously there.

Without `TRADIER_ACCOUNT_ID`, trades stay internal simulations. Production
account IDs are refused by `main.py`; this bundle has not placed an order
against a credentialed Tradier sandbox account in this environment.
