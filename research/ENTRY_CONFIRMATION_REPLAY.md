# Entry confirmation study — October 2, 2026

No trading rule was changed. The evidence is incomplete and mixed.

Input: engine_2026-10-02330pm.zip, JSONL through 15:30 ET. Study 49 fully
closed recorded trades, using live entry asks and recorded exit-trigger bids.
Sandbox fill prices are excluded from all return calculations.

Candidate rule: completed PSAR flip; enter a CALL strictly above its candle
high or a PUT strictly below its candle low, during the next 120-second bar.
Expire at that bar's close or an observed opposing flip. Hold the recorded
contract, quantity, exit time, and exit-trigger bid fixed for comparison.

| Cohort | Count | Recorded-entry P&L | Confirmation-entry P&L |
| --- | ---: | ---: | ---: |
| Observed crossing with a usable live ask | 16 | -$90 | -$170 |
| No crossing in recorded next-bar OHLC | 22 | -$112 | $0 if skipped |
| Crossing in OHLC but timing/quote missing | 9 | Not comparable | Unknown |
| Next-bar OHLC missing | 2 | Not comparable | Unknown |

Within the 16 matched trades: 12 deteriorated, 3 were unchanged, 1 improved.
The median observed confirmation delay from next-bar start was 30.925 seconds.
The mechanical comparison of the first two cohorts is -$202 versus -$170
(+$32), but it excludes 11 trades and does not simulate portfolio changes.
All 49 baseline trades total -$222. Do not compare that full total against
-$170 from the smaller cohort as if it were a complete strategy result.

Examples with the same recorded exit-trigger bid:

| Trade | Baseline live ask | Confirmation ask | Baseline P&L | Confirmation P&L |
| --- | ---: | ---: | ---: | ---: |
| QQQ CALL near 12:00 | $1.42 | $1.54 | +$9 | -$3 |
| MSFT CALL near 12:02 | $1.26 | $1.35 | +$9 | $0 |
| QQQ CALL near 13:00 | $0.84 | $0.98 | +$35 | +$21 |

## Limits

Underlying price snapshots are sampled, not the complete tick tape. Nine
OHLC-confirmed crossings were missed by the sampled price records. Option
quotes before the original broker fill are mostly absent. Completed OHLC
is used only to classify missing crossings, never to invent a crossing time
or an entry premium. Quote receipt time must precede the candidate entry;
future quotes are never used backward. Live bid/ask validity, <=10-second
quote age, and <=15% option spread are checked.

Exits are held fixed mechanically, not recomputed against the new entry ask
and time. Original exits include active stall logic; they do not represent
the newly disabled stall policy. Fees, hypothetical fills, order latency,
portfolio capacity, rejected/unfilled candidates, and newly available trades
are not modeled. Different symbols and PSAR versions are mixed within this
single day. These results do not establish profitability or justify promotion.

## Reproduce

```powershell
python .\replay_entry_confirmation.py 'C:\path\engine_2026-10-02330pm.zip' --out entry_confirmation_results.json
python -m unittest tests.test_entry_confirmation_replay -q
```

The next test needs complete underlying ticks and live bid/ask observations
for selected contracts before either candidate entry. Keep current PSAR
entry behavior while collecting that evidence.
