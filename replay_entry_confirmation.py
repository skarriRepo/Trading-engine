"""Compare recorded entries with next-bar price confirmation, without orders.

This is a sampled, matched-contract study, not a full strategy backtest:
underlying ticks were not logged continuously and quotes start after fills.
Recorded exit-trigger bids remain fixed; no sandbox fill is used as a price.
"""
from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
import zipfile


def records(path):
    if str(path).lower().endswith('.zip'):
        with zipfile.ZipFile(path) as archive:
            name = next(n for n in archive.namelist() if n.endswith('.jsonl'))
            with archive.open(name) as stream:
                for line in stream:
                    yield json.loads(line)
    else:
        with open(path, encoding='utf-8') as stream:
            for line in stream:
                yield json.loads(line)


def stamp(row):
    return datetime.fromisoformat(row['recorded_at_et']).timestamp()


def fresh_quote(quotes, times, at):
    index = bisect_right(times, at) - 1
    if index < 0:
        return None
    quote = quotes[index]
    if (0 <= at - quote['market_ts'] <= 10
            and 0 < quote['bid'] <= quote['ask']
            and quote['ask'] - quote['bid'] <= quote['ask'] * .15):
        return quote
    return None


def analyze(rows):
    signals = {}
    selection = {}
    intentions = {}
    entries = {}
    exits = {}
    references = {}
    quotes = defaultdict(list)
    prices = defaultdict(dict)
    candles = defaultdict(dict)
    flips = defaultdict(list)
    for row in rows:
        event, symbol, tid = row.get('event'), row.get('symbol'), row.get('trade_id')
        at = stamp(row)
        if event == 'SIGNAL_DECISION' and row.get('source') == 'PSAR_FLIP':
            signals[symbol] = row
            flips[symbol].append((at, row['direction']))
            if row.get('price_state') == 'FRESH' and row.get('underlying_price'):
                prices[symbol][at] = row['underlying_price']
        elif event == 'CONTRACT_SELECTED':
            selection[symbol] = row
        elif event == 'ORDER_INTENT' and row.get('side') == 'buy_to_open':
            signal, contract = signals.get(symbol), selection.get(symbol)
            if signal and contract and signal['direction'] == contract['direction']:
                intentions[row['tag']] = (signal, contract)
        elif event == 'BROKER_ENTRY_FILLED':
            entries[tid] = row
        elif event == 'BROKER_EXIT_FILLED':
            exits[tid] = row
        elif event == 'BID_EXIT_SHADOW_AT_ACTIVE_EXIT':
            references[tid] = row
        elif event == 'OPTION_QUOTE' and row.get('accepted'):
            quotes[tid].append({**row, 'at': at})
        elif event in ('POSITION_OBSERVATION', 'UW_POSITION_SAMPLE', 'EXIT_DECISION'):
            if row.get('price_state') == 'FRESH' and row.get('underlying_price'):
                prices[symbol][at] = row['underlying_price']
            candle = row.get('completed_bar')
            if candle:
                candles[symbol][candle['ts']] = candle

    details = []
    for tid, close in exits.items():
        reference, entry = references.get(tid), entries.get(tid)
        pair = intentions.get(tid)
        if not reference or not entry or not pair:
            details.append({'trade_id': tid, 'outcome': 'MISSING_BASELINE_DATA'})
            continue
        signal, contract = pair
        candle = signal.get('signal_bar')
        if not candle:
            details.append({'trade_id': tid, 'outcome': 'MISSING_SIGNAL_CANDLE'})
            continue
        side, symbol = signal['direction'], close['symbol']
        start, expiry = signal['bar_ts'] + 120, signal['bar_ts'] + 240
        threshold = candle['high'] if side == 'CALL' else candle['low']
        baseline_ask, exit_bid = reference['entry_live_ask'], reference['option_bid']
        quantity = close['quantity']
        observation_end = min(expiry, stamp(close))
        opposite = next((t for t, d in flips[symbol] if t > stamp(signal) and d != side), expiry)
        observation_end = min(observation_end, opposite)
        available = sorted(quotes[tid], key=lambda q: q['at'])
        quote_times = [q['at'] for q in available]
        confirmed = None
        crossing_seen = False
        samples = 0
        for at, price in sorted(prices[symbol].items()):
            if not max(start, stamp(signal)) <= at < observation_end:
                continue
            samples += 1
            crossed = price > threshold if side == 'CALL' else price < threshold
            if not crossed:
                continue
            crossing_seen = True
            quote = fresh_quote(available, quote_times, at)
            # At selection, a live ask is known and the signal's underlying
            # observation is still recent. Never use a later quote backwards.
            if quote is None and at == stamp(signal) and stamp(contract) - at <= 5:
                if stamp(contract) < observation_end and contract.get('ask', 0) > 0:
                    confirmed = (stamp(contract), contract['ask'], price)
                    break
            elif quote is not None:
                confirmed = (at, quote['ask'], price)
                break
        baseline = (exit_bid - baseline_ask) * quantity * 100
        result = dict(trade_id=tid, symbol=symbol, direction=side,
                      signal_time=signal['recorded_at_et'], threshold=threshold,
                      baseline_live_ask=baseline_ask, fixed_exit_bid=exit_bid,
                      baseline_pnl=round(baseline, 2), exit_reason=close['reasons'],
                      observed_price_samples=samples)
        if confirmed:
            at, ask, price = confirmed
            result.update(outcome='OBSERVED_CONFIRMATION', confirmed_ask=ask,
                          confirmation_delay_sec=round(at - start, 3),
                          confirmation_underlying=price,
                          confirmed_pnl=round((exit_bid - ask) * quantity * 100, 2))
        else:
            result['outcome'] = ('CROSSING_WITHOUT_FRESH_QUOTE' if crossing_seen else
                                 'NO_OBSERVED_CROSSING' if samples else 'NO_PRICE_SAMPLES')
        next_candle = candles[symbol].get(start)
        if next_candle:
            result['next_bar_ohlc_crossing'] = (next_candle['high'] > threshold if side == 'CALL'
                                               else next_candle['low'] < threshold)
        details.append(result)
    matched = [r for r in details if r['outcome'] == 'OBSERVED_CONFIRMATION']
    no_cross = [r for r in details if r['outcome'] == 'NO_OBSERVED_CROSSING']
    return dict(
        study='Sampled next-bar confirmation; same contract and recorded exit trigger bid',
        limitations=['Not a full tick replay: no crossing observed does not prove no crossing occurred.',
                     'Only filled historical trades are studied; blocked/unfilled candidates are excluded.',
                     'Option quotes before the original fill are mostly absent.',
                     'Fixed recorded exits are not recomputed for changed entry time and price.',
                     'No hypothetical fills, fees, portfolio capacity, or missed new trades are modeled.'],
        outcomes=dict(Counter(r['outcome'] for r in details)),
        baseline_pnl_all=round(sum(r.get('baseline_pnl', 0) for r in details), 2),
        matched_baseline_pnl=round(sum(r['baseline_pnl'] for r in matched), 2),
        matched_confirmation_pnl=round(sum(r['confirmed_pnl'] for r in matched), 2),
        no_observed_crossing_baseline_pnl=round(sum(r['baseline_pnl'] for r in no_cross), 2),
        missed_sampled_crossings_proven_by_ohlc=sum(r.get('next_bar_ohlc_crossing', False)
                                                  for r in no_cross),
        details=details)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('log')
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    result = analyze(records(args.log))
    Path(args.out).write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: v for k, v in result.items() if k != 'details'}, indent=2))
