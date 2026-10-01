"""Quote-only exit hypothesis. Produces observations and never submits orders."""
from __future__ import annotations

from dataclasses import dataclass

from .exit_pipeline import ExitConfig, PositionState, _past_et_cutoff


@dataclass(frozen=True)
class BidExitRead:
    reason: str | None
    gain_pct: float
    peak_gain_pct: float
    armed: bool
    trail: float
    giveback: float


def read_bid_exit(pos: PositionState, now: float, quote_fresh: bool,
                  config: ExitConfig) -> BidExitRead:
    """Evaluate only EOD, live-bid emergency loss and spread-aware giveback.

    This does not change ``pos``. Missing/stale quotes cannot produce a price
    exit; EOD remains observable even if the quote feed has stopped.
    """
    spread = pos.last_quote_spread
    peak_spread = pos.peak_quote_spread or spread
    arm_gain = max(config.spread_arm_multiple * peak_spread,
                   config.premium_arm_fraction * pos.entry_option_price)
    armed = pos.armed or (quote_fresh and spread > 0 and peak_spread > 0 and
                          pos.peak_option_price >= pos.entry_option_price +
                          arm_gain - config.epsilon)
    earned = max(0.0, pos.peak_option_price - pos.entry_option_price)
    trail = max(config.spread_trail_multiple * peak_spread,
                config.peak_profit_giveback_fraction * earned)
    giveback = max(0.0, pos.peak_option_price - pos.current_option_price)
    reason = None
    if _past_et_cutoff(now, config.eod_force_close_et):
        reason = "EOD_FORCE_CLOSE"
    elif quote_fresh and pos.current_option_price > 0:
        if pos.gain_pct() <= config.emergency_loss_pct + config.epsilon:
            reason = "EMERGENCY_OPTION_STOP"
        elif armed and spread > 0 and giveback >= trail - config.epsilon:
            reason = "PROFIT_GIVEBACK_EXIT"
    return BidExitRead(reason, pos.gain_pct(), pos.peak_gain_pct(), armed, trail, giveback)
