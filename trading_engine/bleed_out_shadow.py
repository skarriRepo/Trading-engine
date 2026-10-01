"""Bleed-out exit hypothesis for a never-armed, actively declining position.
Produces observations and never submits orders -- same contract as
bid_exit_shadow.py, and intended to be read alongside it.

REASONED FROM REAL DATA, NOT YET A LIVE GATE. Replayed against every
EXIT_DECISION row from the two full real trading days available
(2026-09-30, 97162 log lines; 2026-10-01, 19358 log lines -- 39 closed
trades, 200 decision snapshots total; see the exit-analysis document
delivered 2026-10-01 for the full writeup):

  - Fired on 11 of 39 real trades. Where it would have changed the outcome
    (2 trades), it improved both: one rode to -33.33% (EMERGENCY_OPTION_
    STOP) that this would have cut at -14.81%; one rode to -26.19%
    (STALL_EXIT, but late) that this would have cut at -18.25%.
  - Zero cases of cutting short a trade that went on to finish positive.

STALL_EXIT (exit_pipeline.py) is bar-based (fires after one completed bar
with no new bid high), not purely time-based -- but a small intermediate
bounce still resets last_new_peak_ts and restarts its clock, which is
exactly how both real trades above kept riding down unprotected despite
STALL_EXIT already being active. This hypothesis is a different, simpler
backstop: it does not care whether some local peak reset the stall clock,
only whether the loss right now, relative to entry, already exceeds a flat
threshold. The two mechanisms are complementary, not redundant.

What this has NOT been checked against: only 2 real days are available, and
every trade where this rule actually changed anything came from 2026-09-30
specifically -- 2026-10-01 contributed zero differentiating evidence. Do
not promote bleed_out_loss_pct off ExitConfig's shadow-only role, and do
not treat -10.0% as tuned, until this has been replayed against more real
sessions the same way bid_exit_shadow and the deferred-confirmation design
were before being trusted.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .exit_pipeline import ExitConfig, PositionState


@dataclass(frozen=True)
class BleedOutRead:
    reason: Optional[str]
    gain_pct: float
    peak_gain_pct: float
    armed: bool


def read_bleed_out_exit(pos: PositionState, quote_fresh: bool,
                         config: ExitConfig) -> BleedOutRead:
    """Evaluate only the flat-loss backstop for a never-armed position.

    This does not change ``pos``. A stale or missing quote cannot produce a
    reading -- the same discipline as bid_exit_shadow.py: missing evidence
    must read as missing evidence, never as a confident exit call.
    """
    gain = pos.gain_pct()
    peak = pos.peak_gain_pct()
    reason = None
    if (not pos.armed and quote_fresh and pos.current_option_price > 0
            and gain <= config.bleed_out_loss_pct + config.epsilon):
        reason = "BLEED_OUT_EXIT"
    return BleedOutRead(reason=reason, gain_pct=gain, peak_gain_pct=peak, armed=pos.armed)
