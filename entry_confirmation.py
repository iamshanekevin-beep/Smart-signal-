"""FCB and RSI next-candle confirmation."""
import math

ENTRY_WINDOW_SECONDS = 5.0
LATE_ORDER_REASON = "LATE_ORDER.Op(Skipped)"
CLOSED_BREAKOUT_POLICY = "closed_breakout_next_candle"
ALIGNED_BREAKOUT_POLICY = "directional_closed_breakout_next_candle"
CLEAN_BREAKOUT_POLICY = "clean_breakout_win_only"
STANDARD_BREAKOUT_POLICY = "standard_fcb_ab_next_candle"
CLEAN_POLE_POLICY = "clean_verified_win_pole"


class EntryNotConfirmed(RuntimeError):
    """A stale decision was safely cancelled before broker submission."""


def order_window_open(entry, now, broker_now, window_seconds=ENTRY_WINDOW_SECONDS):
    slot = entry.get("execution_candle")
    return (all(number(value) for value in (slot, now, broker_now))
            and number(window_seconds) and window_seconds > 0
            and 0 <= now - slot <= window_seconds
            and 0 <= broker_now - slot <= window_seconds)


def enforce_order_window(entry, now, broker_now):
    slot = entry.get("execution_candle")
    if number(slot) and any(number(value) and value > slot + ENTRY_WINDOW_SECONDS
                            for value in (now, broker_now)):
        raise EntryNotConfirmed(LATE_ORDER_REASON)
    if not order_window_open(entry, now, broker_now):
        raise EntryNotConfirmed("Order candle-open timing cannot be verified")


def current_decision(entry, now, broker_now, window_seconds=ENTRY_WINDOW_SECONDS):
    if not order_window_open(entry, now, broker_now, window_seconds):
        return False
    if (isinstance(broker_now, bool) or not isinstance(broker_now, (int, float))
            or not math.isfinite(broker_now) or abs(broker_now - now) > 3):
        return False
    slot = entry["execution_candle"]
    if not slot <= now < slot + 60 or not slot <= broker_now < slot + 60:
        return False
    if entry.get("engine") in ("FCB", "RSI"):
        confirmed = entry.get("confirmed_at")
        win_pole = (entry["engine"] == "FCB" and entry.get("kind") == "pole"
                    and entry.get("trigger") == "fcb_win" and entry.get("win_token")
                    and entry.get("win_expiry") == slot)
        expected = ("fcb_win" if win_pole else "candle" if entry["engine"] == "FCB"
                    else "rsi_reversal" if entry.get("kind") == "RSI reversal"
                    else "rsi_momentum" if entry.get("kind") == "RSI momentum"
                    else "loss_recovery")
        if (not number(confirmed) or not slot <= confirmed <= now
                or entry.get("trigger") != expected
                or now > slot + window_seconds or broker_now > slot + window_seconds):
            return False
    return entry["signal_time"] + 60 == slot


def number(value):
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)


def clear_fcb_price(entry, candle):
    """Known, non-collapsed bands and price strictly outside in signal direction."""
    if not candle:
        return False
    upper, lower, price = entry.get("band_high"), entry.get("band_low"), candle.get("close")
    if not all(number(v) and v > 0 for v in (upper, lower, price)) or upper <= lower:
        return False
    if entry.get("band_model") == "quadcode_fractal":
        live_upper, live_lower = candle.get("fcb_high"), candle.get("fcb_low")
        if not all(number(v) and v > 0 for v in (live_upper, live_lower)) or live_upper <= live_lower:
            return False
        upper, lower = max(upper, live_upper), min(lower, live_lower)
    return (entry.get("side") == "CALL" and price > upper
            or entry.get("side") == "PUT" and price < lower)


def pole_quote_ready(entry, candle, now, broker_now, window_seconds=ENTRY_WINDOW_SECONDS):
    """A verified win needs a fresh outside-FCB quote, not extra price extension."""
    if (entry.get("kind") != "pole" or entry.get("trigger") != "fcb_win"
            or not entry.get("win_token") or entry.get("win_expiry") != entry["execution_candle"]
            or entry.get("side") not in ("CALL", "PUT")
            or not candle or not current_decision(entry, now, broker_now, window_seconds)
            or candle.get("time") != entry["execution_candle"]):
        return False
    if entry.get("confirmation_policy") == CLEAN_POLE_POLICY and not breakout_direction_confirmed(entry):
        return False
    values = [candle.get("open"), candle.get("close"), candle.get("observed_at")]
    if not all(number(value) for value in values):
        return False
    opened, price, observed = values
    return (opened > 0 and price > 0
            and entry["execution_candle"] <= observed <= entry["execution_candle"] + window_seconds
            and abs(observed - now) <= 3 and abs(observed - broker_now) <= 3
            and clear_fcb_price(entry, candle))


def breakout_direction_confirmed(entry, signal=None):
    """A must agree with the order; optionally verify saved A against real history."""
    opened, closed = entry.get("signal_open"), entry.get("signal_close")
    if not all(number(value) and value > 0 for value in (opened, closed)):
        return False
    if signal is not None and (
            signal.get("time") != entry.get("signal_time")
            or signal.get("open") != opened or signal.get("close") != closed):
        return False
    return (entry.get("side") == "CALL" and closed > opened
            or entry.get("side") == "PUT" and closed < opened)


def fcb_price_confirmed(entry, candle, now, broker_now, window_seconds=ENTRY_WINDOW_SECONDS):
    """A confirms new breakouts; B needs a fresh outside-band quote, not extension.

    New breakouts also require A's body to agree with the signal. Archived
    policies retain their recorded rules; candle continuations keep extension.
    """
    if not candle or not current_decision(entry, now, broker_now, window_seconds) or not clear_fcb_price(entry, candle):
        return False
    if candle.get("time") != entry["execution_candle"]:
        return False
    values = [candle.get("open"), candle.get("close"), candle.get("observed_at"),
              entry.get("band_high"), entry.get("band_low"), entry.get("signal_close")]
    if not all(number(v) for v in values):
        return False
    opened, price, observed, upper, lower, previous = values
    if (upper < lower or not entry["execution_candle"] <= observed <= entry["execution_candle"] + window_seconds
            or abs(observed - now) > 3 or abs(observed - broker_now) > 3):
        return False
    policy = entry.get("confirmation_policy")
    if policy in (CLOSED_BREAKOUT_POLICY, ALIGNED_BREAKOUT_POLICY,
                  CLEAN_BREAKOUT_POLICY, STANDARD_BREAKOUT_POLICY):
        if policy != CLOSED_BREAKOUT_POLICY and not breakout_direction_confirmed(entry):
            return False
        return (min(opened, price, upper, lower, previous) > 0 and upper > lower
                and entry.get("kind") == "FCB" and entry.get("trigger") == "candle"
                and (entry.get("side") == "CALL" and previous > upper
                     or entry.get("side") == "PUT" and previous < lower))
    if entry.get("band_model") == "quadcode_fractal":
        live_upper, live_lower = candle.get("fcb_high"), candle.get("fcb_low")
        if not all(number(v) for v in (live_upper, live_lower)) or live_upper < live_lower:
            return False
        upper, lower = max(upper, live_upper), min(lower, live_lower)
    if entry["side"] == "CALL":
        return price > max(upper, opened, previous)
    if entry["side"] == "PUT":
        return price < min(lower, opened, previous)
    return False


def rsi_price_confirmed(entry, candle, now, broker_now):
    """Opposite confirmed-loss recovery needs a fresh quote, not a 70/30 turn."""
    if not candle or not current_decision(entry, now, broker_now):
        return False
    if candle.get("time") != entry["execution_candle"] or not entry.get("loss_token"):
        return False
    values = [candle.get("open"), candle.get("close"), candle.get("observed_at")]
    if not all(number(v) for v in values):
        return False
    opened, price, observed = values
    if (opened <= 0 or price <= 0
            or not entry["execution_candle"] <= observed <= entry["execution_candle"] + ENTRY_WINDOW_SECONDS
            or abs(observed - now) > 3 or abs(observed - broker_now) > 3):
        return False
    return (entry.get("loss_side") in ("CALL", "PUT")
            and entry.get("side") == ("PUT" if entry["loss_side"] == "CALL" else "CALL"))


def reversal_price_confirmed(entry, candle, now, broker_now):
    """Require fresh, directional price action in the immediate next candle."""
    if not candle or not current_decision(entry, now, broker_now):
        return False
    if (entry.get("engine") != "RSI" or entry.get("kind") != "RSI reversal"
            or entry.get("trigger") != "rsi_reversal"
            or candle.get("time") != entry.get("execution_candle")):
        return False
    values = [candle.get("open"), candle.get("close"), candle.get("observed_at"),
              entry.get("signal_close")]
    if not all(number(value) for value in values):
        return False
    opened, price, observed, signal_close = values
    if (opened <= 0 or price <= 0
            or not entry["execution_candle"] <= observed <= entry["execution_candle"] + ENTRY_WINDOW_SECONDS
            or abs(observed - now) > 3 or abs(observed - broker_now) > 3):
        return False
    return ((entry.get("side") == "CALL" and price > opened and price > signal_close)
            or (entry.get("side") == "PUT" and price < opened and price < signal_close))


def momentum_price_confirmed(entry, candle, now, broker_now):
    """Require fresh next-candle continuation in the RSI extreme's direction."""
    if not candle or not current_decision(entry, now, broker_now):
        return False
    if (entry.get("engine") != "RSI" or entry.get("kind") != "RSI momentum"
            or entry.get("trigger") != "rsi_momentum"
            or candle.get("time") != entry.get("execution_candle")):
        return False
    values = [candle.get("open"), candle.get("close"), candle.get("observed_at"),
              entry.get("signal_close")]
    if not all(number(value) for value in values):
        return False
    opened, price, observed, signal_close = values
    if (opened <= 0 or price <= 0
            or not entry["execution_candle"] <= observed <= entry["execution_candle"] + ENTRY_WINDOW_SECONDS
            or abs(observed - now) > 3 or abs(observed - broker_now) > 3):
        return False
    return ((entry.get("side") == "CALL" and price > opened and price > signal_close)
            or (entry.get("side") == "PUT" and price < opened and price < signal_close))