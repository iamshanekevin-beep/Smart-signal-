"""Bounded point-in-time observations, never broker execution/fill prices."""
import copy
import json
from entry_confirmation import number, ENTRY_WINDOW_SECONDS

VERSION = 1
MAX_CANDLES = 40
MAX_BYTES = 24000
FOCUS = ("Anthropic-OTC", "CARDANO-OTC", "EURGBP-OTC", "AUDUSD-OTC")


def observation_context(candle, now, broker_now, window_seconds=ENTRY_WINDOW_SECONDS):
    """Allowlist actual observed fields; absent data stays absent."""
    quote = {key: candle[key] for key in
             ("time", "open", "high", "low", "close", "observed_at", "fcb_high", "fcb_low")
             if candle is not None and key in candle}
    observed = quote.get("observed_at")
    return {
        "quote": quote, "local_time": now, "broker_time": broker_now,
        "clock_skew_seconds": broker_now - now if number(now) and number(broker_now) else None,
        "local_quote_age_seconds": now - observed if number(now) and number(observed) else None,
        "broker_quote_age_seconds": broker_now - observed if number(broker_now) and number(observed) else None,
        "max_freshness_seconds": 3, "confirmation_window_seconds": window_seconds,
        "price_kind": "observed_quote_not_broker_fill",
    }


def capture(engine, entry, candle, now, broker_now):
    slot = entry["execution_candle"]
    rows = [row for row in engine.candles
            if number(row.get("time")) and row["time"] + 60 <= min(now, broker_now, slot)]
    # Never pad gaps, extrapolate bars, or read the chart's later candle cache.
    closed = [{"time": row["time"], "open": row["open"], "close": row["close"],
               "high": row["max"], "low": row["min"]} for row in rows[-MAX_CANDLES:]]
    evidence = {
        "version": VERSION, "source": "broker_closed_candles",
        "timeframe_seconds": 60, "captured_at": now, "closed_candles": closed,
        "closed_bands": {"model": "quadcode_fractal", "high": engine.bands.upper,
                         "low": engine.bands.lower, "as_of": engine.bands.last_time},
        "confirmation": observation_context(candle, now, broker_now),
        "checks": {},
    }
    return bounded_copy(evidence)


def bounded_copy(evidence):
    """Guard both candle count and serialized size at the journal boundary."""
    if not isinstance(evidence, dict) or evidence.get("version") != VERSION:
        raise ValueError("Unsupported market evidence")
    rows = evidence.get("closed_candles")
    if not isinstance(rows, list) or len(rows) > MAX_CANDLES:
        raise ValueError("Market candle evidence exceeds its bound")
    encoded = json.dumps(evidence, allow_nan=False, separators=(",", ":"))
    if len(encoded.encode()) > MAX_BYTES:
        raise ValueError("Market evidence exceeds its byte bound")
    return copy.deepcopy(evidence)
