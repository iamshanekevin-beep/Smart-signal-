"""Passive individual-order acceptance audit. No quotes, buys or expiry inference."""
import time
from order_recovery import evidence_from_row, history_row, number

SOURCE = "portfolio.get-history-positions"
CUTOFF_SECONDS = 2.0


def unknown(reason="Individual broker creation timestamp not verified"):
    return {"status": "unknown", "brokerAcceptedAt": None,
            "secondsToBrokerAcceptance": None, "source": None,
            "timestampField": None, "reason": reason}


def verify_timing(api, meta, row):
    """Only the proven portfolio raw_event.open_time schema establishes creation."""
    slot = (meta.get("entryContext") or {}).get("execution_candle")
    if not number(slot) or slot <= 0 or slot % 60 != 0:
        return unknown("Saved execution candle unavailable or invalid")
    if not isinstance(row, dict) or row.get("historySource") != SOURCE:
        return unknown("Individual broker creation timestamp not verified")
    try:
        evidence = evidence_from_row(api, meta, str(meta["orderId"]), row)
        if evidence["outcome"] != "settled":
            return unknown("No verified accepted individual order")
        # If a settlement is already known, contradictory history cannot audit it.
        terminal = meta.get("terminal") or {}
        if terminal:
            net = terminal["profit_amount"] - terminal["amount"]
            if abs(net - evidence["profit"]) > .01:
                return unknown("Individual history conflicts with saved settlement")
        elif number(meta.get("profit")) and abs(meta["profit"] - evidence["profit"]) > .01:
            return unknown("Individual history conflicts with saved settlement")
        delta = row["created"] - slot
        if delta < 0:
            return unknown("Broker creation precedes saved execution candle")
    except (RuntimeError, KeyError, TypeError, ValueError, OverflowError):
        return unknown("Individual broker identity, amounts or timestamps unverified")
    return {
        "status": "late_after_2s" if delta > CUTOFF_SECONDS else "within_2s",
        "brokerAcceptedAt": row["created"], "secondsToBrokerAcceptance": delta,
        "source": SOURCE, "timestampField": "raw_event.open_time", "reason": None,
    }


def timing_for_history(meta):
    """Old missing evidence stays explicitly unknown without rewriting the archive."""
    return meta.get("brokerTiming") or unknown()


def audit_one(api, store, key):
    """One bounded read-only broker query plus timing-only durable annotation."""
    candidate = store.timing_candidate(key, time.time() - 300)
    if candidate is None:
        return
    row_id, meta = candidate
    try:
        row = history_row(api, meta["orderId"], meta)
        audit = verify_timing(api, meta, row)
    except Exception:
        audit = unknown("Individual broker history unavailable or ambiguous")
    store.record_timing(key, row_id, {**audit, "checkedAt": time.time()})
