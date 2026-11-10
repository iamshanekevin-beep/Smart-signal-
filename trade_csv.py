"""Read-only CSV projection and passive skipped-signal audit. No trading decisions."""
import csv
import hashlib
import io
import json
import math
import queue
import threading
import time
from datetime import datetime, timedelta, timezone
from broker_timing import timing_for_history
from practice_validation import FIELDS

VALIDATION_COLUMNS = (*FIELDS, "band_failed_before")

TRADE_COLUMNS = (
    "date", "time_WAT", "pair", "entry_type", "flow_number", "direction",
    "RSI_at_signal", "body_pct", "breakout_wick_pct", "band_value",
    "prev_close", "signal_close", "payout_pct", "result", "seconds_to_order",
    "submission_timing_source", "order_submitted_at", "broker_accepted_at",
    "seconds_to_broker_acceptance", "broker_timing_status", "broker_timing_source",
    "broker_timestamp_field", "broker_timing_reason", "broker_timing_checked_at",
) + VALIDATION_COLUMNS
SKIP_COLUMNS = ("time_WAT", "pair", "direction", "reason_blocked") + VALIDATION_COLUMNS + ("shadow_result",)
WAT = timezone(timedelta(hours=1), "WAT")


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def first(*values):
    return next((value for value in values if value is not None), None)


def timestamp(value):
    if not numeric(value):
        return None
    try:
        return datetime.fromtimestamp(value, WAT)
    except (ValueError, OverflowError, OSError):
        return None


def trade_row(meta):
    context = meta.get("entryContext") or {}
    stamp = timestamp(meta.get("time"))
    side = meta.get("side")
    entry_type = (
        "REVERSAL_LOSS" if context.get("engine") == "RSI" and context.get("kind") == "LOSS reversal" else
        "REVERSAL_RSI" if context.get("engine") == "RSI" and context.get("kind") == "RSI reversal" else
        "REVERSAL" if context.get("engine") == "RSI" else
        "CONTINUATION" if context.get("engine") == "FCB" and context.get("kind") == "pole" else
        "BREAKOUT" if context.get("engine") == "FCB" and context.get("kind") == "FCB" else None
    )
    body = first(meta.get("body_pct"), context.get("body_pct"))
    wick = first(meta.get("breakout_wick_pct"), context.get("breakout_wick_pct"))
    candle = context.get("signal_candle") or {}
    op, cl = candle.get("open"), candle.get("close")
    hi, lo = first(candle.get("high"), candle.get("max")), first(candle.get("low"), candle.get("min"))
    if all(numeric(v) for v in (op, cl, hi, lo)) and hi > lo and lo <= min(op, cl) <= max(op, cl) <= hi:
        if body is None:
            body = abs(cl - op) / (hi - lo) * 100
        if wick is None and side in ("CALL", "PUT"):
            wick = (hi - max(op, cl) if side == "CALL" else min(op, cl) - lo) / (hi - lo) * 100
    band = first(meta.get("band_value"), context.get("band_value"))
    if band is None and context.get("engine") == "FCB" and side in ("CALL", "PUT"):
        band = context.get("band_high" if side == "CALL" else "band_low")
    terminal = meta.get("terminal") or {}
    payout = first(meta.get("payout_pct"), context.get("payout_pct"), terminal.get("profit_percent"))
    if numeric(payout) and payout > 100:
        payout -= 100
    if payout is None and meta.get("result") == "win":
        profit, stake = meta.get("profit"), meta.get("stake")
        if numeric(profit) and numeric(stake) and stake > 0:
            payout = profit / stake * 100
    delay = first(meta.get("seconds_to_order"), context.get("seconds_to_order"))
    ordered, slot = first(meta.get("order_submitted_at"), meta.get("time")), context.get("execution_candle")
    if delay is None and numeric(ordered) and numeric(slot) and ordered >= slot:
        delay = ordered - slot
    timing_source = ("submission_call" if numeric(meta.get("order_submitted_at")) else
                     "saved_seconds_to_order" if first(meta.get("seconds_to_order"),
                                                       context.get("seconds_to_order")) is not None else
                     "order_intent_estimate" if delay is not None else None)
    broker = timing_for_history(meta)
    validation = context.get("practice_validation") or {}
    body = first(body, validation.get("body_pct"))
    wick = first(wick, validation.get("breakout_wick_pct"))
    return (
        stamp.strftime("%Y-%m-%d") if stamp else None,
        stamp.strftime("%H:%M:%S") if stamp else None,
        meta.get("pair"), entry_type,
        first(meta.get("flow_number"), context.get("flow_number")), side,
        first(meta.get("RSI_at_signal"), context.get("RSI_at_signal"), context.get("rsi_signal"),
              validation.get("RSI_at_signal")),
        body, wick, band, first(meta.get("prev_close"), context.get("prev_close")),
        first(meta.get("signal_close"), context.get("signal_close"), candle.get("close")),
        payout, {"win": "WIN", "loss": "LOSS", "equal": "TIE"}.get(meta.get("result")), delay,
        timing_source, meta.get("order_submitted_at"), broker.get("brokerAcceptedAt"),
        broker.get("secondsToBrokerAcceptance"), broker["status"], broker.get("source"),
        broker.get("timestampField"), broker.get("reason"), broker.get("checkedAt"),
    ) + tuple(validation.get(field) for field in VALIDATION_COLUMNS)


def skip_row(meta):
    stamp = timestamp(meta.get("time"))
    validation = meta.get("practice_validation") or (meta.get("entryContext") or {}).get("practice_validation") or {}
    return (stamp.strftime("%Y-%m-%d %H:%M:%S") if stamp else None,
            meta.get("pair"), meta.get("side"), meta.get("reason_blocked")) + tuple(
                validation.get(field) for field in VALIDATION_COLUMNS
            ) + (meta.get("shadow_result"),)


def safe_cell(value):
    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, (float, int)):
        if not numeric(value):
            return ""
        text = str(value)
        return text[:-2] if text.endswith(".0") else text
    text = str(value)
    # Prevent spreadsheet formula execution in broker names / audit text.
    return "'" + text if text.lstrip().startswith(("=", "+", "-", "@", "\t", "\r")) else text


def render_csv(rows, skipped=False):
    out = io.StringIO(newline="")
    out.write("\ufeff")  # Excel-compatible UTF-8.
    writer = csv.writer(out)
    writer.writerow(SKIP_COLUMNS if skipped else TRADE_COLUMNS)
    project = skip_row if skipped else trade_row
    for row in rows:
        writer.writerow([safe_cell(value) for value in project(row)])
    return out.getvalue()


class SkipAudit:
    """One non-trading writer thread. Never feeds results back to the executor."""
    def __init__(self):
        self.pending = queue.Queue(maxsize=1024)
        self.failures = 0
        self.shadow_bars = {}
        self.worker = threading.Thread(target=self._write, name="csv-skip-audit", daemon=True)
        self.worker.start()

    def record(self, store, key, pair, side, reason, context=None, intent_id=None):
        writer = getattr(store, "record_skip", None)
        if writer is None:
            return  # Offline test doubles without audit storage.
        context = context or {}
        fingerprint = json.dumps(
            [key, pair, side, context.get("engine"), context.get("signal_time"),
             context.get("execution_candle"), reason, intent_id], sort_keys=True)
        event = {"time": time.time(), "pair": pair, "side": side, "reason_blocked": reason}
        if context.get("practice_validation") is not None:
            event["practice_validation"] = dict(context["practice_validation"])
            from rejection_summary import VALIDATION_POLICY
            event["validation_policy"] = VALIDATION_POLICY
            if (reason in ("ZONE_SKIP", "ZONE_EXIT", "WEAK_CLOSE", "NO_EXPANSION",
                           "BAND_FAILED_BEFORE", "BAND_REJECTIONS", "BREAKOUT_BODY",
                           "BREAKOUT_WICK", "BREAKOUT_RSI", "BREAKOUT_DIRECTION", "NOT_FIRST_CLOSE",
                           "INVALID_BREAKOUT", "BREAKOUT_VALIDATION_FAILED")
                    and side in ("CALL", "PUT") and numeric(context.get("signal_time"))):
                event["shadow_expected_time"] = context["signal_time"] + 60
        try:
            self.pending.put_nowait((writer, key, hashlib.sha256(fingerprint.encode()).hexdigest(),
                                     event, intent_id))
        except queue.Full:
            self.failures += 1

    def resolve(self, store, key, pair, candle):
        writer = getattr(store, "resolve_skip_shadows", None)
        stamp = candle.get("time")
        if writer is None or not numeric(stamp) or stamp + 60 > time.time():
            return
        identity = (key, pair)
        if self.shadow_bars.get(identity) == stamp:
            return
        try:
            self.pending.put_nowait((writer, key, pair, dict(candle), None))
            self.shadow_bars[identity] = stamp
        except queue.Full:
            self.failures += 1

    def _write(self):
        while True:
            writer, key, fingerprint, event, intent_id = self.pending.get()
            try:
                writer(key, fingerprint, event, intent_id)
            except Exception:
                self.failures += 1  # No raw exception/credential logging; export surfaces failures.
            finally:
                self.pending.task_done()

    def check_export(self):
        until = time.monotonic() + 2
        while self.pending.unfinished_tasks and time.monotonic() < until:
            time.sleep(.01)
        if self.failures or self.pending.unfinished_tasks:
            raise RuntimeError("Skipped-signal audit is incomplete or still being saved")
