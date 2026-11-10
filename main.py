"""
Entry point for Replit. Run with the green Run button, or `python main.py`.

STAGE 1: auto-trades on IQ_ACCOUNT_MODE=PRACTICE only (set in Secrets).

Trades a rotating set of ACTIVE_COUNT pairs drawn from iq_feed.PAIR_POOL.
Every PAIR_CHECK_SECONDS, any active pair that's gone closed/suspended
(OTC pairs do this outside their own schedule) is automatically swapped
out for an open pair from the pool that isn't already active.

Endpoints:
  GET /latest-candle?pair=EURUSD-OTC   -> latest closed candle (chart data)
  GET /trades                          -> recent trade results
  GET /status                          -> balance, daily P/L, active pairs,
                                           per-pair engine state
  GET /health                          -> connection status, no secret needed

All endpoints except /health require the shared secret, either as
  ?secret=...              or
  header X-API-Secret: ...
"""

import os
import threading
import time

from flask import Flask, request, jsonify
from flask_cors import CORS

import iq_feed
from strategy import PairEngine
from entry_confirmation import EntryNotConfirmed
from pair_selection import DEFAULT_COUNT, select_pairs

app = Flask(__name__)
CORS(app)

API_SECRET = os.environ.get("API_SECRET")

STAKE = float(os.environ.get("STAKE", "1.0"))
DAILY_LOSS_LIMIT = float(os.environ.get("DAILY_LOSS_LIMIT", "-50"))
MAX_DAILY_TRADES = int(os.environ.get("MAX_DAILY_TRADES", "20"))
LOSS_STREAK_STOP = int(os.environ.get("LOSS_STREAK_STOP", "3"))
ACTIVE_COUNT = DEFAULT_COUNT
EXPIRATION_MINUTES = 1
POLL_SECONDS = 5
PAIR_CHECK_SECONDS = 30

_lock = threading.Lock()
active_pairs = []          # the current working set, length <= ACTIVE_COUNT
suspended_log = []          # recent swap events, most recent first
engines = {}                 # pair -> PairEngine, only for pairs ever active
last_seen_time = {}           # pair -> last candle timestamp seen
open_orders = {}               # order_id -> {"pair","side","reason","stake","time"}
trades_log = []                 # most recent first
daily_pl = 0.0
trades_today = 0
loss_streak = 0
_last_pair_check = 0
execution = None  # Installed by runner; direct execution fails closed.


def check_secret():
    if not API_SECRET:
        return True
    provided = request.args.get("secret") or request.headers.get("X-API-Secret")
    return provided == API_SECRET


def guardrails_ok(allow_cooldown=False):
    if execution is None:
        return False, "durable recovery is not ready"
    safe, reason = execution.ready(allow_cooldown=True) if allow_cooldown else execution.ready()
    if not safe:
        return False, reason
    if trades_today >= MAX_DAILY_TRADES:
        return False, "max daily trades reached"
    if daily_pl <= DAILY_LOSS_LIMIT:
        return False, "daily loss limit hit"
    if loss_streak >= LOSS_STREAK_STOP and not allow_cooldown:
        return False, "loss streak stop"
    return True, None


def activate_pair(pair):
    active_pairs.append(pair)
    if pair not in engines:
        engines[pair] = PairEngine()
    last_seen_time[pair] = None
    api = iq_feed.get_session()
    iq_feed.start_stream(api, pair)


def deactivate_pair(pair):
    if pair in active_pairs:
        active_pairs.remove(pair)
    api = iq_feed.get_session()
    iq_feed.stop_stream(api, pair)


def init_active_pairs(api):
    open_now = iq_feed.get_open_pairs(api, iq_feed.PAIR_POOL)
    chosen = select_pairs([], open_now, iq_feed.PAIR_POOL, ACTIVE_COUNT)
    if len(chosen) < ACTIVE_COUNT:
        print(f"[startup] only {len(chosen)} pairs open right now, "
              f"wanted {ACTIVE_COUNT}")
    for pair in chosen:
        activate_pair(pair)
    print(f"[startup] active pairs: {active_pairs}")


def refresh_active_pairs(api):
    """Replace unavailable pairs from the broker's currently open catalog."""
    open_now = iq_feed.get_open_pairs(api, iq_feed.PAIR_POOL)
    for pair in list(active_pairs):
        if pair in open_now and pair in iq_feed.PAIR_POOL:
            continue
        candidates = select_pairs([], open_now, [
            p for p in iq_feed.PAIR_POOL if p not in active_pairs
        ], ACTIVE_COUNT)
        with _lock:
            deactivate_pair(pair)
            if candidates:
                replacement = candidates[0]
                activate_pair(replacement)
                msg = f"{pair} suspended -> replaced with {replacement}"
            else:
                replacement = None
                msg = f"{pair} suspended -> no open replacement available right now"
            suspended_log.insert(0, {
                "time": time.time(), "suspended": pair, "replacement": replacement,
            })
            del suspended_log[20:]
        print(f"[pair swap] {msg}")
    # Fill empty slots when catalog instruments reopen.
    for pair in select_pairs([], open_now, iq_feed.PAIR_POOL, len(iq_feed.PAIR_POOL)):
        if len(active_pairs) >= ACTIVE_COUNT:
            break
        if pair in open_now and pair not in active_pairs:
            with _lock:
                activate_pair(pair)


def record_csv_skip(pair, side, reason, entry_context=None, token=None):
    """Passive audit hook only; its failure must not alter any trading decision."""
    observer = getattr(execution, "record_skipped", None)
    if observer:
        observer(pair, side, reason, entry_context, token)


def place_trade(api, pair, side, reason, entry_context=None):
    global trades_today
    if side not in ("CALL", "PUT"):
        raise ValueError("Signal direction must be CALL or PUT")
    signal_side = side
    ok, why = guardrails_ok()
    if not ok:
        print(f"[guardrail] skipped {pair} {side}: {why}")
        record_csv_skip(pair, side, why, entry_context)
        return
    token = None
    try:
        token = execution.reserve(api, {
            "pair": pair, "side": side, "signalSide": signal_side,
            "executionMode": "FOLLOW_SIGNAL", "reason": reason,
            "stake": STAKE, "time": time.time(), "expirationMinutes": EXPIRATION_MINUTES,
            **({"entryContext": entry_context} if entry_context is not None else {}),
        })
        with execution.submission(api, token):
            try:
                check, order_id = api.buy(STAKE, pair, side.lower(), EXPIRATION_MINUTES)
            except Exception:
                # The broker may have accepted this submission. Keep its durable intent.
                raise RuntimeError("Order submission outcome is unknown") from None
    except EntryNotConfirmed as error:
        record_csv_skip(pair, side, str(error), entry_context, token)
        return None  # Definitely not sent: no recovery or guessed settlement needed.
    if not check:
        if isinstance(order_id, str):
            execution.reject(token)  # Explicit negative broker response.
            record_csv_skip(pair, side, "Broker explicitly rejected the order", entry_context, token)
        else:
            raise RuntimeError("Order submission outcome is unknown")
        return
    execution.acknowledge(token, order_id)
    return token


def poll_open_orders(api):
    global daily_pl, loss_streak
    for order_id, meta in list(open_orders.items()):
        try:
            order = api.get_async_order(order_id)
        except Exception as e:
            print(f"[order poll error] {order_id}: {e}")
            continue
        if order is None:
            continue  # still pending

        # Field names here are from this fork's undocumented order payload --
        # print(order) once on your own account to confirm these match.
        win_status = str(order.get("win") or order.get("status") or "").lower()
        profit = order.get("profit_amount") or order.get("win_amount") or 0.0
        if win_status in ("win", "won"):
            result, profit = "win", abs(profit) if profit else STAKE * 0.80
        elif win_status in ("loose", "lose", "lost"):
            result, profit = "loss", -meta["stake"]
        else:
            result, profit = "equal", 0.0

        with _lock:
            daily_pl = round(daily_pl + profit, 2)
            loss_streak = loss_streak + 1 if result == "loss" else 0
            trades_log.insert(0, {
                "time": meta["time"], "pair": meta["pair"], "side": meta["side"],
                "reason": meta["reason"], "stake": meta["stake"],
                "result": result, "profit": round(profit, 2),
            })
            del trades_log[50:]
            del open_orders[order_id]
        print(f"[resolved] {meta['pair']} {meta['side']} -> {result} ({profit:+.2f})")


def trading_loop():
    global _last_pair_check
    api = iq_feed.get_session()
    init_active_pairs(api)
    _last_pair_check = time.time()

    while True:
        try:
            api = iq_feed.get_session()

            if time.time() - _last_pair_check >= PAIR_CHECK_SECONDS:
                refresh_active_pairs(api)
                _last_pair_check = time.time()

            for pair in list(active_pairs):
                candle = iq_feed.latest_candle(pair)
                if not candle or candle["time"] == last_seen_time.get(pair):
                    continue
                last_seen_time[pair] = candle["time"]
                _, entry = engines[pair].step(candle)
                if entry:
                    place_trade(api, pair, entry["side"], entry["reason"])
            poll_open_orders(api)
        except Exception as e:
            print(f"[trading loop error] {e}")
        time.sleep(POLL_SECONDS)


@app.route("/latest-candle")
def latest_candle_route():
    if not check_secret():
        return jsonify({"error": "unauthorized"}), 401
    pair = request.args.get("pair")
    if not pair:
        return jsonify({"error": "missing ?pair="}), 400
    if pair not in active_pairs:
        return jsonify({"error": f"{pair} is not currently active", "active_pairs": active_pairs}), 404
    try:
        candle = iq_feed.latest_candle(pair)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503
    if candle is None:
        return jsonify({"error": "no candles buffered yet, try again shortly"}), 503
    return jsonify(candle)


@app.route("/candles")
def candles_route():
    """Recent candle history for a pair -- used by the dashboard to backfill
    its chart quickly when you switch pairs, instead of waiting ~40 polls
    for /latest-candle to build up enough history on its own."""
    if not check_secret():
        return jsonify({"error": "unauthorized"}), 401
    pair = request.args.get("pair")
    if not pair:
        return jsonify({"error": "missing ?pair="}), 400
    if pair not in active_pairs:
        return jsonify({"error": f"{pair} is not currently active", "active_pairs": active_pairs}), 404
    try:
        api = iq_feed.get_session()
        raw = api.get_realtime_candles(pair, iq_feed.CANDLE_SIZE)
    except Exception as e:
        return jsonify({"error": str(e)}), 503
    if not raw:
        return jsonify([])
    ordered_ts = sorted(raw.keys())[-40:]
    out = [{
        "time": ts, "open": raw[ts]["open"], "high": raw[ts]["max"],
        "low": raw[ts]["min"], "close": raw[ts]["close"],
    } for ts in ordered_ts]
    return jsonify(out)


@app.route("/trades")
def trades_route():
    if not check_secret():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(trades_log[:20])


@app.route("/status")
def status_route():
    if not check_secret():
        return jsonify({"error": "unauthorized"}), 401
    try:
        api = iq_feed.get_session()
        balance = api.get_balance()
    except Exception as e:
        return jsonify({"error": str(e)}), 503
    ok, why = guardrails_ok()
    return jsonify({
        "balance": balance,
        "daily_pl": daily_pl,
        "trades_today": trades_today,
        "loss_streak": loss_streak,
        "max_loss_hit": not ok,
        "guardrail_reason": why,
        "account_mode": iq_feed.ACCOUNT_MODE,
        "active_pairs": active_pairs,
        "recent_pair_swaps": suspended_log[:5],
        "engines": {p: {"trend": engines[p].trend, "reason": engines[p].reason} for p in active_pairs},
    })


@app.route("/health")
def health():
    try:
        api = iq_feed.get_session()
        return jsonify({"ok": api.check_connect(), "account_mode": iq_feed.ACCOUNT_MODE, "active_pairs": active_pairs})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 503


if __name__ == "__main__":
    raise SystemExit("Run runner.py for durable recovery and manual trading controls.")
