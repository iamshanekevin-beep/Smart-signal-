"""Manual trading controls and durable recovery with confirmed strategy signals."""

import hmac
import math
import os
import threading
import time
from contextlib import contextmanager

os.environ["IQ_ACCOUNT_MODE"] = "PRACTICE"
if not os.environ.get("API_SECRET"):
    raise RuntimeError("API_SECRET must be configured before starting the bot.")

from flask import jsonify, request, Response

import iq_feed
import main as bot
from trade_store import TradeStore
from trade_ledger import TradeLedger, terminal_message, entry_order
from broker_timing import verify_timing, audit_one
from order_recovery import verified_evidence, BrokerHistoryUnavailable
from entry_confirmation import EntryNotConfirmed, current_decision, fcb_price_confirmed, rsi_price_confirmed, reversal_price_confirmed, momentum_price_confirmed, pole_quote_ready, number, breakout_direction_confirmed
from entry_confirmation import enforce_order_window, LATE_ORDER_REASON, ENTRY_WINDOW_SECONDS, ALIGNED_BREAKOUT_POLICY, CLEAN_BREAKOUT_POLICY
from entry_confirmation import STANDARD_BREAKOUT_POLICY
from active_trades import active_trades
from pair_monitoring import PairMonitoring
from trade_csv import SkipAudit, render_csv
from telegram_alerts import TelegramAlerts

skip_audit = SkipAudit()
telegram_alerts = TelegramAlerts()
from market_evidence import bounded_copy, observation_context


def record_skipped(pair, side, reason, context=None, token=None, mode=None):
    """Enqueue an observation, without changing trading or account state."""
    try:
        selected = mode or account_mode
        engine = bot.engines.get(pair)
        if engine is not None and getattr(engine, "practice_zone", False):
            context = engine.annotate_validation(context or {})
        if ledger is not None and selected in ledger.keys:
            skip_audit.record(ledger.store, ledger.keys[selected], pair, side, reason, context, token)
    except Exception:
        skip_audit.failures += 1

from pair_selection import DEFAULT_COUNT, select_pairs, adopt_scan_policy

bot.ACTIVE_COUNT = DEFAULT_COUNT

enabled = threading.Event()
connection_requested = iq_feed.connection_requested
control_lock = threading.RLock()
state_lock = threading.Lock()
connected = False
balance = None
last_error = None
updated_at = time.time()
open_pairs = set()
histories = {}
cached_status = {}
rebuild_pairs = False
account_mode = "PRACTICE"
requested_mode = None
account_states = {}
ledger = None
recovery_required = True
recovery_state = "loading"
recovery_message = "Loading durable account state; entries are blocked."
last_generation = None
scan_pairs = []
scan_updated_at = 0.0
SETTING_ATTRIBUTES = {
    "stake": "STAKE", "dailyLossLimit": "DAILY_LOSS_LIMIT",
    "maxDailyTrades": "MAX_DAILY_TRADES", "lossStreakStop": "LOSS_STREAK_STOP",
    "activePairs": "ACTIVE_COUNT",
}
DEFAULT_SETTINGS = {key: getattr(bot, attr) for key, attr in SETTING_ATTRIBUTES.items()}
MAX_UNRESOLVED_TRADES = 1
# Idle fallback only: confirmed signals still wake the shared executor immediately.
ENTRY_POLL_SECONDS = 0.5
monitor_epoch = 0
monitoring_service = None
timing_checked_at = 0
timing_account = 0


def monitor_context():
    """A nonblocking snapshot; queued entries are revalidated under control_lock."""
    return ((iq_feed.session_generation, account_mode, monitor_epoch),
            enabled.is_set() and requested_mode is None and not recovery_required)


def invalidate_monitor_signals():
    global monitor_epoch
    monitor_epoch += 1
    if monitoring_service is not None:
        monitoring_service.invalidate()


def queue_telegram_signal(pair, entry, token):
    current, allowed = monitor_context()
    if not allowed or current != token:
        return False
    amount = bot.STAKE
    # Account/pause/settings changes must not attach another account's stake.
    current, allowed = monitor_context()
    if not allowed or current != token:
        return False
    return telegram_alerts.enqueue(pair, entry, token[1], amount=amount)


def create_pair_monitoring(background=True):
    def observed(pair, candle):
        bot.last_seen_time[pair] = candle["time"]
        remember_candle(pair, candle)

    def live(api, pair):
        return iq_feed.forming_candle(api, pair, time.time()), api.get_server_timestamp()

    return PairMonitoring(
        context=monitor_context,
        observe=lambda api, pair: iq_feed.latest_candle(pair, api=api),
        live_observe=live,
        on_candle=observed,
        on_failure=enabled.clear,
        on_skip=lambda pair, entry, reason, token: record_skipped(
            pair, entry.get("side"), reason, entry, mode=token[1]),
        on_signal=queue_telegram_signal,
        background=background,
        clock=lambda: time.time(),
    )


def restore_selected():
    """Only publish state that has committed to the durable ledger."""
    global account_states
    if ledger is None:
        return
    with bot._lock:
        account_states = ledger.accounts
        saved = ledger.accounts[account_mode]
        bot.daily_pl = saved["daily_pl"]
        bot.trades_today = saved["trades_today"]
        bot.loss_streak = saved["loss_streak"]
        bot.trades_log[:] = saved["trades_log"]
        bot.open_orders.clear()
        bot.open_orders.update({
            meta["orderId"] or f"intent:{token}": meta
            for token, meta in saved["pending"].items()
        })
        for key, attr in SETTING_ATTRIBUTES.items():
            setattr(bot, attr, saved["settings"][key])


def initialize_ledger(api):
    global ledger, recovery_state, recovery_message, account_mode, requested_mode
    if ledger is not None:
        return
    enabled.clear()
    # Startup never resumes Live. Read-only history can reconcile Live orders
    # without selecting its balance or submitting a new order.
    api.change_balance("PRACTICE")
    if api.get_balance_mode() != "PRACTICE":
        raise RuntimeError("Startup Demo account was not confirmed")
    iq_feed.read_balance(api)
    rows = api.api.balances_raw.get("msg", [])
    identities = {}
    for row in rows:
        mode = {1: "REAL", 4: "PRACTICE"}.get(row.get("type"))
        if mode:
            if mode in identities:
                raise RuntimeError("Ambiguous broker balance identity")
            identities[mode] = row["id"]
    if set(identities) != {"PRACTICE", "REAL"}:
        raise RuntimeError("Broker did not identify both account balances")
    store = TradeStore()
    try:
        candidate = TradeLedger(store)
        for mode, balance_id in identities.items():
            candidate.load(mode, balance_id, DEFAULT_SETTINGS)
            adopt_scan_policy(candidate, mode)
    except Exception:
        store.close()
        raise
    with control_lock:
        ledger = candidate
        account_mode = "PRACTICE"
        iq_feed.ACCOUNT_MODE = "PRACTICE"
        requested_mode = None
        restore_selected()
        recovery_state = "reconciling"
        recovery_message = "Checking saved Demo and Live orders with the broker; entries are blocked."


class DurableExecution:
    record_skipped = staticmethod(record_skipped)

    def __init__(self):
        # Serialize broker buy calls and allow one unresolved trade total.
        # No extra durable write between the final freshness check and buy.
        self.submission_evidence = None
        self.submission_timing = None

    @staticmethod
    def pending_order_items():
        return [
            (mode, token, meta)
            for mode, saved in ledger.accounts.items()
            for token, meta in saved["pending"].items()
        ]

    def reserve_slot_error(self, pair):
        pending = self.pending_order_items()
        if not pair or any(not meta.get("pair") for _, _, meta in pending):
            return "PENDING_PAIR_IDENTITY_UNKNOWN"
        if any(meta.get("pair") == pair for _, _, meta in pending):
            return "PAIR_ALREADY_OPEN"
        if len(pending) >= MAX_UNRESOLVED_TRADES:
            return "ONE_TRADE_AT_A_TIME"
        return None

    def submission_slot_error(self, token):
        pending = self.pending_order_items()
        if not any(item_token == token for _, item_token, _ in pending):
            return "ORDER_RESERVATION_MISSING"
        if len(pending) > MAX_UNRESOLVED_TRADES:
            return "ONE_TRADE_AT_A_TIME"
        pairs = [meta.get("pair") for _, _, meta in pending]
        if any(not pair for pair in pairs):
            return "PENDING_PAIR_IDENTITY_UNKNOWN"
        if len(set(pairs)) != len(pairs):
            return "PAIR_ALREADY_OPEN"
        return None

    @staticmethod
    def evidence_check(context, observation, now, broker_now):
        evidence = context.get("market_evidence")
        if evidence is None:
            return None
        evidence = bounded_copy(evidence)
        evidence["latest_check"] = observation_context(observation, now, broker_now)
        return evidence

    def check_entry(self, api, meta):
        context = meta.get("entryContext")
        policy_engine = bot.engines.get(meta.get("pair"))
        if not isinstance(context, dict) or not context:
            raise EntryNotConfirmed(
                "Entry must match a verified FCB or standalone RSI reversal or momentum signal")
        if getattr(policy_engine, "strict_rules", False) and (
                not context or not (
                    context.get("engine") == "FCB"
                    or context.get("engine") == "RSI" and (
                        context.get("kind") == "RSI reversal"
                        or context.get("kind") == "LOSS reversal"
                        and policy_engine.reversal_enabled
                        or context.get("kind") == "RSI momentum"
                        and policy_engine.momentum_enabled
                    )
                )):
            raise EntryNotConfirmed(
                "Entry must match a verified FCB or standalone RSI reversal or momentum signal")
        if (getattr(policy_engine, "practice_zone", False)
                and (not context or not policy_engine.validation_allowed(context))):
            raise EntryNotConfirmed("ZONE_SKIP" if policy_engine.zone_data["zone"] != "CLEAR"
                                    else "BREAKOUT_VALIDATION_FAILED")
        if context is not None:
            signal_side = context.get("side")
            if (signal_side not in ("CALL", "PUT") or meta.get("signalSide") != signal_side
                    or meta.get("side") != signal_side
                    or meta.get("executionMode") != "FOLLOW_SIGNAL"):
                raise EntryNotConfirmed("Order direction must match the verified signal")
            if (context.get("engine") == "FCB"
                    and (getattr(policy_engine, "strict_rules", False)
                         or context.get("confirmation_policy") in (
                             ALIGNED_BREAKOUT_POLICY, CLEAN_BREAKOUT_POLICY, STANDARD_BREAKOUT_POLICY))
                    and (policy_engine is None or not policy_engine.candles
                         or not breakout_direction_confirmed(context, policy_engine.candles[-1]))):
                raise EntryNotConfirmed("BREAKOUT_DIRECTION; closed signal candle must agree with trade")
            now = time.time()
            broker_now = api.get_server_timestamp()
            enforce_order_window(context, time.time(), broker_now)
            if not current_decision(context, now, broker_now):
                raise EntryNotConfirmed("FAST TRADE decision or broker clock is stale")
            if context.get("engine") == "FCB":
                engine = bot.engines.get(meta["pair"])
                if ((engine is None and "fcb_cycle" in context)
                        or (engine is not None and not engine.fcb_entry_allowed(context))):
                    raise EntryNotConfirmed("Previous pole cycle ended; a fresh FCB signal is required")
                observation = iq_feed.forming_candle(api, meta["pair"], now)
                if context.get("trigger") == "fcb_win":
                    win = next((row for row in ledger.accounts[account_mode]["trades_log"]
                                if row["intentId"] == context.get("win_token")), None)
                    if (win is None or win["result"] != "win" or win["pair"] != meta["pair"]
                            or win["side"] != meta["side"]
                            or win.get("entryContext", {}).get("engine") != "FCB"
                            or win.get("terminal", {}).get("expired") != context["execution_candle"]):
                        raise EntryNotConfirmed("Pole entry does not match a verified same-direction FCB win")
                    if engine is None or engine.last_time != context["signal_time"]:
                        raise EntryNotConfirmed("Pole FCB closed-candle history is unavailable")
                    observation = engine.fcb_observation(observation)
                    now, broker_now = time.time(), api.get_server_timestamp()
                    enforce_order_window(context, time.time(), broker_now)
                    if not pole_quote_ready(context, observation, now, broker_now):
                        raise EntryNotConfirmed("Pole price is not clearly outside FCB, or its quote/window is stale")
                    return self.evidence_check(context, observation, now, broker_now)
                if context.get("band_model") == "quadcode_fractal":
                    engine = bot.engines.get(meta["pair"])
                    if engine is None or engine.last_time != context["signal_time"]:
                        raise EntryNotConfirmed("Fractal FCB signal history is unavailable")
                    observation = engine.fcb_observation(observation)
                now, broker_now = time.time(), api.get_server_timestamp()
                enforce_order_window(context, time.time(), broker_now)
                if not fcb_price_confirmed(context, observation, now, broker_now):
                    raise EntryNotConfirmed("FCB opening-candle price action is no longer confirmed")
            elif context.get("engine") == "RSI":
                engine = bot.engines.get(meta["pair"])
                if context.get("kind") == "LOSS reversal":
                    if engine is None or not engine.reversal_enabled:
                        raise EntryNotConfirmed("Loss reversal signal is no longer current")
                    loss = next((row for row in ledger.accounts[account_mode]["trades_log"]
                                 if row["intentId"] == context.get("loss_token")), None)
                    loss_context = loss.get("entryContext", {}) if loss else {}
                    if (loss is None or loss["result"] != "loss" or loss["pair"] != meta["pair"]
                            or loss_context.get("engine") != "FCB"
                            or loss_context.get("kind") not in ("FCB", "pole")
                            or loss["side"] != context.get("loss_side")
                            or meta["side"] != ("PUT" if loss["side"] == "CALL" else "CALL")
                            or loss.get("terminal", {}).get("expired")
                            != context["signal_time"] + 60):
                        raise EntryNotConfirmed(
                            "Loss reversal requires the verified opposite-direction breakout/continuation loss")
                    observation = iq_feed.forming_candle(api, meta["pair"], now)
                    now, broker_now = time.time(), api.get_server_timestamp()
                    enforce_order_window(context, time.time(), broker_now)
                    if not engine.loss_reversal_entry_allowed(context, observation):
                        raise EntryNotConfirmed("Loss reversal signal is no longer current")
                    if not rsi_price_confirmed(context, observation, now, broker_now):
                        raise EntryNotConfirmed(
                            "Loss reversal is not confirmed by a fresh next-candle quote")
                    return self.evidence_check(context, observation, now, broker_now)
                if context.get("kind") == "RSI reversal":
                    if (engine is None or not engine.reversal_enabled
                            or not engine.reversal_entry_allowed(context)):
                        raise EntryNotConfirmed("Standalone RSI reversal signal is no longer current")
                    observation = iq_feed.forming_candle(api, meta["pair"], now)
                    now, broker_now = time.time(), api.get_server_timestamp()
                    enforce_order_window(context, time.time(), broker_now)
                    if not reversal_price_confirmed(context, observation, now, broker_now):
                        raise EntryNotConfirmed("RSI reversal is not confirmed by fresh next-candle price action")
                    return self.evidence_check(context, observation, now, broker_now)
                if context.get("kind") == "RSI momentum":
                    if (engine is None or not engine.momentum_enabled
                            or not engine.momentum_entry_allowed(context)):
                        raise EntryNotConfirmed("RSI momentum signal is no longer current")
                    observation = iq_feed.forming_candle(api, meta["pair"], now)
                    now, broker_now = time.time(), api.get_server_timestamp()
                    enforce_order_window(context, time.time(), broker_now)
                    if not momentum_price_confirmed(context, observation, now, broker_now):
                        raise EntryNotConfirmed("RSI momentum is not confirmed by fresh next-candle price action")
                    return self.evidence_check(context, observation, now, broker_now)
                if engine is not None and not engine.rsi_enabled:
                    raise EntryNotConfirmed("RSI recovery disabled")
                if getattr(engine, "strict_rules", False):
                    raise EntryNotConfirmed("RSI loss recovery is disabled under strict rules")
                if engine is None or not engine.rsi_entry_allowed(context):
                    raise EntryNotConfirmed("RSI recovery is no longer active for this confirmed loss")
                loss = next((row for row in ledger.accounts[account_mode]["trades_log"]
                             if row["intentId"] == context.get("loss_token")), None)
                if (loss is None or loss["result"] != "loss" or loss["pair"] != meta["pair"]
                        or loss.get("entryContext", {}).get("engine") != "FCB"
                        or loss.get("entryContext", {}).get("kind") not in ("FCB", "pole")
                        or loss["side"] != context.get("loss_side")
                        or meta["side"] != ("PUT" if loss["side"] == "CALL" else "CALL")
                        or loss.get("terminal", {}).get("expired") != context["execution_candle"]):
                    raise EntryNotConfirmed("RSI requires a verified breakout/continuation loss in the opposite direction")
                observation = iq_feed.forming_candle(api, meta["pair"], now)
                now, broker_now = time.time(), api.get_server_timestamp()
                enforce_order_window(context, time.time(), broker_now)
                if not rsi_price_confirmed(context, observation, now, broker_now):
                    raise EntryNotConfirmed("RSI opening-candle reversal is no longer confirmed")
            if context.get("engine") in ("FCB", "RSI"):
                return self.evidence_check(context, observation, now, broker_now)

    def ready(self, allow_cooldown=False):
        if not verify_generation():
            return False, recovery_message
        if ledger is None or recovery_required or recovery_state != "ready":
            return False, recovery_message
        try:
            ledger.store.check()
        except Exception:
            enabled.clear()
            begin_recovery("Durable storage is unavailable; entries are blocked.")
            return False, "Durable storage is unavailable; entries are blocked."
        if ledger.accounts[account_mode].get("cooldown_until", 0) and not allow_cooldown:
            return False, "Loss streak cooldown: five-minute break before fresh signals."
        return True, None

    def reserve(self, api, meta):
        with control_lock, iq_feed._lock:
            safe, reason = self.ready()
            if not safe:
                raise RuntimeError(reason)
            slot_error = self.reserve_slot_error(meta.get("pair"))
            if slot_error:
                raise EntryNotConfirmed(slot_error)
            if (not enabled.is_set() or requested_mode is not None
                    or api.get_balance_mode() != account_mode
                    or str(api.get_balance_id()) != ledger.keys[account_mode].split(":")[1]):
                raise RuntimeError("Order account or manual start was not verified")
            evidence = self.check_entry(api, meta)
            if evidence is not None:
                evidence["checks"]["reservation"] = evidence.pop("latest_check")
                meta = {**meta, "entryContext": {**meta["entryContext"], "market_evidence": evidence}}
            token = ledger.reserve(account_mode, meta)
            restore_selected()
            return token

    @contextmanager
    def submission(self, api, token):
        # get_session cannot reconnect between this verification and buy.
        # Check again AFTER the intent commits: storage may take seconds.
        with control_lock, iq_feed._lock:
            safe, reason = self.ready()
            if (not safe or not enabled.is_set() or not api.check_connect()
                    or api.get_balance_mode() != account_mode
                    or str(api.get_balance_id()) != ledger.keys[account_mode].split(":")[1]):
                # This journaled request has not been sent yet, so its
                # reservation can be released without guessing a buy outcome.
                self.reject(token)
                begin_recovery(reason or "Broker session changed before submission; entries are blocked.")
                raise RuntimeError("Submission cancelled before contacting the broker")
            try:
                slot_error = self.submission_slot_error(token)
                if slot_error:
                    raise EntryNotConfirmed(slot_error)
                evidence = self.check_entry(api, ledger.accounts[account_mode]["pending"][token])
            except EntryNotConfirmed:
                self.reject(token)
                raise
            if evidence is not None:
                evidence["checks"]["submission"] = evidence.pop("latest_check")
            self.submission_evidence = (token, evidence)
            # No storage write, quote fetch or evidence copy after this final
            # timing check: yield immediately into the existing api.buy call.
            context = ledger.accounts[account_mode]["pending"][token].get("entryContext")
            broker_now = api.get_server_timestamp() if context is not None else None
            now = time.time()
            try:
                if context is not None:
                    enforce_order_window(context, now, broker_now)
            except EntryNotConfirmed:
                self.submission_evidence = None
                self.reject(token)  # Definitely not sent; release only this intent.
                raise
            self.submission_timing = (token, {
                "order_submitted_at": now,
                **({"seconds_to_order": now - context["execution_candle"]} if context else {}),
            })
            yield

    def acknowledge(self, token, order_id):
        evidence = (self.submission_evidence[1] if self.submission_evidence is not None
                    and self.submission_evidence[0] == token else None)
        timing = (self.submission_timing[1] if self.submission_timing is not None
                  and self.submission_timing[0] == token else None)
        ledger.acknowledge(account_mode, token, order_id, market_evidence=evidence,
                           submission_timing=timing)
        self.submission_evidence = None
        self.submission_timing = None
        restore_selected()

    def reject(self, token):
        ledger.reject(account_mode, token)
        restore_selected()

    def settle(self, api):
        settle_orders(api)


bot.execution = DurableExecution()


def begin_recovery(message):
    global recovery_required, recovery_state, recovery_message
    enabled.clear()
    invalidate_monitor_signals()
    recovery_required = True
    recovery_state = "blocked"
    recovery_message = message


def verify_generation():
    if last_generation != iq_feed.session_generation:
        begin_recovery(
            "The broker reconnected. Entries are blocked until reconciliation; "
            "a fresh manual Start is required."
        )
        return False
    return True


def discard_ledger():
    global ledger
    if ledger is not None:
        try:
            ledger.store.close()
        finally:
            ledger = None

original_open_pairs = iq_feed.get_open_pairs


def track_open_pairs(api, candidates):
    result = original_open_pairs(api, candidates)
    global open_pairs
    with state_lock:
        open_pairs = set(result)
    return result


iq_feed.get_open_pairs = track_open_pairs


@bot.app.before_request
def require_secret():
    # Header-only, fail-closed protection, including the original private routes.
    provided = request.headers.get("X-API-Secret", "")
    if not hmac.compare_digest(provided, bot.API_SECRET):
        return jsonify({"error": "unauthorized"}), 401


def settings():
    return {
        "stake": bot.STAKE,
        "dailyLossLimit": bot.DAILY_LOSS_LIMIT,
        "maxDailyTrades": bot.MAX_DAILY_TRADES,
        "lossStreakStop": bot.LOSS_STREAK_STOP,
        "activePairs": bot.ACTIVE_COUNT,
        "accountMode": account_mode,
        "expirationMinutes": bot.EXPIRATION_MINUTES,
    }


def publish_status():
    global cached_status
    with control_lock, bot._lock:
        ok, reason = bot.guardrails_ok()
        cooldown_until = (ledger.accounts[account_mode].get("cooldown_until", 0)
                          if ledger is not None else 0)
        engines = []
        for pair in bot.active_pairs:
            engine = bot.engines.get(pair)
            if engine is not None:
                with engine.lock:
                    signal = engine.reversal_signal
                    momentum = engine.momentum_signal
                    engines.append({
                        "pair": pair, "trend": engine.trend, "reason": engine.reason,
                        "reversalStatus": engine.reversal_status,
                        "reversalSignal": ({
                            "signalTime": signal["signal_time"], "side": signal["side"],
                            "rsi": signal["rsi"], "extremeRsi": signal["extreme_rsi"],
                        } if signal else None),
                        "momentumStatus": engine.momentum_status,
                        "momentumSignal": ({
                            "signalTime": momentum["signal_time"], "side": momentum["side"],
                            "rsi": momentum["rsi"], "extremeRsi": momentum["extreme_rsi"],
                        } if momentum else None),
                    })
        with state_lock:
            cached_status = {
                "running": enabled.is_set(),
                "_entryGuardReason": reason if not ok else None,
                "_lossStreakStop": bot.LOSS_STREAK_STOP,
                "connected": connected,
                "accountMode": account_mode,
                "requestedAccountMode": requested_mode,
                "accountSwitchPending": requested_mode is not None,
                "recoveryState": recovery_state,
                "recoveryMessage": recovery_message,
                "recoveryPendingOrders": sum(
                    len(saved["pending"]) for saved in account_states.values()
                ),
                "scanPairs": list(scan_pairs),
                "scanUpdatedAt": scan_updated_at,
                "lastError": last_error or (reason if not ok else None),
                "balance": balance if connected else None,
                "dailyPL": bot.daily_pl,
                "tradesToday": bot.trades_today,
                "lossStreak": bot.loss_streak,
                "cooldownActive": bool(cooldown_until),
                "cooldownUntil": cooldown_until or None,
                "cooldownRemainingSeconds": math.ceil(max(0, cooldown_until - time.time())),
                "maxDailyTrades": bot.MAX_DAILY_TRADES,
                "dailyLossLimit": bot.DAILY_LOSS_LIMIT,
                "activePairs": list(bot.active_pairs),
                "openOrders": len(bot.open_orders),
                "activeTrades": (
                    active_trades(ledger.accounts[account_mode]["pending"])
                    if ledger is not None and account_mode in ledger.accounts else None
                ),
                "updatedAt": updated_at,
                "recentPairSwaps": list(bot.suspended_log[:5]),
                "engines": engines,
            }


def status():
    with state_lock:
        return dict(cached_status)


def remember_candle(pair, candle):
    """Accumulate real broker candles, never synthetic market data."""
    row = {key: candle[key] for key in ("time", "open", "high", "low", "close")}
    if ledger is not None and account_mode == "PRACTICE" and account_mode in ledger.keys:
        skip_audit.resolve(ledger.store, ledger.keys[account_mode], pair, row)
    with state_lock:
        history = histories.setdefault(pair, {})
        history[row["time"]] = row
        while len(history) > 40:
            del history[min(history)]


def sync_active_pairs(api, available, reset=False):
    """Keep open pairs and replace unavailable pairs without broker-call locks."""
    with control_lock:
        if reset:
            invalidate_monitor_signals()
        previous = list(bot.active_pairs)
        desired = select_pairs([] if reset else previous, available,
                               iq_feed.PAIR_POOL, bot.ACTIVE_COUNT)
    removed = [pair for pair in previous if reset or pair not in desired]
    added = [pair for pair in desired if reset or pair not in previous]
    # Retired pairs must stop being entry-eligible before subscription/backfill I/O.
    with control_lock, bot._lock:
        if removed and not reset:
            invalidate_monitor_signals()
        bot.active_pairs[:] = [pair for pair in previous if pair not in removed]
        for pair in removed:
            if pair in bot.engines:
                with bot.engines[pair].lock:
                    bot.engines[pair].discard_candidates()
    for pair in removed:
        iq_feed.stop_stream(api, pair)
    for pair in added:
        iq_feed.start_stream(api, pair)
    seeds = {pair: iq_feed.load_history(api, pair) for pair in added}
    with control_lock, bot._lock:
        bot.active_pairs[:] = desired
        for pair in added:
            if pair not in bot.engines:
                bot.engines[pair] = bot.PairEngine(
                    strict_rules=True, reversal_enabled=True, practice_zone=True,
                    validation_audit=lambda entry, reason, pair=pair, mode=account_mode:
                        record_skipped(pair, entry.get("side"), reason, entry, mode=mode))
            with bot.engines[pair].lock:
                # Retained engines are reseeded on account switches too.
                bot.engines[pair].strict_rules = True
                bot.engines[pair].practice_zone = True
                bot.engines[pair].rsi_enabled = False
                bot.engines[pair].reversal_enabled = True
                bot.engines[pair].momentum_enabled = True
                bot.engines[pair].validation_audit = (
                    lambda entry, reason, pair=pair, mode=account_mode:
                    record_skipped(pair, entry.get("side"), reason, entry, mode=mode))
                bot.engines[pair].seed(seeds[pair])
                restore_pair_flow_boundary(bot.engines[pair], pair)
            bot.last_seen_time[pair] = bot.engines[pair].last_time
            for candle in seeds[pair]:
                remember_candle(pair, candle)
        if not reset:
            for index, pair in enumerate(removed):
                bot.suspended_log.insert(0, {
                    "time": time.time(),
                    "suspended": pair,
                    "replacement": added[index] if index < len(added) else None,
                })
            del bot.suspended_log[20:]


def restore_pair_flow_boundary(engine, pair):
    """A reconnect must not make pre-loss/recovery candles into new breakouts."""
    if ledger is None:
        return
    saved = ledger.accounts[account_mode]
    boundaries = [row["settledAt"] for row in saved["trades_log"]
                  if row.get("pair") == pair and number(row.get("settledAt"))
                  and ((row.get("entryContext") or {}).get("engine") == "RSI"
                       or (row.get("entryContext") or {}).get("engine") == "FCB"
                       and row.get("result") == "loss")]
    engine.fcb_resume_after = max(engine.fcb_resume_after,
                                  saved.get("signal_resume_after", 0), *boundaries)


def apply_account_request(api):
    global account_mode, requested_mode, rebuild_pairs, balance
    with control_lock:
        target = requested_mode
    if target is None:
        return
    api.change_balance(target)
    if api.get_balance_mode() != target:
        raise RuntimeError("Broker account switch was not confirmed")
    with control_lock:
        if ledger is None or str(api.get_balance_id()) != ledger.keys[target].split(":")[1]:
            raise RuntimeError("Broker balance identity changed")
        account_mode = target
        invalidate_monitor_signals()
        iq_feed.ACCOUNT_MODE = target
        restore_selected()
        requested_mode = None
        balance = None
        rebuild_pairs = True
        bot.engines.clear()
        bot.last_seen_time.clear()


def verified_settlement_expiry(api, meta, message):
    """Use the terminal event's expiry first; never infer expiry from arrival time."""
    expiries = [message[key] for key in ("expired", "expiration_time") if key in message]
    if not expiries:
        try:
            evidence = verified_evidence(api, meta, str(meta["orderId"]))
            profit = message["profit_amount"] - message["amount"]
            if (evidence["outcome"] != "settled"
                    or not number(evidence["profit"]) or evidence["profit"] == 0
                    or (evidence["profit"] > 0) != (profit > 0)
                    or not math.isclose(evidence["profit"],
                                        profit, abs_tol=0.01)):
                return None
            expiries = [evidence["expiresAt"]]
            if evidence.get("terminal", {}).get("history_evidence") is not None:
                message["history_evidence"] = evidence["terminal"]["history_evidence"]
        except Exception:
            return None  # Outcomes still settle; unverifiable expiry cannot trigger another trade.
    expiry = expiries[0]
    duration = meta["expirationMinutes"] * 60
    if (not all(number(value) and value == expiry for value in expiries)
            or expiry % 60 != 0 or expiry > time.time()
            or not duration - 30 <= expiry - meta["time"] <= duration + 30):
        return None
    return int(expiry)


def enqueue_settlement_entries(api, pair, engine):
    """Verify post-settlement candidates and promptly notify the shared executor."""
    win_pole = (engine.fcb_candidate is not None
                and engine.fcb_candidate.get("trigger") == "fcb_win")
    rsi_recovery = engine.rsi_enabled and (engine.rsi_candidate is not None or engine.rsi_loss is not None)
    loss_reversal = engine.loss_reversal_candidate is not None
    if monitoring_service is None or not (win_pole or rsi_recovery or loss_reversal):
        return
    token, allowed = monitor_context()
    if not allowed:
        return
    now = time.time()
    quote = iq_feed.forming_candle(api, pair, now)
    now, broker_now = time.time(), api.get_server_timestamp()
    if win_pole:
        engine.confirm_fcb(quote, now, broker_now)
    if rsi_recovery or loss_reversal:
        engine.confirm_rsi(quote, now, broker_now)
    for entry in engine.take_entries(now):
        monitoring_service.enqueue_entry(pair, engine, token, entry)

def audit_paused_orders(api):
    """Extra history I/O never runs during entries, pending settlement or recovery."""
    global timing_checked_at, timing_account
    # Start and account switching share this lock, so neither can race this audit.
    with control_lock:
        if (ledger is None or enabled.is_set() or recovery_required
                or requested_mode is not None
                or any(state["pending"] for state in ledger.accounts.values())
                or time.time() - timing_checked_at < 5):
            return
        timing_checked_at = time.time()
        keys = list(ledger.keys.values())
        if not keys:
            return
        key = keys[timing_account % len(keys)]
        timing_account += 1
        try:
            audit_one(api, ledger.store, key)
        except Exception:
            pass  # Observational failures cannot change trading or risk state.


def settle_orders(api):
    """Reconcile both balances, including orders missed during a disconnect."""
    global recovery_required, recovery_state, recovery_message, last_error
    if ledger is None:
        raise RuntimeError("Durable ledger is not ready")
    history_timed_out = False
    for mode, saved in list(ledger.accounts.items()):
        # JSONB does not preserve object insertion order. Restore submission
        # order so replay does not reorder a run of wins/losses after restart.
        for token, meta in sorted(saved["pending"].items(), key=lambda item: entry_order(item[1])):
            overdue = time.time() > meta["time"] + meta["expirationMinutes"] * 60 + 60
            try:
                message = terminal_message(api, meta, historical=recovery_required or overdue)
            except BrokerHistoryUnavailable:
                # Keep the healthy broker session and durable ledger. A missing
                # history reply cannot release the reservation or start trading.
                with control_lock:
                    enabled.clear()
                    recovery_required = True
                history_timed_out = True
                continue
            if message is not None:
                loss_candle = None
                win_expiry = None
                engine = bot.engines.get(meta.get("pair"))
                if (mode == account_mode and engine is not None and enabled.is_set()
                        and not recovery_required and meta.get("pair") in bot.active_pairs
                        and meta.get("entryContext", {}).get("engine") == "FCB"
                        and number(message.get("profit_amount")) and number(message.get("amount"))
                        and message["profit_amount"] > message["amount"]):
                    win_expiry = verified_settlement_expiry(api, meta, message)
                    if win_expiry is not None:
                        message = {**message, "expired": win_expiry}
                if (mode == account_mode and engine is not None and enabled.is_set()
                        and not recovery_required and meta.get("pair") in bot.active_pairs
                        and meta.get("entryContext", {}).get("engine") == "FCB"
                        and meta.get("entryContext", {}).get("kind") in ("FCB", "pole")
                        and number(message.get("profit_amount")) and number(message.get("amount"))
                        and message["profit_amount"] < message["amount"]):
                    # The matched terminal event can establish actual expiry,
                    # just as for a win. Fully verified history is the fallback.
                    expiry = verified_settlement_expiry(api, meta, message)
                    if expiry is not None:
                        loss_candle = expiry - 60
                        message = {**message, "expired": expiry}
                if (mode == account_mode and engine is not None and enabled.is_set()
                        and not recovery_required and meta.get("pair") in bot.active_pairs
                        and meta.get("entryContext", {}).get("engine") == "RSI"
                        and meta.get("entryContext", {}).get("kind") == "RSI reversal"
                        and meta.get("entryContext", {}).get("attempt", 1) == 1
                        and number(message.get("profit_amount")) and number(message.get("amount"))
                        and message["profit_amount"] < message["amount"]):
                    expiry = verified_settlement_expiry(api, meta, message)
                    if expiry is not None:
                        loss_candle = expiry - 60
                        message = {**message, "expired": expiry}
                with control_lock:
                    message = {**message, "brokerTiming":
                               verify_timing(api, meta, message.get("history_evidence"))}
                    ledger.settle(mode, token, message)
                    restore_selected()
                    engine = bot.engines.get(meta.get("pair"))
                    if mode == account_mode and engine is not None:
                        trade = next(item for item in ledger.accounts[mode]["trades_log"]
                                     if item["intentId"] == token)
                        with engine.lock:
                            late_attempt = engine.broker_settled(
                                token, trade["result"], time.time(),
                                allow_entries=enabled.is_set() and not recovery_required
                                and meta.get("pair") in bot.active_pairs,
                                loss_candle=loss_candle,
                                win_expiry=win_expiry,
                                settled_entry=meta.get("entryContext"),
                            )
                            if late_attempt:
                                record_skipped(meta["pair"], late_attempt["side"],
                                               LATE_ORDER_REASON, late_attempt, mode=mode)
                            enqueue_settlement_entries(api, meta["pair"], engine)
            elif meta["orderId"] is None or overdue:
                enabled.clear()
                recovery_required = True
    if recovery_required:
        enabled.clear()
        pending = [meta for saved in ledger.accounts.values() for meta in saved["pending"].values()]
        if any(meta["orderId"] is None for meta in pending):
            recovery_state = "blocked"
            recovery_message = (
                "A saved submission has no confirmed broker order ID. Its outcome is unknown. "
                "Entries and account switching are blocked; verify the order directly with the broker. "
                "The bot will not resubmit it or clear its risk reservation."
            )
        elif pending:
            recovery_state = "reconciling"
            recovery_message = (
                "Broker individual-order history could not verify saved trades. Saved orders remain reserved; "
                "entries and account switching stay blocked until each outcome is verified."
                if history_timed_out else
                "Waiting for verified broker settlements on saved Demo or Live orders; entries are blocked."
            )
        else:
            recovery_required = False
            recovery_state = "ready"
            recovery_message = "Account history and limits restored. Recovery complete; trading stays paused until you start it."
    if history_timed_out:
        with state_lock:
            last_error = (
                "Broker individual-order history is unverified; saved orders cannot yet be reconciled. "
                "No reservations were cleared and no outcomes were inferred."
            )


def dispatch_entries(api, pair, engine, entries=None):
    """Serialize buys and allow at most one unresolved trade across all pairs."""
    entries = engine.take_entries(time.time()) if entries is None else entries
    for entry in entries:
        if getattr(engine, "practice_zone", False):
            entry = engine.annotate_validation(entry)
            zone_blocked = (
                not engine.validation_allowed(entry)
                if entry.get("kind") in ("RSI reversal", "LOSS reversal")
                else engine.zone_data["zone"] != "CLEAR"
            )
            if zone_blocked:
                if entry.get("kind") == "RSI reversal":
                    engine.reversal_skipped(entry)
                elif entry.get("kind") == "LOSS reversal":
                    engine.loss_reversal_skipped(entry)
                elif entry.get("kind") == "RSI momentum":
                    engine.momentum_skipped(entry)
                record_skipped(pair, entry["side"], "ZONE_SKIP", entry)
                continue
        if entry["engine"] == "RSI":
            if entry.get("kind") == "RSI reversal":
                if not engine.reversal_enabled:
                    engine.reversal_skipped(entry)
                    record_skipped(pair, entry["side"], "Standalone RSI reversal disabled", entry)
                    continue
            elif entry.get("kind") == "LOSS reversal":
                if not engine.reversal_enabled:
                    engine.loss_reversal_skipped(entry)
                    record_skipped(pair, entry["side"], "Loss reversal disabled", entry)
                    continue
            elif entry.get("kind") == "RSI momentum":
                if not engine.momentum_enabled:
                    engine.momentum_skipped(entry)
                    record_skipped(pair, entry["side"], "RSI momentum disabled", entry)
                    continue
            elif not engine.rsi_enabled:
                engine.recovery_skipped(entry)
                record_skipped(pair, entry["side"], "RSI recovery disabled", entry)
                continue
        if (number(entry.get("execution_candle"))
                and time.time() > entry["execution_candle"] + ENTRY_WINDOW_SECONDS):
            record_skipped(pair, entry["side"], LATE_ORDER_REASON, entry)
            if entry["engine"] == "RSI":
                if entry.get("kind") == "RSI reversal":
                    engine.reversal_skipped(entry)
                elif entry.get("kind") == "LOSS reversal":
                    engine.loss_reversal_skipped(entry)
                elif entry.get("kind") == "RSI momentum":
                    engine.momentum_skipped(entry)
                else:
                    engine.recovery_skipped(entry)
            continue
        if entry["engine"] == "FCB" and not engine.fcb_entry_allowed(entry):
            record_skipped(pair, entry["side"], "Previous FCB cycle ended or RSI recovery is open", entry)
            continue
        if entry["engine"] == "RSI" and not engine.rsi_entry_allowed(entry):
            if entry.get("kind") == "RSI reversal":
                engine.reversal_skipped(entry)
            elif entry.get("kind") == "LOSS reversal":
                engine.loss_reversal_skipped(entry)
            elif entry.get("kind") == "RSI momentum":
                engine.momentum_skipped(entry)
            record_skipped(pair, entry["side"], "Reversal or recovery signal is no longer current", entry)
            continue
        if (not verify_generation() or not enabled.is_set()
                or requested_mode is not None or recovery_required):
            record_skipped(pair, entry["side"], "Paused, account switching or session recovery", entry)
            continue
        resume_after = ledger.accounts[account_mode].get("signal_resume_after", 0)
        if entry["signal_time"] + iq_feed.CANDLE_SIZE <= resume_after:
            record_skipped(pair, entry["side"], "Signal predates the account resume boundary", entry)
            continue
        slot_error = bot.execution.reserve_slot_error(pair)
        if slot_error:
            record_skipped(pair, entry["side"], slot_error, entry)
            continue
        if engine.pending[entry["engine"]] is not None:
            record_skipped(pair, entry["side"], "PAIR_ENGINE_OCCUPIED", entry)
            continue
        if api.get_balance_mode() != account_mode:
            enabled.clear()
            raise RuntimeError("Refusing an order on an unexpected account")
        token = bot.place_trade(
            api, pair, entry["side"], entry["reason"], entry_context=entry,
        )
        if token:
            engine.submitted(token, entry)
        else:
            if entry["engine"] == "RSI":
                if entry.get("kind") == "RSI reversal":
                    engine.reversal_skipped(entry)
                elif entry.get("kind") == "LOSS reversal":
                    engine.loss_reversal_skipped(entry)
                elif entry.get("kind") == "RSI momentum":
                    engine.momentum_skipped(entry)
                else:
                    engine.recovery_skipped(entry)
            engine.reason = "FAST TRADE: entry skipped by risk limits, stale data or failed entry confirmation"


def safe_failure_location(error):
    """Static code location only; never broker error text, payloads or locals."""
    frame = error.__traceback__
    while frame is not None and frame.tb_next is not None:
        frame = frame.tb_next
    kind = type(error).__name__
    location = kind if frame is None else (
        f"{kind} at {os.path.basename(frame.tb_frame.f_code.co_filename)}:{frame.tb_lineno}"
    )
    return location


def drain_execution_queue(api):
    """The supervisor is the sole executor; monitors never touch the ledger."""
    monitoring_service.raise_if_failed()
    if not verify_generation():
        monitoring_service.invalidate()
        return
    while True:
        signal = monitoring_service.take()
        if signal is None:
            break
        with control_lock:
            token, allowed = monitor_context()
            if (not allowed or signal.token != token or signal.pair not in bot.active_pairs
                    or bot.engines.get(signal.pair) is not signal.engine):
                record_skipped(signal.pair, signal.entry["side"],
                               "Queued signal is stale or no longer active", signal.entry,
                               mode=signal.token[1])
                continue
            with signal.engine.lock:
                dispatch_entries(api, signal.pair, signal.engine, entries=[signal.entry])


def supervised_loop(background_monitors=True):
    """Production uses pair threads; offline tests use identical synchronous polls."""
    global monitoring_service
    monitoring_service = create_pair_monitoring(background=background_monitors)
    try:
        _supervised_cycles()
    finally:
        monitoring_service.close()
        monitoring_service = None


def _supervised_cycles():
    global connected, balance, last_error, updated_at, rebuild_pairs, requested_mode
    global recovery_required, recovery_state, recovery_message, last_generation, ledger
    global scan_pairs, scan_updated_at
    global monitoring_service
    balance_checked_at = 0
    while True:
        try:
            if not connection_requested.is_set():
                disconnect_if_requested()
                time.sleep(0.25)
                continue
            api = iq_feed.get_session()
            if last_generation != iq_feed.session_generation:
                enabled.clear()
                invalidate_monitor_signals()
                recovery_required = True
                recovery_state = "reconciling"
                last_generation = iq_feed.session_generation
            initialize_ledger(api)
            ledger.store.check()
            apply_account_request(api)
            if api.get_balance_mode() != account_mode:
                api.change_balance(account_mode)
            if api.get_balance_mode() != account_mode:
                enabled.clear()
                raise RuntimeError("Refusing an unexpected broker account")
            # Do not hold the state lock during any broker/network operation.
            refresh_balance = balance is None or time.time() - balance_checked_at >= bot.POLL_SECONDS
            current_balance = iq_feed.read_balance(api) if refresh_balance else balance
            if current_balance is None:
                raise RuntimeError("Broker did not return a balance")
            if refresh_balance:
                balance_checked_at = time.time()
            with state_lock:
                connected = bool(api.check_connect())
                balance = float(current_balance)
                last_error = None
                updated_at = time.time()

            # Reconcile before even considering strategy entries.
            settle_orders(api)
            drain_execution_queue(api)
            with control_lock:
                for mode in ledger.accounts:
                    if ledger.refresh_cooldown(mode, time.time()):
                        restore_selected()
            with control_lock:
                reset = rebuild_pairs
                rebuild_pairs = False
                needs_sync = (
                    reset or not bot.active_pairs
                    or time.time() - bot._last_pair_check >= bot.PAIR_CHECK_SECONDS
                )
            if needs_sync:
                available = track_open_pairs(api, iq_feed.PAIR_POOL)
                sync_active_pairs(api, available, reset)
                bot._last_pair_check = time.time()
            with control_lock:
                pairs = {pair: bot.engines[pair] for pair in bot.active_pairs}
            monitoring_service.configure(pairs, api, iq_feed.session_generation)
            monitoring_service.poll_once()  # No-op with production background workers.
            monitoring_service.raise_if_failed()
            observations = monitoring_service.snapshot()
            with control_lock:
                scan_pairs = list(observations)
                scan_updated_at = max(observations.values(), default=0.0)
            drain_execution_queue(api)
            settle_orders(api)
            drain_execution_queue(api)
            publish_status()
            audit_paused_orders(api)
        except Exception as error:
            # Raw unofficial-library errors can contain private account details.
            # Deliberately expose only a safe diagnostic, never credentials.
            enabled.clear()
            invalidate_monitor_signals()
            background = monitoring_service.background
            monitoring_service.close()
            monitoring_service = create_pair_monitoring(background=background)
            with control_lock:
                requested_mode = None
                scan_pairs = []
                scan_updated_at = 0.0
                recovery_required = True
                recovery_state = "blocked"
                recovery_message = "Recovery or durable storage could not be verified. Entries are blocked; reconnecting safely in Demo."
                discard_ledger()
            with state_lock:
                connected = False
                last_error = (
                    "Broker connection, account selection, feed, or settlement "
                    "could not be verified. Entries are paused; check the account. "
                    f"Diagnostic: {safe_failure_location(error)}."
                )
                updated_at = time.time()
            publish_status()
        if monitoring_service.background:
            monitoring_service.wait_for_signal(ENTRY_POLL_SECONDS)
        else:
            time.sleep(ENTRY_POLL_SECONDS)


@bot.app.get("/ready")
def ready():
    return jsonify({"status": "ready"})


@bot.app.get("/dashboard/status")
def dashboard_status():
    return jsonify(status())


@bot.app.get("/dashboard/trades")
def dashboard_trades():
    mode = request.args.get("accountMode", account_mode)
    cursor = request.args.get("cursor")
    timing_filter = request.args.get("timingFilter")
    try:
        limit = int(request.args.get("limit", "50"))
        if (mode not in ("PRACTICE", "REAL") or not 1 <= limit <= 100
                or (timing_filter is not None and timing_filter not in
                    ("within_2s", "late_after_2s", "unknown_checked", "never_checked"))
                or (cursor is not None and (not cursor.isascii() or not cursor.isdigit()
                    or not 0 < int(cursor) <= 9223372036854775807))):
            raise ValueError("Invalid pagination parameters")
    except ValueError:
        return jsonify({"error": "Invalid account, timing filter or pagination parameters"}), 400
    with control_lock:
        if ledger is None:
            return jsonify({"error": "Durable account history is not ready"}), 503
        try:
            return jsonify(ledger.store.history(ledger.keys[mode], cursor, limit, timing_filter=timing_filter))
        except Exception:
            return jsonify({"error": "Durable account history is unavailable"}), 503


@bot.app.get("/dashboard/timing-summary")
def dashboard_timing_summary():
    mode = request.args.get("accountMode", account_mode)
    if mode not in ("PRACTICE", "REAL"):
        return jsonify({"error": "Choose Demo (PRACTICE) or Live (REAL)."}), 400
    with control_lock:
        current_ledger = ledger
        key = current_ledger.keys.get(mode) if current_ledger else None
    if key is None:
        return jsonify({"error": "Saved account history is unavailable."}), 503
    try:
        return jsonify(current_ledger.store.timing_summary(key))
    except Exception:
        return jsonify({"error": "Saved broker timing summary is unavailable. Retry shortly."}), 503


@bot.app.get("/dashboard/rejection-summary")
def dashboard_rejection_summary():
    from rejection_summary import current_guards, VALIDATION_POLICY
    mode = request.args.get("accountMode")
    hours = request.args.get("hours")
    if mode not in ("PRACTICE", "REAL") or hours not in ("1", "6", "24"):
        return jsonify({"error": "Choose Practice/Live and a 1, 6 or 24 hour window."}), 400
    with control_lock:
        current_ledger = ledger
        key = current_ledger.keys.get(mode) if current_ledger else None
    if key is None:
        return jsonify({"error": "Saved account history is unavailable."}), 503
    end = time.time()
    start = end - int(hours) * 3600
    try:
        summary = current_ledger.store.rejection_summary(key, start, end)
        return jsonify({
            **summary, "accountMode": mode, "windowStart": start, "windowEnd": end,
            "currentPolicy": VALIDATION_POLICY if mode == "PRACTICE" else None,
            "auditWriteFailures": skip_audit.failures,
            "auditWritesPending": skip_audit.pending.unfinished_tasks,
            "currentGuards": current_guards(status(), mode),
        })
    except Exception:
        return jsonify({"error": "Saved rejection summary is unavailable. Retry shortly."}), 503


@bot.app.get("/dashboard/trades.csv")
@bot.app.get("/dashboard/skipped.csv")
def dashboard_csv():
    mode = request.args.get("accountMode", account_mode)
    if mode not in ("PRACTICE", "REAL"):
        return jsonify({"error": "Choose Demo (PRACTICE) or Live (REAL)."}), 400
    skipped = request.path.endswith("/skipped.csv")
    with control_lock:
        current_ledger = ledger
        key = current_ledger.keys.get(mode) if current_ledger else None
    if key is None:
        return jsonify({"error": "Saved account history is unavailable."}), 503
    try:
        if skipped:
            skip_audit.check_export()
        contents = render_csv(current_ledger.store.export_rows(key, skipped), skipped)
    except Exception:
        return jsonify({"error": "CSV history is unavailable; skipped audit may be incomplete. Retry shortly."}), 503
    filename = f"{'skipped-signals' if skipped else 'trades'}-{mode.lower()}-WAT.csv"
    return Response(contents, content_type="text/csv; charset=utf-8", headers={
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
    })


@bot.app.get("/dashboard/performance")
def dashboard_performance():
    with control_lock:
        if ledger is None or account_mode not in ledger.accounts:
            return jsonify({"error": "Durable account state is unavailable"}), 503
        try:
            snapshot = ledger.store.performance(ledger.keys[account_mode])
        except Exception:
            return jsonify({"error": "Confirmed trade performance is unavailable"}), 503
        return jsonify(snapshot)


def recovery_snapshot():
    return {
        "intents": [meta for saved in ledger.accounts.values()
                    for meta in saved["pending"].values() if meta["orderId"] is None],
        "audit": ledger.store.audit_history(list(ledger.keys.values())),
    }


@bot.app.get("/dashboard/recovery")
def dashboard_recovery():
    with control_lock:
        if ledger is None:
            return jsonify({"error": "Durable account state is unavailable"}), 503
        return jsonify(recovery_snapshot())


@bot.app.post("/dashboard/recovery/review")
@bot.app.post("/dashboard/recovery/resolve")
def dashboard_recovery_action():
    data = request.get_json(silent=True)
    resolving = request.path.endswith("/resolve")
    mode = data.get("accountMode") if isinstance(data, dict) else None
    if mode not in ("PRACTICE", "REAL"):
        return jsonify({"error": "Choose the intent's Demo or Live account"}), 400
    with control_lock, iq_feed._lock:
        if ledger is None:
            return jsonify({"error": "Durable account state is unavailable"}), 503
        # Recovery is read-only at the broker. Never select Live or call buy.
        if enabled.is_set() or requested_mode is not None or not connected:
            return jsonify({"error": "Pause entries and connect before reviewing recovery"}), 409
        saved = ledger.accounts[mode]
        review = None
        if resolving:
            review = next((item for item in saved.get("recovery_reviews", {}).values()
                           if item["reviewId"] == data.get("reviewId")), None)
            if review is None:
                return jsonify({"error": "Review not found or already resolved; fetch a fresh review"}), 409
            token, order_id = review["intentId"], review["orderId"]
        else:
            token, order_id = data.get("intentId"), data.get("orderId")
        if not isinstance(token, str) or not isinstance(order_id, str):
            return jsonify({"error": "Intent and broker order ID are required"}), 400
        meta = saved["pending"].get(token)
        if meta is None or meta["orderId"] is not None:
            return jsonify({"error": "Intent is no longer unconfirmed"}), 409
        enabled.clear()
        begin_recovery("Operator recovery review in progress; trading stays paused.")
        try:
            api = iq_feed.get_session()
            if not api.check_connect() or not verify_generation():
                raise RuntimeError("Broker session changed; wait for reconnection and review again")
            ledger.assert_unused_order(order_id)
            evidence = verified_evidence(api, meta, order_id)
            if resolving:
                ledger.resolve(mode, review, evidence, data.get("confirmation"), data.get("note"))
                restore_selected()
                # Do not call the settlement loop or start here. Its next pass
                # evaluates every remaining intent across both balances.
                publish_status()
                return jsonify(recovery_snapshot())
            result = ledger.review(mode, token, evidence)
            publish_status()
            return jsonify(result)
        except RuntimeError as error:
            # Only our safe diagnostics, not raw broker exceptions, reach UI.
            safe_messages = (
                "Broker session changed", "Broker order is already", "Provide a positive",
                "Broker order history request timed out", "Broker did not verify",
                "Broker individual-order history could not verify the saved trade",
                "Broker order evidence is missing", "Broker balance, asset",
                "Broker rejection evidence", "Broker record is not",
                "Broker record matches multiple",
                "Review expired", "Explicit outcome", "Intent is no longer",
                "Unverified terminal",
            )
            message = str(error) if str(error).startswith(safe_messages) else "Recovery could not be verified; reservation retained"
            try:
                ledger.change(mode, lambda state: ledger.audit(
                    state, mode, token, order_id[:100], "refused", message))
            except Exception:
                return jsonify({"error": "Audit storage unavailable; recovery remains blocked"}), 503
            publish_status()
            return jsonify({"error": message}), 409
        except Exception:
            # Storage, malformed broker payloads and transport errors fail closed.
            try:
                ledger.change(mode, lambda state: ledger.audit(
                    state, mode, token, order_id[:100], "refused", "Broker evidence or storage unavailable"))
            except Exception:
                pass
            publish_status()
            return jsonify({"error": "Broker evidence or audit storage unavailable; reservation retained"}), 503


@bot.app.get("/dashboard/pairs")
def dashboard_pairs():
    with state_lock:
        active = set(cached_status.get("activePairs", []))
        available = set(open_pairs)
    return jsonify([
        {"pair": pair, "open": pair in available, "active": pair in active}
        for pair in iq_feed.PAIR_POOL
    ])


@bot.app.get("/dashboard/candles")
def dashboard_candles():
    pair = request.args.get("pair")
    if pair not in iq_feed.PAIR_POOL:
        return jsonify({"error": "Unknown pair"}), 400
    with state_lock:
        history = histories.get(pair, {})
        rows = [history[key] for key in sorted(history)]
    return jsonify(rows)


@bot.app.post("/dashboard/start")
def dashboard_start():
    with control_lock:
        data = request.get_json(silent=True) or {}
        if "telegramExpectedSettings" in data or "telegramExpectedRevision" in data:
            if (data.get("telegramExpectedSettings") != settings()
                    or type(data.get("telegramExpectedRevision")) is not int
                    or data["telegramExpectedRevision"] != monitor_epoch):
                return jsonify({"error": "Account, settings or trading controls changed. Request Resume again."}), 409
        if not connection_requested.is_set():
            return jsonify({"error": "Connect the broker before resuming."}), 409
        with state_lock:
            ready_to_trade = connected
        if not ready_to_trade:
            return jsonify({"error": "IQ Option is not connected yet. Wait for the feed or check credentials."}), 409
        if requested_mode is not None:
            return jsonify({"error": "Wait for the account switch to finish."}), 409
        if recovery_required or recovery_state != "ready":
            return jsonify({"error": recovery_message}), 409
        # Explicit Start may arm post-break trading, never bypass the break.
        cooling = bool(ledger and ledger.accounts[account_mode].get("cooldown_until", 0))
        ok, reason = bot.guardrails_ok(allow_cooldown=cooling)
        if not ok:
            return jsonify({"error": reason}), 409
        invalidate_monitor_signals()
        enabled.set()
        publish_status()
    return jsonify(status())


@bot.app.post("/dashboard/stop")
def dashboard_stop():
    # Clear immediately, including when an individual buy is awaiting its reply.
    enabled.clear()
    with control_lock:
        invalidate_monitor_signals()
        for engine in bot.engines.values():
            with engine.lock:
                engine.discard_candidates()
        publish_status()
    return jsonify(status())


@bot.app.route("/dashboard/settings", methods=["GET", "PATCH"])
def dashboard_settings():
    with control_lock:
        if request.method == "GET":
            return jsonify(settings())
        return save_settings(request.get_json(silent=True))


@bot.app.get("/dashboard/settings/defaults")
def dashboard_settings_defaults():
    with control_lock:
        return jsonify({**DEFAULT_SETTINGS, "accountMode": account_mode,
                        "expirationMinutes": bot.EXPIRATION_MINUTES})


@bot.app.post("/dashboard/settings/reset")
def dashboard_settings_reset():
    with control_lock:
        data = request.get_json(silent=True)
        if (not isinstance(data, dict) or set(data) - {"accountMode", "freshStart"}
                or data.get("accountMode") not in ("PRACTICE", "REAL")
                or type(data.get("freshStart", False)) is not bool):
            return jsonify({"error": "Choose the account whose reset you confirmed."}), 400
        if data["accountMode"] != account_mode:
            return jsonify({"error": "The selected account changed. Review its settings before resetting."}), 409
        if data.get("freshStart", False):
            return reset_daily_limits()
        return save_settings(dict(DEFAULT_SETTINGS))


def reset_daily_limits():
    """Explicit manual budget renewal, never an automatic reset or history deletion."""
    with control_lock:
        if (enabled.is_set() or bot.open_orders or requested_mode is not None
                or recovery_required or recovery_state != "ready" or ledger is None
                or any(saved["pending"] for saved in ledger.accounts.values())):
            return jsonify({"error": "Pause the bot and wait for all orders and recovery to finish before resetting daily limits."}), 409
        def reset_usage(state):
            if state["pending"]:
                raise RuntimeError("Pending orders cannot be reset")
            state["daily_pl"] = 0.0
            state["trades_today"] = 0
            state["daily_limits_reset_at"] = time.time()
        try:
            ledger.change(account_mode, reset_usage)
        except Exception:
            begin_recovery("Daily limit reset could not be confirmed in durable storage; entries are blocked.")
            discard_ledger()
            publish_status()
            return jsonify({"error": "Daily limits were not confirmed in durable storage. Entries are blocked."}), 503
        invalidate_monitor_signals()
        restore_selected()
        publish_status()
        return jsonify(settings())


def save_settings(data):
    """Caller holds control_lock. Change configuration, never risk usage/history."""
    global rebuild_pairs
    with control_lock:
        if (enabled.is_set() or bot.open_orders or requested_mode is not None
                or recovery_required or ledger is None
                or any(saved["pending"] for saved in ledger.accounts.values())):
            return jsonify({"error": "Pause the bot and wait for pending orders to settle before changing settings."}), 409
        rules = {
            "stake": ("STAKE", 0, 100, False),
            "dailyLossLimit": ("DAILY_LOSS_LIMIT", -10000, 0, False),
            "maxDailyTrades": ("MAX_DAILY_TRADES", 1, 500, True),
            "lossStreakStop": ("LOSS_STREAK_STOP", 1, 25, True),
            "activePairs": ("ACTIVE_COUNT", 1, 18, True),
        }
        if not isinstance(data, dict) or not data or set(data) - set(rules):
            return jsonify({"error": "Provide one or more valid bot settings."}), 400
        for key, value in data.items():
            _, low, high, integer = rules[key]
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < low
                or value > high
                or (key == "stake" and value <= 0)
                or (integer and int(value) != value)
            ):
                return jsonify({"error": f"Invalid value for {key}."}), 400
        normalized = {key: int(value) if rules[key][3] else float(value) for key, value in data.items()}
        try:
            ledger.change(account_mode, lambda state: state["settings"].update(normalized))
        except Exception:
            begin_recovery("Settings could not be confirmed in durable storage; entries are blocked.")
            discard_ledger()
            publish_status()
            return jsonify({"error": "Settings were not confirmed in durable storage. Entries are blocked."}), 503
        rebuild_pairs = rebuild_pairs or normalized.get("activePairs", bot.ACTIVE_COUNT) != bot.ACTIVE_COUNT
        restore_selected()
        publish_status()
        return jsonify(settings())


@bot.app.post("/dashboard/account")
def dashboard_account():
    global requested_mode, connected, balance
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or data.get("accountMode") not in ("PRACTICE", "REAL"):
        return jsonify({"error": "Choose Demo (PRACTICE) or Live (REAL)."}), 400
    target = data["accountMode"]
    if target == "REAL" and data.get("confirmation") != "USE REAL MONEY":
        return jsonify({"error": "Live uses real money. Confirm with USE REAL MONEY."}), 400
    with control_lock, bot._lock:
        if ("telegramSnapshot" in data
                and data["telegramSnapshot"] != {"settings": settings(), "revision": monitor_epoch}):
            return jsonify({"error": "Account, settings or trading controls changed. Request the account switch again."}), 409
        if (enabled.is_set() or bot.open_orders or requested_mode is not None or recovery_required
                or ledger is None or any(saved["pending"] for saved in ledger.accounts.values())):
            return jsonify({"error": "Pause entries and wait for all orders to settle before switching accounts."}), 409
        if not connected:
            return jsonify({"error": "Connect to the broker before switching accounts."}), 409
        if target != account_mode:
            requested_mode = target
            with state_lock:
                connected = False
                balance = None
    publish_status()
    return jsonify(status()), 202


@bot.app.get("/dashboard/telegram-state")
def dashboard_telegram_state():
    with control_lock:
        return jsonify({"settings": settings(), "revision": monitor_epoch})


@bot.app.post("/dashboard/telegram-settings")
def dashboard_telegram_settings():
    data = request.get_json(silent=True) or {}
    with control_lock:
        if data.get("snapshot") != {"settings": settings(), "revision": monitor_epoch}:
            return jsonify({"error": "Account, settings or trading controls changed. Request the setting again."}), 409
        result = save_settings(data.get("changes"))
        if not isinstance(result, tuple):
            # Even setting changes invalidate outstanding remote confirmations.
            invalidate_monitor_signals()
        return result


@bot.app.post("/dashboard/connection")
def dashboard_connection():
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or type(data.get("connected")) is not bool:
        return jsonify({"error": "Choose whether to connect the broker."}), 400
    with control_lock:
        if data["connected"]:
            connection_requested.set()
        else:
            if not connection_requested.is_set() and recovery_state == "disconnected":
                return jsonify({"connectionRequested": False}), 202
            if (enabled.is_set() or bot.open_orders or requested_mode is not None
                    or recovery_required or ledger is None
                    or any(saved["pending"] for saved in ledger.accounts.values())):
                return jsonify({"error": "Entries are paused, but disconnect is blocked until every order and recovery is settled."}), 409
            connection_requested.clear()
            invalidate_monitor_signals()
    return jsonify({"connectionRequested": connection_requested.is_set()}), 202


def disconnect_if_requested():
    """Runs on the single executor, never on the Telegram/control thread."""
    global connected, balance, recovery_required, recovery_state, recovery_message
    global monitoring_service, scan_pairs
    with control_lock:
        if connection_requested.is_set() or recovery_state == "disconnected":
            return
        if (recovery_required or ledger is None or bot.open_orders
                or any(saved["pending"] for saved in ledger.accounts.values())):
            # A request accepted before an in-flight reconciliation completed
            # must not abandon orders that the reconciliation subsequently found.
            connection_requested.set()
            enabled.clear()
            return
        enabled.clear()
        invalidate_monitor_signals()
        if monitoring_service is not None:
            background = monitoring_service.background
            monitoring_service.close()
            monitoring_service = create_pair_monitoring(background=background)
        iq_feed.disconnect_session()
        discard_ledger()
        recovery_required = True
        recovery_state = "disconnected"
        recovery_message = "Broker deliberately disconnected. Connect initializes Demo and does not resume trading."
        scan_pairs = []
        with state_lock:
            connected = False
            balance = None
        publish_status()


if __name__ == "__main__":
    from telegram_bot import TelegramBot
    publish_status()
    threading.Thread(target=supervised_loop, daemon=True).start()
    TelegramBot(telegram_alerts).start()
    bot.app.run(
        host="127.0.0.1",
        port=int(os.environ["BOT_INTERNAL_PORT"]),
        use_reloader=False,
    )
