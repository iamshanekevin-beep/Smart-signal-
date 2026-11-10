"""Order journal and idempotent settlement, independent of strategy signals."""
import math
import time
import uuid
import copy
from market_evidence import bounded_copy
from order_recovery import history_row, assert_unused_in_state, assert_unique_intent, BrokerHistoryUnavailable
from loss_cooldown import recount_streak, refresh, needs_refresh
from broker_timing import unknown


def empty_state(settings):
    return {
        "daily_pl": 0.0, "trades_today": 0, "loss_streak": 0,
        "trades_log": [], "pending": {}, "settings": settings, "next_sequence": 0,
        "cooldown_until": 0, "loss_streak_reset_sequence": -1, "signal_resume_after": 0,
    }


def entry_order(meta):
    # Older ledger records have no sequence; retain their submission-time order
    # before new sequenced entries. Sequences do not depend on the wall clock.
    return (meta.get("entrySequence", 0), meta["time"], meta.get("intentId", ""))


class TradeLedger:
    def __init__(self, store):
        self.store = store
        self.accounts = {}
        self.keys = {}

    def load(self, mode, balance_id, settings):
        if (mode not in ("PRACTICE", "REAL") or isinstance(balance_id, bool)
                or not isinstance(balance_id, int) or balance_id <= 0):
            raise RuntimeError("Unverified broker balance identity")
        key = f"{mode}:{balance_id}"
        state = self.store.load(key, empty_state(settings))
        self.keys[mode] = key
        self.accounts[mode] = state

    def change(self, mode, mutate, order_id=None):
        if order_id is None:
            self.accounts[mode] = self.store.change(self.keys[mode], mutate)
        else:
            self.accounts[mode] = self.store.change(
                self.keys[mode], mutate, order_id=order_id, account_keys=list(self.keys.values()))

    def reserve(self, mode, meta):
        meta = copy.deepcopy(meta)
        evidence = (meta.get("entryContext") or {}).get("market_evidence")
        if evidence is not None:
            meta["entryContext"]["market_evidence"] = bounded_copy(evidence)
        token = str(uuid.uuid4())
        def apply(state):
            state["next_sequence"] = max(
                state.get("next_sequence", 0),
                max((item.get("entrySequence", 0) for item in
                     [*state["pending"].values(), *state["trades_log"]]), default=0),
            ) + 1
            state["pending"][token] = {
                **meta, "intentId": token, "orderId": None,
                "accountMode": mode, "balanceId": int(self.keys[mode].split(":")[1]),
                "entrySequence": state["next_sequence"],
            }
            # Reserve before sending: a missing buy reply must not bypass limits.
            state["trades_today"] += 1
        self.change(mode, apply)
        return token

    def acknowledge(self, mode, token, order_id, market_evidence=None, submission_timing=None):
        if isinstance(order_id, bool) or not str(order_id).isdigit() or int(order_id) <= 0:
            raise RuntimeError("Broker did not provide a verifiable order ID")
        evidence = bounded_copy(market_evidence) if market_evidence is not None else None
        def apply(state):
            if any(str(item["orderId"]) == str(order_id) for item in state["pending"].values()):
                raise RuntimeError("Duplicate broker order ID")
            if any(str(item.get("orderId")) == str(order_id) for item in state["trades_log"]):
                raise RuntimeError("Broker order was already settled")
            state["pending"][token]["orderId"] = str(order_id)
            if submission_timing is not None:
                for field in ("order_submitted_at", "seconds_to_order"):
                    if field in submission_timing:
                        state["pending"][token][field] = submission_timing[field]
            if evidence is not None:
                state["pending"][token]["entryContext"]["market_evidence"] = evidence
        self.change(mode, apply)

    def reject(self, mode, token):
        def apply(state):
            del state["pending"][token]
            state["trades_today"] -= 1
        self.change(mode, apply)

    @staticmethod
    def apply_settlement(state, token, message):
        meta = state["pending"].get(token)
        if meta is None:
            return
        payout, deposit = message.get("profit_amount"), message.get("amount")
        if (
            meta["orderId"] is None
            or str(message.get("id")) != str(meta["orderId"])
            or (message.get("user_balance_id") is not None
                and message["user_balance_id"] != meta["balanceId"])
            or any(isinstance(v, bool) or not isinstance(v, (int, float))
                   or not math.isfinite(v) for v in (payout, deposit))
            or payout < 0 or deposit <= 0
            or not math.isclose(deposit, meta["stake"], rel_tol=0, abs_tol=0.01)
        ):
            raise RuntimeError("Unverified terminal broker amounts or identity")
        profit = round(payout - deposit, 2)
        result = "win" if profit > 0 else "loss" if profit < 0 else "equal"
        state["daily_pl"] = round(state["daily_pl"] + profit, 2)
        state["trades_log"].insert(0, {
            **meta, "result": result, "profit": profit, "settledAt": time.time(),
            "terminal": dict(message),
            "brokerTiming": message.get("brokerTiming") or unknown(),
        })
        state["trades_log"].sort(key=entry_order, reverse=True)
        recount_streak(state)
        del state["pending"][token]
        # Start a break once; later pending outcomes must not shorten it.
        # Expiry/reset is handled by the worker after reconciliation.
        if not state.get("cooldown_until", 0):
            refresh(state, time.time())

    def settle(self, mode, token, message):
        self.change(mode, lambda state: self.apply_settlement(state, token, message))

    def refresh_cooldown(self, mode, now):
        if not needs_refresh(self.accounts[mode], now):
            return False
        self.change(mode, lambda state: refresh(state, now))
        return True

    def assert_unused_order(self, order_id):
        for state in self.accounts.values():
            self.assert_unused_in_state(state, order_id)

    @staticmethod
    def assert_unused_in_state(state, order_id):
        assert_unused_in_state(state, order_id)

    def review(self, mode, token, evidence):
        self.assert_unused_order(evidence["orderId"])
        review_id = str(uuid.uuid4())
        def apply(state):
            self.assert_unused_in_state(state, evidence["orderId"])
            assert_unique_intent(state, token, evidence)
            meta = state["pending"].get(token)
            if meta is None or meta["orderId"] is not None:
                raise RuntimeError("Intent is no longer unconfirmed")
            review = {**evidence, "reviewId": review_id, "intentId": token,
                      "accountMode": mode, "reviewedAt": time.time()}
            # One current review per intent. Earlier evidence remains audited.
            state.setdefault("recovery_reviews", {})[token] = review
            self.audit(state, mode, token, evidence["orderId"], "reviewed", "", review)
        self.change(mode, apply, order_id=evidence["orderId"])
        return self.accounts[mode]["recovery_reviews"][token]

    @staticmethod
    def audit(state, mode, token, order_id, action, note, evidence=None):
        state.setdefault("recovery_audit", []).append({
            "at": time.time(), "accountMode": mode, "intentId": token,
            "orderId": order_id, "action": action, "actor": "authenticated API-secret operator",
            "note": note, "evidence": evidence,
        })

    def resolve(self, mode, review, evidence, confirmation, note):
        self.assert_unused_order(evidence["orderId"])
        token = review["intentId"]
        def apply(state):
            self.assert_unused_in_state(state, evidence["orderId"])
            assert_unique_intent(state, token, evidence)
            current = state.get("recovery_reviews", {}).get(token)
            meta = state["pending"].get(token)
            if (current != review or time.time() - review["reviewedAt"] > 600
                    or meta is None or meta["orderId"] is not None
                    or any(review.get(key) != value for key, value in evidence.items())):
                raise RuntimeError("Review expired, changed or already resolved; check broker again")
            expected = ("CONFIRM BROKER REJECTION" if evidence["outcome"] == "rejected"
                        else "RECONCILE VERIFIED ORDER")
            if confirmation != expected or not isinstance(note, str) or not 10 <= len(note.strip()) <= 1000:
                raise RuntimeError("Explicit outcome confirmation and review note are required")
            if evidence["outcome"] == "rejected":
                del state["pending"][token]
                state["trades_today"] -= 1
            else:
                meta["orderId"] = evidence["orderId"]
                self.apply_settlement(state, token, evidence["terminal"])
            self.audit(state, mode, token, evidence["orderId"], evidence["outcome"], note.strip(), current)
            del state["recovery_reviews"][token]
        # Mapping, result, risk counters, removal and audit share one row lock
        # and PostgreSQL transaction. Any failure retains the full reservation.
        self.change(mode, apply, order_id=evidence["orderId"])


def terminal_message(api, meta, historical=False):
    """Read actual terminal events, or a bounded per-ID terminal history query."""
    order_id = meta["orderId"]
    if order_id is None:
        return None  # Unknown submission outcome: never guess or resubmit.
    raw = api.get_async_order(int(order_id))
    event = raw.get("option-closed") if isinstance(raw, dict) else None
    if event:
        message = event.get("msg", {})
        return {**message, "id": message.get("option_id", message.get("id"))}
    if not historical:
        return None
    # The library's get_betinfo loops/reconnects indefinitely. Use its transport
    # directly with a deadline instead. History is ID-addressed across balances.
    try:
        row = history_row(api, order_id, meta)
    except BrokerHistoryUnavailable:
        raise
    except RuntimeError as error:
        if "timed out" in str(error):
            raise
        return None
    if not isinstance(row, dict) or str(row.get("win", "")).lower() not in (
        "win", "equal", "loose", "lose", "lost",
    ):
        return None
    # No inferred payouts or fabricated zero-profit settlements.
    return {
        "id": order_id, "amount": row.get("deposit"),
        "profit_amount": row.get("profit"),
        "user_balance_id": row.get("user_balance_id"),
        **({"history_evidence": row, "expired": row["expired"]}
           if row.get("historySource") == "portfolio.get-history-positions" else {}),
    }
