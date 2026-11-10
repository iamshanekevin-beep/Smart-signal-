"""Read-only broker evidence for lost acknowledgements. No buy or reset path."""
import math
import threading
import time

history_lock = threading.RLock()


class BrokerHistoryUnavailable(RuntimeError):
    """Keep reservations when historical evidence cannot verify an outcome."""


class BrokerHistoryTimeout(BrokerHistoryUnavailable):
    """Transport did not confirm a per-order history response; not a rejection."""

def assert_unused_in_state(state, order_id):
    rows = [*state["pending"].values(), *state["trades_log"],
            *state.get("recovery_audit", [])]
    if any(str(row.get("orderId")) == order_id and row.get("action") not in ("reviewed", "refused")
           for row in rows):
        raise RuntimeError("Broker order is already mapped or resolved")

def assert_unique_intent(state, token, evidence):
    for other_token, meta in state["pending"].items():
        if (other_token != token and meta["orderId"] is None
                and meta["balanceId"] == evidence["balanceId"]
                and meta["pair"] == evidence["pair"] and meta["side"] == evidence["side"]
                and math.isclose(meta["stake"], evidence["stake"], rel_tol=0, abs_tol=0.001)
                and meta["time"] - 2 <= evidence["submittedAt"] <= meta["time"] + 30
                and meta["expirationMinutes"] * 60 - 30 <= evidence["expiresAt"] - evidence["submittedAt"]
                    <= meta["expirationMinutes"] * 60 + 30):
            raise RuntimeError("Broker record matches multiple unconfirmed intents; mapping is ambiguous")


def history_row(api, order_id, meta=None):
    # game_betinfo is a shared single-response slot in the unofficial adapter.
    # Serialize all readers and clear stale replies before the bounded request.
    with history_lock:
        if meta is not None and callable(getattr(api.api, "send_websocket_request", None)):
            from broker_history import portfolio_row, IndividualHistoryUnavailable
            try:
                row = portfolio_row(api, {**meta, "orderId": str(order_id)})
                # Use the same full ID/account/asset/side/stake/time/outcome
                # verifier as operator recovery, before any automatic settlement.
                evidence_from_row(api, meta, str(order_id), row)
                return row
            except (IndividualHistoryUnavailable, RuntimeError):
                raise BrokerHistoryUnavailable(
                    "Broker individual-order history could not verify the saved trade"
                ) from None
        slot = api.api.game_betinfo
        slot.isSuccessful = None
        slot.dict = {}
        api.api.get_betinfo(int(order_id))
        deadline = time.monotonic() + 3
        while slot.isSuccessful is None:
            if time.monotonic() >= deadline:
                raise BrokerHistoryTimeout("Broker order history request timed out")
            time.sleep(0.05)
        if slot.isSuccessful is not True:
            raise RuntimeError("Broker did not verify this order; absence is not rejection")
        row = slot.dict.get("result", {}).get("data", {}).get(str(order_id))
        if not isinstance(row, dict):
            raise RuntimeError("Broker order evidence is missing; reservation retained")
        return dict(row)


def number(value):
    return (not isinstance(value, bool) and isinstance(value, (int, float))
            and math.isfinite(value))


def verified_evidence(api, meta, order_id):
    if not isinstance(order_id, str) or not order_id.isascii() or not order_id.isdigit() \
            or len(order_id) > 20 or int(order_id) <= 0:
        raise RuntimeError("Provide a positive broker order ID")
    row = history_row(api, order_id, {**meta, "orderId": order_id})
    return evidence_from_row(api, meta, order_id, row)


def evidence_from_row(api, meta, order_id, row):
    """Shared complete evidence checks for automatic and operator recovery."""
    # Do not accept client-supplied evidence or infer a balance from the selected
    # account. Partial history schemas deliberately fail closed.
    balance_id = row.get("user_balance_id")
    asset_id = row.get("active_id")
    expected_asset = api.get_all_ACTIVES_OPCODE().get(meta["pair"])
    submitted, expires = row.get("created"), row.get("expired")
    deposit = row.get("deposit")
    if (isinstance(balance_id, bool) or not isinstance(balance_id, int)
            or balance_id != meta["balanceId"]
            or isinstance(asset_id, bool) or not isinstance(asset_id, int)
            or expected_asset is None or asset_id != expected_asset
            or str(row.get("id")) != order_id
            or str(row.get("direction", "")).upper() != meta["side"]
            or not number(deposit) or deposit <= 0
            or not math.isclose(deposit, meta["stake"], rel_tol=0, abs_tol=0.001)
            or not number(submitted) or not number(expires)
            # Intent precedes buy. Bound commit/network delay; never match an
            # older similar trade or an arbitrarily delayed submission.
            or not meta["time"] - 2 <= submitted <= meta["time"] + 30
            # Broker rounds short-option expiry to a minute boundary and
            # closes entry 30 seconds before expiry.
            or not meta["expirationMinutes"] * 60 - 30 <= expires - submitted
                <= meta["expirationMinutes"] * 60 + 30
            or expires > time.time()):
        raise RuntimeError("Broker balance, asset, direction, stake or timing does not match the intent")
    rejection = row.get("status") == "rejected"
    reason = row.get("rejection_reason")
    payout = row.get("profit")
    if rejection:
        if not isinstance(reason, str) or not reason.strip() or row.get("win") not in (None, ""):
            raise RuntimeError("Broker rejection evidence is incomplete or contradictory")
    elif (str(row.get("win", "")).lower() not in ("win", "equal", "loose", "lose", "lost")
          or row.get("status") not in (None, "closed")
          or not number(payout) or payout < 0):
        raise RuntimeError("Broker record is not a verified terminal outcome")
    elif ((str(row["win"]).lower() == "win" and payout <= deposit)
          or (str(row["win"]).lower() == "equal" and payout != deposit)
          or (str(row["win"]).lower() in ("loose", "lose", "lost") and payout >= deposit)):
        raise RuntimeError("Broker record is not a verified terminal outcome")
    return {
        "orderId": order_id, "balanceId": balance_id, "pair": meta["pair"],
        "side": meta["side"], "stake": deposit, "submittedAt": submitted,
        "expiresAt": expires, "outcome": "rejected" if rejection else "settled",
        "profit": None if rejection else round(payout - deposit, 2),
        "rejectionReason": reason if rejection else None,
        "terminal": {"id": order_id, "user_balance_id": balance_id,
                     "amount": deposit, "profit_amount": payout,
                     **({"history_evidence": row}
                        if row.get("historySource") == "portfolio.get-history-positions" else {})},
    }