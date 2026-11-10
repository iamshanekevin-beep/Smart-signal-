"""Read-only, account-scoped portfolio history; never split grouped payouts."""
import math
import time
import uuid


class IndividualHistoryUnavailable(RuntimeError):
    """Missing, partial, conflicting, or unconfirmed individual broker evidence."""


def scalar_id(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    text = str(value)
    return text if text.isascii() and text.isdigit() and int(text) > 0 else None


def epoch(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise IndividualHistoryUnavailable("Invalid broker timestamp")
    if value <= 0:
        raise IndividualHistoryUnavailable("Invalid broker timestamp")
    return value / (1e9 if value > 1e17 else 1e6 if value > 1e14 else 1e3 if value > 1e11 else 1)


def contains_id(value, order_id):
    # Locate suspicious grouped records too, so they fail explicitly rather than
    # being mistaken for an absent order. Acceptance requires scalar IDs below.
    return (any(scalar_id(v) == order_id for v in value) if isinstance(value, list)
            else scalar_id(value) == order_id)


def references_order(position, wanted):
    if not isinstance(position, dict):
        return False
    raw = position.get("raw_event")
    return contains_id(position.get("external_id"), wanted) or (
        isinstance(raw, dict) and contains_id(raw.get("option_id"), wanted))


def individual_row(position, meta):
    """Normalize broker fields only; account identity comes from both records."""
    raw = position.get("raw_event")
    wanted = scalar_id(meta["orderId"])
    if not isinstance(raw, dict) or wanted is None:
        raise IndividualHistoryUnavailable("Partial broker order record")
    if (scalar_id(position.get("external_id")) != wanted
            or scalar_id(raw.get("option_id")) != wanted
            or position.get("status") != "closed"
            or position.get("source") != "binary-options"
            or position.get("instrument_type") not in ("binary-option", "turbo-option")
            or type(position.get("user_balance_id")) is not int
            or type(raw.get("balance_id")) is not int
            or position["user_balance_id"] != meta["balanceId"]
            or raw["balance_id"] != meta["balanceId"]
            or type(position.get("active_id")) is not int
            or position["active_id"] != raw.get("active_id")):
        raise IndividualHistoryUnavailable("Conflicting broker order identities")
    return {
        "id": wanted, "user_balance_id": raw["balance_id"], "active_id": raw.get("active_id"),
        "direction": raw.get("direction"), "deposit": raw.get("amount"),
        "profit": raw.get("profit_amount"), "win": raw.get("result"), "status": "closed",
        "created": epoch(raw.get("open_time")), "expired": epoch(raw.get("actual_expire")),
        "historySource": "portfolio.get-history-positions",
    }


def portfolio_row(api, meta):
    """Wait at most three seconds across bounded pages, with response correlation."""
    transport = api.api
    wanted = scalar_id(meta["orderId"])
    if wanted is None or type(meta.get("balanceId")) is not int or meta["balanceId"] <= 0:
        raise IndividualHistoryUnavailable("Unverified history request identity")
    deadline = time.monotonic() + 3
    for page in range(3):
        request_id = str(uuid.uuid4())
        transport.position_history_v2 = None
        transport.send_websocket_request("sendMessage", {
            "name": "portfolio.get-history-positions",
            "body": {
                "instrument_types": ["binary-option", "turbo-option"],
                "limit": 100, "offset": page * 100,
                "start": max(0, int(meta["time"]) - 120),
                "end": int(time.time()) + 1, "user_balance_id": meta["balanceId"],
            },
        }, request_id=request_id)
        reply = None
        while time.monotonic() < deadline:
            candidate = getattr(transport, "position_history_v2", None)
            if isinstance(candidate, dict) and candidate.get("request_id") == request_id:
                reply = candidate
                break
            time.sleep(.05)
        if reply is None or reply.get("status") != 2000:
            raise IndividualHistoryUnavailable("Broker history response unconfirmed")
        msg = reply.get("msg")
        rows = msg.get("positions") if isinstance(msg, dict) else None
        if not isinstance(rows, list):
            raise IndividualHistoryUnavailable("Partial portfolio history response")
        matches = [p for p in rows if references_order(p, wanted)]
        if len(matches) > 1:
            raise IndividualHistoryUnavailable("Ambiguous individual broker records")
        if matches:
            return individual_row(matches[0], meta)
        if len(rows) < 100 or time.monotonic() >= deadline:
            break
    raise IndividualHistoryUnavailable("No individual broker record verified")