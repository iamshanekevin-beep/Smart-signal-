"""Read-only presentation of pending orders; countdowns never settle an order."""
def active_trades(pending):
    rows = []
    for token, meta in sorted(pending.items(), key=lambda item: item[1]["time"], reverse=True):
        acknowledged = meta["orderId"] is not None
        # The installed IQ binary helper uses a wall-clock expiry, not simply
        # entry time + 60 seconds. Reservation time is an ESTIMATE of buy time;
        # never claim a broker-confirmed expiry or completion from this timer.
        # Do not call get_expiration_time with a historical timestamp: that
        # library helper compares candidates to the current time.time(), so
        # its answer would move when refreshing an old order. Reproduce the
        # supported one-minute cutoff deterministically from saved buy intent.
        expiry = None
        if acknowledged and meta["expirationMinutes"] == 1:
            stamp = int(meta["time"])
            boundary = (stamp // 60 + 1) * 60
            expiry = boundary if boundary - stamp > 30 else boundary + 60
        context = meta.get("entryContext") or {}
        rows.append({
            "intentId": token,
            "orderId": str(meta["orderId"]) if acknowledged else None,
            "pair": meta["pair"], "side": meta["side"],
            "engine": context.get("engine"),
            "stake": meta["stake"], "openedAt": meta["time"],
            "expiresAt": expiry, "expiryEstimated": expiry is not None,
            "state": "OPEN" if acknowledged else "UNCONFIRMED",
        })
    return rows