"""Read-only projections of saved, deduplicated skip audits."""

# This labels the candle validation rules, not the timing or execution policy.
# Bump when these filters change; never relabel archived observations.
VALIDATION_POLICY = "practice-candle-filters-v1"
FILTER_REASONS = (
    "ZONE_SKIP", "ZONE_EXIT", "WEAK_CLOSE", "NO_EXPANSION",
    "BAND_FAILED_BEFORE", "BAND_REJECTIONS", "BREAKOUT_BODY",
    "BREAKOUT_WICK", "BREAKOUT_RSI", "NOT_FIRST_CLOSE",
    "INVALID_BREAKOUT", "BREAKOUT_VALIDATION_FAILED",
)


def project_groups(rows):
    groups = []
    filters = missing = other = 0
    for pair, reason, policy, evidence, count in rows:
        is_filter = reason in FILTER_REASONS
        category = "candle_filter" if is_filter and evidence else (
            "missing_validation" if is_filter else "other_skip")
        if category == "candle_filter":
            filters += count
        elif category == "missing_validation":
            missing += count
        else:
            other += count
        groups.append({"pair": pair or "Unknown pair", "reason": reason or "Unrecorded reason",
                       "policy": policy or "Unversioned historical audit",
                       "category": category, "count": count})
    return {"groups": groups, "candleFilterCount": filters,
            "missingValidationCount": missing, "otherSkipCount": other}


def current_guards(snapshot, mode):
    """Never present active-account status as the other account's guards."""
    if not snapshot or snapshot.get("accountMode") != mode:
        return None
    guards = []
    def add(code, blocked, detail):
        guards.append({"code": code, "blocked": bool(blocked), "detail": detail})
    add("pause", not snapshot.get("running"), "Manual pause")
    add("connection", not snapshot.get("connected"), "Broker connection")
    add("account_switch", snapshot.get("accountSwitchPending"), "Pending account switch")
    add("recovery", snapshot.get("recoveryState") != "ready",
        snapshot.get("recoveryMessage") or "Recovery state is unavailable")
    add("cooldown", snapshot.get("cooldownActive"), "Loss-streak cooldown")
    add("daily_trades", snapshot.get("tradesToday", 0) >= snapshot.get("maxDailyTrades", float("inf")),
        "Maximum daily trades")
    add("daily_loss", snapshot.get("dailyPL", 0) <= snapshot.get("dailyLossLimit", float("-inf")),
        "Daily loss limit")
    add("loss_streak", snapshot.get("lossStreak", 0) >= snapshot.get("_lossStreakStop", float("inf")),
        "Loss streak stop")
    add("entry_safety", snapshot.get("_entryGuardReason"),
        snapshot.get("_entryGuardReason") or "Entry safety checks clear")
    return {"observedAt": snapshot["updatedAt"], "items": guards}
