"""Practice-only candle/FCB/RSI gates. No broker I/O or additional indicators."""
from fast_trade import calc_rsi
from entry_confirmation import number

FIELDS = (
    "zone", "zone_trigger", "band_width_ratio", "color_flips_10",
    "avg_body_pct_10", "failed_breakouts_15", "close_distance_pct",
    "range_vs_avg10", "band_rejections_5",
)


def high(c):
    return c.get("high", c.get("max"))


def low(c):
    return c.get("low", c.get("min"))


def valid(c):
    values = [c.get("time"), c.get("open"), c.get("close"), high(c), low(c)]
    return (all(number(v) for v in values)
            and 0 < low(c) <= min(c["open"], c["close"])
            <= max(c["open"], c["close"]) <= high(c))


def body(c):
    span = high(c) - low(c)
    return abs(c["close"] - c["open"]) / span * 100 if span else 0.0


def inside(price, upper, lower):
    return lower <= price <= upper


def failed(rows, upper, lower, side, within=None):
    """Fixed band value; equality is inside. Each first-close break counts once."""
    total = 0
    for i, c in enumerate(rows):
        outside = c["close"] > upper if side == "CALL" else c["close"] < lower
        if not outside:
            continue
        if i and (rows[i - 1]["close"] > upper if side == "CALL"
                  else rows[i - 1]["close"] < lower):
            continue
        following = rows[i + 1:i + 1 + within] if within else rows[i + 1:]
        if any(inside(r["close"], upper, lower) for r in following):
            total += 1
    return total


def zone_metrics(prior, levels):
    """All windows end BEFORE the signal. RSI(14) uses the existing calculation."""
    metrics = {field: None for field in FIELDS}
    metrics.update(zone="UNAVAILABLE", zone_trigger="")
    if (len(prior) < 24 or len(levels) != len(prior)
            or not all(valid(c) for c in prior[-24:])
            or any(b["time"] != a["time"] + 60 for a, b in zip(prior[-24:-1], prior[-23:]))):
        return metrics
    upper, lower = levels[-1]
    if not all(number(v) for v in (upper, lower)) or upper < lower:
        return metrics
    if any(not all(number(v) for v in pair) or pair[0] < pair[1] for pair in levels[-15:]):
        return metrics
    last10 = prior[-10:]
    avg20 = sum(high(c) - low(c) for c in prior[-20:]) / 20
    colors = [(c["close"] > c["open"]) - (c["close"] < c["open"]) for c in last10]
    # Doji is neutral, not a bullish/bearish color flip.
    flips = sum(a * b == -1 for a, b in zip(colors, colors[1:]))
    avg_body = sum(body(c) for c in last10) / 10
    closes = [c["close"] for c in prior]
    rsis = [calc_rsi(closes[:i + 1], 14) for i in range(len(prior) - 10, len(prior))]
    count = 0
    start = len(prior) - 15
    for i in range(start, len(prior)):
        u, l = levels[i]
        if not all(number(v) for v in (u, l)) or u < l:
            continue
        # Count each beyond-band close followed by an inside close within two bars.
        for side in ("CALL", "PUT"):
            outside = prior[i]["close"] > u if side == "CALL" else prior[i]["close"] < l
            if outside and any(
                    inside(c["close"], u, l) for c in prior[i + 1:i + 3]):
                count += 1
    triggers = []
    if upper - lower < 3 * avg20:
        triggers.append("A2a")
    if all(number(r) and 40 <= r <= 60 for r in rsis):
        triggers.append("A2b")
    if flips >= 6:
        triggers.append("A3a")
    if avg_body < 45 or avg_body > 57:
        triggers.append("A3b")
    if count >= 2:
        triggers.append("A3c")
    # Other single indicators remain auditable, but need corroboration to veto.
    blocked = len(triggers) >= 2 or not 45 <= avg_body <= 57
    metrics.update(
        zone=("CHOPPY" if any(t.startswith("A3") for t in triggers)
              else "ACCUMULATION") if blocked else "CLEAR",
        zone_trigger=",".join(triggers),
        band_width_ratio=(upper - lower) / avg20 if avg20 else None,
        color_flips_10=flips, avg_body_pct_10=avg_body, failed_breakouts_15=count,
    )
    return metrics


def candle_only_zone_metrics(prior):
    """Classify the computable Practice Zone warnings without FCB band data.

    This fallback is for standalone RSI reversals only. Invalid, incomplete, or
    gapped candles remain unavailable; band-dependent accumulation and failed-
    breakout checks are never inferred.
    """
    metrics = {field: None for field in FIELDS}
    metrics.update(zone="UNAVAILABLE", zone_trigger="")
    if (len(prior) < 24 or not all(valid(c) for c in prior[-24:])
            or any(b["time"] != a["time"] + 60
                   for a, b in zip(prior[-24:-1], prior[-23:]))):
        return metrics

    last10 = prior[-10:]
    avg20 = sum(high(c) - low(c) for c in prior[-20:]) / 20
    colors = [(c["close"] > c["open"]) - (c["close"] < c["open"]) for c in last10]
    flips = sum(a * b == -1 for a, b in zip(colors, colors[1:]))
    avg_body = sum(body(c) for c in last10) / 10
    closes = [c["close"] for c in prior]
    rsis = [calc_rsi(closes[:i + 1], 14)
            for i in range(len(prior) - 10, len(prior))]

    triggers = []
    if all(number(value) and 40 <= value <= 60 for value in rsis):
        triggers.append("A2b")
    if flips >= 6:
        triggers.append("A3a")
    if avg_body < 40:
        triggers.append("A3b")
    blocked = len(triggers) >= 2
    metrics.update(
        zone=("CHOPPY" if any(t.startswith("A3") for t in triggers)
              else "ACCUMULATION") if blocked else "CLEAR",
        zone_trigger=",".join(triggers),
        color_flips_10=flips,
        avg_body_pct_10=avg_body,
    )
    return metrics


def breakout_metrics(prior, signal, upper, lower, side, zone):
    metrics = dict(zone)
    if (not prior or not valid(signal) or not all(valid(c) for c in prior[-15:])
            or not all(number(v) for v in (upper, lower)) or upper <= lower
            or side not in ("CALL", "PUT")):
        return metrics, "INVALID_BREAKOUT"
    span = high(signal) - low(signal)
    avg10 = sum(high(c) - low(c) for c in prior[-10:]) / 10
    distance = signal["close"] - upper if side == "CALL" else lower - signal["close"]
    rejections = sum(
        inside(c["close"], upper, lower)
        and (high(c) > upper if side == "CALL" else low(c) < lower)
        for c in prior[-5:]
    )
    failed_before = failed(prior[-15:], upper, lower, side) > 0
    rsi = calc_rsi([c["close"] for c in prior] + [signal["close"]], 14)
    wick = (high(signal) - max(signal["open"], signal["close"]) if side == "CALL"
            else min(signal["open"], signal["close"]) - low(signal))
    metrics.update(close_distance_pct=distance / span * 100 if span else None,
                   range_vs_avg10=span / avg10 if avg10 else None,
                   band_rejections_5=rejections, band_failed_before="Y" if failed_before else "N",
                   body_pct=body(signal), breakout_wick_pct=wick / span * 100 if span else None,
                   RSI_at_signal=rsi)
    if (zone["zone"] != "CLEAR" or not number(zone.get("color_flips_10"))
            or zone["color_flips_10"] >= 5):
        return metrics, "ZONE_SKIP"
    if span <= 0 or avg10 <= 0:
        return metrics, "INVALID_BREAKOUT"
    first_close = (prior[-1]["close"] <= upper < signal["close"] if side == "CALL"
                   else prior[-1]["close"] >= lower > signal["close"])
    if not first_close:
        return metrics, "NOT_FIRST_CLOSE"
    # FCB signal quality measurements remain in the audit/export, but the
    # standard A/B breakout policy does not reject a valid first close for
    # body, wick, RSI, distance, range expansion, or prior-rejection metrics.
    return metrics, None
