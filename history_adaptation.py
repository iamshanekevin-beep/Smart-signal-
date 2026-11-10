"""Offline, approval-only FCB rule research. Never imports or connects to a broker.

Fit on the earliest 60%, validate on the next 20%, then evaluate ONCE on the
latest 20%. Saved signal evidence is not an execution-price reconstruction.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import html
import json
import math
from pathlib import Path
from market_evidence import FOCUS, MAX_CANDLES, bounded_copy
from entry_confirmation import fcb_price_confirmed, pole_quote_ready, ALIGNED_BREAKOUT_POLICY, CLEAN_BREAKOUT_POLICY, CLEAN_POLE_POLICY


def finite(value):
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)


def feature(row):
    context = row.get("entryContext") or {}
    if not isinstance(context, dict):
        return None, "not_fcb_or_missing_strategy"
    if context.get("engine") != "FCB":
        return None, "not_fcb_or_missing_strategy"
    if context.get("band_model") != "quadcode_fractal":
        return None, "different_or_unknown_band_model"
    side = row.get("side")
    if (side not in ("CALL", "PUT") or side != context.get("side")
            or row.get("executionMode") != "FOLLOW_SIGNAL"):
        return None, "reversed_or_unknown_execution"
    values = [context.get(k) for k in
              ("band_high", "band_low", "confirmed_at", "execution_candle", "signal_time")]
    values += [row.get(k) for k in ("time", "stake", "profit")]
    if not all(finite(value) for value in values):
        return None, "missing_or_invalid_evidence"
    upper, lower, confirmed, slot, signal, stamp, stake, profit = values
    if (upper <= lower or min(lower, stake) <= 0 or signal + 60 != slot
            or not 0 <= confirmed - slot <= 30):
        return None, "invalid_fcb_geometry_or_timing"
    trigger = context.get("trigger")
    if trigger not in ("candle", "fcb_win"):
        return None, "unknown_entry_trigger"
    evidence = context.get("market_evidence")
    if evidence is None:
        return None, "missing_market_window_or_confirmation"
    try:
        bounded_copy(evidence)
    except (ValueError, TypeError, OverflowError):
        return None, "invalid_or_unbounded_market_evidence"
    candles = evidence["closed_candles"]
    if (evidence.get("source") != "broker_closed_candles"
            or evidence.get("timeframe_seconds") != 60
            or not 5 <= len(candles) <= MAX_CANDLES):
        return None, "incompatible_candle_provenance"
    for index, candle in enumerate(candles):
        if (not isinstance(candle, dict)
                or not all(finite(candle.get(k)) for k in ("time", "open", "high", "low", "close"))):
            return None, "invalid_closed_candles"
        if (candle["time"] % 60 != 0 or candle["time"] + 60 > slot
                or candle["low"] <= 0
                or not candle["low"] <= min(candle["open"], candle["close"])
                <= max(candle["open"], candle["close"]) <= candle["high"]
                or index and candle["time"] != candles[index-1]["time"] + 60):
            return None, "non_point_in_time_or_gapped_candles"
    if candles[-1]["time"] != signal:
        return None, "missing_signal_candle"
    if (context.get("confirmation_policy") in (ALIGNED_BREAKOUT_POLICY, CLEAN_BREAKOUT_POLICY, CLEAN_POLE_POLICY)
            and (context.get("signal_open") != candles[-1]["open"]
                 or context.get("signal_close") != candles[-1]["close"])):
        return None, "signal_body_not_from_saved_candle"
    bands = evidence.get("closed_bands")
    if (not isinstance(bands, dict) or bands != {
            "model": "quadcode_fractal", "high": upper, "low": lower, "as_of": signal}):
        return None, "incompatible_closed_bands"
    if evidence.get("captured_at") != confirmed:
        return None, "invalid_capture_time"
    checks = evidence.get("checks")
    if not isinstance(checks, dict):
        return None, "missing_submission_evidence"
    previous = confirmed
    # Archived observations retain the timing policy they actually used.
    # Tightening live execution must not retroactively invalidate old evidence.
    confirmation = evidence.get("confirmation") or {}
    window = context.get("entry_window_seconds", confirmation.get("confirmation_window_seconds"))
    if window not in (2, 5, 30):
        return None, "invalid_confirmation_context"
    gate = pole_quote_ready if trigger == "fcb_win" else fcb_price_confirmed
    for name, check in (("confirmation", evidence.get("confirmation")),
                        ("reservation", checks.get("reservation")),
                        ("submission", checks.get("submission"))):
        if not isinstance(check, dict):
            return None, "missing_submission_evidence"
        quote = check.get("quote")
        local, broker = check.get("local_time"), check.get("broker_time")
        if (not isinstance(quote, dict) or not finite(local) or not finite(broker)
                or local < previous or check.get("price_kind") != "observed_quote_not_broker_fill"
                or check.get("max_freshness_seconds") != 3
                or check.get("confirmation_window_seconds") != window):
            return None, "invalid_confirmation_context"
        observed = quote.get("observed_at")
        if not finite(observed):
            return None, "invalid_confirmation_context"
        for key, expected in (("clock_skew_seconds", broker - local),
                              ("local_quote_age_seconds", local - observed),
                              ("broker_quote_age_seconds", broker - observed)):
            if not finite(check.get(key)) or not math.isclose(check[key], expected, abs_tol=1e-6):
                return None, "invalid_clock_evidence"
        if not gate(context, quote, local, broker, window_seconds=window):
            return None, "unconfirmed_or_inside_band_quote"
        if name == "confirmation" and local != confirmed:
            return None, "invalid_confirmation_context"
        previous = local
    settled = row.get("settledAt")
    if not finite(settled) or settled < previous or not confirmed <= stamp <= previous:
        return None, "invalid_submission_or_settlement_time"
    outcome = row.get("result")
    if not ((outcome == "win" and profit > 0) or (outcome == "loss" and profit < 0)
            or (outcome == "equal" and profit == 0)):
        return None, "invalid_settlement"
    if trigger == "candle":
        price = context.get("signal_close")
        if not finite(price) or price != candles[-1]["close"]:
            return None, "signal_close_not_from_saved_candle"
    else:
        price = evidence["confirmation"]["quote"]["close"]
    distance = price - upper if side == "CALL" else lower - price
    if distance <= 0:
        return None, "signal_not_clearly_outside_fcb"
    return {
        "time": slot, "settledAt": settled,
        "cohort": ((trigger if window == 30 else f"{trigger}:0-{window:g}s")
                   + (f":{context['confirmation_policy']}" if context.get("confirmation_policy") else "")),
        "margin": distance / (upper - lower),
        "delay": confirmed - slot, "result": outcome,
        "profit": profit, "return": profit / stake,
    }, None


def metrics(rows):
    n = len(rows)
    wins = sum(row["result"] == "win" for row in rows)
    losses = sum(row["result"] == "loss" for row in rows)
    decisive = wins + losses
    p = wins / decisive if decisive else 0
    z = 1.96
    lower = ((p + z*z/(2*decisive) - z*math.sqrt(p*(1-p)/decisive + z*z/(4*decisive**2)))
             / (1 + z*z/decisive)) if decisive else 0
    return {
        "trades": n, "wins": wins, "losses": losses, "refunds": n - decisive,
        "winRate": p if decisive else None, "winRateLower95": lower,
        "netProfit": round(sum(row["profit"] for row in rows), 6),
        "meanReturnPerStake": sum(row["return"] for row in rows) / n if n else None,
    }


def select(rows, rule):
    return [row for row in rows if row["margin"] >= rule["minOutsideMarginInBandWidths"]
            and row["delay"] <= rule["maxConfirmationSeconds"]]


BASELINE = {"minOutsideMarginInBandWidths": 0, "maxConfirmationSeconds": 30}


def fit(rows):
    """Selection uses training only; later splits never choose parameter values."""
    baseline = metrics(rows)
    candidates = []
    for margin in (0, .05, .1, .2, .3, .5, .75, 1):
        for delay in (30, 20, 15, 10, 5):
            rule = {"minOutsideMarginInBandWidths": margin, "maxConfirmationSeconds": delay}
            selected = metrics(select(rows, rule))
            if selected["trades"] >= max(30, math.ceil(.4 * len(rows))):
                candidates.append((selected["winRateLower95"], selected["meanReturnPerStake"],
                                   selected["trades"], -margin, delay, rule))
    chosen = max(candidates, key=lambda item: item[:5])[-1] if candidates else BASELINE.copy()
    return chosen, baseline


def assess(samples):
    samples = sorted(samples, key=lambda row: row["time"])
    # Same-candle orders stay together. Labels not available at a split's
    # boundary cannot inform the earlier split (purge delayed settlements).
    timestamps = sorted({row["time"] for row in samples})
    purged = 0
    boundaries = None
    if len(timestamps) < 3:
        training, validation, testing = samples, [], []
    else:
        first = timestamps[min(len(timestamps)-2, max(1, int(len(timestamps)*.6)))]
        second = timestamps[min(len(timestamps)-1, max(2, int(len(timestamps)*.8)))]
        training = [row for row in samples if row["time"] < first and row["settledAt"] < first]
        validation = [row for row in samples if first <= row["time"] < second and row["settledAt"] < second]
        testing = [row for row in samples if row["time"] >= second]
        purged = len(samples) - len(training) - len(validation) - len(testing)
        boundaries = {"validationStarts": first, "testStarts": second}
    rule, _ = fit(training)
    splits = {
        name: {"baseline": metrics(split), "candidate": metrics(select(split, rule))}
        for name, split in (("training", training), ("validation", validation), ("test", testing))
    }
    sufficient = (len(samples) >= 100 and len(training) >= 60 and
                  all(splits[name]["candidate"]["trades"] >= 20 for name in ("validation", "test")))
    improves = all(
        splits[name]["candidate"]["meanReturnPerStake"] is not None
        and splits[name]["baseline"]["meanReturnPerStake"] is not None
        and splits[name]["candidate"]["meanReturnPerStake"] > splits[name]["baseline"]["meanReturnPerStake"]
        and splits[name]["candidate"]["meanReturnPerStake"] > 0
        and splits[name]["candidate"]["winRateLower95"] >= splits[name]["baseline"]["winRateLower95"]
        for name in ("validation", "test")
    )
    status = ("INSUFFICIENT_EVIDENCE" if not sufficient else
              "BASELINE_RETAINED" if rule == BASELINE else
              "REVIEW_CANDIDATE" if improves else "NO_VALIDATED_IMPROVEMENT")
    return {"eligible": len(samples), "status": status, "proposedRule": rule,
            "splits": splits, "boundaries": boundaries, "purgedLateSettlements": purged, "applied": False}


def research(rows):
    excluded, sources, seen = Counter(), Counter(), set()
    groups = defaultdict(list)
    coverage = defaultdict(Counter)
    local_excluded = defaultdict(Counter)
    for row in rows:
        if not isinstance(row, dict):
            excluded["invalid_record"] += 1
            continue
        sources[str(row.get("source", "unknown"))] += 1
        mode, pair = row.get("accountMode"), row.get("pair")
        if mode not in ("PRACTICE", "REAL"):
            excluded["unknown_account_mode"] += 1
            continue
        if pair not in FOCUS:
            excluded["outside_scan_focus"] += 1
            continue
        coverage[(mode, pair)]["audited"] += 1
        context = row.get("entryContext")
        evidence = context.get("market_evidence") if isinstance(context, dict) else None
        if isinstance(evidence, dict):
            for field, present in (
                ("withClosedWindow", bool(evidence.get("closed_candles"))),
                ("withConfirmation", isinstance(evidence.get("confirmation"), dict)),
                ("withSubmission", isinstance(evidence.get("checks"), dict)
                 and isinstance(evidence["checks"].get("submission"), dict)),
            ):
                coverage[(mode, pair)][field] += bool(present)
        balance_id = row.get("balanceId")
        broker_id = row.get("brokerOrder", row.get("orderId"))
        reason = None
        sample = None
        if isinstance(balance_id, bool) or not isinstance(balance_id, int) or balance_id <= 0:
            reason = "unknown_balance_identity"
        elif (isinstance(broker_id, bool) or not str(broker_id).isdigit()
              or int(broker_id) <= 0):
            reason = "missing_broker_order_identity"
        elif ("orderId" in row and str(row["orderId"]) != str(broker_id)):
            reason = "conflicting_broker_order_identity"
        else:
            key = (mode, balance_id, str(broker_id))
            if key in seen:
                reason = "duplicate_order"
            else:
                seen.add(key)
                sample, reason = feature(row)
        if reason:
            excluded[reason] += 1
            local_excluded[(mode, pair)][reason] += 1
        else:
            coverage[(mode, pair)]["eligible"] += 1
            groups[(mode, pair, balance_id, sample["cohort"])].append(sample)
    results = {}
    for mode in ("PRACTICE", "REAL"):
        instruments = {}
        for pair in FOCUS:
            cohorts = {
                f"{balance_id}:{trigger}": {
                    **assess(samples), "balanceId": balance_id, "trigger": trigger,
                    "marginBasis": "signal_close" if trigger == "candle" else "confirmation_quote",
                }
                for (group_mode, instrument, balance_id, trigger), samples in groups.items()
                if group_mode == mode and instrument == pair
            }
            counts = {field: coverage[(mode, pair)][field] for field in
                      ("audited", "eligible", "withClosedWindow", "withConfirmation", "withSubmission")}
            counts["compatibleFraction"] = counts["eligible"] / counts["audited"] if counts["audited"] else None
            instruments[pair] = {"coverage": counts, "excluded": dict(local_excluded[(mode, pair)]),
                                 "cohorts": cohorts, "applied": False}
        results[mode] = {"label": "Demo" if mode == "PRACTICE" else "Live",
                         "eligible": sum(item["coverage"]["eligible"] for item in instruments.values()),
                         "instruments": instruments, "applied": False}
    timestamps = [row["time"] for row in rows if isinstance(row, dict) and finite(row.get("time"))]
    period = {
        key: datetime.fromtimestamp(value, timezone.utc).isoformat()
        for key, value in (("start", min(timestamps)), ("end", max(timestamps)))
    } if timestamps else None
    return {
        "recordsAudited": len(rows), "sources": dict(sources), "excluded": dict(excluded),
        "periodUTC": period,
        "eligible": sum(len(samples) for samples in groups.values()), "accounts": results,
        "fingerprint": hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest(),
        "automaticApplication": False,
        "limitations": [
            "Only saved current-Fractal-FCB, signal-following orders with valid evidence are fitted.",
            "Ordinary FCB uses signal-close margin; win poles use confirmation-quote margin in a separate cohort.",
            "This evaluates subsets of executed trades, not unexecuted opportunities or a full market replay.",
            "Training-grid search can overfit. Later validation and test data do not select the rule.",
            "Trades can be correlated. Wilson bounds assume independent outcomes and are only diagnostic.",
            "Only bounded actual pre-decision candles are saved; carried bands may originate before that window.",
            "Presence counts are not proof of compatibility. Missing older evidence is excluded, never backfilled.",
            "Quotes are observations, not broker fills; no execution-price reconstruction is attempted.",
            "Instruments, Demo/Live balances and entry triggers never pool fitting data.",
            "No proof of profitability, certainty, or broad chop detection; no candidate is applied automatically.",
            "RSI is unchanged and is not forced outside FCB. Demo and Live are researched separately.",
        ],
    }


def report_html(report):
    assessments = [
        (f'{account["label"]} / {pair} / {key}', cohort)
        for account in report["accounts"].values()
        for pair, instrument in account["instruments"].items()
        for key, cohort in instrument["cohorts"].items()
    ]
    qualified = any(cohort["status"] == "REVIEW_CANDIDATE" for _, cohort in assessments)
    finding = ("A candidate qualifies for further review, not automatic application." if qualified else
               "No tested strategy adjustment qualifies for recommendation. Keep learned parameters inactive.")
    period = html.escape(str(report["periodUTC"]))
    rows = []
    for mode, account in assessments:
        for split, data in account["splits"].items():
            for label, m in data.items():
                win = "—" if m["winRate"] is None else f'{m["winRate"]:.1%}'
                ret = "—" if m["meanReturnPerStake"] is None else f'{m["meanReturnPerStake"]:.1%}'
                rows.append(f"<tr><td>{html.escape(mode)}</td><td>{split}</td><td>{label}</td>"
                            f'<td>{m["trades"]}</td><td>{win}</td><td>{ret}</td><td>{m["netProfit"]:.2f}</td></tr>')
    proposals = "".join(
        f'<h3>{html.escape(mode)}: {account["status"]}</h3><p>Eligible: {account["eligible"]}. '
        f'Candidate minimum outside distance ({account["marginBasis"]}): {account["proposedRule"]["minOutsideMarginInBandWidths"]} '
        f'band widths; maximum confirmation: {account["proposedRule"]["maxConfirmationSeconds"]} seconds. '
        f'Late settlements purged at split boundaries: {account["purgedLateSettlements"]}. '
        'Not applied.</p>' for mode, account in assessments)
    coverage = "".join(
        f'<tr><td>{account["label"]}</td><td>{html.escape(pair)}</td>'
        + "".join(f'<td>{instrument["coverage"][field]}</td>' for field in
                  ("audited", "withClosedWindow", "withConfirmation", "withSubmission", "eligible"))
        + f'<td>{html.escape(str(instrument["excluded"]))}</td></tr>'
        for account in report["accounts"].values() for pair, instrument in account["instruments"].items()
    )
    excluded = "".join(f"<li>{html.escape(reason.replace('_', ' '))}: {count}</li>"
                       for reason, count in report["excluded"].items())
    limitations = "".join(f"<li>{html.escape(text)}</li>" for text in report["limitations"])
    return f"""<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>FCB historical research</title>
<style>body{{font:16px/1.6 system-ui;background:#f5f3ed;color:#183033;max-width:1050px;margin:40px auto;padding:24px}}
table{{border-collapse:collapse;width:100%;font-size:14px}}td,th{{padding:9px;border-bottom:1px solid #c5cecb;text-align:left}}
.notice{{background:#fff;padding:18px;border-left:4px solid #8b6200}}h1,h2{{line-height:1.2}}
.table-wrap{{overflow-x:auto}}table{{min-width:720px}}</style>
<h1>FCB historical rule research</h1><p class="notice">Approval-only research. No trading was started and no learned
parameters were applied. The new development safeguard requires clear outside-band FCB/pole price action; RSI rules remain unchanged.
The published bot was not modified.</p><p><strong>{finding}</strong></p>
<p>Audited {report["recordsAudited"]} settled records: {html.escape(str(report["sources"]))}.
Eligible for fitting: {report["eligible"]}. Covered order period (UTC): {period}.</p>
<p>INSUFFICIENT_EVIDENCE means fewer than 100 compatible orders, fewer than 60 training orders,
or fewer than 20 selected orders in either later split. Both later splits must also show positive mean return
and improvement over baseline before a candidate can qualify for review. These minimums apply separately to each
instrument, account balance and entry-trigger cohort; evidence is never borrowed from another group.</p>
<h2>Evidence coverage by instrument and account</h2><div class="table-wrap"><table><thead><tr>
<th>Account</th><th>Instrument</th><th>Audited</th><th>Closed window present</th><th>Confirmation present</th>
<th>Submission present</th><th>Compatible</th><th>Exclusions</th></tr></thead><tbody>{coverage}</tbody></table></div>
<h2>Candidate assessment</h2>{proposals or '<p>No compatible research cohort yet. Keep the baseline unchanged.</p>'}
<h2>Chronological evaluation</h2><p>Earliest ~60%: training; next ~20%: validation; latest ~20%: untouched test.
Trades from the same decision candle stay in one split. Mean return is actual net profit divided by stake, averaged per selected trade.
Net profit uses recorded account units. Fewer trades can reduce total profit even if per-trade results improve.</p>
<div class="table-wrap"><table><thead><tr><th>Account</th><th>Split</th><th>Rule</th><th>Trades</th><th>Win rate</th>
<th>Mean return/stake</th><th>Net profit</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
<h2>Excluded from fitting, not deleted</h2><ul>{excluded}</ul><h2>Limits of the evidence</h2><ul>{limitations}</ul>
<p>Snapshot fingerprint: <small>{report["fingerprint"]}</small></p></html>"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("history", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = research(json.loads(args.history.read_text()))
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "assessment.json").write_text(json.dumps(report, indent=2))
    (args.output / "report.html").write_text(report_html(report))
    print(json.dumps({key: report[key] for key in ("recordsAudited", "eligible", "excluded")}))
    for mode, account in report["accounts"].items():
        for pair, instrument in account["instruments"].items():
            print(mode, pair, instrument["coverage"])


if __name__ == "__main__":
    main()
