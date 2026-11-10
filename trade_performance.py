"""Account-scoped performance from the complete confirmed settlement history."""
import math
from decimal import Decimal
from broker_timing import timing_for_history


def timing_summary(key, total, within, late, checked):
    """Build account-scoped observational counts; never reinterpret entry policy."""
    unknown_count = total - within - late
    return {
        "accountMode": key.split(":")[0], "balanceId": int(key.split(":")[1]),
        "total": total, "within2s": within, "lateAfter2s": late,
        "unknown": unknown_count, "unknownChecked": checked,
        "unknownNeverChecked": unknown_count - checked,
    }


def timing_snapshot(key, trades):
    """Offline projection of saved annotations only, including legacy unknowns."""
    total = within = late = checked = 0
    for trade in trades:
        total += 1
        timing = timing_for_history(trade)
        if timing.get("status") == "within_2s":
            within += 1
        elif timing.get("status") == "late_after_2s":
            late += 1
        else:
            stamp = timing.get("checkedAt")
            if (isinstance(stamp, (int, float)) and not isinstance(stamp, bool)
                    and math.isfinite(stamp) and stamp > 0):
                checked += 1
    return timing_summary(key, total, within, late, checked)


def performance_snapshot(mode, trades):
    wins = losses = break_even = 0
    total_profit = Decimal("0")
    for trade in trades:
        profit = trade["profit"]
        if isinstance(profit, bool) or not isinstance(profit, (int, float)) or not math.isfinite(profit):
            raise ValueError("Invalid confirmed profit")
        result = trade["result"]
        if result == "win":
            wins += 1
        elif result == "loss":
            losses += 1
        elif result == "equal":
            break_even += 1
        else:
            raise ValueError("Unknown confirmed result")
        total_profit += Decimal(str(profit))
    newest = sorted(trades, key=lambda trade: (trade.get("settledAt", trade["time"]), trade["time"]), reverse=True)
    return {
        "accountMode": mode,
        "total": len(trades),
        "wins": wins,
        "losses": losses,
        "breakEven": break_even,
        "cumulativePL": float(total_profit.quantize(Decimal("0.01"))),
        "recentTrades": [
            {**trade, "id": str(trade.get("orderId") or f"{trade['pair']}:{trade['time']}:{trade['side']}")}
            for trade in newest[:8]
        ],
    }