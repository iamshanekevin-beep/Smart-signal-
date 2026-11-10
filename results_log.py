"""
results_log.py - records each signal's result to results.json for the dashboard.
Logging only: it never changes signals, timing or trade logic.

Use in the bot:
    import results_log
    sid = results_log.record(pair, strategy, direction, entry, close, result)   # result "W", "L" or "D"
    results_log.set_gale(sid, "W")   # optional, after a loss: did the next candle win? "W" or "L"
"""
import json, os, threading, time, subprocess, uuid
from datetime import datetime, timezone

FILE = os.getenv("RESULTS_FILE", "results.json")
PAYOUT = float(os.getenv("PAYOUT", "0.85"))
PUSH_EVERY_S = int(os.getenv("RESULTS_PUSH_EVERY_S", "300"))
_lock = threading.Lock()
_dirty = False


def _load():
    try:
        with open(FILE) as f:
            return json.load(f)
    except Exception:
        return {"updated": None, "payout": PAYOUT, "signals": []}


def _save(d):
    global _dirty
    d["updated"] = datetime.now(timezone.utc).isoformat()
    d["payout"] = PAYOUT
    with open(FILE, "w") as f:
        json.dump(d, f)
    _dirty = True


def record(pair, strategy, direction, entry, close, result):
    sid = uuid.uuid4().hex[:10]
    with _lock:
        d = _load()
        d["signals"].append({"id": sid, "time": datetime.now(timezone.utc).isoformat(), "pair": pair,
                             "strategy": strategy, "direction": direction, "entry": entry,
                             "close": close, "result": result, "gale": None})
        _save(d)
    return sid


def set_gale(sid, gale_result):
    with _lock:
        d = _load()
        for s in d["signals"]:
            if s["id"] == sid:
                s["gale"] = gale_result
                break
        _save(d)


def _pusher():
    """Commits results.json back to the repo every few minutes so the dashboard sees it."""
    global _dirty
    while True:
        time.sleep(PUSH_EVERY_S)
        if not _dirty:
            continue
        try:
            with _lock:
                subprocess.run(["git", "add", FILE], check=True)
                subprocess.run(["git", "-c", "user.name=signalbot", "-c",
                                "user.email=signalbot@users.noreply.github.com",
                                "commit", "-m", "results"], check=False)
                _dirty = False
            subprocess.run(["git", "pull", "--rebase", "--autostash"], check=False)
            subprocess.run(["git", "push"], check=False)
        except Exception as e:
            print(f"results push failed: {e}")


if os.getenv("RESULTS_PUSH", "false").lower() == "true":
    threading.Thread(target=_pusher, daemon=True).start()
