"""
IQ Option connection + candle streaming, read-only.

Keeps one persistent session alive for the life of the process -- this is
the piece Base44's per-request function model can't do, and exactly what
Replit's long-running repl is good for.
"""

import os
import math
import threading
import time

from broker_orders import OneMinuteIQOption as IQ_Option
from iqoptionapi import constants as broker_assets

# The pool is ordered by scan priority: the existing ten OTC pairs, five
# requested regular pairs, then three OTC reserves. Only ACTIVE_COUNT trade at
# once; unavailable pairs are replaced only with broker-reported open assets.
PAIR_POOL = [
    "EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC", "AUDUSD-OTC", "EURGBP-OTC",
    "USDCHF-OTC", "NZDUSD-OTC", "EURJPY-OTC", "GBPJPY-OTC", "AUDJPY-OTC",
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
    "USDCAD-OTC", "EURCHF-OTC", "CADJPY-OTC",
]
REGULAR_PAIRS = ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD")
CANDLE_SIZE = 60  # seconds -> 1-minute candles

IQ_EMAIL = os.environ.get("IQ_EMAIL")
IQ_PASSWORD = os.environ.get("IQ_PASSWORD")
ACCOUNT_MODE = os.environ.get("IQ_ACCOUNT_MODE", "PRACTICE")

_api = None
# Recovery holds the session lock while calling get_session(), which also
# acquires it. Reentrancy preserves the outer session boundary without deadlock.
_lock = threading.RLock()
_catalog_lock = threading.Lock()
_history_lock = threading.Lock()
_catalog_updated = 0.0
_open_assets = set()
session_generation = 0
connection_requested = threading.Event()
connection_requested.set()


def get_session():
    """Returns the single shared IQ_Option session, connecting if needed.
    Does NOT start any candle streams -- main.py owns which pairs are
    active and starts/stops their streams as that set changes."""
    global _api, session_generation
    with _lock:
        if not connection_requested.is_set():
            raise RuntimeError("Broker deliberately disconnected; Connect first")
        if _api is None:
            if not IQ_EMAIL or not IQ_PASSWORD:
                raise RuntimeError(
                    "IQ_EMAIL / IQ_PASSWORD not set. Add them in Replit's "
                    "Secrets tab (lock icon in the left sidebar), not in code."
                )
            api = IQ_Option(IQ_EMAIL, IQ_PASSWORD)
            check, reason = api.connect()
            if not check:
                raise RuntimeError(f"IQ Option connect failed: {reason}")
            api.change_balance(ACCOUNT_MODE)
            _api = api
            session_generation += 1
        elif not _api.check_connect():
            print("Connection dropped, reconnecting...")
            check, _ = _api.connect()
            if not check:
                raise RuntimeError("Broker reconnect failed")
            session_generation += 1
            _api.change_balance(ACCOUNT_MODE)
        return _api


def disconnect_session():
    """Executor-only deliberate disconnect, after every order is settled."""
    global _api, session_generation
    with _lock:
        if _api is not None:
            _api.api.websocket.close(timeout=2)
            _api.api.websocket_thread.join(timeout=2)
            if _api.api.websocket_thread.is_alive():
                raise RuntimeError("Broker disconnect could not be confirmed")
            _api = None
            session_generation += 1


def start_stream(api, pair):
    # The library's convenience method blocks on historical backfill and stream
    # confirmation. Subscribe directly; actual observations arrive asynchronously.
    api.api.real_time_candles_maxdict_table[pair][CANDLE_SIZE] = 40
    subscription = f"{pair},{CANDLE_SIZE}"
    if subscription not in api.subscribe_candle:
        api.subscribe_candle.append(subscription)
    api.api.subscribe(broker_assets.ACTIVES[pair], CANDLE_SIZE)


def stop_stream(api, pair):
    subscription = f"{pair},{CANDLE_SIZE}"
    if subscription in api.subscribe_candle:
        api.subscribe_candle.remove(subscription)
    api.api.unsubscribe(broker_assets.ACTIVES[pair], CANDLE_SIZE)


def get_open_pairs(api, candidates):
    """Discover eligible assets for the one-minute binary product used by api.buy.

    Avoid get_all_open_time(): it also requests unrelated digital/CFD catalogs,
    some of which can hang indefinitely in this unofficial library.
    """
    global _catalog_updated, _open_assets
    with _catalog_lock:
        if time.monotonic() - _catalog_updated >= 30:
            api.api.api_option_init_all_result_v2 = None
            api.api.get_api_option_init_all_v2()
            deadline = time.monotonic() + 8
            while api.api.api_option_init_all_result_v2 is None:
                if time.monotonic() >= deadline:
                    raise RuntimeError("Asset discovery timed out")
                time.sleep(0.05)
            data = api.api.api_option_init_all_result_v2
            if not isinstance(data, dict) or not isinstance(data.get("turbo"), dict):
                raise RuntimeError("Broker returned an invalid asset catalog")
            rows = data["turbo"].get("actives")
            if not isinstance(rows, dict):
                raise RuntimeError("Broker returned an invalid asset list")
            discovered = {}
            available = set()
            for asset_id, asset in rows.items():
                name = str(asset.get("name", "")).split(".", 1)[-1]
                if not (name.endswith("-OTC") or name in REGULAR_PAIRS):
                    continue
                discovered[name] = int(asset_id)
                if asset.get("enabled") and not asset.get("is_suspended"):
                    available.add(name)
            broker_assets.ACTIVES.update(discovered)
            # Keep the mixed scan priority, then include additional broker-listed
            # OTC assets. Regular pairs outside the requested five are excluded.
            preferred = [pair for pair in PAIR_POOL if pair in discovered]
            preferred += [pair for pair in REGULAR_PAIRS
                          if pair in discovered and pair not in preferred]
            PAIR_POOL[:] = preferred + sorted(set(discovered) - set(preferred))
            _open_assets = available
            _catalog_updated = time.monotonic()
        return _open_assets.intersection(candidates)


def latest_candle(pair, api=None):
    if pair not in PAIR_POOL:
        raise ValueError(f"unknown pair {pair}")
    # Monitors receive the existing session: never reconnect from a pair thread.
    api = get_session() if api is None else api
    raw = api.get_realtime_candles(pair, CANDLE_SIZE)
    if not raw:
        return None
    raw = dict(raw)
    closed = [stamp for stamp in raw if stamp + CANDLE_SIZE <= time.time()]
    if not closed:
        return None
    latest_ts = max(closed)
    c = dict(raw[latest_ts])
    return {
        "pair": pair,
        "time": latest_ts,
        "open": c["open"],
        "high": c["max"],
        "low": c["min"],
        "close": c["close"],
    }


def forming_candle(api, pair, now=None):
    """Read the entry candle with its broker update time, never invent a quote."""
    now = time.time() if now is None else now
    slot = int(now // CANDLE_SIZE) * CANDLE_SIZE
    raw = api.get_realtime_candles(pair, CANDLE_SIZE)
    if not isinstance(raw, dict) or not isinstance(raw.get(slot), dict):
        return None
    row = dict(raw[slot])
    values = [row.get(key) for key in ("open", "close", "max", "min", "at")]
    if any(isinstance(v, bool) or not isinstance(v, (int, float))
           or not math.isfinite(v) for v in values):
        return None
    opened, price, high, low, observed = values
    # IQ streams have used epoch ns, us and ms; normalize by epoch magnitude.
    observed /= 1e9 if observed > 1e17 else 1e6 if observed > 1e14 else 1e3 if observed > 1e11 else 1
    if (row.get("from", slot) != slot or low > min(opened, price)
            or high < max(opened, price) or low > high
            or not slot <= observed <= now + 3 or abs(observed - now) > 3):
        return None
    return {"pair": pair, "time": slot, "open": opened, "close": price,
            "high": high, "low": low, "observed_at": observed}


def load_history(api, pair):
    """The upload's 80-candle backfill, with a bounded transport wait."""
    if pair not in PAIR_POOL:
        raise ValueError(f"unknown pair {pair}")
    end = int(time.time() // CANDLE_SIZE) * CANDLE_SIZE - 1
    with _history_lock:
        api.api.candles.candles_data = None
        api.api.getcandles(broker_assets.ACTIVES[pair], CANDLE_SIZE, 80, end)
        deadline = time.monotonic() + 8
        while api.api.candles.candles_data is None:
            if time.monotonic() >= deadline:
                raise RuntimeError("Broker candle history timed out")
            time.sleep(0.05)
        rows = api.api.candles.candles_data
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("Broker candle history is unavailable")
        history = {}
        for row in rows:
            values = [row.get(key) for key in ("from", "open", "max", "min", "close")]
            if any(isinstance(v, bool) or not isinstance(v, (int, float))
                   or not math.isfinite(v) for v in values):
                raise RuntimeError("Broker candle history is invalid")
            stamp, opened, high, low, closed = values
            if (stamp % CANDLE_SIZE or low > min(opened, closed)
                    or high < max(opened, closed)):
                raise RuntimeError("Broker candle history is invalid")
            if stamp + CANDLE_SIZE <= end + 1:
                history[stamp] = {
                    "pair": pair, "time": stamp, "open": opened,
                    "high": high, "low": low, "close": closed,
                }
        if len(history) < 15:
            raise RuntimeError("Broker candle history is insufficient")
        return [history[stamp] for stamp in sorted(history)][-80:]


def read_balance(api):
    """Bound the library's otherwise unbounded balance-response wait."""
    api.api.balances_raw = None
    api.api.get_balances()
    deadline = time.monotonic() + 8
    while api.api.balances_raw is None:
        if time.monotonic() >= deadline:
            raise RuntimeError("Broker balance request timed out")
        time.sleep(0.05)
    for row in api.api.balances_raw.get("msg", []):
        if row.get("id") == api.get_balance_id():
            return row.get("amount")
    raise RuntimeError("Selected broker balance was not returned")


def keepalive_loop():
    """Background thread: touches the session periodically so the
    connection itself stays warm even if nothing else calls get_session()."""
    while True:
        try:
            get_session()
        except Exception as e:
            print(f"keepalive error: {e}")
        time.sleep(20)
