"""Best-effort signal notifications; HTTP never runs on trading threads."""
import json
import logging
import os
import queue
import threading
import time
from collections import OrderedDict
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from entry_confirmation import ENTRY_WINDOW_SECONDS, number
from telegram_transport import send_html

logger = logging.getLogger(__name__)

CURRENCY_FLAGS = {
    "AUD": "🇦🇺", "BRL": "🇧🇷", "CAD": "🇨🇦", "CHF": "🇨🇭",
    "EUR": "🇪🇺", "GBP": "🇬🇧", "HKD": "🇭🇰", "INR": "🇮🇳",
    "JPY": "🇯🇵", "MXN": "🇲🇽", "NOK": "🇳🇴", "NZD": "🇳🇿",
    "RUB": "🇷🇺", "SEK": "🇸🇪", "SGD": "🇸🇬", "TRY": "🇹🇷",
    "USD": "🇺🇸", "ZAR": "🇿🇦",
}


def format_signal_time(stamp, timezone_name):
    local = datetime.fromtimestamp(stamp, ZoneInfo(timezone_name))
    return local.strftime("%I:%M %p").lstrip("0")


def pair_flags(pair):
    symbol = str(pair).upper().split("-", 1)[0].replace("/", "")
    if len(symbol) != 6:
        return "🌐/🌐"
    return f"{CURRENCY_FLAGS.get(symbol[:3], '🌐')}/{CURRENCY_FLAGS.get(symbol[3:], '🌐')}"


def signal_window_open(slot, confirmed, now):
    """Send only in the original candle-open window, not confirmation + 15s."""
    return (all(number(value) for value in (slot, confirmed, now))
            and slot <= confirmed <= now <= slot + ENTRY_WINDOW_SECONDS)


class TelegramDeliveryError(Exception):
    """Only sanitized messages may leave the HTTP transport."""


def send_message(token, chat_id, text):
    request = Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps({"chat_id": chat_id, "text": text}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            result = json.load(response)
        if result.get("ok") is not True:
            raise TelegramDeliveryError("Telegram rejected the notification")
    except HTTPError as error:
        raise TelegramDeliveryError(f"Telegram HTTP {error.code}") from None
    except TelegramDeliveryError:
        raise
    except Exception:
        # urllib errors can contain the bot token in the request URL.
        raise TelegramDeliveryError("Telegram transport unavailable") from None


class TelegramAlerts:
    def __init__(self, environ=None, sender=send_message, clock=time.time,
                 capacity=64, start_worker=True):
        config = os.environ if environ is None else environ
        self.token = config.get("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = config.get("TELEGRAM_CHAT_ID", "")
        self.channel_id = config.get("TELEGRAM_CHANNEL_ID", "")
        self.timezone = config.get("TELEGRAM_TIMEZONE", "Africa/Lagos")
        enabled = config.get("TELEGRAM_ALERTS_ENABLED", "").lower() in ("true", "1")
        self.ready = bool(enabled and self.token and self.chat_id
                          and config.get("NODE_ENV") == "production")
        self.sender, self.clock = sender, clock
        self.pending = queue.Queue(maxsize=capacity)
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.seen = OrderedDict()
        self.sent = self.failed = self.dropped = 0
        self.worker = None
        if enabled and not self.ready:
            logger.warning("Telegram alerts enabled but configuration is incomplete")
        if self.ready and start_worker:
            self.worker = threading.Thread(target=self._run, name="telegram-alerts", daemon=True)
            self.worker.start()

    def enqueue(self, pair, entry, mode, amount=None):
        """Copy only notification fields, deduplicate and return without I/O."""
        if not self.ready or mode not in ("PRACTICE", "REAL"):
            return False
        slot, confirmed = entry.get("execution_candle"), entry.get("confirmed_at")
        engine, kind, side = entry.get("engine"), entry.get("kind"), entry.get("side")
        fcb_signal = engine == "FCB" and kind in ("FCB", "pole")
        reversal_signal = (
            engine == "RSI" and kind == "RSI reversal"
            and entry.get("trigger") == "rsi_reversal"
            and type(entry.get("attempt", 1)) is int and entry.get("attempt", 1) == 1
            and number(entry.get("signal_time"))
            and number(slot) and slot == entry["signal_time"] + 60
        )
        if (not (fcb_signal or reversal_signal) or side not in ("CALL", "PUT")
                or not number(slot) or slot % 60 != 0
                or not signal_window_open(slot, confirmed, self.clock())):
            return False
        key = (mode, pair, engine, kind, int(slot), side)
        observation = entry.get("confirmation_quote")
        quote = observation.get("close") if isinstance(observation, dict) else None
        extreme_rsi = entry.get("extreme_rsi")
        event = {"pair": str(pair), "engine": engine, "kind": kind, "side": side,
                 "mode": mode, "slot": int(slot), "confirmed": confirmed,
                 "timezone": self.timezone,
                 "quote": quote if number(quote) and quote > 0 else None,
                 "stake": amount if number(amount) and amount > 0 else None}
        if reversal_signal:
            event["extreme_rsi"] = extreme_rsi if number(extreme_rsi) else None
        with self.lock:
            if key in self.seen or self.stop.is_set():
                return False
            try:
                self.pending.put_nowait(event)
            except queue.Full:
                self.dropped += 1
                logger.warning("Telegram signal queue full; notification dropped")
                return False
            self.seen[key] = None
            if len(self.seen) > 512:
                self.seen.popitem(last=False)
        return True

    @staticmethod
    def text(event):
        entry_time = format_signal_time(event["slot"], event.get("timezone", "Africa/Lagos"))
        pair_type = " [OTC]" if event["pair"].endswith("-OTC") else " [REGULAR]"
        if event.get("kind") == "RSI reversal":
            direction = "BUY 🟢" if event["side"] == "CALL" else "SELL 🔴"
            rsi = event.get("extreme_rsi")
            rsi_line = f"📊 RSI extreme: {rsi:.2f}\n" if number(rsi) else ""
            return (
                "🛎️ NEW RSI REVERSAL SIGNAL!\n"
                f"{pair_flags(event['pair'])}  {event['pair']}{pair_type}\n"
                "⏳Timer: 1 minute\n"
                f"➡️ Entry: {entry_time}\n"
                f"📉 Direction: {direction}\n"
                f"{rsi_line}"
                "↪️ One trade per signal · base stake; no Martingale\n"
                "⚠️ Signal only · not an executed-trade confirmation"
            )
        martingale_time = format_signal_time(event["slot"] + 60, event.get("timezone", "Africa/Lagos"))
        direction = "BUY 🟢" if event["side"] == "CALL" else "SELL 🔴"
        return (
            "🛎️ NEW SIGNAL!\n"
            f"{pair_flags(event['pair'])}  {event['pair']}{pair_type}\n"
            "⏳Timer: 1 minutes\n"
            f"➡️ Entry: {entry_time}\n"
            f"📉 Direction: {direction}\n\n"
            f"↪️Martingale: 1 🔂: {martingale_time}"
        )

    def reply(self, text, markup=None):
        if not self.ready or self.stop.is_set():
            return False
        try:
            self.pending.put_nowait({"reply": str(text), "markup": markup, "created": self.clock()})
            return True
        except queue.Full:
            self.dropped += 1
            logger.warning("Telegram response queue full; response dropped")
            return False

    def _deliver(self, event):
        now = self.clock()
        if "reply" in event:
            if now - event["created"] > 60:
                self.dropped += 1
                return
            try:
                send_html(self.token, self.chat_id, event["reply"], event["markup"])
                self.sent += 1
            except Exception:
                self.failed += 1
                logger.warning("Telegram command response delivery failed")
            return
        if not signal_window_open(event["slot"], event["confirmed"], now):
            self.dropped += 1
            logger.warning("Telegram notification expired before delivery; dropped")
            return
        # Prioritize the broadcast channel, then notify the private control chat.
        # A failure or timeout for either destination must not suppress the other.
        targets = []
        if self.channel_id:
            targets.append(("channel", self.channel_id))
        if self.chat_id and self.chat_id != self.channel_id:
            targets.append(("private chat", self.chat_id))
        delivered = False
        message = self.text(event)
        for target_name, chat_id in targets:
            try:
                self.sender(self.token, chat_id, message)
                delivered = True
            except Exception:
                self.failed += 1
                # Do not retry uncertain deliveries or expose provider/credential text.
                logger.warning(
                    "Telegram %s signal delivery failed; trading is unaffected",
                    target_name,
                )
        if delivered:
            self.sent += 1

    def _run(self):
        while not self.stop.is_set():
            try:
                event = self.pending.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                self._deliver(event)
            finally:
                self.pending.task_done()
            # Respect Telegram's per-chat message rate; only the sender waits.
            self.stop.wait(1)

    def close(self):
        self.stop.set()
        if self.worker is not None:
            self.worker.join(timeout=0.1)
