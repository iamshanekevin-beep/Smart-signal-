"""Private-chat command polling, separate from monitors and order execution."""
import json
import logging
import os
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from telegram_commands import COMMANDS, MENU, TelegramCommands
from telegram_transport import TelegramDeliveryError, telegram_request

logger = logging.getLogger(__name__)


def bot_call(route, method="GET", data=None):
    # This port is allocated by the Node supervisor; it is not browser routing.
    port = int(os.environ["BOT_INTERNAL_PORT"])
    request = Request(f"http://127.0.0.1:{port}/dashboard/{route}", method=method,
                      data=None if data is None else json.dumps(data).encode(),
                      headers={"X-API-Secret": os.environ["API_SECRET"],
                               "Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=15) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


class TelegramBot:
    def __init__(self, alerts, call=bot_call, transport=telegram_request, clock=time.time):
        self.alerts, self.transport, self.clock = alerts, transport, clock
        self.commands = TelegramCommands(call, clock)
        self.stop = threading.Event()
        self.offset = None
        self.thread = None

    def process(self, update):
        # Advance before executing; uncertain mutations must not be retried.
        if self.offset is not None and update["update_id"] < self.offset:
            return
        self.offset = max(self.offset or 0, update["update_id"] + 1)
        message = update.get("message", {})
        chat, sender = message.get("chat", {}), message.get("from", {})
        if (chat.get("type") != "private" or sender.get("is_bot")
                or str(chat.get("id")) != self.alerts.chat_id
                or str(sender.get("id")) != self.alerts.chat_id
                or message.get("forward_origin") or message.get("forward_from")
                or message.get("via_bot")
                or not isinstance(message.get("text"), str)
                or not 0 <= self.clock() - message.get("date", 0) <= 60):
            return
        self.alerts.reply(self.commands.handle(message["text"]), MENU)

    def start(self):
        if self.alerts.ready and self.thread is None:
            self.thread = threading.Thread(target=self.run, name="telegram-commands", daemon=True)
            self.thread.start()

    def run(self):
        bootstrapped = False
        while not self.stop.is_set():
            try:
                if not bootstrapped:
                    # Never replay trading controls left pending during downtime.
                    updates = self.transport(self.alerts.token, "getUpdates",
                                             {"offset": -1, "timeout": 0})
                    self.offset = max((u["update_id"] + 1 for u in updates), default=0)
                    self.transport(self.alerts.token, "setMyCommands", {
                        "commands": COMMANDS,
                        "scope": {"type": "chat", "chat_id": self.alerts.chat_id}})
                    self.alerts.reply(self.commands.handle("/start"), MENU)
                    bootstrapped = True
                updates = self.transport(self.alerts.token, "getUpdates", {
                    "offset": self.offset, "timeout": 20,
                    "allowed_updates": ["message"]}, timeout=25)
                for update in updates:
                    if self.stop.is_set():
                        break
                    self.process(update)
            except TelegramDeliveryError:
                logger.warning("Telegram command polling unavailable; trading is unaffected")
                self.stop.wait(5)
            except Exception:
                logger.warning("Telegram command handling failed; trading is unaffected")
                self.stop.wait(5)

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=0.1)
