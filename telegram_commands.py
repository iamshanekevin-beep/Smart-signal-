"""Authorized command responses using the existing guarded trading controls."""
import html
import math
import secrets
import time
from datetime import datetime, timezone
from entry_confirmation import ENTRY_WINDOW_SECONDS

MENU = {
    "keyboard": [
        ["📊 Status", "📋 Trades", "📈 Report"],
        ["⚙️ Config", "❓ Help"],
        ["⏸ Pause", "▶️ Resume"],
        ["Demo", "Live"],
        ["🔌 Connect", "🔌 Disconnect"],
        ["🤖 Auto", "💰 Amount"],
        ["$1", "$5", "$10", "$25"],
        ["🔢 Maxtrades", "🛑 Losslimit"],
    ],
    "resize_keyboard": True, "is_persistent": True,
}
COMMANDS = [
    {"command": name, "description": description} for name, description in (
        ("start", "Show the control menu (does not start trading)"),
        ("status", "Connection, account and trading status"),
        ("trades", "Last five confirmed trades"),
        ("report", "Confirmed performance report"),
        ("config", "Current stake and risk settings"),
        ("help", "Commands and safety information"),
        ("pause", "Pause new entries; continue settling open orders"),
        ("resume", "Resume trading with existing safeguards"),
        ("demo", "Select Demo account while paused; does not start trading"),
        ("live", "Select Live account with confirmation; does not start trading"),
        ("connect", "Connect the broker without starting trades"),
        ("disconnect", "Pause, then disconnect once all orders are settled"),
        ("auto", "Automatic trading controls: on or off"),
        ("amount", "Set stake, e.g. /amount 5"),
        ("maxtrades", "Set daily trade limit, e.g. /maxtrades 20"),
        ("losslimit", "Set maximum daily loss, e.g. /losslimit 50"),
        ("cancel", "Cancel a pending prompt or Live confirmation"),
    )
]


def escape(value):
    return html.escape(str(value))


def money(value):
    return "unavailable" if value is None else f"${float(value):,.2f}"


class TelegramCommands:
    def __init__(self, call, clock=time.time):
        self.call, self.clock = call, clock
        self.prompt = None
        self.confirmation = None

    def api(self, route, method="GET", data=None):
        code, result = self.call(route, method, data)
        if code >= 400:
            raise ValueError(result.get("error", "Bot service unavailable"))
        return result

    def handle(self, text):
        """Called only after private-chat/sender/staleness checks by the poller."""
        try:
            return self._handle(text.strip())
        except ValueError as error:
            return f"<b>Request not applied</b>\n{escape(error)}"
        except Exception:
            # A timed-out control may already have applied: never auto-retry it.
            self.confirmation = self.prompt = None
            return ("<b>Outcome could not be verified</b>\n"
                    "Check /status and /config before retrying. Trading safeguards remain active.")

    def _handle(self, text):
        aliases = {label: label.split()[-1].lower() for row in MENU["keyboard"]
                   for label in row if not label.startswith("$")}
        if text.startswith("$"):
            command, argument = "amount", text[1:]
        elif text in aliases:
            command, argument = aliases[text], ""
        elif text.startswith("/"):
            first, _, argument = text.partition(" ")
            command = first[1:].split("@")[0].lower()
            argument = argument.strip()
        elif self.prompt and self.clock() < self.prompt[1]:
            command, _, snapshot = self.prompt
            return self.change_setting(command, text, snapshot)
        else:
            return "Use the menu or /help. /start shows the menu; it never starts trading."
        if command not in ("resume", "live"):
            self.confirmation = None
        if command not in ("amount", "maxtrades", "losslimit"):
            self.prompt = None
        if command in ("start", "help"):
            return (
                "<b>OTC.desk · Telegram control</b>\n"
                "Your private menu for confirmed FCB signals and bot controls.\n\n"
                "<b>Monitor</b> · /status /trades /report /config\n"
                "<b>Trading</b> · /pause /resume /auto on|off\n"
                "<b>Account</b> · /demo /live (selection only; stays paused)\n"
                "<b>Connection</b> · /connect /disconnect\n"
                "<b>Settings</b> · /amount 5 · /maxtrades 20 · /losslimit 50\n"
                "/cancel clears a prompt or confirmation.\n\n"
                "Pause and let orders settle before changing settings or disconnecting. "
                "Losslimit is a positive dollar amount; it is stored as a negative daily P/L floor.\n"
                "<b>Live Resume requires a short-lived confirmation.</b> "
                "Live account selection also requires confirmation. Reconnect never starts trading.\n\n"
                "Signal cards are not executed-trade confirmations. "
                f"Telegram delivery does not extend the 0–{ENTRY_WINDOW_SECONDS:g}-second entry window."
            )
        if command == "cancel":
            self.prompt = self.confirmation = None
            return "Pending prompt and Live confirmation cancelled. Trading state is unchanged."
        if command == "status":
            row = self.api("status")
            return (
                "<b>📊 BOT STATUS</b>\n"
                f"Account · <b>{'Live' if row.get('accountMode') == 'REAL' else 'Demo'}</b>\n"
                f"Broker · {'Connected' if row.get('connected') else 'Disconnected / connecting'}\n"
                f"Trading · {'Running' if row.get('running') else 'Paused'}\n"
                f"Balance · {escape(money(row.get('balance')))}\n"
                f"Today P/L · {escape(money(row.get('dailyPL')))}\n"
                f"Trades today · {row.get('tradesToday', 0)} / {row.get('maxDailyTrades', 0)}\n"
                f"Open orders · {row.get('openOrders', 0)}\n"
                f"Recovery · {escape(row.get('recoveryState', 'loading'))}\n"
                f"Cooldown · {row.get('cooldownRemainingSeconds', 0)} seconds\n"
                f"Note · {escape(row.get('lastError') or row.get('recoveryMessage') or 'Ready')}"
            )
        if command == "config":
            row = self.api("settings")
            return (
                "<b>⚙️ CONFIGURATION</b>\n"
                f"Account · {escape(row['accountMode'])}\n"
                f"Stake · {money(row['stake'])}\n"
                f"Daily loss floor · {money(row['dailyLossLimit'])}\n"
                f"Daily trade limit · {row['maxDailyTrades']}\n"
                f"Loss streak stop · {row['lossStreakStop']}\n"
                f"Active pairs · {row['activePairs']}\n"
                "Expiry · 1 minute\nStrategy · FCB breakout + continuation; RSI disabled"
            )
        if command == "trades":
            row = self.api("trades?limit=5")
            trades = row.get("items", [])
            if not trades:
                return "<b>📋 RECENT TRADES</b>\nNo confirmed trades for the selected account."
            lines = ["<b>📋 LAST FIVE CONFIRMED TRADES</b>"]
            for trade in trades:
                stamp = datetime.fromtimestamp(trade["time"], timezone.utc).strftime("%m-%d %H:%M UTC")
                lines.append(
                    f"{escape(trade['pair'])} · {escape(trade['side'])} · "
                    f"<b>{escape(trade.get('result', 'unknown')).upper()}</b>\n"
                    f"{stamp} · P/L {escape(money(trade.get('profit')))}")
            return "\n\n".join(lines)
        if command == "report":
            row = self.api("performance")
            decided = row["wins"] + row["losses"]
            rate = f"{100 * row['wins'] / decided:.1f}%" if decided else "n/a"
            return (
                "<b>📈 CONFIRMED PERFORMANCE</b>\n"
                f"Account · {escape(row['accountMode'])}\nSettled trades · {row['total']}\n"
                f"Wins · {row['wins']}   Losses · {row['losses']}   Equal · {row['breakEven']}\n"
                f"Win rate (wins + losses) · {rate}\n"
                f"Cumulative P/L · {money(row['cumulativePL'])}\n"
                "Confirmed broker outcomes only; not a forecast."
            )
        if command == "pause":
            self.api("stop", "POST")
            return "<b>⏸ Entries paused</b>\nOpen orders continue to settle. No positions were closed."
        if command == "auto":
            if argument.lower() in ("off", "pause"):
                return self._handle("/pause")
            if argument.lower() == "on":
                return self._handle("/resume")
            row = self.api("status")
            return (f"<b>🤖 Automatic trading · {'ON' if row.get('running') else 'OFF'}</b>\n"
                    "Use /auto on to resume or /auto off to pause. "
                    "Auto does not change the strategy or bypass risk controls.")
        if command in ("demo", "live"):
            snapshot = self.api("telegram-state")
            if command == "live":
                pending = self.confirmation
                self.confirmation = None
                if not (pending and pending.get("action") == "live"
                        and self.clock() <= pending["expires"]
                        and argument == f"USE REAL MONEY {pending['nonce']}"
                        and snapshot == pending["snapshot"]):
                    nonce = secrets.token_hex(3)
                    self.confirmation = {"action": "live", "nonce": nonce, "snapshot": snapshot,
                                         "expires": self.clock() + 60}
                    return (
                        "<b>Live account uses real money</b>\n"
                        "This selects Live only; it does not start trading.\n"
                        "Pause and wait for all orders to settle first.\n"
                        "To confirm within 60 seconds, send:\n"
                        f"<code>/live USE REAL MONEY {nonce}</code>\n"
                        "Use /cancel to cancel."
                    )
            elif argument:
                raise ValueError("Use /demo without arguments to select Demo.")
            target = "REAL" if command == "live" else "PRACTICE"
            row = self.api("account", "POST", {
                "accountMode": target, "telegramSnapshot": snapshot,
                **({"confirmation": "USE REAL MONEY"} if target == "REAL" else {}),
            })
            label = "Live" if target == "REAL" else "Demo"
            if row.get("accountSwitchPending"):
                return (f"<b>{label} account switch requested</b>\n"
                        "The broker must confirm the switch. Trading remains paused.\n"
                        "Check /status before using /resume.")
            return f"<b>{label} account selected</b>\nTrading remains paused. Use /status to review."
        if command == "resume":
            snapshot = self.api("telegram-state")
            mode = snapshot["settings"]["accountMode"]
            if mode != "REAL" and argument:
                self.confirmation = None
                return "<b>Not resumed</b>\nThe account changed. Review /status and send /resume again."
            if mode == "REAL":
                pending = self.confirmation
                self.confirmation = None
                if not (pending and pending.get("action") == "resume"
                        and self.clock() <= pending["expires"]
                        and argument == f"USE REAL MONEY {pending['nonce']}"
                        and snapshot == pending["snapshot"]):
                    nonce = secrets.token_hex(3)
                    self.confirmation = {"action": "resume", "nonce": nonce, "snapshot": snapshot,
                                         "expires": self.clock() + 60}
                    return (
                        "<b>⚠️ Live trading uses real money</b>\n"
                        f"Stake · {money(snapshot['settings']['stake'])}\n"
                        f"Daily loss floor · {money(snapshot['settings']['dailyLossLimit'])}\n"
                        f"Daily trade limit · {snapshot['settings']['maxDailyTrades']}\n\n"
                        "To resume within 60 seconds, send:\n"
                        f"<code>/resume USE REAL MONEY {nonce}</code>\n"
                        "Any pause, account or settings change invalidates this confirmation."
                    )
            self.api("start", "POST", {"telegramExpectedSettings": snapshot["settings"],
                                      "telegramExpectedRevision": snapshot["revision"]})
            return f"<b>▶️ {'Live' if mode == 'REAL' else 'Demo'} trading resumed</b>\nExisting entry and risk checks remain active."
        if command == "connect":
            self.api("connection", "POST", {"connected": True})
            return ("<b>🔌 Connection requested</b>\nIf reconnecting, the bot initializes in Demo "
                    "and stays paused. Check /status before /resume.")
        if command == "disconnect":
            self.api("stop", "POST")
            self.api("connection", "POST", {"connected": False})
            return "<b>🔌 Disconnect requested; entries paused</b>\nCheck /status for completion. No positions were closed."
        if command in ("amount", "maxtrades", "losslimit"):
            snapshot = self.api("telegram-state")
            if argument:
                return self.change_setting(command, argument, snapshot)
            self.prompt = (command, self.clock() + 60, snapshot)
            examples = {"amount": "stake in dollars, e.g. 5 (maximum 100)",
                        "maxtrades": "daily trade limit, e.g. 20 (1–500)",
                        "losslimit": "maximum daily loss in dollars, e.g. 50 (above 0, up to 10000)"}
            return f"<b>{command.title()}</b>\nSend the {examples[command]} within 60 seconds, or /cancel."
        return "Unknown command. Use /help or the menu."

    def change_setting(self, command, value, snapshot):
        try:
            number = float(value)
        except (ValueError, TypeError):
            raise ValueError("Enter a valid number.")
        if not math.isfinite(number) or number <= 0:
            raise ValueError("Enter a finite positive number.")
        if command == "maxtrades" and not number.is_integer():
            raise ValueError("Maxtrades must be a whole number.")
        key = {"amount": "stake", "maxtrades": "maxDailyTrades", "losslimit": "dailyLossLimit"}[command]
        normalized = -number if command == "losslimit" else number
        row = self.api("telegram-settings", "POST", {
            "snapshot": snapshot, "changes": {key: normalized}})
        self.prompt = None
        return f"<b>Setting saved</b>\n{escape(key)} · {escape(row[key])}\nRisk usage and trade history were not reset."
