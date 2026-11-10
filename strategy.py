"""FCB flow ends on every confirmed loss; RSI recovery is disabled by default."""
import time
import threading
from fast_trade import FCBEngine, RSIEngine, calc_rsi
from fractal_bands import FractalBands
from entry_confirmation import fcb_price_confirmed, pole_quote_ready, number, breakout_direction_confirmed
from entry_confirmation import rsi_price_confirmed, reversal_price_confirmed, momentum_price_confirmed
from entry_confirmation import ENTRY_WINDOW_SECONDS, LATE_ORDER_REASON, CLOSED_BREAKOUT_POLICY, ALIGNED_BREAKOUT_POLICY
from entry_confirmation import CLEAN_BREAKOUT_POLICY, CLEAN_POLE_POLICY, STANDARD_BREAKOUT_POLICY
from market_evidence import capture
from practice_validation import zone_metrics, candle_only_zone_metrics, breakout_metrics, FIELDS
from rsi_reversal import standalone_momentum_signal, standalone_reversal_signal, reversal_signal


class PairEngine:
    def __init__(self, *, rsi_enabled=False, reversal_enabled=False, practice_zone=False,
                 validation_audit=None, strict_rules=False):
        self.lock = threading.RLock()
        # The managed supervisor always enables strict rules in BOTH accounts.
        # The legacy mode remains solely for old source-parity/offline harnesses.
        self.strict_rules = strict_rules
        self.rsi_enabled = rsi_enabled and not strict_rules
        self.reversal_enabled = bool(reversal_enabled)
        self.momentum_enabled = False
        self.fcb = FCBEngine()
        self.bands = FractalBands()
        self.rsi = RSIEngine()
        self.candles = []
        self.last_time = None
        self.pending = {"FCB": None, "RSI": None}
        self.fcb_cycle = 0
        self.entries = []
        self.fcb_candidate = None
        self.pole_win_slot = None
        self.rsi_loss = None
        self.rsi_candidate = None
        self.rsi_recovery = None
        self.reversal_candidate = None
        self.reversal_signal = None
        self.loss_reversal_candidate = None
        self.pending_loss_reversal = None
        self.momentum_candidate = None
        self.momentum_signal = None
        self.momentum_status = "Waiting for 15 actual one-minute candles"
        self.reversal_status = "Waiting for 15 actual one-minute candles"
        self.fcb_resume_after = 0
        self.rsi_reason = self._rsi_idle_reason()
        self.reason = "FAST TRADE: warming up"
        self.practice_zone = practice_zone or strict_rules
        self.validation_audit = validation_audit
        self.band_history = []
        self.zone_data = {field: None for field in FIELDS}
        self.zone_data.update(zone="UNAVAILABLE", zone_trigger="")
        self.zone_recovery_cooldown = False

    def _rsi_idle_reason(self):
        return ("RSI awaiting a confirmed breakout or continuation loss"
                if self.rsi_enabled else "RSI disabled")

    @property
    def trend(self):
        return self.fcb.trend.upper() if self.fcb.trend else None

    @staticmethod
    def _source_candle(candle):
        return {
            "time": candle["time"], "open": candle["open"], "close": candle["close"],
            "max": candle["high"], "min": candle["low"],
        }

    def seed(self, candles):
        """Use actual broker history, without executing historical signals."""
        self.candles = [self._source_candle(c) for c in candles[-100:]]
        self.bands = FractalBands()
        self.band_history = []
        self.zone_data = {field: None for field in FIELDS}
        self.zone_data.update(zone="UNAVAILABLE", zone_trigger="")
        self.zone_recovery_cooldown = False
        for row in self.candles:
            self.band_history.append(self.bands.update(row))
        self.last_time = self.candles[-1]["time"] if self.candles else None
        self.entries.clear()
        self.fcb_candidate = None
        self.pole_win_slot = None
        self.rsi_loss = None
        self.rsi_candidate = None
        self.rsi_recovery = None
        self.reversal_candidate = None
        self.reversal_signal = None
        self.loss_reversal_candidate = None
        self.pending_loss_reversal = None
        self.momentum_candidate = None
        self.momentum_signal = None
        self.momentum_status = "Waiting for a new closed one-minute candle"
        self.reversal_status = "Waiting for a new closed one-minute candle"
        self.rsi_reason = self._rsi_idle_reason()
        self.reason = "FAST TRADE: broker history loaded; waiting for a new closed candle"

    def _entry(self, side, engine, trigger, now, kind):
        entry = {
            "side": side.upper(), "engine": engine, "kind": kind, "trigger": trigger,
            **({"fcb_cycle": self.fcb_cycle} if engine == "FCB" else {}),
            "signal_time": self.last_time,
            "execution_candle": int(now // 60) * 60,
            "reason": {
                "FCB": "FCB breakout; FAST TRADE",
                "pole": "pole position; FAST TRADE FCB continuation",
                "RSI recovery": "opposite RSI recovery after confirmed breakout/continuation loss",
                "RSI reversal": "standalone RSI(14) overbought/oversold reversal on a closed one-minute candle",
                "LOSS reversal": "opposite wick reversal after a confirmed breakout/continuation loss",
                "RSI momentum": "same-direction RSI(14) overbought/oversold momentum signal",
            }[kind],
        }
        if self.practice_zone:
            entry["practice_validation"] = dict(self.zone_data)
            if self.candles:
                entry["signal_candle"] = dict(self.candles[-1])
        return entry

    def audit_validation(self, entry, reason):
        if self.validation_audit:
            self.validation_audit(entry, reason)

    def validation_allowed(self, entry):
        if not self.practice_zone:
            return True
        if self.last_time != entry.get("signal_time"):
            return False
        if entry.get("kind") == "RSI reversal":
            return (entry.get("engine") == "RSI"
                    and (entry.get("practice_validation") or {}).get("zone") == "CLEAR")
        if entry.get("kind") == "LOSS reversal":
            return (entry.get("engine") == "RSI" and entry.get("trigger") == "loss_recovery"
                    and (entry.get("practice_validation") or {}).get("zone") == "CLEAR")
        if (self.zone_data["zone"] != "CLEAR"
                or entry.get("kind") in ("FCB", "pole")
                and (not number((entry.get("practice_validation") or {}).get("range_vs_avg10"))
                     or (entry.get("practice_validation") or {}).get("range_vs_avg10") > 1.5)):
            return False
        if entry.get("kind") == "FCB":
            if (not self.candles or self.last_time != entry.get("signal_time")
                    or not entry.get("practice_validation")):
                return False
            _, reason = breakout_metrics(self.candles[:-1], self.candles[-1],
                entry.get("band_high"), entry.get("band_low"), entry.get("side"), self.zone_data)
            return reason is None
        return ((entry.get("engine"), entry.get("kind"))
                in (("FCB", "pole"), ("RSI", "RSI recovery"), ("RSI", "RSI reversal")))

    def annotate_validation(self, entry):
        if (not self.practice_zone
                or entry.get("kind") == "RSI reversal"
                and (entry.get("practice_validation") or {}).get("zone") == "CLEAR"
                or entry.get("kind") == "LOSS reversal"
                and (entry.get("practice_validation") or {}).get("zone") == "CLEAR"
                or (entry.get("practice_validation") or {}).get("range_vs_avg10") is not None):
            return entry
        metrics = dict(entry.get("practice_validation") or self.zone_data)
        if self.candles and self.last_time == entry.get("signal_time"):
            metrics, _ = breakout_metrics(self.candles[:-1], self.candles[-1],
                entry.get("band_high", self.bands.upper), entry.get("band_low", self.bands.lower),
                entry.get("side"), self.zone_data)
        return {**entry, "practice_validation": metrics,
                "signal_candle": entry.get("signal_candle") or (
                    dict(self.candles[-1]) if self.candles
                    and self.last_time == entry.get("signal_time") else {})}

    def step(self, candle, now=None, allow_entries=True):
        """Process closed candles; never revive signals from an ended flow."""
        now = time.time() if now is None else now
        self._expire_recovery(now)
        stamp = candle["time"]
        if stamp + 60 > now:
            return None, None
        if self.last_time is not None and stamp <= self.last_time:
            return None, None
        self.candles.append(self._source_candle(candle))
        self.candles = self.candles[-100:]
        self.last_time = stamp
        if (self.reversal_candidate
                and self.reversal_candidate.get("execution_candle", 0) <= stamp):
            self.reversal_candidate = None
            self.reversal_status = "Previous reversal signal expired; waiting for a new setup"
        if (self.loss_reversal_candidate
                and self.loss_reversal_candidate.get("execution_candle", 0) <= stamp):
            self.loss_reversal_candidate = None
            self.reversal_status = "Loss reversal candle passed; waiting for a new setup"
        if (self.momentum_candidate
                and self.momentum_candidate.get("execution_candle", 0) <= stamp):
            self.momentum_candidate = None
            self.momentum_status = "Previous momentum signal expired; waiting for a new setup"
        latest_reversal = standalone_reversal_signal(self.candles) if self.reversal_enabled else None
        latest_momentum = standalone_momentum_signal(self.candles) if self.momentum_enabled else None
        if latest_reversal:
            self.reversal_signal = latest_reversal
            self.reversal_status = (
                f"{'Overbought' if latest_reversal['side'] == 'PUT' else 'Oversold'} reversal "
                f"detected; RSI {latest_reversal['extreme_rsi']:.2f}"
            )
        elif self.reversal_enabled and len(self.candles) >= 15:
            self.reversal_status = "Waiting for a confirmed 1-minute overbought/oversold reversal"
        if latest_momentum:
            self.momentum_signal = latest_momentum
            self.momentum_status = (
                f"{'Overbought' if latest_momentum['side'] == 'CALL' else 'Oversold'} momentum "
                f"detected; RSI {latest_momentum['rsi']:.2f}"
            )
            if latest_reversal:
                self.momentum_status += "; standalone reversal has priority"
        elif self.momentum_enabled and len(self.candles) >= 15:
            self.momentum_status = "Waiting for a closed-candle RSI(14) extreme"
        upper, lower = self.bands.update(candle)
        self.band_history.append((upper, lower))
        self.band_history = self.band_history[-100:]
        # Candle A qualifies from its close only; a wick crossing is not a breakout.
        outside_side = ("CALL" if number(upper) and candle["close"] > upper else
                        "PUT" if number(lower) and candle["close"] < lower else None)
        reversal_zone_data = None
        if self.practice_zone:
            self.zone_data = zone_metrics(self.candles[:-1], self.band_history[:-1])
            if self.reversal_enabled and self.zone_data["zone"] == "UNAVAILABLE":
                reversal_zone_data = candle_only_zone_metrics(self.candles[:-1])
        self._arm_pending_loss_reversal(now)
        if self.practice_zone:
            if self.zone_data["zone"] != "CLEAR":
                if latest_momentum:
                    self.momentum_status = (
                        f"RSI momentum detected; Practice Zone {self.zone_data['zone']} blocks entries"
                    )
                if latest_reversal:
                    fallback_clear = (
                        self.zone_data["zone"] == "UNAVAILABLE"
                        and reversal_zone_data is not None
                        and reversal_zone_data["zone"] == "CLEAR"
                    )
                    if fallback_clear:
                        self.reversal_status = (
                            "Reversal detected; FCB bands unavailable; "
                            "candle-only Practice Zone is clear"
                        )
                        can_arm_reversal = (
                            allow_entries and self.reversal_candidate is None
                            and self.loss_reversal_candidate is None
                            and self.pending["RSI"] is None and self.pending["FCB"] is None
                            and self.rsi_candidate is None and self.rsi_recovery is None
                            and self.fcb_candidate is None and outside_side is None
                            and self.pole_win_slot != stamp + 60
                            and stamp + 60 > self.fcb_resume_after
                        )
                        if can_arm_reversal:
                            self.reversal_candidate = {
                                **self._entry(
                                    latest_reversal["side"], "RSI", "rsi_reversal",
                                    now, "RSI reversal"),
                                **latest_reversal,
                                "attempt": 1,
                                "execution_candle": stamp + 60,
                                "practice_validation": dict(reversal_zone_data),
                            }
                            self._end_flow(stamp + 60)
                            self.reversal_status += "; waiting for next-candle price confirmation"
                            self.reason = self.reversal_status
                            return None, None
                        self.reversal_status += "; another entry guard prevented this setup"
                    else:
                        if self.zone_data["zone"] == "UNAVAILABLE":
                            detail = (
                                f"candle-only Practice Zone {reversal_zone_data['zone']}"
                                if reversal_zone_data is not None
                                else "valid candle-only Practice Zone data is unavailable"
                            )
                            self.reversal_status = (
                                f"Reversal detected; FCB bands unavailable and {detail}; "
                                "entry blocked"
                            )
                        else:
                            self.reversal_status = (
                                f"Reversal detected; Practice Zone {self.zone_data['zone']} "
                                "guard blocks this entry"
                            )
                recovery = bool(self.rsi_loss or self.rsi_candidate or self.rsi_recovery)
                active = self.fcb.active
                side = (self.rsi_candidate or {}).get("side")
                if not side and recovery and self.rsi_loss:
                    side = "PUT" if self.rsi_loss["side"] == "CALL" else "CALL"
                side = side or self.trend or (
                    "CALL" if number(upper) and candle["close"] > upper else
                    "PUT" if number(lower) and candle["close"] < lower else None)
                reason = "ZONE_EXIT" if active else "ZONE_SKIP"
                if side and (allow_entries or active or recovery):
                    entry = self._entry(side, "RSI" if recovery else "FCB", "candle", now,
                                        "RSI recovery" if recovery else "pole" if active else "FCB")
                    metrics, _ = breakout_metrics(self.candles[:-1], self.candles[-1],
                        upper, lower, side, self.zone_data)
                    entry.update(practice_validation=metrics, execution_candle=stamp + 60)
                    self.audit_validation(entry, "ZONE_SKIP" if recovery else reason)
                self._end_flow(stamp + 60)
                self.zone_recovery_cooldown = recovery or self.zone_recovery_cooldown
                self.reason = f"{reason}; {self.zone_data['zone']} ({self.zone_data['zone_trigger']}); " + (
                    "COOLDOWN until zone clears" if self.zone_recovery_cooldown else "WAIT until zone clears")
                return None, None
            self.zone_recovery_cooldown = False
        if (self.strict_rules and self.fcb.active and outside_side
                and (outside_side != self.trend or not breakout_direction_confirmed({
                    "side": self.trend, "signal_open": candle["open"], "signal_close": candle["close"]}))):
            entry = self._entry(self.trend, "FCB", "candle", now, "pole")
            entry.update(execution_candle=stamp + 60, signal_open=candle["open"],
                         signal_close=candle["close"], band_high=upper, band_low=lower)
            self.audit_validation(entry, "BREAKOUT_DIRECTION")
            self._end_flow(stamp + 60)
            self.reason = "BREAKOUT_DIRECTION; opposing outside candle ended flow; hunt fresh breakout"
            return None, None
        win_pole = self.fcb_candidate if (
            self.fcb_candidate is not None and self.fcb_candidate.get("trigger") == "fcb_win"
            and self.fcb_candidate["execution_candle"] == stamp + 60) else None
        self.fcb_candidate = win_pole
        if not allow_entries:
            self.discard_candidates()
            self.reason = "Paused; actual fractal FCB levels updated without entries"
            return None, None
        if (latest_reversal and self.reversal_candidate is None
                and self.loss_reversal_candidate is None
                and self.pending["RSI"] is None and self.pending["FCB"] is None
                and self.rsi_candidate is None and self.rsi_recovery is None
                and self.fcb_candidate is None and outside_side is None
                and self.pole_win_slot != stamp + 60
                and stamp + 60 > self.fcb_resume_after):
            self.reversal_candidate = {
                **self._entry(latest_reversal["side"], "RSI", "rsi_reversal", now, "RSI reversal"),
                **latest_reversal,
                "attempt": 1,
                "execution_candle": stamp + 60,
            }
            if self.practice_zone:
                self.reversal_candidate["practice_validation"] = dict(self.zone_data)
            self.reversal_status += "; waiting for next-candle price confirmation"
        if (self.strict_rules and self.fcb.active and win_pole is None
                and self.pending["FCB"] is None):
            self._end_flow(stamp + 60)
            self.reason = "No verified win for this next candle; hunt fresh breakout"
            return None, None
        # A reversal remains the higher-priority standalone RSI strategy. Do not
        # queue an opposing momentum signal from the same closed candle.
        if (latest_momentum and latest_reversal is None
                and self.momentum_candidate is None and self.reversal_candidate is None
                and self.loss_reversal_candidate is None
                and self.pending["RSI"] is None and self.pending["FCB"] is None
                and self.rsi_candidate is None and self.rsi_recovery is None
                and self.fcb_candidate is None and outside_side is None
                and self.pole_win_slot != stamp + 60
                and stamp + 60 > self.fcb_resume_after):
            candidate = {
                **self._entry(latest_momentum["side"], "RSI", "rsi_momentum", now, "RSI momentum"),
                **latest_momentum,
                "execution_candle": stamp + 60,
            }
            if self.practice_zone:
                candidate["practice_validation"] = dict(self.zone_data)
            self._end_flow(stamp + 60)
            self.momentum_candidate = candidate
            self.momentum_status += "; waiting for next-candle price confirmation"
        if (self.pending["FCB"] is None and self.pending["RSI"] is None
                and self.rsi_loss is None and self.rsi_candidate is None
                and self.rsi_recovery is None and self.reversal_candidate is None
                and self.loss_reversal_candidate is None
                and self.momentum_candidate is None
                and stamp + 60 > self.fcb_resume_after
                and self.pole_win_slot != stamp + 60):
            was_active = self.fcb.active
            validation = None
            side = outside_side
            if not was_active and side and not breakout_direction_confirmed({
                    "side": side, "signal_open": candle["open"], "signal_close": candle["close"]}):
                entry = self._entry(side, "FCB", "candle", now, "FCB")
                entry.update(execution_candle=stamp + 60, signal_open=candle["open"],
                             signal_close=candle["close"], band_high=upper, band_low=lower,
                             band_model="quadcode_fractal", confirmation_policy=ALIGNED_BREAKOUT_POLICY)
                self.audit_validation(entry, "BREAKOUT_DIRECTION")
                if self.strict_rules:
                    self._end_flow(stamp + 60)
                self.reason = "BREAKOUT_DIRECTION; signal candle opposes trade or is doji; WAIT"
                return None, None
            if self.practice_zone:
                if side:
                    validation, rejection = breakout_metrics(self.candles[:-1], self.candles[-1],
                        upper, lower, side, self.zone_data)
                    if not was_active and rejection:
                        entry = self._entry(side, "FCB", "candle", now, "FCB")
                        entry.update(practice_validation=validation, execution_candle=stamp + 60)
                        self.audit_validation(entry, rejection)
                        self.reason = f"{rejection}; breakout rejected; WAIT"
                        return None, None
            side = self.fcb.decide(self.candles, bands=(upper, lower))
            if side:
                kind = "pole" if was_active else "FCB"
                self.fcb_candidate = {
                    **self._entry(side, "FCB", "candle", now, kind),
                    "execution_candle": stamp + 60,
                    "band_high": upper, "band_low": lower,
                    "band_model": "quadcode_fractal",
                    "signal_open": candle["open"],
                    "signal_close": candle["close"],
                    **({"confirmation_policy": STANDARD_BREAKOUT_POLICY if self.strict_rules
                        else ALIGNED_BREAKOUT_POLICY} if kind == "FCB" else {}),
                    **({"practice_validation": validation} if self.practice_zone else {}),
                }
        self._arm_rsi(now)
        value = calc_rsi([c["close"] for c in self.candles]) if self.rsi_enabled else "disabled"
        fcb_state = "waiting for confirmed upper/lower fractals" if (
            upper is None or lower is None) else "loss watch" if self.fcb.loss_watch else (
            f"pole {self.trend}" if self.fcb.active else "hunting breakout")
        if self.pending["RSI"] is not None:
            fcb_state = "RSI recovery open; waiting for settlement and a fresh FCB signal"
        self.reason = (f"FAST TRADE | FCB: {fcb_state} | "
                       f"RSI: {value if value is not None else 'warming up'}; {self.rsi_reason}")
        return None, None

    def fcb_observation(self, candle):
        if not candle:
            return None
        upper, lower = self.bands.preview(candle)
        return {**candle, "fcb_high": upper, "fcb_low": lower}

    def confirm_fcb(self, candle, now, broker_now):
        candidate = self.fcb_candidate
        if candidate is None:
            return
        if self.strict_rules and candidate.get("trigger") == "fcb_win":
            # A settlement may reach the supervisor before the winning closed
            # candle reaches its monitor. Wait for actual history, never invent A.
            if not self.candles or self.last_time != candidate["signal_time"]:
                if now > candidate["execution_candle"] + ENTRY_WINDOW_SECONDS:
                    self._end_flow(now)
                    self.reason = LATE_ORDER_REASON
                return
            signal = self.candles[-1]
            candidate = {**candidate, "signal_open": signal["open"], "signal_close": signal["close"],
                         "confirmation_policy": CLEAN_POLE_POLICY}
            self.fcb_candidate = candidate
            if (not breakout_direction_confirmed(candidate, signal)
                    or not number(self.bands.upper) or not number(self.bands.lower)
                    or not (candidate["side"] == "CALL" and signal["close"] > self.bands.upper
                            or candidate["side"] == "PUT" and signal["close"] < self.bands.lower)):
                self.audit_validation(candidate, "BREAKOUT_DIRECTION")
                self._end_flow(now)
                self.reason = "BREAKOUT_DIRECTION; winning candle is not a clean continuation; hunt fresh breakout"
                return
        if (candidate.get("kind") == "FCB" and candidate.get("trigger") == "candle"
                and (not self.candles or not breakout_direction_confirmed(candidate, self.candles[-1]))):
            self.audit_validation(candidate, "BREAKOUT_DIRECTION")
            self.fcb_candidate = None
            self.reason = "BREAKOUT_DIRECTION; closed signal direction could not be verified; WAIT"
            return
        candidate = self.annotate_validation(candidate)
        if not self.validation_allowed(candidate):
            self.audit_validation(candidate, "ZONE_SKIP" if self.zone_data["zone"] != "CLEAR"
                                  else "BREAKOUT_VALIDATION_FAILED")
            self.fcb_candidate = None
            if self.strict_rules:
                self._end_flow(now)
            return
        slot = candidate["execution_candle"]
        if now > slot + ENTRY_WINDOW_SECONDS or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS:
            self.fcb_candidate = None
            if self.strict_rules:
                self._end_flow(now)
                self.reason = LATE_ORDER_REASON + "; hunt fresh breakout"
                return
            self.reason = LATE_ORDER_REASON + "; FCB entry window expired, pole direction retained | " + self.rsi_reason
            return
        if self.pending["FCB"] is not None:
            self.fcb_candidate = None
            return
        entry = {**candidate, "confirmed_at": now, "entry_window_seconds": ENTRY_WINDOW_SECONDS}
        win_pole = entry.get("trigger") == "fcb_win"
        if win_pole:
            entry.update(band_high=self.bands.upper, band_low=self.bands.lower,
                         band_model="quadcode_fractal")
        observation = self.fcb_observation(candle)
        confirmed = (pole_quote_ready(entry, observation, now, broker_now) if win_pole else
                     fcb_price_confirmed(entry, observation, now, broker_now))
        if confirmed:
            entry["market_evidence"] = capture(self, entry, observation, now, broker_now)
            entry["confirmation_quote"] = {
                key: observation.get(key)
                for key in ("time", "observed_at", "open", "close", "fcb_high", "fcb_low")
            }
            entry["reason"] += (f"; candle A closed outside fractal band; execute candle B within 0–{ENTRY_WINDOW_SECONDS:g}s"
                                if entry.get("confirmation_policy") in (
                                    CLOSED_BREAKOUT_POLICY, ALIGNED_BREAKOUT_POLICY,
                                    CLEAN_BREAKOUT_POLICY, STANDARD_BREAKOUT_POLICY) else
                                "; verified FCB win; next-minute price clearly outside fractal band" if win_pole else
                                f"; actual fractal FCB; next candle confirmed within 0–{ENTRY_WINDOW_SECONDS:g}s")
            self.entries.insert(0, entry)
            self.fcb_candidate = None
            self.reason = entry["reason"] + " | " + self.rsi_reason
        else:
            self.reason = (
                f"Candle A breakout confirmed; waiting for a fresh outside-FCB quote in candle B (0–{ENTRY_WINDOW_SECONDS:g}s)"
                if entry.get("confirmation_policy") in (
                    CLOSED_BREAKOUT_POLICY, ALIGNED_BREAKOUT_POLICY,
                    CLEAN_BREAKOUT_POLICY, STANDARD_BREAKOUT_POLICY) else
                "Pole position waiting for a fresh, clearly outside-FCB quote after verified win" if win_pole else
                f"FCB waiting for next-candle directional confirmation outside the band (0–{ENTRY_WINDOW_SECONDS:g}s)"
            ) + " | " + self.rsi_reason

    def _rsi_note(self, text):
        self.rsi_reason = text
        self.reason = text

    def _arm_rsi(self, now):
        if not self.rsi_enabled:
            self.rsi_loss = None
            self.rsi_candidate = None
            self.rsi_recovery = None
            self.rsi_reason = "RSI disabled"
            return
        loss = self.rsi_loss
        if loss is None:
            return
        slot = loss["signal_time"] + 60
        if now > slot + ENTRY_WINDOW_SECONDS or self.pending["RSI"] is not None:
            self.rsi_loss = None
            self.rsi_recovery = None
            self._rsi_note(LATE_ORDER_REASON if now > slot + ENTRY_WINDOW_SECONDS
                           else "RSI recovery skipped: RSI already occupied")
            return
        if now < slot:
            return
        self.rsi_loss = None
        self.rsi_candidate = {
            **self._entry("PUT" if loss["side"] == "CALL" else "CALL",
                          "RSI", "loss_recovery", now, "RSI recovery"),
            "signal_time": loss["signal_time"], "execution_candle": slot,
            "loss_token": loss["token"], "loss_side": loss["side"],
            "recovery_cycle": self.fcb_cycle,
        }
        self._rsi_note("Confirmed loss; opposite RSI recovery awaiting a fresh next-candle quote")

    def confirm_rsi(self, candle, now, broker_now):
        if self.pending_loss_reversal is not None:
            slot = self.pending_loss_reversal["signal_time"] + 60
            if (now > slot + ENTRY_WINDOW_SECONDS
                    or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS):
                self.pending_loss_reversal = None
                self.reversal_status = "Loss reversal expired; the immediate next-candle window passed"
            elif not self.pending_loss_reversal.get("ready"):
                return
            else:
                gap_result = self._confirm_pending_loss_gap(candle, now, broker_now)
                if gap_result != "no_gap":
                    return
        if self.loss_reversal_candidate is not None:
            slot = self.loss_reversal_candidate["execution_candle"]
            if (self.reversal_candidate
                    and self.reversal_candidate.get("execution_candle") == slot):
                self.reversal_candidate = None
                self.reversal_status = "RSI reversal skipped; loss reversal has priority"
            self.confirm_loss_reversal(candle, now, broker_now)
            return
        if self.reversal_candidate is not None:
            self.confirm_reversal(candle, now, broker_now)
            if self.reversal_candidate is None:
                return
            # A stale standalone candidate must never fall through into loss recovery.
            if self.reversal_candidate.get("kind") == "RSI reversal":
                return
        if self.momentum_candidate is not None:
            self.confirm_momentum(candle, now, broker_now)
            if self.momentum_candidate is not None:
                return
        if self.practice_zone and self.zone_data["zone"] != "CLEAR":
            self.rsi_candidate = self.rsi_loss = self.rsi_recovery = None
            self.zone_recovery_cooldown = True
            return
        self._arm_rsi(now)
        candidate = self.rsi_candidate
        if candidate is None:
            return
        slot = candidate["execution_candle"]
        if now > slot + ENTRY_WINDOW_SECONDS or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS or self.pending["RSI"] is not None:
            self.rsi_candidate = None
            self.rsi_recovery = None
            self._rsi_note(LATE_ORDER_REASON if now > slot + ENTRY_WINDOW_SECONDS
                           or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS
                           else "RSI reversal skipped: RSI occupied")
            return
        entry = {**candidate, "confirmed_at": now, "entry_window_seconds": ENTRY_WINDOW_SECONDS}
        if rsi_price_confirmed(entry, candle, now, broker_now):
            entry["market_evidence"] = capture(self, entry, candle, now, broker_now)
            self.entries.insert(0, entry)
            self.rsi_candidate = None
            self._rsi_note(f"Opposite loss recovery ready within 0–{ENTRY_WINDOW_SECONDS:g}s")
        else:
            self._rsi_note(f"RSI waiting for a fresh next-candle quote (0–{ENTRY_WINDOW_SECONDS:g}s)")

    def _arm_pending_loss_reversal(self, now):
        pending = self.pending_loss_reversal
        if pending is None or self.last_time is None or self.last_time < pending["signal_time"]:
            return
        if self.last_time > pending["signal_time"]:
            self.pending_loss_reversal = None
            return
        slot = pending["signal_time"] + 60
        if not self.reversal_enabled:
            self.pending_loss_reversal = None
            return
        if now > slot + ENTRY_WINDOW_SECONDS:
            self.pending_loss_reversal = None
            self.reversal_status = "Loss reversal expired; the immediate next-candle window passed"
            return
        if self.pending["RSI"] is not None or self.pending["FCB"] is not None:
            self.pending_loss_reversal = None
            self.reversal_status = "Loss reversal skipped; a pair order is still unresolved"
            return
        if self.practice_zone and self.zone_data["zone"] != "CLEAR":
            self.pending_loss_reversal = None
            self.reversal_status = (
                f"Loss reversal skipped; Practice Zone {self.zone_data['zone']} blocks this entry"
            )
            return
        signal = reversal_signal(self.candles, pending["signal_time"], pending["side"])
        if signal is None:
            loss_candle = self.candles[-1] if self.candles else {}
            loss_close = loss_candle.get("close")
            if (loss_candle.get("time") != pending["signal_time"]
                    or not number(loss_close) or loss_close <= 0):
                self.pending_loss_reversal = None
                self.reversal_status = "Loss reversal skipped; the settled candle close is unavailable"
                return
            pending["ready"] = True
            pending["loss_close"] = loss_close
            self.reversal_status = "Verified FCB loss; checking for an opposite candle-open gap"
            return
        self.pending_loss_reversal = None
        self.loss_reversal_candidate = self._new_loss_reversal_candidate(
            pending, now, "wick", signal["signal_close"], signal["side"])
        if (self.reversal_candidate
                and self.reversal_candidate.get("execution_candle") == slot):
            self.reversal_candidate = None
        if self.fcb_candidate and self.fcb_candidate.get("execution_candle") == slot:
            self.fcb_candidate = None
            self.pole_win_slot = None
        self.reversal_status = "Confirmed breakout/continuation loss; loss reversal awaiting next-candle confirmation"

    def _new_loss_reversal_candidate(self, pending, now, source, loss_close, side=None):
        side = side or ("PUT" if pending["side"] == "CALL" else "CALL")
        candidate = {
            **self._entry(side, "RSI", "loss_recovery", now, "LOSS reversal"),
            "side": side,
            "signal_time": pending["signal_time"],
            "execution_candle": pending["signal_time"] + 60,
            "signal_close": loss_close,
            "loss_token": pending["token"],
            "loss_side": pending["side"],
            "loss_candle_close": loss_close,
            "loss_reversal_source": source,
            "attempt": 1,
        }
        if self.practice_zone:
            candidate["practice_validation"] = dict(self.zone_data)
        return candidate

    @staticmethod
    def _loss_open_gap_matches(loss_side, loss_close, candle, slot):
        opening = candle.get("open") if candle else None
        return (bool(candle) and candle.get("time") == slot
                and number(opening) and number(loss_close)
                and (loss_side == "CALL" and opening < loss_close
                     or loss_side == "PUT" and opening > loss_close))

    def _confirm_pending_loss_gap(self, candle, now, broker_now):
        pending = self.pending_loss_reversal
        if pending is None or not pending.get("ready"):
            return "waiting"
        slot = pending["signal_time"] + 60
        loss_candle = self.candles[-1] if self.candles else {}
        if (not self.reversal_enabled or self.last_time != pending["signal_time"]
                or loss_candle.get("time") != pending["signal_time"]
                or loss_candle.get("close") != pending.get("loss_close")
                or self.pending["RSI"] is not None or self.pending["FCB"] is not None):
            self.pending_loss_reversal = None
            self.reversal_status = "Loss reversal skipped; its source candle or pair slot changed"
            return "blocked"
        if self.practice_zone and self.zone_data["zone"] != "CLEAR":
            self.pending_loss_reversal = None
            self.reversal_status = "Loss reversal skipped; Practice Zone guard is active"
            return "blocked"
        entry = {
            "engine": "RSI", "kind": "LOSS reversal", "trigger": "loss_recovery",
            "side": "PUT" if pending["side"] == "CALL" else "CALL",
            "signal_time": pending["signal_time"], "execution_candle": slot,
            "loss_token": pending["token"], "loss_side": pending["side"],
            "confirmed_at": now, "entry_window_seconds": ENTRY_WINDOW_SECONDS,
        }
        if not rsi_price_confirmed(entry, candle, now, broker_now):
            return "waiting"
        loss_close = pending["loss_close"]
        if not self._loss_open_gap_matches(pending["side"], loss_close, candle, slot):
            self.pending_loss_reversal = None
            self.reversal_status = "Loss reversal skipped; candle C did not open against candle B"
            return "no_gap"

        candidate = self._new_loss_reversal_candidate(
            pending, now, "open_gap", loss_close)
        candidate.update(
            loss_reversal_rule="open_gap",
            confirmed_candle_open=candle["open"],
        )
        if self.practice_zone and not self.validation_allowed(candidate):
            self.pending_loss_reversal = None
            self.reversal_status = "Loss reversal skipped; Practice Zone guard is active"
            return "blocked"
        self.pending_loss_reversal = None
        if (self.reversal_candidate
                and self.reversal_candidate.get("execution_candle") == slot):
            self.reversal_candidate = None
            self.reversal_status = "RSI reversal skipped; loss reversal has priority"
        self.loss_reversal_candidate = candidate
        confirmed = {**candidate, "confirmed_at": now,
                     "entry_window_seconds": ENTRY_WINDOW_SECONDS}
        confirmed["market_evidence"] = capture(self, confirmed, candle, now, broker_now)
        self.entries.insert(0, confirmed)
        candidate["queued"] = True
        self.reversal_status = "Candle C opened against the FCB loss candle; loss reversal confirmed"
        return "confirmed"

    def confirm_loss_reversal(self, candle, now, broker_now):
        candidate = self.loss_reversal_candidate
        if candidate is None or candidate.get("queued"):
            return
        if not self.reversal_enabled:
            self.loss_reversal_skipped(candidate)
            return
        if self.practice_zone and not self.validation_allowed(candidate):
            self.loss_reversal_skipped(candidate)
            self.reversal_status = "Loss reversal skipped; Practice Zone guard is active"
            return
        slot = candidate["execution_candle"]
        if (now > slot + ENTRY_WINDOW_SECONDS
                or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS
                or self.pending["RSI"] is not None or self.pending["FCB"] is not None):
            self.loss_reversal_skipped(candidate)
            self.reversal_status = (
                LATE_ORDER_REASON if now > slot + ENTRY_WINDOW_SECONDS
                or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS
                else "Loss reversal skipped; RSI strategy slot is occupied"
            )
            return
        entry = {**candidate, "confirmed_at": now, "entry_window_seconds": ENTRY_WINDOW_SECONDS}
        if not rsi_price_confirmed(entry, candle, now, broker_now):
            self.reversal_status = (
                f"Loss reversal awaiting a fresh next-candle quote (0–{ENTRY_WINDOW_SECONDS:g}s)"
            )
            return
        gap_confirmed = self._loss_open_gap_matches(
            candidate.get("loss_side"), candidate.get("loss_candle_close"), candle, slot)
        if gap_confirmed:
            rule = "open_gap"
        elif candidate.get("loss_reversal_source") == "wick":
            rule = "wick"
        else:
            self.loss_reversal_skipped(candidate)
            self.reversal_status = "Loss reversal skipped; candle C did not open against candle B"
            return
        candidate.update(loss_reversal_rule=rule, confirmed_candle_open=candle["open"])
        entry = {**candidate, "confirmed_at": now, "entry_window_seconds": ENTRY_WINDOW_SECONDS}
        entry["market_evidence"] = capture(self, entry, candle, now, broker_now)
        self.entries.insert(0, entry)
        candidate["queued"] = True
        self.reversal_status = (
            "Loss reversal confirmed on the immediate next 1-minute candle"
            if rule == "wick" else
            "Candle C opened against the FCB loss candle; loss reversal confirmed"
        )

    def confirm_momentum(self, candle, now, broker_now):
        candidate = self.momentum_candidate
        if candidate is None or candidate.get("queued"):
            return
        if not self.momentum_enabled:
            self.momentum_candidate = None
            self.momentum_status = "RSI momentum strategy disabled"
            return
        if self.reversal_candidate is not None:
            self.momentum_candidate = None
            self.momentum_status = "Momentum signal skipped; standalone reversal has priority"
            return
        if self.practice_zone and not self.validation_allowed(candidate):
            self.momentum_candidate = None
            self.momentum_status = "Momentum signal skipped; Practice Zone guard is active"
            return
        slot = candidate["execution_candle"]
        if (now > slot + ENTRY_WINDOW_SECONDS
                or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS
                or self.pending["RSI"] is not None):
            self.momentum_candidate = None
            self.momentum_status = (
                LATE_ORDER_REASON if now > slot + ENTRY_WINDOW_SECONDS
                or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS
                else "Momentum signal skipped; RSI strategy slot is occupied"
            )
            return
        entry = {**candidate, "confirmed_at": now, "entry_window_seconds": ENTRY_WINDOW_SECONDS}
        if momentum_price_confirmed(entry, candle, now, broker_now):
            entry["market_evidence"] = capture(self, entry, candle, now, broker_now)
            self.entries.insert(0, entry)
            candidate["queued"] = True
            self.momentum_status = f"{candidate['side']} momentum confirmed on the next 1-minute candle"
        else:
            self.momentum_status = (
                f"{candidate['side']} momentum awaiting a fresh directional quote "
                f"(0–{ENTRY_WINDOW_SECONDS:g}s)"
            )

    def confirm_reversal(self, candle, now, broker_now):
        candidate = self.reversal_candidate
        if candidate is None or candidate.get("queued"):
            return
        if not self.reversal_enabled:
            self.reversal_candidate = None
            self.reversal_status = "Standalone reversal strategy disabled"
            return
        if self.practice_zone and not self.validation_allowed(candidate):
            self.reversal_candidate = None
            self.reversal_status = "Reversal skipped; Practice zone guard is active"
            return
        slot = candidate["execution_candle"]
        if (now > slot + ENTRY_WINDOW_SECONDS
                or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS
                or self.pending["RSI"] is not None):
            self.reversal_candidate = None
            self.reversal_status = (
                LATE_ORDER_REASON if now > slot + ENTRY_WINDOW_SECONDS
                or number(broker_now) and broker_now > slot + ENTRY_WINDOW_SECONDS
                else "Reversal skipped; RSI strategy slot is occupied"
            )
            return
        if not self.reversal_entry_allowed(candidate):
            self.reversal_candidate = None
            self.reversal_status = "Reversal skipped; closed-candle evidence is stale or invalid"
            return
        entry = {**candidate, "confirmed_at": now, "entry_window_seconds": ENTRY_WINDOW_SECONDS}
        if reversal_price_confirmed(entry, candle, now, broker_now):
            entry["market_evidence"] = capture(self, entry, candle, now, broker_now)
            self.entries.insert(0, entry)
            candidate["queued"] = True
            self.reversal_status = (
                f"{candidate['side']} reversal confirmed on the next 1-minute candle"
            )
        else:
            self.reversal_status = (
                f"{candidate['side']} reversal awaiting a fresh directional quote "
                f"(0–{ENTRY_WINDOW_SECONDS:g}s)"
            )

    def _expire_recovery(self, now):
        if self.rsi_recovery and now > self.rsi_recovery["signal_time"] + 60 + ENTRY_WINDOW_SECONDS:
            self.rsi_recovery = None
            self.rsi_candidate = None
            self._rsi_note(LATE_ORDER_REASON)

    def take_entries(self, now):
        self._expire_recovery(now)
        self._arm_rsi(now)
        entries, self.entries = self.entries, []
        entries = [
            entry for entry in entries
            if entry.get("engine") != "RSI"
            or (entry.get("kind") == "RSI reversal" and self.reversal_enabled)
            or (entry.get("kind") == "LOSS reversal" and self.reversal_enabled)
            or (entry.get("kind") == "RSI momentum" and self.momentum_enabled)
            or (entry.get("kind") not in (
                "RSI reversal", "LOSS reversal", "RSI momentum") and self.rsi_enabled)
        ]
        entries.sort(key=lambda entry: entry.get("kind") != "LOSS reversal")
        selected, seen = [], set()
        for entry in entries:
            slot = entry.get("execution_candle")
            if slot in seen:
                if entry.get("kind") == "RSI reversal":
                    self.reversal_skipped(entry)
                elif entry.get("kind") == "LOSS reversal":
                    self.loss_reversal_skipped(entry)
                continue
            if slot is not None:
                seen.add(slot)
            selected.append(entry)
        return selected

    def submitted(self, token, entry):
        self.pending[entry["engine"]] = {"token": token, "entry": entry}
        if entry["engine"] == "RSI":
            # RSI trades end the old FCB flow; standalone reversals are not
            # loss recoveries and can never create a follow-up trade.
            self._end_flow(self.fcb_resume_after)
            if entry.get("kind") == "RSI reversal":
                self.reversal_candidate = None
                self.reversal_status = f"{entry['side']} RSI reversal order acknowledged"
                self.reason = f"{entry['reason']}; broker order acknowledged"
                return
            if entry.get("kind") == "LOSS reversal":
                self.loss_reversal_candidate = None
                self.reversal_status = f"{entry['side']} loss reversal order acknowledged"
                self.reason = f"{entry['reason']}; broker order acknowledged"
                return
            if entry.get("kind") == "RSI momentum":
                self.momentum_candidate = None
                self.momentum_status = f"{entry['side']} RSI momentum order acknowledged"
                self.reason = f"{entry['reason']}; broker order acknowledged"
                return
        self.reason = f"{entry['reason']}; broker order acknowledged"
        if entry["engine"] == "RSI":
            self.rsi_reason = "RSI recovery open; next trade requires a fresh FCB signal"
            self.reason += "; old pole cycle ended; waiting for a fresh FCB signal after settlement"

    def fcb_entry_allowed(self, entry):
        if not self.validation_allowed(entry):
            return False
        if self.strict_rules and (
                entry.get("engine") != "FCB"
                or entry.get("kind") == "pole" and entry.get("trigger") != "fcb_win"
                or entry.get("kind") not in ("FCB", "pole")):
            return False
        return self.fcb_flow_current(entry)

    def fcb_flow_current(self, entry):
        """Settled A's order need not still be the current signal candle B.

        A win authorizes only its expiry candle; zone/closed-direction and fresh
        quote gates are applied to that new pole entry, not the old breakout.
        """
        return (entry.get("fcb_cycle", 0) == self.fcb_cycle
                and self.pending["RSI"] is None and self.rsi_recovery is None
                and (not self.fcb_resume_after
                     or number(entry.get("signal_time"))
                     and entry["signal_time"] + 60 > self.fcb_resume_after))

    def rsi_entry_allowed(self, entry):
        if entry.get("kind") == "LOSS reversal":
            return self.loss_reversal_entry_allowed(entry)
        if entry.get("kind") == "RSI reversal":
            return self.reversal_entry_allowed(entry)
        if entry.get("kind") == "RSI momentum":
            return self.momentum_entry_allowed(entry)
        if not self.validation_allowed(entry):
            return False
        return (self.rsi_enabled and self.rsi_recovery is not None and self.pending["RSI"] is None
                and self.rsi_recovery["token"] == entry.get("loss_token")
                and entry.get("recovery_cycle") == self.fcb_cycle)

    def momentum_entry_allowed(self, entry):
        candidate = self.momentum_candidate
        verified = standalone_momentum_signal(self.candles)
        return bool(
            self.momentum_enabled and candidate is not None
            and self.pending["RSI"] is None
            and entry.get("engine") == "RSI" and entry.get("kind") == "RSI momentum"
            and entry.get("trigger") == "rsi_momentum"
            and entry.get("execution_candle") == entry.get("signal_time", 0) + 60
            and self.last_time == entry.get("signal_time")
            and verified is not None
            and all(candidate.get(key) == entry.get(key) == verified.get(key) for key in (
                "side", "signal_time", "execution_candle", "signal_open", "signal_close",
                "rsi", "extreme_rsi",
            ))
            and not self.reversal_candidate
            and self.validation_allowed(entry)
        )

    def reversal_entry_allowed(self, entry):
        candidate = self.reversal_candidate
        if (not self.reversal_enabled or candidate is None
                or self.pending["RSI"] is not None
                or not self.validation_allowed(entry)
                or entry.get("kind") != "RSI reversal"
                or entry.get("attempt", 1) != 1
                or entry.get("execution_candle") != entry.get("signal_time", 0) + 60):
            return False
        if self.last_time != entry.get("signal_time") or entry.get("trigger") != "rsi_reversal":
            return False
        verified = standalone_reversal_signal(self.candles)
        return bool(
            verified
            and all(candidate.get(key) == entry.get(key) == verified.get(key)
                    for key in ("side", "signal_time", "execution_candle",
                                "signal_open", "signal_close", "rsi", "extreme_rsi"))
        )

    def loss_reversal_entry_allowed(self, entry, candle=None):
        candidate = self.loss_reversal_candidate
        if (not self.reversal_enabled or candidate is None
                or self.pending["RSI"] is not None or self.pending["FCB"] is not None
                or entry.get("engine") != "RSI" or entry.get("kind") != "LOSS reversal"
                or entry.get("trigger") != "loss_recovery" or entry.get("attempt", 1) != 1
                or entry.get("execution_candle") != entry.get("signal_time", 0) + 60
                or not entry.get("loss_token")
                or entry.get("loss_side") not in ("CALL", "PUT")
                or entry.get("loss_reversal_source") not in ("wick", "open_gap")
                or entry.get("loss_reversal_rule") not in ("wick", "open_gap")
                or not self.validation_allowed(entry)):
            return False
        if (self.last_time != entry.get("signal_time")
                or any(candidate.get(key) != entry.get(key) for key in (
                    "side", "signal_time", "execution_candle", "signal_close",
                    "loss_token", "loss_side", "loss_candle_close",
                    "loss_reversal_source", "loss_reversal_rule",
                    "confirmed_candle_open"))):
            return False
        loss_candle = self.candles[-1] if self.candles else {}
        if (loss_candle.get("time") != entry.get("signal_time")
                or not number(loss_candle.get("close"))
                or loss_candle["close"] != entry.get("loss_candle_close")
                or entry.get("signal_close") != entry.get("loss_candle_close")):
            return False
        if entry.get("loss_reversal_rule") == "open_gap":
            confirmed_open = entry.get("confirmed_candle_open")
            if not self._loss_open_gap_matches(
                    entry["loss_side"], entry["loss_candle_close"],
                    {"time": entry["execution_candle"], "open": confirmed_open},
                    entry["execution_candle"]):
                return False
            return (candle is None
                    or candle.get("time") == entry["execution_candle"]
                    and candle.get("open") == confirmed_open)
        if entry.get("loss_reversal_source") != "wick":
            return False
        verified = reversal_signal(self.candles, entry["signal_time"], entry["loss_side"])
        return bool(verified and all(candidate.get(key) == entry.get(key) == verified.get(key)
                                     for key in (
                                         "side", "signal_time", "execution_candle", "signal_close")))

    def recovery_skipped(self, entry):
        if self.rsi_recovery and self.rsi_recovery["token"] == entry.get("loss_token"):
            self.rsi_recovery = None
            self.rsi_loss = None
            self.rsi_candidate = None

    def momentum_skipped(self, entry):
        if (self.momentum_candidate
                and self.momentum_candidate.get("signal_time") == entry.get("signal_time")):
            self.momentum_candidate = None
            self.momentum_status = "Momentum entry was skipped; waiting for a new RSI extreme"

    def reversal_skipped(self, entry):
        if (self.reversal_candidate
                and self.reversal_candidate.get("signal_time") == entry.get("signal_time")):
            self.reversal_candidate = None
            self.reversal_status = "Reversal entry was skipped; waiting for a new 1-minute setup"

    def loss_reversal_skipped(self, entry):
        if (self.loss_reversal_candidate
                and self.loss_reversal_candidate.get("signal_time") == entry.get("signal_time")
                and self.loss_reversal_candidate.get("loss_token") == entry.get("loss_token")):
            self.loss_reversal_candidate = None
            self.reversal_status = "Loss reversal entry was skipped; waiting for a new eligible loss"

    def _end_flow(self, now, advance=True):
        """Invalidate every old entry without touching already-submitted orders."""
        if advance:
            self.fcb_cycle += 1
        self.fcb_resume_after = max(self.fcb_resume_after, now)
        self.fcb.reset()
        self.fcb_candidate = None
        self.pole_win_slot = None
        self.rsi_loss = None
        self.rsi_candidate = None
        self.rsi_recovery = None
        self.loss_reversal_candidate = None
        self.pending_loss_reversal = None
        self.momentum_candidate = None
        self.entries.clear()

    def broker_settled(self, token, result, now, allow_entries=True, loss_candle=None,
                       win_expiry=None, settled_entry=None):
        late_attempt = None
        engine = next((key for key, pending in self.pending.items()
                       if pending and pending["token"] == token), None)
        if engine is None:
            # Reconnect/reseed can lose the in-memory mapping. The runner passes
            # the durable order only after its actual broker outcome is verified.
            if not settled_entry or settled_entry.get("engine") not in ("FCB", "RSI"):
                return
            engine = settled_entry["engine"]
        else:
            settled_entry = self.pending[engine]["entry"]
            self.pending[engine] = None
        is_reversal = bool(settled_entry and settled_entry.get("kind") in (
            "RSI reversal", "LOSS reversal"))
        if self.practice_zone and self.zone_data["zone"] != "CLEAR" and not is_reversal:
            allow_entries = False
        if engine == "FCB":
            if settled_entry.get("fcb_cycle", 0) == self.fcb_cycle:
                self.fcb.report_result(result)
            # RSI belongs to this losing order, not the FCB trend lifecycle.
            # Price action may already have armed loss-watch or reset the trend.
            lost_direction = (settled_entry.get("side")
                              if result == "loss" and settled_entry.get("kind") in ("FCB", "pole")
                              and settled_entry.get("side") in ("CALL", "PUT")
                              else None)
            if (result == "win" and allow_entries and self.fcb_flow_current(settled_entry)
                    and self.fcb.active and not self.fcb.loss_watch
                    and settled_entry.get("side") in ("CALL", "PUT")
                    and number(win_expiry) and win_expiry % 60 == 0
                    and win_expiry <= now <= win_expiry + ENTRY_WINDOW_SECONDS):
                self.fcb_candidate = {
                    "engine": "FCB", "kind": "pole", "trigger": "fcb_win",
                    "fcb_cycle": self.fcb_cycle,
                    "side": settled_entry["side"], "signal_time": win_expiry - 60,
                    "execution_candle": win_expiry, "win_expiry": win_expiry,
                    "win_token": token, "reason": "pole position; same direction after FCB win",
                }
                self.pole_win_slot = win_expiry
            elif (result == "win" and allow_entries and self.fcb_flow_current(settled_entry)
                    and self.fcb.active and not self.fcb.loss_watch
                    and settled_entry.get("side") in ("CALL", "PUT")
                    and number(win_expiry) and win_expiry % 60 == 0
                    and now > win_expiry + ENTRY_WINDOW_SECONDS):
                late_attempt = {"engine": "FCB", "kind": "pole", "side": settled_entry["side"],
                                "signal_time": win_expiry - 60, "execution_candle": win_expiry}
                self.reason = LATE_ORDER_REASON
                if self.strict_rules:
                    self._end_flow(now)
            if result == "loss":
                # ANY FCB loss ends this pair's current flow, even from an older
                # cycle or while RSI is open. Never condition reset on eligibility.
                self._end_flow(now)
                if (self.reversal_enabled and lost_direction and allow_entries
                        and number(loss_candle) and loss_candle % 60 == 0):
                    self.pending_loss_reversal = {
                        "side": lost_direction.upper(),
                        "signal_time": loss_candle,
                        "token": token,
                    }
                    slot = loss_candle + 60
                    if (self.reversal_candidate
                            and self.reversal_candidate.get("execution_candle") == slot):
                        self.reversal_candidate = None
                        self.reversal_status = "RSI reversal skipped; loss reversal has priority"
                    self._arm_pending_loss_reversal(now)
                if (self.rsi_enabled and lost_direction and allow_entries and number(loss_candle)
                        and loss_candle % 60 == 0 and self.pending["RSI"] is None):
                    self.rsi_loss = {"side": lost_direction.upper(), "signal_time": loss_candle, "token": token}
                    if now > loss_candle + 60 + ENTRY_WINDOW_SECONDS:
                        late_attempt = {"engine": "RSI", "kind": "RSI recovery",
                                        "side": "PUT" if lost_direction == "CALL" else "CALL",
                                        "signal_time": loss_candle, "execution_candle": loss_candle + 60,
                                        "loss_token": token, "trigger": "loss_recovery"}
                    else:
                        self.rsi_recovery = dict(self.rsi_loss)
        else:
            self.rsi.report_result(result)
            self._end_flow(now, advance=False)
            if settled_entry and settled_entry.get("kind") == "RSI reversal":
                self.reversal_candidate = None
                self.reversal_status = (
                    f"RSI reversal settled {result}; waiting for a new independent signal"
                )
            elif settled_entry and settled_entry.get("kind") == "LOSS reversal":
                self.loss_reversal_candidate = None
                self.reversal_status = (
                    f"Loss reversal settled {result}; WAIT for a fresh eligible signal"
                )
            elif settled_entry and settled_entry.get("kind") == "RSI momentum":
                self.momentum_status = f"RSI momentum settled {result}; waiting for a new extreme"
            else:
                self.rsi_reason = "RSI recovery finished; waiting for a fresh FCB signal"
        if not allow_entries:
            self.discard_candidates()
        else:
            self.reason = f"FAST TRADE {engine}: broker-confirmed {result}"
            if engine == "RSI":
                self.reason += "; waiting for a fresh FCB signal"
            if engine == "FCB" and result == "win" and self.fcb_candidate is None:
                self.reason += "; pole entry skipped: winning expiry or immediate next-minute window unavailable"
            if self.rsi_enabled and engine == "FCB" and lost_direction and loss_candle is None:
                self._rsi_note("RSI recovery skipped: broker expiry was not verified")
            self._arm_rsi(now)
            if not self.rsi_enabled:
                self.reason += "; RSI disabled"
        if late_attempt:
            self.reason = LATE_ORDER_REASON
        return late_attempt

    def discard_candidates(self):
        # Pause/recovery must not retain a reversal or confirmation trigger.
        self.entries.clear()
        self.fcb_candidate = None
        self.pole_win_slot = None
        self.rsi_loss = None
        self.rsi_candidate = None
        self.rsi_recovery = None
        self.loss_reversal_candidate = None
        self.pending_loss_reversal = None
        self.reversal_candidate = None
        self.momentum_candidate = None
        if self.reversal_enabled:
            self.reversal_status = "Paused; no reversal entries are queued"
        if self.momentum_enabled:
            self.momentum_status = "Paused; no momentum entries are queued"
        self.rsi_reason = self._rsi_idle_reason()
        self.fcb.reset()
