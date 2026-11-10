"""Indexed PostgreSQL journal; schema is applied by dev push / Publish only."""
import copy
import os
import threading
import time
from decimal import Decimal

import psycopg
from psycopg.types.json import Jsonb
from loss_cooldown import refresh
from market_evidence import bounded_copy
from broker_timing import timing_for_history, unknown
from order_recovery import number
from trade_performance import timing_summary


# Shared with the account-wide summary: annotations are not recomputed from
# submission times or current strategy policy.
TIMING_STATUS_SQL = "COALESCE(payload->'brokerTiming'->>'status','unknown')"
TIMING_CHECKED_SQL = (
    "CASE WHEN jsonb_typeof(payload->'brokerTiming'->'checkedAt')='number' "
    "THEN (payload->'brokerTiming'->>'checkedAt')::numeric > 0 ELSE false END"
)
TIMING_FILTERS = {
    "within_2s": f"{TIMING_STATUS_SQL}='within_2s'",
    "late_after_2s": f"{TIMING_STATUS_SQL}='late_after_2s'",
    "unknown_checked": (
        f"{TIMING_STATUS_SQL} NOT IN ('within_2s','late_after_2s') AND ({TIMING_CHECKED_SQL})"
    ),
    "never_checked": (
        f"{TIMING_STATUS_SQL} NOT IN ('within_2s','late_after_2s') AND NOT ({TIMING_CHECKED_SQL})"
    ),
}


def history_timing(meta):
    """Contract-safe view of sparse annotations; never persist or re-audit it."""
    timing = {**unknown(), **timing_for_history(meta)}
    if timing["status"] not in ("within_2s", "late_after_2s", "unknown"):
        timing["status"] = "unknown"
    if "checkedAt" in timing and not number(timing["checkedAt"]):
        del timing["checkedAt"]
    return timing


class TradeStore:
    def __init__(self, ownership_key=1):
        self.connection = psycopg.connect(
            os.environ["DATABASE_URL"], connect_timeout=5, autocommit=True,
            options="-c statement_timeout=5000 -c lock_timeout=3000",
        )
        self.lock = threading.RLock()
        if not self.connection.execute(
            "SELECT pg_try_advisory_lock(824017, %s)", (ownership_key,),
        ).fetchone()[0]:
            self.connection.close()
            raise RuntimeError("Another bot worker owns the trading ledger")

    def _write_order(self, key, token, meta, status):
        evidence = (meta.get("entryContext") or {}).get("market_evidence")
        if evidence is not None:
            bounded_copy(evidence)
        self.connection.execute(
            "INSERT INTO bot_orders "
            "(account_key, intent_id, order_id, status, entry_sequence, submitted_at, result, payload) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (account_key, intent_id) DO UPDATE SET "
            "order_id = EXCLUDED.order_id, status = EXCLUDED.status, "
            "result = EXCLUDED.result, payload = EXCLUDED.payload",
            (key, token, str(meta["orderId"]) if meta.get("orderId") is not None else None,
             status, meta.get("entrySequence", 0), meta["time"], meta.get("result"), Jsonb(meta)),
        )

    def _write_audit(self, key, event):
        self.connection.execute(
            "INSERT INTO bot_recovery_events (account_key, order_id, action, payload) "
            "VALUES (%s,%s,%s,%s)",
            (key, str(event["orderId"]) if event.get("orderId") is not None else None,
             event["action"], Jsonb(event)),
        )

    def _migrate(self, key, state):
        """Data-only conversion under the account lock; failure retains snapshot.

        No counter is inferred from history: snapshots can contain operator
        settings and reservations that must survive byte-for-byte.
        """
        state = copy.deepcopy(state)
        history = state.pop("trades_log", [])
        pending = state.pop("pending", {})
        audit = state.pop("recovery_audit", [])
        # Older snapshots lacked intent IDs; assign deterministic ledger IDs
        # without modifying their original payload or merging similar trades.
        entries = [(meta.get("intentId") or f"legacy:{ordinal:020d}", meta, "settled")
                   for ordinal, meta in enumerate(reversed(history))]
        entries.extend((token, meta, "pending") for token, meta in pending.items())
        with self.connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO bot_orders "
                "(account_key,intent_id,order_id,status,entry_sequence,submitted_at,result,payload) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                [(key, token, str(meta["orderId"]) if meta.get("orderId") is not None else None,
                  status, meta.get("entrySequence", 0), meta["time"], meta.get("result"), Jsonb(meta))
                 for token, meta, status in entries],
            )
            cursor.executemany(
                "INSERT INTO bot_recovery_events (account_key,order_id,action,payload) VALUES (%s,%s,%s,%s)",
                [(key, str(event["orderId"]) if event.get("orderId") is not None else None,
                  event["action"], Jsonb(event)) for event in audit],
            )
        count = self.connection.execute(
            "SELECT count(*) FROM bot_orders WHERE account_key = %s", (key,),
        ).fetchone()[0]
        events = self.connection.execute(
            "SELECT count(*) FROM bot_recovery_events WHERE account_key = %s", (key,),
        ).fetchone()[0]
        if count != len(history) + len(pending) or events != len(audit):
            raise RuntimeError("Snapshot conversion did not preserve every journal record")
        state["next_sequence"] = max(
            state.get("next_sequence", 0),
            max((meta.get("entrySequence", 0) for meta in [*history, *pending.values()]), default=0),
        )
        self.connection.execute(
            "UPDATE bot_accounts SET state = %s, normalized = true WHERE account_key = %s",
            (Jsonb(state), key),
        )
        return state

    def _read(self, key, state):
        state = copy.deepcopy(state)
        # Independent reads share the caller's locked transaction. Queue all
        # three before fetching instead of paying three network round trips
        # twice per reservation. Never replace durable state with a cache.
        with self.connection.pipeline():
            pending = self.connection.execute(
                "SELECT intent_id, payload FROM bot_orders WHERE account_key = %s AND status = 'pending'",
                (key,),
            )
            trades = self.connection.execute(
                "SELECT payload FROM bot_orders WHERE account_key = %s AND status = 'settled' "
                "ORDER BY entry_sequence DESC, submitted_at DESC, intent_id DESC LIMIT 50", (key,),
            )
            audit = self.connection.execute(
                "SELECT payload FROM bot_recovery_events WHERE account_key = %s ORDER BY id DESC LIMIT 100",
                (key,),
            )
            state["pending"] = dict(pending.fetchall())
            state["trades_log"] = [row[0] for row in trades.fetchall()]
            state["recovery_audit"] = [row[0] for row in reversed(audit.fetchall())]
        return state

    def load(self, key, defaults):
        with self.lock, self.connection.transaction():
            self.connection.execute(
                "INSERT INTO bot_accounts (account_key, state) VALUES (%s, %s) "
                "ON CONFLICT (account_key) DO NOTHING", (key, Jsonb(defaults)),
            )
            state, normalized = self.connection.execute(
                "SELECT state, normalized FROM bot_accounts WHERE account_key = %s FOR UPDATE", (key,),
            ).fetchone()
            if not normalized:
                state = self._migrate(key, state)
            return self._read(key, state)

    def _assert_unused(self, keys, order_id):
        used = self.connection.execute(
            "SELECT 1 FROM bot_orders WHERE account_key = ANY(%s) AND order_id = %s "
            "UNION ALL SELECT 1 FROM bot_recovery_events WHERE account_key = ANY(%s) "
            "AND order_id = %s AND action NOT IN ('reviewed','refused') LIMIT 1",
            (list(keys), str(order_id), list(keys), str(order_id)),
        ).fetchone()
        if used:
            raise RuntimeError("Broker order is already mapped or resolved")

    def _loss_streak(self, key, baseline=-1):
        # Find the last non-loss with an indexed seek, then count only its
        # trailing losses in submission order, including settlements older
        # than the bounded worker window.
        boundary = self.connection.execute(
            "SELECT * FROM ("
            "(SELECT entry_sequence, submitted_at, intent_id FROM bot_orders "
            "WHERE account_key = %s AND status = 'settled' AND result = 'win' "
            "ORDER BY entry_sequence DESC, submitted_at DESC, intent_id DESC LIMIT 1) UNION ALL "
            "(SELECT entry_sequence, submitted_at, intent_id FROM bot_orders "
            "WHERE account_key = %s AND status = 'settled' AND result = 'equal' "
            "ORDER BY entry_sequence DESC, submitted_at DESC, intent_id DESC LIMIT 1)"
            ") boundaries ORDER BY entry_sequence DESC, submitted_at DESC, intent_id DESC LIMIT 1",
            (key, key),
        ).fetchone()
        sql = ("SELECT count(*) FROM bot_orders WHERE account_key = %s "
               "AND status = 'settled' AND result = 'loss' AND entry_sequence > %s")
        args = (key, baseline)
        if boundary:
            sql += " AND (entry_sequence, submitted_at, intent_id) > (%s,%s,%s)"
            args += tuple(boundary)
        return self.connection.execute(sql, args).fetchone()[0]

    def change(self, key, mutate, order_id=None, account_keys=()):
        """Small account state and changed journal rows commit together."""
        # Batch writes/reloads but still await COMMIT before returning an intent
        # to submission. A pipeline failure rolls the entire transaction back.
        with self.lock, self.connection.transaction(), self.connection.pipeline():
            if order_id is not None:
                rows = self.connection.execute(
                    "SELECT account_key FROM bot_accounts WHERE account_key = ANY(%s) "
                    "ORDER BY account_key FOR UPDATE", (list(account_keys),),
                ).fetchall()
                if len(rows) != len(set(account_keys)) or key not in account_keys:
                    raise RuntimeError("Recovery account ledgers are unavailable")
                self._assert_unused(account_keys, order_id)
            row = self.connection.execute(
                "SELECT state, normalized FROM bot_accounts WHERE account_key = %s FOR UPDATE", (key,),
            ).fetchone()
            if row is None or not row[1]:
                raise RuntimeError("Account ledger was not loaded")
            before = self._read(key, row[0])
            state = copy.deepcopy(before)
            mutate(state)
            settled = False
            # Existing terminal rows are immutable. Only newly settled entries
            # and changed pending intents need writes; old history is untouched.
            old_trades = {item.get("intentId") for item in before["trades_log"]}
            for meta in state["trades_log"]:
                token = meta.get("intentId")
                if token not in old_trades:
                    if token not in before["pending"]:
                        raise RuntimeError("Settlement has no durable pending intent")
                    self._write_order(key, token, meta, "settled")
                    settled = True
            settled_tokens = {item.get("intentId") for item in state["trades_log"]}
            for token, meta in state["pending"].items():
                if meta != before["pending"].get(token):
                    if meta.get("orderId") is not None and (
                        before["pending"].get(token, {}).get("orderId") != meta["orderId"]
                    ):
                        self._assert_unused([key], meta["orderId"])
                    self._write_order(key, token, meta, "pending")
            for token in before["pending"].keys() - state["pending"].keys() - settled_tokens:
                self.connection.execute(
                    "UPDATE bot_orders SET status = 'rejected' "
                    "WHERE account_key = %s AND intent_id = %s AND status = 'pending'", (key, token),
                )
            for event in state["recovery_audit"][len(before["recovery_audit"]):]:
                self._write_audit(key, event)
            if settled:
                state["loss_streak"] = self._loss_streak(key, state.get("loss_streak_reset_sequence", -1))
                if not state.get("cooldown_until", 0):
                    refresh(state, time.time())
            compact = {k: v for k, v in state.items() if k not in ("pending", "trades_log", "recovery_audit")}
            self.connection.execute(
                "UPDATE bot_accounts SET state = %s WHERE account_key = %s", (Jsonb(compact), key),
            )
            return self._read(key, compact)

    def history(self, key, cursor=None, limit=50, timing_filter=None):
        if not 1 <= limit <= 100 or (cursor is not None and not 0 < int(cursor) <= 9223372036854775807):
            raise ValueError("Invalid pagination parameters")
        if timing_filter is not None and timing_filter not in TIMING_FILTERS:
            raise ValueError("Invalid saved timing filter")
        with self.lock:
            sql = "SELECT id, payload FROM bot_orders WHERE account_key = %s AND status = 'settled'"
            args = (key,)
            if timing_filter is not None:
                sql += " AND (" + TIMING_FILTERS[timing_filter] + ")"
            if cursor is not None:
                sql += " AND id < %s"
                args += (int(cursor),)
            rows = self.connection.execute(sql + " ORDER BY id DESC LIMIT %s", args + (limit + 1,)).fetchall()
            return {
                "items": [{**meta, "id": str(row_id), "brokerTiming": history_timing(meta)}
                          for row_id, meta in rows[:limit]],
                "nextCursor": str(rows[limit - 1][0]) if len(rows) > limit else None,
                "accountMode": key.split(":")[0], "balanceId": int(key.split(":")[1]),
            }

    def timing_summary(self, key):
        """One complete account/balance snapshot, saved annotations only."""
        with self.lock:
            counts = self.connection.execute(
                "SELECT count(*), "
                "count(*) FILTER (WHERE timing_status='within_2s'), "
                "count(*) FILTER (WHERE timing_status='late_after_2s'), "
                "count(*) FILTER (WHERE timing_status NOT IN ('within_2s','late_after_2s') "
                "AND checked) "
                f"FROM (SELECT {TIMING_STATUS_SQL} AS timing_status, "
                f"({TIMING_CHECKED_SQL}) AS checked "
                "FROM bot_orders WHERE account_key=%s AND status='settled') saved",
                (key,),
            ).fetchone()
            return timing_summary(key, *counts)

    def timing_candidate(self, key, retry_before):
        """Complete archive, not the latest-50 cache; never reconstruct old candles."""
        with self.lock:
            return self.connection.execute(
                "SELECT id,payload FROM bot_orders WHERE account_key=%s AND status='settled' "
                "AND payload->>'orderId' IS NOT NULL "
                "AND payload->'entryContext'->>'execution_candle' IS NOT NULL "
                "AND COALESCE(payload->'brokerTiming'->>'status','unknown')='unknown' "
                "AND COALESCE((payload->'brokerTiming'->>'checkedAt')::double precision,0)<%s "
                "ORDER BY COALESCE((payload->'brokerTiming'->>'checkedAt')::double precision,0),id "
                "LIMIT 1", (key, retry_before),
            ).fetchone()

    def record_timing(self, key, row_id, audit):
        """Only observational metadata; no account, outcome, counter or intent writes."""
        with self.lock:
            self.connection.execute(
                "UPDATE bot_orders SET payload=jsonb_set(payload,'{brokerTiming}',%s) "
                "WHERE account_key=%s AND id=%s AND status='settled' "
                "AND COALESCE(payload->'brokerTiming'->>'status','unknown')='unknown'",
                (Jsonb(audit), key, row_id),
            )

    def check(self):
        with self.lock:
            # Never silently reconnect: a dead session lost worker ownership.
            self.connection.execute("SELECT 1")

    def record_skip(self, key, fingerprint, event, intent_id=None):
        """Audit only: never modifies orders, accounts, limits or recovery state."""
        with self.lock:
            self.connection.execute(
                "INSERT INTO bot_skipped_signals "
                "(account_key,signal_key,intent_id,observed_at,payload) VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (account_key,signal_key) DO NOTHING",
                (key, fingerprint, intent_id, event["time"], Jsonb(event)),
            )

    def rejection_summary(self, key, start, end):
        """Indexed saved audits only. No broker reads, shadow outcomes or replay."""
        from rejection_summary import project_groups
        with self.lock, self.connection.transaction():
            self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            rows = self.connection.execute(
                "SELECT payload->>'pair',payload->>'reason_blocked',"
                "payload->>'validation_policy',"
                "(jsonb_typeof(payload->'practice_validation')='object' AND "
                "payload->'practice_validation'->>'zone' IN ('CLEAR','CHOPPY','ACCUMULATION')),count(*) "
                "FROM bot_skipped_signals WHERE account_key=%s "
                "AND observed_at >= %s AND observed_at < %s "
                "GROUP BY 1,2,3,4 ORDER BY count(*) DESC,1,2,3,4",
                (key, start, end),
            ).fetchall()
            legacy = self.connection.execute(
                "SELECT count(*) FROM bot_orders o WHERE account_key=%s "
                "AND status='rejected' AND submitted_at >= %s AND submitted_at < %s "
                "AND NOT EXISTS (SELECT 1 FROM bot_skipped_signals s "
                "WHERE s.account_key=o.account_key AND s.intent_id=o.intent_id)",
                (key, start, end),
            ).fetchone()[0]
        return {**project_groups(rows), "legacyRejectedIntents": legacy}

    def resolve_skip_shadows(self, key, pair, candle, unused=None):
        """Observed next-candle direction only; never a broker outcome or an order."""
        values = [candle.get("time"), candle.get("open"), candle.get("close")]
        if not all(number(v) for v in values) or min(values[1:]) <= 0:
            return
        with self.lock, self.connection.transaction():
            rows = self.connection.execute(
                "SELECT id,payload FROM bot_skipped_signals WHERE account_key=%s "
                "AND payload->>'pair'=%s AND payload->'shadow_expected_time'=%s "
                "AND NOT (payload ? 'shadow_result') FOR UPDATE",
                (key, pair, Jsonb(candle["time"])),
            ).fetchall()
            for row_id, payload in rows:
                side = payload.get("side")
                if side not in ("CALL", "PUT"):
                    continue
                won = (candle["close"] > candle["open"] if side == "CALL"
                       else candle["close"] < candle["open"])
                payload.update(shadow_result="WIN" if won else "LOSS",
                               shadow_candle={k: candle[k] for k in ("time", "open", "close")})
                self.connection.execute(
                    "UPDATE bot_skipped_signals SET payload=%s WHERE id=%s AND account_key=%s",
                    (Jsonb(payload), row_id, key))

    def export_rows(self, key, skipped=False):
        """One database statement/snapshot, without the worker's 50-row limit."""
        with self.lock:
            if not skipped:
                return [row[0] for row in self.connection.execute(
                    "SELECT payload FROM bot_orders WHERE account_key=%s AND status='settled' "
                    "ORDER BY submitted_at,id", (key,),
                ).fetchall()]
            return [row[0] for row in self.connection.execute(
                "SELECT payload FROM ("
                "SELECT payload,observed_at AS stamp,id FROM bot_skipped_signals WHERE account_key=%s "
                "UNION ALL SELECT payload || jsonb_build_object('reason_blocked',"
                "'Legacy rejected intent: blocking reason was not recorded'), submitted_at,id "
                "FROM bot_orders o WHERE account_key=%s AND status='rejected' AND NOT EXISTS "
                "(SELECT 1 FROM bot_skipped_signals s WHERE s.account_key=o.account_key "
                "AND s.intent_id=o.intent_id)"
                ") entries ORDER BY stamp,id", (key, key),
            ).fetchall()]

    def audit_history(self, keys):
        # Preserve the existing recovery endpoint's complete audit response.
        # Reading the archive on demand must not enlarge per-write state.
        with self.lock:
            return sorted([row[0] for row in self.connection.execute(
                "SELECT payload FROM bot_recovery_events WHERE account_key = ANY(%s) ORDER BY id DESC",
                (list(keys),),
            ).fetchall()], key=lambda event: event["at"], reverse=True)

    def performance(self, key):
        # Use database aggregates rather than the bounded worker history.
        # One snapshot keeps totals and recent rows consistent with settlement.
        with self.lock, self.connection.transaction():
            self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            total, wins, losses, equal, profit, valid = self.connection.execute(
                "SELECT count(*), count(*) FILTER (WHERE result = 'win'), "
                "count(*) FILTER (WHERE result = 'loss'), count(*) FILTER (WHERE result = 'equal'), "
                "coalesce(sum((payload->>'profit')::numeric),0), "
                "count(*) FILTER (WHERE jsonb_typeof(payload->'profit') = 'number' "
                "AND result IN ('win','loss','equal')) "
                "FROM bot_orders WHERE account_key = %s AND status = 'settled'", (key,),
            ).fetchone()
            if valid != total:
                raise ValueError("Invalid confirmed trade performance")
            rows = self.connection.execute(
                "SELECT payload FROM bot_orders WHERE account_key = %s AND status = 'settled' "
                "ORDER BY coalesce((payload->>'settledAt')::double precision, submitted_at) DESC, "
                "submitted_at DESC, id DESC LIMIT 8", (key,),
            ).fetchall()
            return {
                "accountMode": key.split(":")[0], "total": total, "wins": wins,
                "losses": losses, "breakEven": equal,
                "cumulativePL": float(profit.quantize(Decimal("0.01"))),
                "recentTrades": [
                    {**trade, "id": str(trade.get("orderId")
                     or f"{trade['pair']}:{trade['time']}:{trade['side']}")}
                    for (trade,) in rows
                ],
            }

    def close(self):
        self.connection.close()
