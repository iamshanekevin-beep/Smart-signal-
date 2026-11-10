"""Independent read-only pair monitors feeding one bounded execution queue.

Workers never reserve funds, submit orders, reconnect, or settle trades. Engine
locks are pair-local; no worker acquires the account/control lock. The executor
must validate the queued session/account/run token and all risk checks again.
"""
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from entry_confirmation import ENTRY_WINDOW_SECONDS, LATE_ORDER_REASON, number


@dataclass(frozen=True)
class QueuedEntry:
    pair: str
    engine: object
    token: tuple
    entry: dict


class PairMonitoring:
    def __init__(self, context, observe, live_observe, on_candle, on_failure,
                 background=True, interval=0.05, capacity=40, clock=time.time, on_skip=None,
                 on_signal=None):
        self.context = context
        self.observe = observe
        self.live_observe = live_observe
        self.on_candle = on_candle
        self.on_failure = on_failure
        self.on_skip = on_skip
        self.on_signal = on_signal
        self.background = background
        self.interval = interval
        self.capacity = capacity
        self.clock = clock
        self._lock = threading.RLock()
        self._available = threading.Event()
        self._workers = {}
        self._signals = OrderedDict()
        self._errors = []
        self._stopped = False

    def report_skip(self, pair, entry, reason, token):
        if self.on_skip:
            try:
                self.on_skip(pair, dict(entry), reason, token)
            except Exception:
                pass  # Observers cannot pause monitors or alter trading decisions.

    def configure(self, pairs, api, generation):
        """Replace subscriptions without waiting on another pair's observation."""
        removed, added = [], []
        with self._lock:
            if self._stopped:
                return
            for pair, worker in list(self._workers.items()):
                if (pairs.get(pair) is not worker.engine or worker.api is not api
                        or worker.generation != generation):
                    removed.append(self._workers.pop(pair))
                    worker.stop.set()
                    for key in list(self._signals):
                        if key[0] == pair:
                            del self._signals[key]
            if not self._signals:
                self._available.clear()
            for pair, engine in pairs.items():
                if pair not in self._workers:
                    worker = _PairWorker(self, pair, engine, api, generation)
                    self._workers[pair] = worker
                    added.append(worker)
        for worker in removed:
            worker.join()
        for worker in added:
            if self.background:
                worker.start()

    def poll_once(self):
        """Deterministic execution of the same monitors for offline tests only."""
        if self.background:
            return
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            worker.poll()

    def enqueue(self, worker, token, entry):
        with self._lock:
            if self._stopped or self._workers.get(worker.pair) is not worker or worker.stop.is_set():
                return
            key = (worker.pair, entry["engine"])
            previous = self._signals.get(key)
            if self.on_skip and previous and (previous.token != token or
                             previous.entry.get("execution_candle") != entry.get("execution_candle")):
                self.report_skip(previous.pair, previous.entry,
                                 "Queued signal superseded by a newer decision candle", previous.token)
            if key not in self._signals and len(self._signals) >= self.capacity:
                raise RuntimeError("Pair signal queue capacity exceeded")
            # At most one waiting signal per pair/engine; newer candles replace
            # old signals rather than building an unbounded backlog.
            self._signals[key] = QueuedEntry(worker.pair, worker.engine, token, dict(entry))
            self._available.set()
            if self.on_signal:
                try:
                    # Observers must enqueue only, never perform network I/O here.
                    self.on_signal(worker.pair, dict(entry), token)
                except Exception:
                    pass  # Notification failures must never affect trading.

    def enqueue_entry(self, pair, engine, token, entry):
        """The executor can publish a freshly verified win into the same queue."""
        with self._lock:
            worker = self._workers.get(pair)
            if worker is not None and worker.engine is engine:
                self.enqueue(worker, token, entry)

    def wait_for_signal(self, timeout):
        self._available.wait(timeout)

    def take(self):
        with self._lock:
            if not self._signals:
                self._available.clear()
                return None
            signal = self._signals.popitem(last=False)[1]
            if not self._signals:
                self._available.clear()
            return signal

    def invalidate(self):
        with self._lock:
            for signal in self._signals.values():
                self.report_skip(signal.pair, signal.entry,
                                 "Queued signal invalidated by pause, account or session change", signal.token)
            self._signals.clear()
            self._available.clear()

    def snapshot(self):
        with self._lock:
            return {pair: worker.observed_at for pair, worker in self._workers.items()
                    if worker.observed_at is not None and not worker.stop.is_set()}

    def raise_if_failed(self):
        with self._lock:
            if self._errors:
                raise RuntimeError("Independent pair monitoring failed; entries are paused") from None

    def failed(self, worker, error):
        worker.stop.set()
        with self._lock:
            if self._workers.get(worker.pair) is not worker or self._stopped:
                return
            self._errors.append(type(error).__name__)  # Never retain broker exception text.
            self._signals.clear()
            self._available.set()
        self.on_failure()

    def close(self):
        with self._lock:
            self._stopped = True
            workers = list(self._workers.values())
            self._workers.clear()
            self._signals.clear()
            self._available.set()
            for worker in workers:
                worker.stop.set()
        for worker in workers:
            worker.join()


class _PairWorker:
    def __init__(self, owner, pair, engine, api, generation):
        self.owner, self.pair, self.engine = owner, pair, engine
        self.api, self.generation = api, generation
        self.stop = threading.Event()
        self.thread = None
        self.last_token = None
        self.observed_at = None

    def start(self):
        self.thread = threading.Thread(
            target=self.run, name=f"pair-monitor:{self.pair}", daemon=True)
        self.thread.start()

    def join(self):
        if self.thread is not None and self.thread is not threading.current_thread():
            self.thread.join(timeout=0.1)

    def run(self):
        while not self.stop.is_set():
            self.poll()
            self.stop.wait(self.owner.interval)

    def poll(self):
        try:
            token, allowed = self.owner.context()
            allowed = allowed and token[0] == self.generation
            closed = self.owner.observe(self.api, self.pair)
            if self.stop.is_set():
                return
            now = self.owner.clock()
            with self.engine.lock:
                if self.stop.is_set():
                    return
                current_token, current_allowed = self.owner.context()
                if current_token != token:
                    return  # Pause/reconnect/account changes invalidate in-flight reads.
                allowed = allowed and current_allowed
                if self.last_token is not None and token != self.last_token:
                    self.engine.discard_candidates()
                self.last_token = token
                if not allowed:
                    self.engine.discard_candidates()
                if allowed and self.owner.on_skip:
                    for candidate in (self.engine.fcb_candidate, self.engine.rsi_candidate,
                                      self.engine.reversal_candidate,
                                      self.engine.loss_reversal_candidate,
                                      self.engine.momentum_candidate):
                        if candidate and now > candidate["execution_candle"] + ENTRY_WINDOW_SECONDS:
                            self.owner.report_skip(self.pair, candidate,
                                                   LATE_ORDER_REASON, token)
                if closed:
                    self.engine.step(closed, now=now, allow_entries=allowed)
                    self.owner.on_candle(self.pair, closed)
                    self.observed_at = now
                if allowed:
                    if (self.engine.fcb_candidate is not None or self.engine.rsi_candidate is not None
                            or self.engine.reversal_candidate is not None
                            or self.engine.loss_reversal_candidate is not None
                            or self.engine.pending_loss_reversal is not None
                            or self.engine.momentum_candidate is not None
                            or self.engine.rsi_loss is not None):
                        live, broker_now = self.owner.live_observe(self.api, self.pair)
                        now = self.owner.clock()
                        if self.owner.on_skip:
                            for candidate in (self.engine.fcb_candidate, self.engine.rsi_candidate,
                                              self.engine.reversal_candidate,
                                              self.engine.loss_reversal_candidate,
                                              self.engine.momentum_candidate):
                                if candidate and (now > candidate["execution_candle"] + ENTRY_WINDOW_SECONDS
                                                  or number(broker_now) and broker_now > candidate["execution_candle"] + ENTRY_WINDOW_SECONDS):
                                    self.owner.report_skip(self.pair, candidate,
                                                           LATE_ORDER_REASON, token)
                        self.engine.confirm_fcb(live, now, broker_now)
                        self.engine.confirm_rsi(live, now, broker_now)
                    for entry in self.engine.take_entries(now):
                        self.owner.enqueue(self, token, entry)
        except Exception as error:
            self.owner.failed(self, error)