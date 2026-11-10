"""Quadcode Fractal Chaos Bands, matching the user's chart script.

Source: quadcode-tech/quadcodescript-library, other/fractal_chaos_bands.lua.
The center of five bars is a fractal with inclusive comparisons. Levels carry
forward, rather than being replaced with a rolling channel. Only closed bars
commit state; the forming-bar preview can veto an entry but cannot arm one.
"""
from collections import deque
from entry_confirmation import number


class FractalBands:
    def __init__(self):
        self.window = deque(maxlen=4)
        self.upper = self.lower = None
        self.last_time = None

    @staticmethod
    def _bar(candle):
        high = candle.get("high", candle.get("max"))
        low = candle.get("low", candle.get("min"))
        stamp = candle.get("time")
        if not all(number(v) for v in (high, low, stamp)) or high < low:
            raise ValueError("Invalid fractal candle")
        return {"time": stamp, "high": high, "low": low}

    def _levels(self, bar):
        upper, lower = self.upper, self.lower
        if len(self.window) == 4:
            rows = [*self.window, bar]
            center = rows[2]
            if all(row["high"] <= center["high"] for row in rows):
                upper = center["high"]
            if all(row["low"] >= center["low"] for row in rows):
                lower = center["low"]
        return upper, lower

    def update(self, candle):
        bar = self._bar(candle)
        if self.last_time is not None:
            if bar["time"] <= self.last_time:
                return self.upper, self.lower
            if bar["time"] != self.last_time + 60:
                # Missing candles may hide a new fractal: never reuse stale levels.
                self.window.clear()
                self.upper = self.lower = None
        self.upper, self.lower = self._levels(bar)
        self.window.append(bar)
        self.last_time = bar["time"]
        return self.upper, self.lower

    def preview(self, candle):
        """Evaluate the chart's forming bar without committing provisional levels."""
        try:
            bar = self._bar(candle)
        except (AttributeError, ValueError):
            return None, None
        if self.last_time is None or bar["time"] != self.last_time + 60:
            return None, None
        return self._levels(bar)