"""Decision rules from the user's FAST TRADE upload.

Legacy math remains for uploaded-definition parity. The streaming adapter
supplies the user's later chart-matched fractal bands to FCBEngine.decide.
Broker execution belongs to the durable account executor.
"""
FCB_PERIOD = 5
RSI_PERIOD = 14
RSI_OB = 70
RSI_OS = 30


def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = losses = 0
    for i in range(1, period + 1):
        d = closes[-period - 1 + i] - closes[-period - 2 + i]
        if d > 0:
            gains += d
        else:
            losses += abs(d)
    avg_g = gains / period
    avg_l = losses / period
    if avg_l == 0:
        return 100
    rs = avg_g / avg_l
    return round(100 - 100 / (1 + rs), 2)


def calc_fcb(candles, period=5):
    if len(candles) < period + 1:
        return None, None
    lookback = candles[-(period + 1):-1]
    high_band = max(c["max"] for c in lookback)
    low_band = min(c["min"] for c in lookback)
    return high_band, low_band


class FCBEngine:
    def __init__(self):
        self.reset()

    def reset(self):
        self.trend = None
        self.active = False
        self.loss_watch = False

    def decide(self, candles, bands=None):
        if len(candles) < FCB_PERIOD + 2:
            return None
        last = candles[-1]
        close = last["close"]
        is_bull = close > last["open"]
        high_band, low_band = calc_fcb(candles, FCB_PERIOD) if bands is None else bands
        if high_band is None or low_band is None or high_band < low_band:
            return None
        above_band = close > high_band
        below_band = close < low_band
        inside = not above_band and not below_band
        if not self.active:
            if inside:
                return None
            if above_band:
                self.trend = "call"
                self.active = True
                self.loss_watch = False
                return "call"
            if below_band:
                self.trend = "put"
                self.active = True
                self.loss_watch = False
                return "put"
        if self.loss_watch:
            continues = (self.trend == "call" and above_band) or \
                        (self.trend == "put" and below_band)
            against = (self.trend == "call" and (below_band or (not is_bull and inside))) or \
                      (self.trend == "put" and (above_band or (is_bull and inside)))
            if continues:
                self.loss_watch = False
                return self.trend
            if against:
                self.reset()
                return None
            return None
        if self.active:
            if inside:
                return None
            continues = (self.trend == "call" and above_band) or \
                        (self.trend == "put" and below_band)
            reversed_ = (self.trend == "call" and below_band) or \
                        (self.trend == "put" and above_band)
            if continues:
                return self.trend
            if reversed_:
                self.loss_watch = True
                return None
        return None

    def report_result(self, result):
        if result == "loss" and self.active and not self.loss_watch:
            self.loss_watch = True
            return self.trend
        return None


class RSIEngine:
    def __init__(self):
        self.trades = 0
        self.wins = 0

    def decide(self, candles, fcb_loss_direction=None):
        closes = [c["close"] for c in candles]
        rsi = calc_rsi(closes, RSI_PERIOD)
        if rsi is None:
            return None
        if fcb_loss_direction is not None:
            return "put" if fcb_loss_direction == "call" else "call"
        if rsi <= RSI_OS:
            return "call"
        if rsi >= RSI_OB:
            return "put"
        return None

    def report_result(self, result):
        self.trades += 1
        if result == "win":
            self.wins += 1