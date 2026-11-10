"""Submit only explicitly timed one-minute turbo options."""
import time

from iqoptionapi.stable_api import IQ_Option
from entry_confirmation import ENTRY_WINDOW_SECONDS, LATE_ORDER_REASON, number


def one_minute_expiry(broker_now, local_now):
    """The entry candle's close, never a duration selected by local-clock search."""
    if not all(number(value) and value > 0 for value in (broker_now, local_now)):
        return None
    slot = int(broker_now // 60) * 60
    if not (0 <= broker_now - slot <= ENTRY_WINDOW_SECONDS
            and 0 <= local_now - slot <= ENTRY_WINDOW_SECONDS):
        return None
    return slot + 60


class OneMinuteIQOption(IQ_Option):
    def buy(self, price, active, direction, expirations):
        # Keep the established buy(success, order_id) interface and the one
        # shared executor, but bypass the library's expiry/product auto-selection.
        if isinstance(expirations, bool) or expirations != 1:
            return False, "Only one-minute turbo orders are supported"
        if direction not in ("call", "put"):
            return False, "Order direction must be CALL or PUT"
        expired = one_minute_expiry(self.get_server_timestamp(), time.time())
        if expired is None:
            # Definitely not sent. The executor can safely release this intent.
            return False, LATE_ORDER_REASON
        return self.buy_by_raw_expirations(price, active, direction, "turbo", expired)
