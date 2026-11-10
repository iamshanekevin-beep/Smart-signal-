"""Stable mixed-pair selection and one-time adoption of scan settings."""

DEFAULT_COUNT = 15
SCAN_POLICY = "available-mixed-fifteen"


def select_pairs(previous, available, pool, count=DEFAULT_COUNT):
    """Retain open subscriptions, replace closed ones, and never invent assets."""
    eligible = {pair for pair in pool if pair in available}
    selected = []
    for pair in [*previous, *pool]:
        if len(selected) >= count:
            break
        if pair in eligible and pair not in selected:
            selected.append(pair)
    return selected


def adopt_scan_policy(ledger, mode):
    """Apply ten once to older saved configurations; preserve later manual edits."""
    if ledger.accounts[mode].get("scan_policy") == SCAN_POLICY:
        return
    def apply(state):
        state["settings"]["activePairs"] = DEFAULT_COUNT
        state["scan_policy"] = SCAN_POLICY
    ledger.change(mode, apply)
