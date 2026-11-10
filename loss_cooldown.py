"""Durable, account-local five-minute breaks after consecutive losses."""
COOLDOWN_SECONDS = 300


def recount_streak(state):
    baseline = state.get("loss_streak_reset_sequence", -1)
    state["loss_streak"] = 0
    for trade in state["trades_log"]:
        if trade.get("entrySequence", 0) <= baseline or trade["result"] != "loss":
            break
        state["loss_streak"] += 1


def needs_refresh(state, now):
    until = state.get("cooldown_until", 0)
    if until:
        return now >= until and not state["pending"]
    return state["loss_streak"] >= state["settings"].get("lossStreakStop", 3)


def refresh(state, now):
    until = state.get("cooldown_until", 0)
    if until:
        if now >= until and not state["pending"]:
            # Keep history/P&L/trade limits; reset only the completed loss run.
            state["loss_streak_reset_sequence"] = max(
                state.get("next_sequence", 0),
                max((trade.get("entrySequence", 0)
                     for trade in state["trades_log"]), default=0),
            )
            state["loss_streak"] = 0
            state["signal_resume_after"] = now
            state["cooldown_until"] = 0
    elif state["loss_streak"] >= state["settings"].get("lossStreakStop", 3):
        state["cooldown_until"] = now + COOLDOWN_SECONDS