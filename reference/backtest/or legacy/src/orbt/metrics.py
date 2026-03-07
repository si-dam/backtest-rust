from __future__ import annotations

import pandas as pd


def summarize_trades(trades: pd.DataFrame) -> dict[str, float | int]:
    if trades.empty:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "full_tp_wins": 0,
            "full_losses": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "avg_pnl": 0.0,
            "total_r": 0.0,
            "avg_r": 0.0,
        }

    wins = int((trades["pnl"] > 0).sum())
    losses = int((trades["pnl"] < 0).sum())
    full_tp_wins = int(((trades["exit_reason"] == "target") & (trades["pnl"] > 0)).sum())
    full_losses = int(((trades["exit_reason"] == "stop") & (trades["pnl"] < 0)).sum())
    total = int(len(trades))

    return {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "full_tp_wins": full_tp_wins,
        "full_losses": full_losses,
        "win_rate": float(wins / total),
        "total_pnl": float(trades["pnl"].sum()),
        "avg_pnl": float(trades["pnl"].mean()),
        "total_r": float(trades["r_multiple"].sum()),
        "avg_r": float(trades["r_multiple"].mean()),
    }


def equity_curve_from_trades(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["time", "equity"])

    eq = trades.sort_values("exit_time").copy()
    eq["equity"] = eq["pnl"].cumsum()
    return eq[["exit_time", "equity"]].rename(columns={"exit_time": "time"})
