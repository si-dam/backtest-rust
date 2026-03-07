from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator


class StrategyConfig(BaseModel):
    data_path: str
    ohlc_1m_path: str | None = None
    session_start: str = "09:30:00"
    session_end: str = "16:00:00"
    ib_windows_min: list[int] = Field(default_factory=lambda: [15, 30, 60])
    timeframes_min: list[int] = Field(default_factory=lambda: [1, 3, 5])
    big_trade_threshold: int = 25
    stop_mode: Literal["or_boundary", "or_mid"]
    tp_r_multiple: float = 2.0
    entry_mode: Literal["first_outside", "reentry_after_stop"] = "first_outside"
    strategy_mode: Literal["big_order_required", "breakout_only"] = "big_order_required"
    cost_model: Literal["none"] = "none"
    symbol: str = "NQ"

    @model_validator(mode="after")
    def validate_lists(self) -> "StrategyConfig":
        if not self.ib_windows_min:
            raise ValueError("ib_windows_min must not be empty")
        if not self.timeframes_min:
            raise ValueError("timeframes_min must not be empty")
        if any(v <= 0 for v in self.ib_windows_min):
            raise ValueError("ib_windows_min values must be positive")
        if any(v <= 0 for v in self.timeframes_min):
            raise ValueError("timeframes_min values must be positive")
        if self.big_trade_threshold <= 0:
            raise ValueError("big_trade_threshold must be positive")
        if self.tp_r_multiple <= 0:
            raise ValueError("tp_r_multiple must be positive")
        return self


def load_config(path: str | Path) -> StrategyConfig:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid config file: {path}")
    return StrategyConfig(**raw)
