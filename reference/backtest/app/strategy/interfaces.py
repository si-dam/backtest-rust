from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


@dataclass
class TickEvent:
    ts: datetime
    symbol_contract: str
    trade_price: float
    trade_size: float
    bid_price: float | None = None
    ask_price: float | None = None


@dataclass
class BarEvent:
    ts: datetime
    symbol_contract: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Order:
    symbol_contract: str
    side: str
    qty: float
    order_type: str = "market"
    limit_price: float | None = None


@dataclass
class FillConfig:
    commission_per_contract: float = 0.0
    slippage_ticks: float = 0.0
    tick_size: float = 0.25


@dataclass
class FillResult:
    filled: bool
    fill_price: float | None
    fill_qty: float
    fees: float
    reason: str = ""


@dataclass
class MarketSlice:
    ts: datetime
    last_price: float
    bid_price: float | None = None
    ask_price: float | None = None


@dataclass
class StrategyContext:
    symbol_contract: str
    timeframe: str
    params: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)


class Strategy(Protocol):
    id: str
    version: str
    config_model: type[Any]

    def on_start(self, ctx: StrategyContext) -> None: ...

    def on_bar(self, event: BarEvent, ctx: StrategyContext) -> None: ...

    def on_tick(self, event: TickEvent, ctx: StrategyContext) -> None: ...

    def on_end(self, ctx: StrategyContext) -> None: ...


class FillModel(Protocol):
    def simulate_fill(self, order: Order, market: MarketSlice, cfg: FillConfig) -> FillResult: ...


class MarketDataStore(Protocol):
    def get_bars(self, symbol_contract: str, timeframe: str, start: datetime, end: datetime): ...

    def get_ticks(self, symbol_contract: str, start: datetime, end: datetime): ...

    def get_session_profile(self, symbol_contract: str, session_date): ...
