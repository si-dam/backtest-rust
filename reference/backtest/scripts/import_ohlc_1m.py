from __future__ import annotations

import argparse
from pathlib import Path

from app.config import get_settings
from app.services.aggregation import ensure_bars
from app.services.ingest import infer_symbol_from_filename, read_market_data_file, upsert_ohlc_1m_bars
from app.services.market_processing import TIMEFRAME_RULES, normalize_sierra_ohlc_1m


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Sierra 1-minute OHLC BarData into DuckDB bars table.")
    parser.add_argument(
        "--file",
        default="data/NQH26.scid_BarData.txt",
        help="Path to Sierra 1m OHLC file (default: data/NQH26.scid_BarData.txt)",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Optional symbol_contract override (default: infer from filename)",
    )
    parser.add_argument(
        "--rebuild-higher-timeframes",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Rebuild non-1m timeframes after import (default: true)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()

    path = Path(args.file)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    symbol_hint = str(args.symbol).strip() if args.symbol else infer_symbol_from_filename(path)
    raw = read_market_data_file(path)
    bars_1m = normalize_sierra_ohlc_1m(raw, dataset_tz=settings.dataset_tz, symbol_contract=symbol_hint)
    if bars_1m.empty:
        print("No rows imported.")
        return 0

    symbol = str(bars_1m["symbol_contract"].iloc[0])
    start = bars_1m["ts"].iloc[0].to_pydatetime()
    end = bars_1m["ts"].iloc[-1].to_pydatetime()

    imported_rows = upsert_ohlc_1m_bars(bars_1m, symbol_contract=symbol)

    rebuilt = []
    if args.rebuild_higher_timeframes:
        for timeframe in TIMEFRAME_RULES:
            if timeframe == "1m":
                continue
            ensure_bars(
                symbol_contract=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
                force_recompute=True,
            )
            rebuilt.append(timeframe)

    print(f"Imported {imported_rows} rows into bars(1m) for {symbol}.")
    print(f"Range: {start.isoformat()} -> {end.isoformat()}")
    if rebuilt:
        print(f"Rebuilt higher timeframes: {', '.join(rebuilt)}")
    else:
        print("Skipped higher timeframe rebuild.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
