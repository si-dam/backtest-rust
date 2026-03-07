from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.duck import get_duckdb_connection
from app.db.models import IngestedFile, Job
from app.services.aggregation import (
    clear_runtime_caches,
    ensure_bars,
    ensure_large_orders,
    ensure_session_profile,
    load_bars,
    precompute_non_time_bar_presets,
    upsert_bars,
)
from app.services.market_processing import TIMEFRAME_RULES, normalize_sierra_market_data
from app.services.vwap import clear_vwap_runtime_caches

SUPPORTED_INGEST_SUFFIXES = (".csv", ".txt")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def infer_symbol_from_filename(path: Path) -> str:
    stem = path.stem.upper()
    for token in ("NQ", "ES"):
        if token in stem:
            idx = stem.find(token)
            return stem[idx : idx + 4].strip("_-")
    return "UNKNOWN"


def insert_ticks(df: pd.DataFrame, source_file: str) -> int:
    row_count = 0
    con = get_duckdb_connection()
    try:
        payload = df.copy()
        payload["source_file"] = source_file
        con.register("ticks_df", payload)
        con.execute(
            """
            INSERT INTO ticks (ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price, source_file)
            SELECT ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price, source_file
            FROM ticks_df
            """
        )
        con.unregister("ticks_df")
        row_count = len(payload)
    finally:
        con.close()
    clear_runtime_caches()
    clear_vwap_runtime_caches()
    return row_count


def upsert_ohlc_1m_bars(df: pd.DataFrame, symbol_contract: str) -> int:
    if df.empty:
        return 0
    payload = df.sort_values("ts").reset_index(drop=True)
    start = payload["ts"].iloc[0].to_pydatetime()
    end = payload["ts"].iloc[-1].to_pydatetime()
    upsert_bars(payload, symbol_contract=symbol_contract, timeframe="1m", start=start, end=end)
    clear_runtime_caches()
    clear_vwap_runtime_caches()
    return len(payload)


def rebuild_higher_timeframes_from_1m(
    symbol_contract: str,
    start: datetime,
    end: datetime,
    dataset_tz: str,
) -> list[dict]:
    source = load_bars(symbol_contract=symbol_contract, timeframe="1m", start=start, end=end)
    if source.empty:
        return []

    df = source.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values("ts").set_index("ts")
    if df.empty:
        return []

    zone = ZoneInfo(dataset_tz)
    output_columns = ["ts", "session_date", "timeframe", "symbol_contract", "open", "high", "low", "close", "volume", "trade_count"]
    rebuilt: list[dict] = []
    for timeframe, rule in TIMEFRAME_RULES.items():
        if timeframe == "1m" or str(timeframe).endswith("s"):
            continue
        agg = df.resample(rule).agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            trade_count=("trade_count", "sum"),
        )
        agg = agg.dropna(subset=["open", "high", "low", "close"]).reset_index()
        agg["symbol_contract"] = symbol_contract
        agg["timeframe"] = timeframe
        agg["session_date"] = agg["ts"].dt.tz_convert(zone).dt.date
        frame = agg[output_columns] if not agg.empty else pd.DataFrame(columns=output_columns)
        upsert_bars(frame, symbol_contract=symbol_contract, timeframe=timeframe, start=start, end=end)
        rebuilt.append({"timeframe": timeframe, "row_count": int(len(frame))})
    return rebuilt


def delete_source_ticks(source_file: str) -> None:
    con = get_duckdb_connection()
    try:
        con.execute("DELETE FROM ticks WHERE source_file = ?", [source_file])
    finally:
        con.close()
    clear_runtime_caches()
    clear_vwap_runtime_caches()


def rebuild_source_files(current_name: str, existing_name: str | None) -> set[str]:
    names = {current_name}
    if existing_name:
        names.add(existing_name)
    return names


def is_supported_ingest_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in SUPPORTED_INGEST_SUFFIXES


def list_watch_market_files(watch_dir: Path) -> list[Path]:
    watch_dir.mkdir(parents=True, exist_ok=True)
    files = [p for p in watch_dir.rglob("*") if is_supported_ingest_file(p)]
    return sorted(files, key=lambda p: str(p.relative_to(watch_dir)).lower())


def _detect_delimiter(path: Path, sample_bytes: int = 65536) -> str | None:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        sample = f.read(sample_bytes)

    if not sample.strip():
        return None

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        return dialect.delimiter
    except csv.Error:
        return None


def read_market_data_file(path: Path, nrows: int | None = None) -> pd.DataFrame:
    read_kwargs = {"engine": "python"}
    delimiter = _detect_delimiter(path)
    if delimiter:
        read_kwargs["sep"] = delimiter
    if nrows is not None:
        read_kwargs["nrows"] = nrows
    return pd.read_csv(path, **read_kwargs)


def validate_market_data_file(
    path: Path,
    dataset_tz: str,
    symbol_contract: str | None = None,
    sample_rows: int = 5000,
) -> dict:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    if not is_supported_ingest_file(path):
        raise ValueError(f"Unsupported file extension for {path.name}; expected .csv or .txt")

    raw = read_market_data_file(path, nrows=sample_rows)
    if raw.empty:
        raise ValueError("File has no rows.")

    inferred_symbol = symbol_contract or infer_symbol_from_filename(path)
    data_kind, normalized = normalize_sierra_market_data(raw, dataset_tz=dataset_tz, symbol_contract=inferred_symbol)
    if normalized.empty:
        raise ValueError("No valid rows after normalization.")

    return {
        "file_name": path.name,
        "data_kind": data_kind,
        "sample_rows": len(raw),
        "normalized_rows": len(normalized),
        "symbol_contract": str(normalized["symbol_contract"].iloc[0]),
    }


def process_csv_ingest(
    db: Session,
    job: Job,
    file_path: str,
    symbol_contract: str | None,
    rebuild: bool,
) -> dict:
    settings = get_settings()
    path = Path(file_path)

    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    file_hash = sha256_file(path)
    existing = db.scalar(select(IngestedFile).where(IngestedFile.file_hash == file_hash))
    if existing and not rebuild:
        return {
            "status": "skipped",
            "reason": "duplicate_file_hash",
            "ingested_file_id": str(existing.id),
            "data_kind": "unknown",
            "symbol_contract": existing.symbol_contract,
            "row_count": existing.row_count,
            "tick_row_count": 0,
            "bar_row_count": 0,
            "non_time_bar_presets": {"symbol_contract": existing.symbol_contract, "full_rebuild": False, "presets": []},
        }

    raw = read_market_data_file(path)
    inferred_symbol = symbol_contract or infer_symbol_from_filename(path)
    data_kind, normalized = normalize_sierra_market_data(raw, dataset_tz=settings.dataset_tz, symbol_contract=inferred_symbol)

    if rebuild and data_kind == "ticks":
        for source_name in rebuild_source_files(path.name, existing.filename if existing else None):
            delete_source_ticks(source_name)

    normalized_symbol = str(normalized["symbol_contract"].iloc[0]) if not normalized.empty else inferred_symbol

    if data_kind == "ticks":
        tick_row_count = insert_ticks(normalized, source_file=path.name)
        bar_row_count = 0
    else:
        tick_row_count = 0
        bar_row_count = upsert_ohlc_1m_bars(normalized, symbol_contract=normalized_symbol)
    row_count = tick_row_count + bar_row_count

    symbol = normalized_symbol if row_count else inferred_symbol
    start = normalized["ts"].iloc[0].to_pydatetime() if row_count else None
    end = normalized["ts"].iloc[-1].to_pydatetime() if row_count else None
    rebuilt_timeframes: list[dict] = []

    if start and end:
        if data_kind == "ticks":
            for timeframe in TIMEFRAME_RULES:
                ensure_bars(symbol_contract=symbol, timeframe=timeframe, start=start, end=end)

            session_dates = sorted({str(x) for x in normalized["session_date"].unique()})
            for session_date in session_dates:
                ensure_session_profile(symbol_contract=symbol, session_date=session_date)

            ensure_large_orders(
                symbol_contract=symbol,
                start=start,
                end=end,
                method="fixed",
                fixed_threshold=25.0,
                percentile=99.0,
            )
            ensure_large_orders(
                symbol_contract=symbol,
                start=start,
                end=end,
                method="relative",
                fixed_threshold=25.0,
                percentile=99.0,
            )
            non_time_bar_presets = precompute_non_time_bar_presets(symbol_contract=symbol, full_rebuild=bool(rebuild))
        else:
            rebuilt_timeframes = rebuild_higher_timeframes_from_1m(
                symbol_contract=symbol,
                start=start,
                end=end,
                dataset_tz=settings.dataset_tz,
            )
            non_time_bar_presets = {"symbol_contract": symbol, "full_rebuild": bool(rebuild), "presets": []}
    else:
        non_time_bar_presets = {"symbol_contract": symbol, "full_rebuild": bool(rebuild), "presets": []}

    if existing and rebuild:
        existing.filename = path.name
        existing.symbol_contract = symbol
        existing.row_count = row_count
        existing.job_id = job.id
        existing.created_at = datetime.now(timezone.utc)
        ingested_ref = existing
    else:
        ingested_ref = IngestedFile(
            job_id=job.id,
            filename=path.name,
            file_hash=file_hash,
            symbol_contract=symbol,
            row_count=row_count,
        )
        db.add(ingested_ref)

    db.commit()

    return {
        "status": "ingested",
        "file_name": path.name,
        "data_kind": data_kind,
        "symbol_contract": symbol,
        "row_count": row_count,
        "tick_row_count": tick_row_count,
        "bar_row_count": bar_row_count,
        "non_time_bar_presets": non_time_bar_presets,
        "rebuilt_timeframes": rebuilt_timeframes if data_kind == "ohlc_1m" else [],
        "time_range": {
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        },
    }
