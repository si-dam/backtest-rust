from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REQUIRED_COLUMNS = [
    "Date",
    "Time",
    "Open",
    "High",
    "Low",
    "Last",
    "Volume",
    "NumberOfTrades",
    "BidVolume",
    "AskVolume",
]


class SierraIngestError(ValueError):
    pass


def _ensure_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SierraIngestError(f"Missing required columns: {missing}")


def _normalize_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    dt_str = chunk["Date"].astype(str).str.strip() + " " + chunk["Time"].astype(str).str.strip()
    timestamp = pd.to_datetime(dt_str, format="mixed", errors="raise")

    out = pd.DataFrame(
        {
            "timestamp": timestamp,
            "open": pd.to_numeric(chunk["Open"], errors="raise"),
            "high": pd.to_numeric(chunk["High"], errors="raise"),
            "low": pd.to_numeric(chunk["Low"], errors="raise"),
            "last": pd.to_numeric(chunk["Last"], errors="raise"),
            "volume": pd.to_numeric(chunk["Volume"], errors="raise").astype("int64"),
            "number_of_trades": pd.to_numeric(chunk["NumberOfTrades"], errors="raise").astype("int64"),
            "bid_volume": pd.to_numeric(chunk["BidVolume"], errors="raise").astype("int64"),
            "ask_volume": pd.to_numeric(chunk["AskVolume"], errors="raise").astype("int64"),
        }
    )

    mismatch = (out["bid_volume"] + out["ask_volume"]) != out["volume"]
    if mismatch.any():
        bad = int(mismatch.sum())
        raise SierraIngestError(f"Volume mismatch: bid_volume + ask_volume != volume in {bad} rows")

    return out


def read_sierra_export(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    df = pd.read_csv(path, skipinitialspace=True)
    _ensure_columns(df)

    out = _normalize_chunk(df)
    out["seq"] = out.groupby("timestamp").cumcount()
    out = out.sort_values(["timestamp", "seq"], kind="stable").reset_index(drop=True)
    return out


def ingest_to_parquet(input_path: str | Path, output_path: str | Path) -> Path:
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    chunk_iter = pd.read_csv(input_path, skipinitialspace=True, chunksize=250_000)

    writer: pq.ParquetWriter | None = None
    carry_ts: pd.Timestamp | None = None
    carry_count = 0

    try:
        for i, raw_chunk in enumerate(chunk_iter):
            if i == 0:
                _ensure_columns(raw_chunk)

            chunk = _normalize_chunk(raw_chunk)
            chunk = chunk.sort_values("timestamp", kind="stable").reset_index(drop=True)
            chunk["seq"] = chunk.groupby("timestamp").cumcount()

            if carry_ts is not None:
                same_ts_mask = chunk["timestamp"] == carry_ts
                if same_ts_mask.any():
                    chunk.loc[same_ts_mask, "seq"] = chunk.loc[same_ts_mask, "seq"] + carry_count

            last_ts = chunk["timestamp"].iloc[-1]
            last_ts_count = int((chunk["timestamp"] == last_ts).sum())
            if carry_ts is not None and last_ts == carry_ts:
                carry_count += last_ts_count
            else:
                carry_ts = last_ts
                carry_count = last_ts_count

            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema, compression="snappy")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        raise SierraIngestError(f"No rows found in input file: {input_path}")

    return output_path
