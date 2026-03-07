from __future__ import annotations

import argparse
import json
import os
import sys
import tracemalloc
from datetime import timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.config import get_settings
from app.services.aggregation import clear_runtime_caches, ensure_preset_volume_profiles

PRESETS = ("week", "day", "rth", "eth")


def _deep_sizeof(obj: Any, seen: set[int] | None = None) -> int:
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        return size + sum(_deep_sizeof(key, seen) + _deep_sizeof(value, seen) for key, value in obj.items())
    if isinstance(obj, (list, tuple, set, frozenset)):
        return size + sum(_deep_sizeof(item, seen) for item in obj)
    if hasattr(obj, "__dict__"):
        return size + _deep_sizeof(vars(obj), seen)
    return size


def _bytes_human(n: int) -> str:
    value = float(n)
    units = ("B", "KiB", "MiB", "GiB")
    unit = units[0]
    for candidate in units:
        unit = candidate
        if value < 1024 or candidate == units[-1]:
            break
        value /= 1024.0
    return f"{value:,.2f} {unit}"


def _read_proc_mem() -> tuple[int | None, int | None]:
    status_path = Path("/proc/self/status")
    if not status_path.exists():
        return None, None

    rss_kb: int | None = None
    hwm_kb: int | None = None
    for line in status_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                rss_kb = int(parts[1])
        elif line.startswith("VmHWM:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                hwm_kb = int(parts[1])
    return ((rss_kb * 1024) if rss_kb is not None else None, (hwm_kb * 1024) if hwm_kb is not None else None)


def _resolve_symbol_and_latest_tick(db_path: str, symbol: str | None) -> tuple[str, pd.Timestamp]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        if symbol:
            row = con.execute(
                """
                SELECT symbol_contract, MAX(ts) AS latest_ts
                FROM ticks
                WHERE symbol_contract = ?
                GROUP BY symbol_contract
                """,
                [symbol],
            ).fetchone()
            if row is None or row[1] is None:
                raise RuntimeError(f"No ticks found for symbol_contract={symbol}")
            return str(row[0]), pd.Timestamp(row[1])

        row = con.execute(
            """
            SELECT symbol_contract, MAX(ts) AS latest_ts, COUNT(*) AS tick_count
            FROM ticks
            GROUP BY symbol_contract
            ORDER BY latest_ts DESC, tick_count DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None or row[0] is None or row[1] is None:
            raise RuntimeError("No ticks available in the dataset")
        return str(row[0]), pd.Timestamp(row[1])
    finally:
        con.close()


def _build_report(symbol: str, profile_timezone: str, window_start: pd.Timestamp, window_end: pd.Timestamp) -> dict[str, Any]:
    rss_before, hwm_before = _read_proc_mem()
    clear_runtime_caches()

    tracemalloc.start()
    _, trace_before_peak = tracemalloc.get_traced_memory()

    profiles_by_preset: dict[str, dict[str, Any]] = {}
    preset_stats: dict[str, dict[str, Any]] = {}
    for preset in PRESETS:
        profile = ensure_preset_volume_profiles(
            symbol_contract=symbol,
            start=window_start.to_pydatetime(),
            end=window_end.to_pydatetime(),
            preset=preset,
            profile_timezone=profile_timezone,
            tick_aggregation=1,
        )
        profiles_by_preset[preset] = profile

        profile_count = len(profile.get("profiles", []))
        level_count = sum(len(row.get("levels", [])) for row in profile.get("profiles", []))
        deep_size_bytes = _deep_sizeof(profile)
        json_size_bytes = len(json.dumps(profile, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
        preset_stats[preset] = {
            "profile_count": profile_count,
            "level_count": level_count,
            "deep_size_bytes": deep_size_bytes,
            "deep_size_human": _bytes_human(deep_size_bytes),
            "json_size_bytes": json_size_bytes,
            "json_size_human": _bytes_human(json_size_bytes),
        }

    trace_current, trace_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_after, hwm_after = _read_proc_mem()

    combined_deep_size_bytes = _deep_sizeof(profiles_by_preset)
    combined_json_size_bytes = len(
        json.dumps(profiles_by_preset, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    )

    return {
        "symbol_contract": symbol,
        "profile_timezone": profile_timezone,
        "tick_aggregation": 1,
        "window": {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
            "duration_days": 7,
        },
        "presets": list(PRESETS),
        "per_preset": preset_stats,
        "combined": {
            "profile_count": int(sum(row["profile_count"] for row in preset_stats.values())),
            "level_count": int(sum(row["level_count"] for row in preset_stats.values())),
            "deep_size_bytes": combined_deep_size_bytes,
            "deep_size_human": _bytes_human(combined_deep_size_bytes),
            "json_size_bytes": combined_json_size_bytes,
            "json_size_human": _bytes_human(combined_json_size_bytes),
        },
        "memory_runtime": {
            "tracemalloc_current_bytes": trace_current,
            "tracemalloc_current_human": _bytes_human(trace_current),
            "tracemalloc_peak_bytes": trace_peak,
            "tracemalloc_peak_human": _bytes_human(trace_peak),
            "tracemalloc_peak_delta_bytes": trace_peak - trace_before_peak,
            "tracemalloc_peak_delta_human": _bytes_human(max(0, trace_peak - trace_before_peak)),
            "vmrss_before_bytes": rss_before,
            "vmrss_before_human": _bytes_human(rss_before) if rss_before is not None else None,
            "vmrss_after_bytes": rss_after,
            "vmrss_after_human": _bytes_human(rss_after) if rss_after is not None else None,
            "vmrss_delta_bytes": (rss_after - rss_before) if rss_before is not None and rss_after is not None else None,
            "vmrss_delta_human": (
                _bytes_human(rss_after - rss_before) if rss_before is not None and rss_after is not None else None
            ),
            "vmhwm_before_bytes": hwm_before,
            "vmhwm_before_human": _bytes_human(hwm_before) if hwm_before is not None else None,
            "vmhwm_after_bytes": hwm_after,
            "vmhwm_after_human": _bytes_human(hwm_after) if hwm_after is not None else None,
            "vmhwm_delta_bytes": (hwm_after - hwm_before) if hwm_before is not None and hwm_after is not None else None,
            "vmhwm_delta_human": (
                _bytes_human(hwm_after - hwm_before) if hwm_before is not None and hwm_after is not None else None
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build last-week 1-tick volume profile presets (week/day/rth/eth) and report memory usage."
    )
    parser.add_argument("--symbol", help="symbol_contract override (default: auto-select latest symbol)")
    parser.add_argument("--profile-timezone", help="profile timezone (default: DATASET_TZ from settings)")
    parser.add_argument(
        "--out",
        default="experiment/volume_profile_memory_report.json",
        help="output report JSON path",
    )
    args = parser.parse_args()

    settings = get_settings()
    profile_timezone = args.profile_timezone or settings.dataset_tz
    symbol, latest_tick = _resolve_symbol_and_latest_tick(settings.duckdb_path, args.symbol)
    if latest_tick.tzinfo is None:
        latest_tick = latest_tick.tz_localize("UTC")
    window_end = latest_tick
    window_start = window_end - timedelta(days=7)

    report = _build_report(
        symbol=symbol,
        profile_timezone=profile_timezone,
        window_start=window_start,
        window_end=window_end,
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Report written: {out_path}")
    print(
        "Window:",
        report["window"]["start"],
        "to",
        report["window"]["end"],
        f"(symbol={report['symbol_contract']})",
    )
    print("Combined deep size:", report["combined"]["deep_size_human"])
    print("Combined JSON size:", report["combined"]["json_size_human"])
    print("Tracemalloc peak delta:", report["memory_runtime"]["tracemalloc_peak_delta_human"])
    print("VmRSS delta:", report["memory_runtime"]["vmrss_delta_human"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
