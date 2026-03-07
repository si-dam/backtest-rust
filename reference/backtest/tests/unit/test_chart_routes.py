from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_chart as chart_routes
from app.db.duck import get_duckdb_connection, init_duckdb


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(chart_routes.router)
    return TestClient(app)


def _bars_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts": datetime(2026, 2, 24, 8, 30, tzinfo=ZoneInfo("America/Chicago")),
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
            }
        ]
    )


def test_bars_route_normalizes_timeframe(monkeypatch):
    seen = []

    def _fake_ensure_bars(symbol_contract, timeframe, start, end, bar_type="time", bar_size=None):
        seen.append((timeframe, bar_type, bar_size))
        return _bars_frame()

    monkeypatch.setattr(chart_routes, "ensure_bars", _fake_ensure_bars)

    client = _build_client()
    response = client.get(
        "/chart/bars",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "4H",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
        },
    )
    assert response.status_code == 200
    assert seen[-1] == ("4h", "time", None)

    response_day = client.get(
        "/chart/bars",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "1D",
            "start": "2026-02-24T00:00:00-06:00",
            "end": "2026-02-24T23:59:59-06:00",
        },
    )
    assert response_day.status_code == 200
    assert seen[-1] == ("1d", "time", None)

    response_3m = client.get(
        "/chart/bars",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "3m",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
        },
    )
    assert response_3m.status_code == 200
    assert seen[-1] == ("3m", "time", None)


def test_volume_overlay_route_normalizes_timeframe(monkeypatch):
    seen = []

    def _fake_ensure_bars(symbol_contract, timeframe, start, end, bar_type="time", bar_size=None):
        seen.append((timeframe, bar_type, bar_size))
        return _bars_frame()

    monkeypatch.setattr(chart_routes, "ensure_bars", _fake_ensure_bars)

    client = _build_client()
    response = client.get(
        "/chart/overlays/volume",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "1D",
            "start": "2026-02-24T00:00:00-06:00",
            "end": "2026-02-24T23:59:59-06:00",
        },
    )
    assert response.status_code == 200
    assert seen == [("1d", "time", None)]


def test_timeframe_validation_returns_400(monkeypatch):
    called = {"value": False}

    def _fake_ensure_bars(symbol_contract, timeframe, start, end, bar_type="time", bar_size=None):
        called["value"] = True
        return _bars_frame()

    monkeypatch.setattr(chart_routes, "ensure_bars", _fake_ensure_bars)

    client = _build_client()
    response = client.get(
        "/chart/bars",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "7m",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
        },
    )
    assert response.status_code == 400
    assert "Unsupported timeframe" in response.json()["detail"]
    assert called["value"] is False


def test_bars_route_accepts_non_time_bar_params(monkeypatch):
    seen = {}

    def _fake_ensure_bars(symbol_contract, timeframe, start, end, bar_type="time", bar_size=None):
        seen["timeframe"] = timeframe
        seen["bar_type"] = bar_type
        seen["bar_size"] = bar_size
        return _bars_frame()

    monkeypatch.setattr(chart_routes, "ensure_bars", _fake_ensure_bars)
    client = _build_client()
    response = client.get(
        "/chart/bars",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "1m",
            "bar_type": "tick",
            "bar_size": 1500,
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
        },
    )
    assert response.status_code == 200
    assert seen["bar_type"] == "tick"
    assert seen["bar_size"] == 1500


def test_bars_route_requires_bar_size_for_non_time():
    client = _build_client()
    response = client.get(
        "/chart/bars",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "1m",
            "bar_type": "range",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
        },
    )
    assert response.status_code == 400
    assert "bar_size is required for non-time bar_type" in response.json()["detail"]


def test_volume_overlay_route_accepts_non_time_bar_params(monkeypatch):
    seen = {}

    def _fake_ensure_bars(symbol_contract, timeframe, start, end, bar_type="time", bar_size=None):
        seen["bar_type"] = bar_type
        seen["bar_size"] = bar_size
        return _bars_frame()

    monkeypatch.setattr(chart_routes, "ensure_bars", _fake_ensure_bars)
    client = _build_client()
    response = client.get(
        "/chart/overlays/volume",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "1m",
            "bar_type": "volume",
            "bar_size": 750,
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
        },
    )
    assert response.status_code == 200
    assert seen["bar_type"] == "volume"
    assert seen["bar_size"] == 750


def test_bars_route_rejects_invalid_bar_type():
    client = _build_client()
    response = client.get(
        "/chart/bars",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "1m",
            "bar_type": "foobar",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
        },
    )
    assert response.status_code == 400
    assert "Unsupported bar_type" in response.json()["detail"]


def test_bars_route_reads_persisted_non_time_bars(temp_duckdb):
    init_duckdb()

    con = get_duckdb_connection()
    try:
        con.execute(
            """
            INSERT INTO bars (ts, session_date, timeframe, symbol_contract, open, high, low, close, volume, trade_count)
            VALUES
              ('2026-02-24T14:30:00+00:00', '2026-02-24', 'tick:1500', 'NQH6', 22000.0, 22001.0, 21999.75, 22000.75, 25.0, 6)
            """
        )
        con.execute(
            """
            INSERT INTO ticks (ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price, source_file)
            VALUES
              ('2026-02-24T14:30:00+00:00', '2026-02-24', 'NQH6', 22000.75, 2.0, 22000.5, 22000.75, 'fixture.csv')
            """
        )
    finally:
        con.close()

    client = _build_client()
    response = client.get(
        "/chart/bars",
        params={
            "symbol_contract": "NQH6",
            "timeframe": "1m",
            "bar_type": "tick",
            "bar_size": 1500,
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["timeframe"] == "tick:1500"


def test_volume_profiles_preset_route_returns_profiles(monkeypatch):
    seen = {}

    def _fake_load_persisted_preset_profiles(
        symbol_contract,
        start,
        end,
        preset,
        profile_timezone,
        metric,
        tick_aggregation,
        max_segments,
    ):
        seen["symbol_contract"] = symbol_contract
        seen["preset"] = preset
        seen["profile_timezone"] = profile_timezone
        seen["metric"] = metric
        seen["tick_aggregation"] = tick_aggregation
        seen["max_segments"] = max_segments
        return {
            "symbol_contract": symbol_contract,
            "preset": preset.lower(),
            "metric": metric,
            "tick_size": 0.25,
            "tick_aggregation": tick_aggregation,
            "value_area_enabled": False,
            "value_area_percent": 70.0,
            "profiles": [
                {
                    "id": "day-2026-02-24",
                    "label": "Day 2026-02-24",
                    "start": "2026-02-24T00:00:00-06:00",
                    "end": "2026-02-24T23:59:59-06:00",
                    "max_value": 120.0,
                    "total_value": 450.0,
                    "max_volume": 120.0,
                    "total_volume": 450.0,
                    "levels": [{"price_level": 22000.25, "value": 30.0, "volume": 30.0}],
                }
            ],
        }

    monkeypatch.setattr(chart_routes, "load_persisted_preset_profiles", _fake_load_persisted_preset_profiles)
    client = _build_client()
    response = client.get(
        "/chart/overlays/volume-profiles/preset",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T16:00:00-06:00",
            "preset": "day",
            "metric": "delta",
            "tick_aggregation": 3,
            "timezone": "America/New_York",
            "value_area_enabled": True,
            "value_area_percent": 68,
            "max_segments": 10,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol_contract"] == "NQH6"
    assert payload["preset"] == "day"
    assert payload["metric"] == "delta"
    assert len(payload["profiles"]) == 1
    assert seen["metric"] == "delta"
    assert seen["tick_aggregation"] == 3
    assert seen["profile_timezone"] == "America/New_York"
    assert seen["max_segments"] == 10


def test_volume_profiles_preset_route_rejects_invalid_preset(monkeypatch):
    def _fake_load_persisted_preset_profiles(
        symbol_contract,
        start,
        end,
        preset,
        profile_timezone,
        metric,
        tick_aggregation,
        max_segments,
    ):
        raise ValueError("Unsupported preset: invalid")

    monkeypatch.setattr(chart_routes, "load_persisted_preset_profiles", _fake_load_persisted_preset_profiles)
    client = _build_client()
    response = client.get(
        "/chart/overlays/volume-profiles/preset",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T16:00:00-06:00",
            "preset": "invalid",
        },
    )
    assert response.status_code == 400
    assert "Unsupported preset" in response.json()["detail"]


def test_volume_profiles_preset_route_rejects_invalid_metric(monkeypatch):
    called = {"value": False}

    def _fake_load_persisted_preset_profiles(
        symbol_contract,
        start,
        end,
        preset,
        profile_timezone,
        metric,
        tick_aggregation,
        max_segments,
    ):
        called["value"] = True
        return {}

    monkeypatch.setattr(chart_routes, "load_persisted_preset_profiles", _fake_load_persisted_preset_profiles)
    client = _build_client()
    response = client.get(
        "/chart/overlays/volume-profiles/preset",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T16:00:00-06:00",
            "preset": "day",
            "metric": "foo",
        },
    )
    assert response.status_code == 400
    assert "Unsupported metric" in response.json()["detail"]
    assert called["value"] is False


def test_volume_profiles_area_route_normalizes_and_returns_profile(monkeypatch):
    seen = {}

    def _fake_ensure_area_volume_profile(
        symbol_contract,
        start,
        end,
        price_min,
        price_max,
        area_id,
        profile_timezone,
        metric,
        tick_size,
        tick_aggregation,
        value_area_enabled,
        value_area_percent,
    ):
        seen["price_min"] = price_min
        seen["price_max"] = price_max
        seen["area_id"] = area_id
        seen["profile_timezone"] = profile_timezone
        seen["metric"] = metric
        seen["tick_aggregation"] = tick_aggregation
        seen["value_area_enabled"] = value_area_enabled
        seen["value_area_percent"] = value_area_percent
        return {
            "symbol_contract": symbol_contract,
            "mode": "area",
            "metric": metric,
            "tick_size": 0.25,
            "tick_aggregation": tick_aggregation,
            "value_area_enabled": value_area_enabled,
            "value_area_percent": value_area_percent,
            "profile": {
                "id": area_id or "vp-test",
                "start": start.isoformat(),
                "end": end.isoformat(),
                "price_min": min(price_min, price_max),
                "price_max": max(price_min, price_max),
                "max_value": 50.0,
                "total_value": 200.0,
                "max_volume": 50.0,
                "total_volume": 200.0,
                "levels": [{"price_level": 22001.0, "value": 50.0, "volume": 50.0}],
            },
        }

    monkeypatch.setattr(chart_routes, "ensure_area_volume_profile", _fake_ensure_area_volume_profile)
    client = _build_client()
    response = client.get(
        "/chart/overlays/volume-profiles/area",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T09:00:00-06:00",
            "end": "2026-02-24T08:00:00-06:00",
            "price_min": 22010.0,
            "price_max": 22000.0,
            "area_id": "vp-abc123",
            "metric": "delta",
            "timezone": "America/New_York",
            "tick_aggregation": 2,
            "value_area_enabled": True,
            "value_area_percent": 65,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "area"
    assert payload["metric"] == "delta"
    assert payload["profile"]["id"] == "vp-abc123"
    assert seen["metric"] == "delta"
    assert seen["price_min"] == 22010.0
    assert seen["price_max"] == 22000.0
    assert seen["profile_timezone"] == "America/New_York"
    assert seen["tick_aggregation"] == 2
    assert seen["value_area_enabled"] is False
    assert seen["value_area_percent"] == 65


def test_volume_profiles_preset_route_returns_empty_profiles(monkeypatch):
    def _fake_load_persisted_preset_profiles(
        symbol_contract,
        start,
        end,
        preset,
        profile_timezone,
        metric,
        tick_aggregation,
        max_segments,
    ):
        return {
            "symbol_contract": symbol_contract,
            "preset": preset,
            "metric": metric,
            "tick_size": 0.25,
            "tick_aggregation": tick_aggregation,
            "value_area_enabled": False,
            "value_area_percent": 70.0,
            "profiles": [],
        }

    monkeypatch.setattr(chart_routes, "load_persisted_preset_profiles", _fake_load_persisted_preset_profiles)
    client = _build_client()
    response = client.get(
        "/chart/overlays/volume-profiles/preset",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
            "preset": "eth",
        },
    )
    assert response.status_code == 200
    assert response.json()["profiles"] == []


def test_volume_profiles_area_route_rejects_invalid_metric(monkeypatch):
    called = {"value": False}

    def _fake_ensure_area_volume_profile(
        symbol_contract,
        start,
        end,
        price_min,
        price_max,
        area_id,
        profile_timezone,
        metric,
        tick_size,
        tick_aggregation,
        value_area_enabled,
        value_area_percent,
    ):
        called["value"] = True
        return {}

    monkeypatch.setattr(chart_routes, "ensure_area_volume_profile", _fake_ensure_area_volume_profile)
    client = _build_client()
    response = client.get(
        "/chart/overlays/volume-profiles/area",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T09:00:00-06:00",
            "end": "2026-02-24T08:00:00-06:00",
            "price_min": 22010.0,
            "price_max": 22000.0,
            "metric": "foo",
        },
    )
    assert response.status_code == 400
    assert "Unsupported metric" in response.json()["detail"]
    assert called["value"] is False


def test_volume_profiles_preset_route_rejects_invalid_timezone(monkeypatch):
    called = {"value": False}

    def _fake_load_persisted_preset_profiles(
        symbol_contract,
        start,
        end,
        preset,
        profile_timezone,
        metric,
        tick_aggregation,
        max_segments,
    ):
        called["value"] = True
        return {}

    monkeypatch.setattr(chart_routes, "load_persisted_preset_profiles", _fake_load_persisted_preset_profiles)
    client = _build_client()
    response = client.get(
        "/chart/overlays/volume-profiles/preset",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T16:00:00-06:00",
            "preset": "day",
            "timezone": "Mars/Phobos",
        },
    )
    assert response.status_code == 400
    assert "Invalid timezone" in response.json()["detail"]
    assert called["value"] is False


def test_large_orders_route_uses_persisted_rows_when_available(monkeypatch):
    seen = {"load_called": False, "compute_called": False}

    def _fake_load_large_orders(symbol_contract, start, end, method, fixed_threshold):
        seen["load_called"] = True
        return pd.DataFrame(
            [
                {
                    "ts": datetime(2026, 2, 24, 8, 30, tzinfo=ZoneInfo("America/Chicago")),
                    "session_date": datetime(2026, 2, 24).date(),
                    "symbol_contract": symbol_contract,
                    "trade_price": 22000.25,
                    "trade_size": 50.0,
                    "method": method,
                    "threshold": fixed_threshold,
                }
            ]
        )

    def _fake_ensure_large_orders(*args, **kwargs):
        seen["compute_called"] = True
        return pd.DataFrame()

    monkeypatch.setattr(chart_routes, "load_large_orders", _fake_load_large_orders)
    monkeypatch.setattr(chart_routes, "ensure_large_orders", _fake_ensure_large_orders, raising=False)
    client = _build_client()
    response = client.get(
        "/chart/overlays/large-orders",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
            "method": "fixed",
            "fixed_threshold": 25,
        },
    )
    assert response.status_code == 200
    assert seen["load_called"] is True
    assert seen["compute_called"] is False


def test_large_orders_route_returns_empty_when_persisted_missing(monkeypatch):
    seen = {"compute_called": False}

    def _fake_load_large_orders(symbol_contract, start, end, method, fixed_threshold):
        return pd.DataFrame()

    def _fake_ensure_large_orders(*args, **kwargs):
        seen["compute_called"] = True
        return pd.DataFrame()

    monkeypatch.setattr(chart_routes, "load_large_orders", _fake_load_large_orders)
    monkeypatch.setattr(chart_routes, "ensure_large_orders", _fake_ensure_large_orders, raising=False)
    client = _build_client()
    response = client.get(
        "/chart/overlays/large-orders",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T09:00:00-06:00",
            "method": "fixed",
            "fixed_threshold": 25,
        },
    )
    assert response.status_code == 200
    assert response.json() == []
    assert seen["compute_called"] is False
