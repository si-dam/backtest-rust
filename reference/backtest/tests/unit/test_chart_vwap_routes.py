from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_chart_vwap


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(routes_chart_vwap.router)
    return TestClient(app)


def test_vwap_preset_route_returns_payload(monkeypatch):
    seen = {}

    def _fake_ensure_preset_vwap(symbol_contract, start, end, preset, profile_timezone, max_segments):
        seen["symbol_contract"] = symbol_contract
        seen["preset"] = preset
        seen["profile_timezone"] = profile_timezone
        seen["max_segments"] = max_segments
        return {
            "symbol_contract": symbol_contract,
            "timezone": profile_timezone or "America/Chicago",
            "preset": str(preset).lower(),
            "segments": [
                {
                    "id": "day-2026-02-24",
                    "label": "Day 2026-02-24",
                    "start": "2026-02-24T09:30:00-05:00",
                    "end": "2026-02-24T16:00:00-05:00",
                    "points": [
                        {
                            "ts": "2026-02-24T09:31:00-05:00",
                            "vwap": 100.0,
                            "upper_1": 101.0,
                            "lower_1": 99.0,
                            "upper_2": 102.0,
                            "lower_2": 98.0,
                        }
                    ],
                }
            ],
        }

    monkeypatch.setattr(routes_chart_vwap, "ensure_preset_vwap", _fake_ensure_preset_vwap)
    client = _build_client()
    response = client.get(
        "/chart/overlays/vwap/preset",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T16:00:00-06:00",
            "preset": "day",
            "timezone": "America/New_York",
            "max_segments": 10,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["preset"] == "day"
    assert len(payload["segments"]) == 1
    assert seen["symbol_contract"] == "NQH6"
    assert seen["preset"] == "day"
    assert seen["profile_timezone"] == "America/New_York"
    assert seen["max_segments"] == 10


def test_vwap_preset_route_rejects_invalid_preset(monkeypatch):
    def _fake_ensure_preset_vwap(symbol_contract, start, end, preset, profile_timezone, max_segments):
        raise ValueError("Unsupported preset: bad")

    monkeypatch.setattr(routes_chart_vwap, "ensure_preset_vwap", _fake_ensure_preset_vwap)
    client = _build_client()
    response = client.get(
        "/chart/overlays/vwap/preset",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-24T08:00:00-06:00",
            "end": "2026-02-24T16:00:00-06:00",
            "preset": "bad",
        },
    )
    assert response.status_code == 400
    assert "Unsupported preset" in response.json()["detail"]


def test_vwap_preset_route_rejects_invalid_timezone(monkeypatch):
    called = {"value": False}

    def _fake_ensure_preset_vwap(symbol_contract, start, end, preset, profile_timezone, max_segments):
        called["value"] = True
        return {}

    monkeypatch.setattr(routes_chart_vwap, "ensure_preset_vwap", _fake_ensure_preset_vwap)
    client = _build_client()
    response = client.get(
        "/chart/overlays/vwap/preset",
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
