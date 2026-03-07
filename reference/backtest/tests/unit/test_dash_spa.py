from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes_dash_spa import router


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_dash_spa_routes_return_shell() -> None:
    client = _client()
    for path in ("/dash", "/dash/", "/dash/chart", "/dash/datasets", "/dash/backtests", "/dash/unknown/path"):
        response = client.get(path)
        assert response.status_code == 200
        assert '<div id="root"></div>' in response.text
        assert '/static/dash/assets/' in response.text
