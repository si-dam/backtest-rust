import os

import pytest


@pytest.fixture
def temp_duckdb(tmp_path):
    db_path = tmp_path / "test_market.duckdb"
    os.environ["DUCKDB_PATH"] = str(db_path)
    os.environ["DATASET_TZ"] = "America/Chicago"

    from app.config import get_settings
    from app.services.aggregation import clear_runtime_caches
    from app.services.vwap import clear_vwap_runtime_caches

    get_settings.cache_clear()
    clear_runtime_caches()
    clear_vwap_runtime_caches()
    yield db_path
    clear_runtime_caches()
    clear_vwap_runtime_caches()
    get_settings.cache_clear()
