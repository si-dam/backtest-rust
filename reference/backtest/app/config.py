import os
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


_ENV_FILES: tuple[str, ...] = (".env", ".env.devcontainer") if os.getenv("DEVCONTAINER") == "1" else (".env",)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=_ENV_FILES, env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Futures Backtest Platform"
    app_env: str = "dev"

    secret_key: str = "change-me"
    access_token_expire_minutes: int = 720

    postgres_dsn: str = "postgresql+psycopg://backtest:backtest@localhost:5432/backtest"
    redis_url: str = "redis://localhost:6379/0"
    duckdb_path: str = "./data/datasets/market.duckdb"
    watch_dir: str = "./data/watch"

    dataset_tz: str = "America/Chicago"
    internal_api_base: str = "http://127.0.0.1:8000"
    enable_watcher: bool = False
    watcher_interval_seconds: int = 30

    default_owner_email: str = "owner@example.com"
    default_owner_password: str = "owner123"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
