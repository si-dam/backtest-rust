import asyncio
import contextlib
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from app.api.routes_backtests import router as backtests_router
from app.api.routes_chart import router as chart_router
from app.api.routes_chart_vwap import router as chart_vwap_router
from app.api.routes_dash_spa import router as dash_spa_router
from app.api.routes_ingest import router as ingest_router
from app.api.routes_symbols import router as symbols_router
from app.config import get_settings
from app.db.duck import init_duckdb
from app.db.postgres import Base, SessionLocal, engine
from app.services.watcher import watcher_loop

watcher_task: asyncio.Task | None = None


def _wait_for_postgres(max_attempts: int = 30, delay_seconds: float = 2.0) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            with engine.connect() as conn:
                conn.execute(select(1))
            return
        except OperationalError:
            if attempt >= max_attempts:
                raise
            import time

            time.sleep(delay_seconds)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global watcher_task
    settings = get_settings()

    await asyncio.to_thread(_wait_for_postgres)
    Base.metadata.create_all(bind=engine)
    init_duckdb()

    watcher_task = None
    if settings.enable_watcher:
        watcher_task = asyncio.create_task(watcher_loop(SessionLocal))
    yield

    if watcher_task:
        watcher_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher_task


app = FastAPI(title=get_settings().app_name, lifespan=lifespan)

app.include_router(ingest_router)
app.include_router(symbols_router)
app.include_router(chart_router)
app.include_router(chart_vwap_router)
app.include_router(backtests_router)
app.include_router(dash_spa_router)


@app.get("/")
def root():
    return {"name": get_settings().app_name, "dash": "/dash/", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}


app.mount("/static/dash", StaticFiles(directory="app/static/dash"), name="dash-static")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
