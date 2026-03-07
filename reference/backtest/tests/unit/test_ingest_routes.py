from pathlib import Path
from types import SimpleNamespace
import uuid
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_ingest
from app.config import get_settings
from app.db.models import Job, JobStatus, JobType
from app.db.postgres import get_db_session


@pytest.fixture
def ingest_client(monkeypatch, tmp_path):
    monkeypatch.setenv("WATCH_DIR", str(tmp_path))
    get_settings.cache_clear()

    app = FastAPI()
    app.include_router(routes_ingest.router)

    def _fake_db():
        yield object()

    app.dependency_overrides[get_db_session] = _fake_db
    client = TestClient(app)
    yield client

    get_settings.cache_clear()


def _write_valid_market_file(path):
    path.write_text(
        "Date Time,Price,Volume,Bid,Ask\n"
        "2026-02-24 08:30:00,22000.25,3,22000.00,22000.25\n"
        "2026-02-24 08:30:01,22000.50,2,22000.25,22000.50\n",
        encoding="utf-8",
    )


def _write_valid_ohlc_1m_file(path):
    path.write_text(
        "Date,Time,Open,High,Low,Last,Volume,NumberOfTrades\n"
        "2026/02/24,08:30:00,22000.25,22001.00,22000.00,22000.75,10,4\n"
        "2026/02/24,08:31:00,22000.50,22001.25,22000.25,22001.00,12,5\n",
        encoding="utf-8",
    )


def test_watch_files_lists_csv_and_txt(ingest_client):
    watch_dir = get_settings().watch_dir
    csv_path = f"{watch_dir}/sample.csv"
    txt_path = f"{watch_dir}/sample.txt"
    other_path = f"{watch_dir}/notes.md"

    _write_valid_market_file(Path(csv_path))
    _write_valid_market_file(Path(txt_path))
    Path(other_path).write_text("ignore", encoding="utf-8")

    response = ingest_client.get("/ingest/watch-files")
    assert response.status_code == 200
    names = {item["name"] for item in response.json()}
    assert names == {"sample.csv", "sample.txt"}


def test_create_ingest_jobs_rejects_invalid_preflight(monkeypatch, ingest_client):
    watch_dir = get_settings().watch_dir
    bad_path = Path(watch_dir) / "bad.txt"
    bad_path.write_text("foo,bar\n1,2\n", encoding="utf-8")

    called = {"enqueue": False}

    def _fake_enqueue(_db, _file_path, _symbol_contract, _rebuild):
        called["enqueue"] = True
        return SimpleNamespace(id="ignored")

    monkeypatch.setattr(routes_ingest, "_enqueue_ingest_job", _fake_enqueue)

    response = ingest_client.post(
        "/ingest/jobs",
        json={"file_name": "bad.txt", "scan_watch_dir": False, "rebuild": False},
    )
    assert response.status_code == 400
    detail = response.json().get("detail", "")
    assert "Preflight failed" in detail
    assert "bad.txt" in detail
    assert called["enqueue"] is False


def test_create_ingest_jobs_accepts_valid_txt(monkeypatch, ingest_client):
    watch_dir = get_settings().watch_dir
    good_path = Path(watch_dir) / "good.txt"
    _write_valid_market_file(good_path)

    monkeypatch.setattr(
        routes_ingest,
        "_enqueue_ingest_job",
        lambda _db, file_path, symbol_contract, rebuild: SimpleNamespace(id="11111111-1111-1111-1111-111111111111"),
    )

    response = ingest_client.post(
        "/ingest/jobs",
        json={"file_name": "good.txt", "scan_watch_dir": False, "rebuild": False},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["job_ids"] == ["11111111-1111-1111-1111-111111111111"]


def test_create_ingest_jobs_accepts_valid_ohlc_txt(monkeypatch, ingest_client):
    watch_dir = get_settings().watch_dir
    ohlc_path = Path(watch_dir) / "NQH26.scid_BarData.txt"
    _write_valid_ohlc_1m_file(ohlc_path)

    monkeypatch.setattr(
        routes_ingest,
        "_enqueue_ingest_job",
        lambda _db, file_path, symbol_contract, rebuild: SimpleNamespace(id="22222222-2222-2222-2222-222222222222"),
    )

    response = ingest_client.post(
        "/ingest/jobs",
        json={"file_name": "NQH26.scid_BarData.txt", "scan_watch_dir": False, "rebuild": False},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["job_ids"] == ["22222222-2222-2222-2222-222222222222"]


def test_queue_health_contract(monkeypatch, ingest_client):
    monkeypatch.setattr(
        routes_ingest,
        "get_queue_health",
        lambda: {
            "queue_name": "backtest",
            "queued_count": 2,
            "started_count": 1,
            "failed_count": 0,
            "active_worker_count": 0,
        },
    )

    response = ingest_client.get("/ingest/queue-health")
    assert response.status_code == 200
    payload = response.json()
    assert payload == {
        "queue_name": "backtest",
        "queued_count": 2,
        "started_count": 1,
        "failed_count": 0,
        "active_worker_count": 0,
    }


def test_create_large_orders_backfill_job_enqueues(monkeypatch, ingest_client):
    monkeypatch.setattr(
        routes_ingest,
        "_enqueue_large_orders_backfill_job",
        lambda _db, symbols, threshold: SimpleNamespace(id="22222222-2222-2222-2222-222222222222", rq_job_id="rq-222"),
    )

    response = ingest_client.post(
        "/ingest/large-orders/jobs",
        json={"symbols": ["NQH26"], "threshold": 20.0},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["job_id"] == "22222222-2222-2222-2222-222222222222"
    assert payload["rq_job_id"] == "rq-222"


def test_create_large_orders_backfill_job_validation_error(ingest_client):
    response = ingest_client.post(
        "/ingest/large-orders/jobs",
        json={"symbols": [], "threshold": 20.0},
    )
    assert response.status_code == 422


class _FakeDB:
    def __init__(self, job: Job, jobs: list[Job]):
        self.job = job
        self.jobs = jobs
        self.scalar_queries: list = []
        self.scalars_queries: list = []

    def scalar(self, query):
        self.scalar_queries.append(query)
        return self.job

    def scalars(self, query):
        self.scalars_queries.append(query)

        class _Result:
            def __init__(self, rows):
                self._rows = rows

            def all(self):
                return self._rows

        return _Result(self.jobs)


def _job(job_type: JobType = JobType.build_large_orders) -> Job:
    job = Job(
        job_type=job_type,
        status=JobStatus.queued,
        payload={"symbols": ["NQH26"], "threshold": 20.0},
        result={},
        error={},
    )
    job.id = uuid.uuid4()
    job.rq_job_id = "rq-test"
    now = datetime.now(timezone.utc)
    job.created_at = now
    job.updated_at = now
    return job


def test_get_large_orders_backfill_job_filters_job_type():
    job = _job()
    db = _FakeDB(job=job, jobs=[])
    result = routes_ingest.get_large_orders_backfill_job(str(job.id), db)
    assert result is job
    assert db.scalar_queries
    params = db.scalar_queries[0].compile().params
    assert JobType.build_large_orders in params.values()


def test_list_large_orders_backfill_jobs_filters_job_type():
    job = _job()
    db = _FakeDB(job=job, jobs=[job])
    rows = routes_ingest.list_large_orders_backfill_jobs(limit=10, db=db)
    assert len(rows) == 1
    assert rows[0]["id"] == str(job.id)
    assert db.scalars_queries
    params = db.scalars_queries[0].compile().params
    assert JobType.build_large_orders in params.values()
