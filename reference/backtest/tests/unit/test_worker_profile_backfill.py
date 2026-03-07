from __future__ import annotations

import uuid
from types import SimpleNamespace

from app.db.models import Job, JobStatus, JobType
from app.worker import enqueue_profile_backfill
from app.worker import run_worker
from app.worker import tasks as worker_tasks


class _FakeSession:
    def __init__(self, job: Job):
        self.job = job
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.added = []

    def scalar(self, _query):
        return self.job

    def add(self, obj):
        self.added.append(obj)
        if isinstance(obj, Job):
            self.job = obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def refresh(self, obj):
        if isinstance(obj, Job) and not getattr(obj, "id", None):
            obj.id = uuid.uuid4()

    def close(self):
        self.closed = True


def _job() -> Job:
    job = Job(job_type=JobType.build_preset_profiles_backfill, payload={})
    job.id = uuid.uuid4()
    job.status = JobStatus.queued
    return job


def test_build_preset_profiles_backfill_job_success(monkeypatch):
    job = _job()
    fake_db = _FakeSession(job)

    monkeypatch.setattr(worker_tasks, "SessionLocal", lambda: fake_db)
    monkeypatch.setattr(
        worker_tasks,
        "run_preset_profile_backfill",
        lambda symbols, profile_timezone, full_rebuild: {
            "status": "ok",
            "symbols_requested": symbols,
            "profile_timezone": profile_timezone,
            "full_rebuild": full_rebuild,
        },
    )

    result = worker_tasks.build_preset_profiles_backfill_job(
        str(job.id),
        ["NQH26"],
        "America/New_York",
        True,
    )

    assert result["status"] == "ok"
    assert job.status == JobStatus.succeeded
    assert fake_db.commits >= 2
    assert fake_db.closed is True


def test_build_preset_profiles_backfill_job_failure(monkeypatch):
    job = _job()
    fake_db = _FakeSession(job)

    monkeypatch.setattr(worker_tasks, "SessionLocal", lambda: fake_db)

    def _fail(*_args, **_kwargs):
        raise RuntimeError("backfill exploded")

    monkeypatch.setattr(worker_tasks, "run_preset_profile_backfill", _fail)

    try:
        worker_tasks.build_preset_profiles_backfill_job(
            str(job.id),
            ["NQH26"],
            "America/New_York",
            True,
        )
        raise AssertionError("Expected RuntimeError")
    except RuntimeError as exc:
        assert "backfill exploded" in str(exc)

    assert job.status == JobStatus.failed
    assert job.error["type"] == "RuntimeError"
    assert fake_db.rollbacks == 1
    assert fake_db.closed is True


def test_build_large_orders_backfill_job_success(monkeypatch):
    job = _job()
    job.job_type = JobType.build_large_orders
    fake_db = _FakeSession(job)

    monkeypatch.setattr(worker_tasks, "SessionLocal", lambda: fake_db)
    monkeypatch.setattr(
        worker_tasks,
        "run_large_orders_backfill",
        lambda symbols, threshold: {
            "status": "ok",
            "symbols_requested": symbols,
            "threshold": threshold,
        },
    )

    result = worker_tasks.build_large_orders_backfill_job(
        str(job.id),
        ["NQH26"],
        20.0,
    )

    assert result["status"] == "ok"
    assert result["symbols_requested"] == ["NQH26"]
    assert result["threshold"] == 20.0
    assert job.status == JobStatus.succeeded
    assert fake_db.commits >= 2
    assert fake_db.closed is True


def test_build_large_orders_backfill_job_failure(monkeypatch):
    job = _job()
    job.job_type = JobType.build_large_orders
    fake_db = _FakeSession(job)

    monkeypatch.setattr(worker_tasks, "SessionLocal", lambda: fake_db)

    def _fail(*_args, **_kwargs):
        raise RuntimeError("large-order backfill exploded")

    monkeypatch.setattr(worker_tasks, "run_large_orders_backfill", _fail)

    try:
        worker_tasks.build_large_orders_backfill_job(
            str(job.id),
            ["NQH26"],
            20.0,
        )
        raise AssertionError("Expected RuntimeError")
    except RuntimeError as exc:
        assert "large-order backfill exploded" in str(exc)

    assert job.status == JobStatus.failed
    assert job.error["type"] == "RuntimeError"
    assert fake_db.rollbacks == 1
    assert fake_db.closed is True


def test_run_worker_burst_mode(monkeypatch):
    calls = {"burst": None}

    class _FakeWorker:
        def __init__(self, queues, connection):
            self.queues = queues
            self.connection = connection

        def work(self, burst=False):
            calls["burst"] = bool(burst)

    monkeypatch.setattr(run_worker, "Worker", _FakeWorker)
    monkeypatch.setattr(run_worker.Redis, "from_url", lambda _url: object())
    monkeypatch.setattr(run_worker, "get_settings", lambda: SimpleNamespace(redis_url="redis://example:6379/0"))

    run_worker.main(["--burst"])
    assert calls["burst"] is True


def test_run_worker_default_non_burst(monkeypatch):
    calls = {"burst": None}

    class _FakeWorker:
        def __init__(self, queues, connection):
            self.queues = queues
            self.connection = connection

        def work(self, burst=False):
            calls["burst"] = bool(burst)

    monkeypatch.setattr(run_worker, "Worker", _FakeWorker)
    monkeypatch.setattr(run_worker.Redis, "from_url", lambda _url: object())
    monkeypatch.setattr(run_worker, "get_settings", lambda: SimpleNamespace(redis_url="redis://example:6379/0"))

    run_worker.main([])
    assert calls["burst"] is False


def test_enqueue_profile_backfill_job_defaults(monkeypatch):
    fake_db = _FakeSession(_job())
    enqueued = {}

    class _FakeQueue:
        def enqueue(self, fn_name, *args, **kwargs):
            enqueued["fn_name"] = fn_name
            enqueued["args"] = args
            enqueued["kwargs"] = kwargs
            return SimpleNamespace(id="rq-123")

    monkeypatch.setattr(enqueue_profile_backfill, "SessionLocal", lambda: fake_db)
    monkeypatch.setattr(enqueue_profile_backfill, "get_queue", lambda: _FakeQueue())

    result = enqueue_profile_backfill.enqueue_profile_backfill_job(
        symbols=[],
        profile_timezone="America/New_York",
        full_rebuild=True,
    )

    assert result["symbols"] == ["NQH26"]
    assert result["profile_timezone"] == "America/New_York"
    assert result["full_rebuild"] is True
    assert enqueued["fn_name"] == "app.worker.tasks.build_preset_profiles_backfill_job"
    assert fake_db.closed is True
