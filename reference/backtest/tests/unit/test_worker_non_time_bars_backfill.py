from __future__ import annotations

import uuid
from types import SimpleNamespace

from app.db.models import Job, JobStatus, JobType
from app.worker import enqueue_non_time_bars_backfill
from app.worker import tasks as worker_tasks


class _FakeSession:
    def __init__(self, job: Job):
        self.job = job
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def scalar(self, _query):
        return self.job

    def add(self, obj):
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
    job = Job(job_type=JobType.build_bars, payload={})
    job.id = uuid.uuid4()
    job.status = JobStatus.queued
    return job


def test_build_non_time_bars_backfill_job_success(monkeypatch):
    job = _job()
    fake_db = _FakeSession(job)

    monkeypatch.setattr(worker_tasks, "SessionLocal", lambda: fake_db)
    monkeypatch.setattr(worker_tasks, "list_symbols_with_ticks", lambda: ["NQH26"])
    monkeypatch.setattr(
        worker_tasks,
        "precompute_non_time_bar_presets",
        lambda symbol_contract, full_rebuild, presets=None: {
            "symbol_contract": symbol_contract,
            "full_rebuild": full_rebuild,
            "preset_count": len(tuple(presets or ())),
            "presets": [],
        },
    )

    result = worker_tasks.build_non_time_bars_backfill_job(
        str(job.id),
        [],
        True,
    )

    assert result["symbols_requested"] == ["NQH26"]
    assert result["symbol_count"] == 1
    assert result["full_rebuild"] is True
    assert job.status == JobStatus.succeeded
    assert fake_db.commits >= 2
    assert fake_db.closed is True


def test_build_non_time_bars_backfill_job_failure(monkeypatch):
    job = _job()
    fake_db = _FakeSession(job)

    monkeypatch.setattr(worker_tasks, "SessionLocal", lambda: fake_db)

    def _fail(*_args, **_kwargs):
        raise RuntimeError("non-time backfill exploded")

    monkeypatch.setattr(worker_tasks, "precompute_non_time_bar_presets", _fail)

    try:
        worker_tasks.build_non_time_bars_backfill_job(
            str(job.id),
            ["NQH26"],
            False,
        )
        raise AssertionError("Expected RuntimeError")
    except RuntimeError as exc:
        assert "non-time backfill exploded" in str(exc)

    assert job.status == JobStatus.failed
    assert job.error["type"] == "RuntimeError"
    assert fake_db.rollbacks == 1
    assert fake_db.closed is True


def test_enqueue_non_time_bars_backfill_defaults(monkeypatch):
    fake_db = _FakeSession(_job())
    enqueued = {}

    class _FakeQueue:
        def enqueue(self, fn_name, *args, **kwargs):
            enqueued["fn_name"] = fn_name
            enqueued["args"] = args
            enqueued["kwargs"] = kwargs
            return SimpleNamespace(id="rq-bars-123")

    monkeypatch.setattr(enqueue_non_time_bars_backfill, "SessionLocal", lambda: fake_db)
    monkeypatch.setattr(enqueue_non_time_bars_backfill, "get_queue", lambda: _FakeQueue())
    monkeypatch.setattr(enqueue_non_time_bars_backfill, "list_symbols_with_ticks", lambda: ["NQH26", "ESH26"])

    result = enqueue_non_time_bars_backfill.enqueue_non_time_bars_backfill_job(
        symbols=[],
        full_rebuild=True,
        preset_keys=[],
    )

    assert result["symbols"] == ["NQH26", "ESH26"]
    assert result["full_rebuild"] is True
    assert result["preset_keys"] == []
    assert enqueued["fn_name"] == "app.worker.tasks.build_non_time_bars_backfill_job"
    assert fake_db.closed is True


def test_enqueue_non_time_bars_backfill_with_single_preset(monkeypatch):
    fake_db = _FakeSession(_job())
    enqueued = {}

    class _FakeQueue:
        def enqueue(self, fn_name, *args, **kwargs):
            enqueued["fn_name"] = fn_name
            enqueued["args"] = args
            enqueued["kwargs"] = kwargs
            return SimpleNamespace(id="rq-bars-456")

    monkeypatch.setattr(enqueue_non_time_bars_backfill, "SessionLocal", lambda: fake_db)
    monkeypatch.setattr(enqueue_non_time_bars_backfill, "get_queue", lambda: _FakeQueue())

    result = enqueue_non_time_bars_backfill.enqueue_non_time_bars_backfill_job(
        symbols=["NQH26"],
        full_rebuild=False,
        preset_keys=["volume:750"],
    )

    assert result["symbols"] == ["NQH26"]
    assert result["preset_keys"] == ["volume:750"]
    assert enqueued["args"][3] == ["volume:750"]
