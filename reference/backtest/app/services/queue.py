from redis import Redis
from rq import Queue, Worker

from app.config import get_settings


DEFAULT_QUEUE_NAME = "backtest"


def get_queue() -> Queue:
    settings = get_settings()
    redis_conn = Redis.from_url(settings.redis_url)
    return Queue(name=DEFAULT_QUEUE_NAME, connection=redis_conn)


def get_queue_health() -> dict:
    queue = get_queue()
    workers = Worker.all(connection=queue.connection)
    active_worker_count = 0
    for worker in workers:
        queue_names = {q.name for q in worker.queues}
        if queue.name in queue_names:
            active_worker_count += 1

    return {
        "queue_name": queue.name,
        "queued_count": int(queue.count),
        "started_count": int(queue.started_job_registry.count),
        "failed_count": int(queue.failed_job_registry.count),
        "active_worker_count": active_worker_count,
    }
