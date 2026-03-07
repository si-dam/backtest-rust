import argparse

from redis import Redis
from rq import Worker

from app.config import get_settings
from app.services.queue import DEFAULT_QUEUE_NAME


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run RQ worker for backtest queue.")
    parser.add_argument(
        "--burst",
        action="store_true",
        help="Process all queued jobs and exit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    settings = get_settings()
    redis_conn = Redis.from_url(settings.redis_url)
    worker = Worker([DEFAULT_QUEUE_NAME], connection=redis_conn)
    worker.work(burst=bool(args.burst))


if __name__ == "__main__":
    main()
