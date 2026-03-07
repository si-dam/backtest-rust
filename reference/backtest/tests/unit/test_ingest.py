from pathlib import Path
import uuid

from app.db.duck import get_duckdb_connection, init_duckdb
from app.db.models import Job, JobType
from app.services.ingest import (
    list_watch_market_files,
    process_csv_ingest,
    rebuild_source_files,
    validate_market_data_file,
)


class _FakeSession:
    def __init__(self):
        self.added = []
        self.commits = 0

    def scalar(self, _query):
        return None

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1


def test_rebuild_source_files_covers_current_and_previous_names():
    assert rebuild_source_files("new_name.csv", "old_name.csv") == {"new_name.csv", "old_name.csv"}


def test_rebuild_source_files_handles_missing_previous_name():
    assert rebuild_source_files("same_name.csv", None) == {"same_name.csv"}


def test_list_watch_market_files_supports_csv_and_txt(tmp_path):
    (tmp_path / "one.csv").write_text("x\n1\n", encoding="utf-8")
    (tmp_path / "two.txt").write_text("x\n1\n", encoding="utf-8")
    (tmp_path / "three.md").write_text("x\n1\n", encoding="utf-8")

    files = list_watch_market_files(Path(tmp_path))
    assert [p.name for p in files] == ["one.csv", "two.txt"]


def test_validate_market_data_file_accepts_ohlc_1m_txt(tmp_path):
    path = tmp_path / "NQH26.scid_BarData.txt"
    path.write_text(
        "Date,Time,Open,High,Low,Last,Volume,NumberOfTrades\n"
        "2026/02/24,08:30:00,22000.25,22001.00,22000.00,22000.75,10,4\n"
        "2026/02/24,08:31:00,22000.50,22001.25,22000.25,22001.00,12,5\n",
        encoding="utf-8",
    )

    result = validate_market_data_file(path=path, dataset_tz="America/Chicago", symbol_contract="NQH26")
    assert result["data_kind"] == "ohlc_1m"
    assert result["normalized_rows"] == 2
    assert result["symbol_contract"] == "NQH26"


def test_process_csv_ingest_ohlc_writes_1m_bars_without_bid_ask(temp_duckdb, tmp_path):
    init_duckdb()
    path = tmp_path / "NQH26.scid_BarData.txt"
    path.write_text(
        "Date,Time,Open,High,Low,Last,Volume,NumberOfTrades\n"
        "2026/02/24,08:30:00,22000.25,22001.00,22000.00,22000.75,10,4\n"
        "2026/02/24,08:31:00,22000.50,22001.25,22000.25,22001.00,12,5\n",
        encoding="utf-8",
    )

    db = _FakeSession()
    job = Job(job_type=JobType.ingest_csv, payload={})
    job.id = uuid.uuid4()
    result = process_csv_ingest(
        db=db,
        job=job,
        file_path=str(path),
        symbol_contract="NQH26",
        rebuild=False,
    )

    assert result["status"] == "ingested"
    assert result["data_kind"] == "ohlc_1m"
    assert result["tick_row_count"] == 0
    assert result["bar_row_count"] == 2
    assert result["row_count"] == 2

    con = get_duckdb_connection()
    try:
        count_1m = con.execute(
            "SELECT COUNT(*) FROM bars WHERE symbol_contract = 'NQH26' AND timeframe = '1m'"
        ).fetchone()[0]
        count_ticks = con.execute("SELECT COUNT(*) FROM ticks WHERE symbol_contract = 'NQH26'").fetchone()[0]
    finally:
        con.close()

    assert count_1m == 2
    assert count_ticks == 0


def test_process_csv_ingest_ticks_precomputes_non_time_presets(temp_duckdb, tmp_path):
    init_duckdb()
    path = tmp_path / "NQH26.ticks.csv"
    path.write_text(
        "Date Time,Price,Volume,Bid,Ask\n"
        "2026-02-24 08:30:00,22000.25,3,22000.00,22000.25\n"
        "2026-02-24 08:30:01,22000.50,2,22000.25,22000.50\n"
        "2026-02-24 08:30:02,22000.75,4,22000.50,22000.75\n"
        "2026-02-24 08:30:03,22001.00,1,22000.75,22001.00\n"
        "2026-02-24 08:30:04,22001.25,2,22001.00,22001.25\n"
        "2026-02-24 08:30:05,22001.50,3,22001.25,22001.50\n",
        encoding="utf-8",
    )

    db = _FakeSession()
    job = Job(job_type=JobType.ingest_csv, payload={})
    job.id = uuid.uuid4()
    result = process_csv_ingest(
        db=db,
        job=job,
        file_path=str(path),
        symbol_contract="NQH26",
        rebuild=False,
    )

    assert result["status"] == "ingested"
    assert result["data_kind"] == "ticks"
    presets = result["non_time_bar_presets"]["presets"]
    assert {row["timeframe"] for row in presets} == {"tick:1500", "volume:500", "volume:750", "volume:1000", "range:40"}

    con = get_duckdb_connection()
    try:
        rows = con.execute(
            """
            SELECT timeframe, COUNT(*) AS c
            FROM bars
            WHERE symbol_contract = 'NQH26'
              AND timeframe IN ('tick:1500', 'volume:500', 'volume:750', 'volume:1000', 'range:40')
            GROUP BY timeframe
            """
        ).fetchall()
    finally:
        con.close()
    counts = {str(timeframe): int(count) for timeframe, count in rows}
    assert counts.keys() == {"tick:1500", "volume:500", "volume:750", "volume:1000", "range:40"}
