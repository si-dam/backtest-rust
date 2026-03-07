from fastapi import APIRouter, HTTPException, status
from app.db.duck import get_duckdb_connection
from app.services.market_data_store import get_symbol_coverage

router = APIRouter(tags=["symbols"])


@router.get("/symbols")
def list_symbols():
    con = get_duckdb_connection()
    try:
        rows = con.execute(
            """
            WITH tick_bounds AS (
                SELECT symbol_contract, min(ts) AS start_ts, max(ts) AS end_ts, count(*) AS tick_count
                FROM ticks
                GROUP BY symbol_contract
            ),
            bar_bounds AS (
                SELECT symbol_contract, min(ts) AS start_ts, max(ts) AS end_ts, count(*) AS bar_count
                FROM bars
                GROUP BY symbol_contract
            ),
            unioned AS (
                SELECT symbol_contract, start_ts, end_ts, tick_count, 0::BIGINT AS bar_count
                FROM tick_bounds
                UNION ALL
                SELECT symbol_contract, start_ts, end_ts, 0::BIGINT AS tick_count, bar_count
                FROM bar_bounds
            )
            SELECT
                symbol_contract,
                min(start_ts) AS start_ts,
                max(end_ts) AS end_ts,
                sum(tick_count) AS tick_count,
                sum(bar_count) AS bar_count
            FROM unioned
            GROUP BY symbol_contract
            ORDER BY symbol_contract
            """
        ).fetchall()
    finally:
        con.close()

    return [
        {
            "symbol_contract": r[0],
            "start_ts": r[1].isoformat() if r[1] else None,
            "end_ts": r[2].isoformat() if r[2] else None,
            "tick_count": int(r[3]),
            "bar_count": int(r[4]),
        }
        for r in rows
    ]


@router.get("/symbols/{symbol_contract}/coverage")
def symbol_coverage(symbol_contract: str):
    coverage = get_symbol_coverage(symbol_contract=symbol_contract)
    if coverage is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Symbol not found")
    return coverage
