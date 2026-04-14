"""
FastAPI analytics server for Circuit protocol.

Exposes:
  GET  /stats              — Protocol statistics (mirrors circuit's /stats endpoint)
  POST /sync_block_stats   — Trigger a block scan (compatible with circuit-cli sync_backend.py)

Configure via environment variables (same as scanner):
  DB_PATH        Path to SQLite database file (default: circuit_analytics.db)
  CHIA_NODES     Comma-separated host:port pairs for Chia full node RPC
  CHIA_ROOT      Chia root directory (default: ~/.chia/mainnet)
  Plus all scanner env vars: BYC_TAIL_HASH, CRT_TAIL_HASH, STATUTES_LAUNCHER_ID, etc.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict

from chia.full_node.full_node_rpc_client import FullNodeRpcClient
from chia.util.config import load_config
from chia_rs.sized_ints import uint16
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from circuit_analytics.scanner.block_scanner import scan_blocks
from circuit_analytics.scanner.stats import ZERO_RUNNING_TOTALS, calculate_stats

log = logging.getLogger(__name__)

INTERVAL_MAP = {
    "5m": 5 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
}
SECONDS_PER_DAY = 86400
_INTERVAL_SECONDS = 5 * 60  # 5-minute buckets for all standard periods

# Standard periods — all use 5-minute buckets.
# "max" is handled separately (queries from genesis, no incremental state).
STANDARD_PERIODS: Dict[str, int] = {
    "1d": 1 * SECONDS_PER_DAY,
    "1w": 7 * SECONDS_PER_DAY,
    "1m": 30 * SECONDS_PER_DAY,
    "3m": 90 * SECONDS_PER_DAY,
    "1y": 365 * SECONDS_PER_DAY,
}

# Single in-process state covering the full _STATE_MAX_DAYS window.
# All standard periods are served by filtering this one copy.
_in_process_state: Optional[dict] = None  # {"stats": [...], "announcers": {...}, "saved_at": int}
# Per-period pre-serialized bytes: warm hits return these directly, bypassing serialization.
_response_caches: Dict[str, dict] = {}  # period -> {"body": bytes, "saved_at": int}
# Separate cache for the "max" period (genesis to now).
_max_cache: Optional[dict] = None  # {"body": bytes, "saved_at": int}
_FRESH_THRESHOLD = 5 * 60  # seconds
_STATE_MAX_DAYS = 370  # covers 1Y + buffer


def _get_db_path() -> str:
    return os.environ.get("DB_PATH", str(Path.home() / ".circuit" / "analytics.db"))


async def _make_client() -> FullNodeRpcClient:
    root_path = Path(os.environ.get("CHIA_ROOT", Path.home() / ".chia" / "mainnet"))
    config = load_config(root_path, "config.yaml")

    node_str = os.environ.get("CHIA_NODES", "").split(",")[0].strip()
    if node_str:
        host, port = node_str.rsplit(":", 1)
    else:
        host = config.get("self_hostname", "127.0.0.1")
        port = config.get("full_node", {}).get("rpc_port", 8555)

    return await FullNodeRpcClient.create(host, uint16(int(port)), root_path, config)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="Circuit Analytics", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def _query_stats(db_path: str, start_timestamp: int, end_timestamp: int, interval_seconds: int) -> dict:
    """Run the DB query and return {"stats": [...], "announcers": {...}}."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    sql = f"""
        SELECT
            (CAST(timestamp / {interval_seconds} AS INTEGER) * {interval_seconds}) AS time_bucket,
            MAX(timestamp) AS timestamp,
            MAX(last_updated) AS last_updated,
            MAX(statutes_price) AS statutes_price,
            AVG(current_stability_fee_df) AS current_stability_fee_df,
            AVG(current_interest_rate_df) AS current_interest_rate_df,
            MAX(cumulative_stability_fee_df) AS cumulative_stability_fee_df,
            MAX(cumulative_interest_rate_df) AS cumulative_interest_rate_df,
            SUM(vault_operations_count) AS vault_operations_count,
            SUM(vault_count_incr) AS vault_count_incr,
            SUM(vault_count_decr) AS vault_count_decr,
            SUM(collateral_deposited) AS collateral_deposited,
            SUM(collateral_withdrawn) AS collateral_withdrawn,
            SUM(collateral_sold) AS collateral_sold,
            SUM(byc_borrowed) AS byc_borrowed,
            SUM(byc_repaid) AS byc_repaid,
            SUM(sf_repaid) AS sf_repaid,
            SUM(sf_transferred) AS sf_transferred,
            SUM(discounted_principal_delta) AS discounted_principal_delta,
            SUM(liquidation_start_count) AS liquidation_start_count,
            SUM(liquidation_restart_count) AS liquidation_restart_count,
            SUM(liquidation_ended_count) AS liquidation_ended_count,
            SUM(lp_incurred) AS lp_incurred,
            SUM(ii_incurred) AS ii_incurred,
            SUM(ii_paid) AS ii_paid,
            SUM(fees_incurred) AS fees_incurred,
            SUM(fees_paid) AS fees_paid,
            SUM(principal_incurred) AS principal_incurred,
            SUM(principal_paid) AS principal_paid,
            SUM(bad_debt_count_incr) AS bad_debt_count_incr,
            SUM(bad_debt_count_decr) AS bad_debt_count_decr,
            SUM(bad_debt_ii_incurred) AS bad_debt_ii_incurred,
            SUM(bad_debt_ii_recovered) AS bad_debt_ii_recovered,
            SUM(bad_debt_fees_incurred) AS bad_debt_fees_incurred,
            SUM(bad_debt_fees_recovered) AS bad_debt_fees_recovered,
            SUM(bad_debt_principal_incurred) AS bad_debt_principal_incurred,
            SUM(bad_debt_principal_recovered) AS bad_debt_principal_recovered,
            SUM(savings_vault_operations_count) AS savings_vault_operations_count,
            SUM(savings_vault_count_incr) AS savings_vault_count_incr,
            SUM(savings_vault_count_decr) AS savings_vault_count_decr,
            SUM(discounted_savings_balance_delta) AS discounted_savings_balance_delta,
            SUM(byc_deposited) AS byc_deposited,
            SUM(byc_withdrawn) AS byc_withdrawn,
            SUM(interest_paid) AS interest_paid,
            SUM(approved_announcer_count_delta) AS approved_announcer_count_delta,
            SUM(treasury_coin_count_delta) AS treasury_coin_count_delta,
            SUM(treasury_balance_delta) AS treasury_balance_delta,
            SUM(recharge_auction_coin_count_delta) AS recharge_auction_coin_count_delta,
            SUM(recharge_auction_count_delta) AS recharge_auction_count_delta,
            SUM(surplus_auction_count_delta) AS surplus_auction_count_delta,
            SUM(governance_operations_count) AS governance_operations_count,
            SUM(governance_coin_count_delta) AS governance_coin_count_delta,
            0 AS governance_coin_count_peak_delta,
            SUM(governance_circulation_delta) AS governance_circulation_delta,
            0 AS governance_circulation_peak_delta,
            SUM(crt_circulation_delta) AS crt_circulation_delta,
            SUM(registry_operations_count) AS registry_operations_count,
            SUM(registered_announcer_count_delta) AS registered_announcer_count_delta
        FROM block_stats_v2
        WHERE timestamp >= ? AND timestamp <= ?
        GROUP BY time_bucket
        ORDER BY time_bucket
    """
    rows = conn.execute(sql, (start_timestamp, end_timestamp)).fetchall()

    announcer_rows = conn.execute(
        """
        SELECT launcher_id, timestamp, price
        FROM announcer_coin
        WHERE timestamp >= ? AND timestamp <= ? AND approved = 1
        ORDER BY timestamp
        """,
        (start_timestamp, end_timestamp),
    ).fetchall()

    cumulative_fields = list(ZERO_RUNNING_TOTALS.keys())
    seed_cols = ", ".join(f"COALESCE(SUM({k}), 0) AS {k}" for k in cumulative_fields)
    seed = conn.execute(f"SELECT {seed_cols} FROM block_stats_v2 WHERE timestamp < ?", (start_timestamp,)).fetchone()

    price_row = conn.execute(
        "SELECT statutes_price FROM block_stats_v2 WHERE timestamp < ? AND statutes_price != 0 ORDER BY timestamp DESC LIMIT 1",
        (start_timestamp,),
    ).fetchone()
    statutes_price = price_row["statutes_price"] if price_row else 0

    conn.close()

    json_announcers: dict = {}
    for ar in announcer_rows:
        lid = ar["launcher_id"]
        if lid not in json_announcers:
            json_announcers[lid] = []
        json_announcers[lid].append({"timestamp": ar["timestamp"], "price": ar["price"]})

    running_totals = {key: int(seed[key] or 0) for key in cumulative_fields}
    stats = []

    for row in rows:
        if row["statutes_price"]:
            statutes_price = row["statutes_price"]

        for key in cumulative_fields:
            running_totals[key] += int(row[key] or 0)

        stats_dict = calculate_stats(
            running_totals,
            int(row["current_stability_fee_df"] or 0),
            int(row["current_interest_rate_df"] or 0),
            int(row["cumulative_stability_fee_df"] or 0),
            int(row["cumulative_interest_rate_df"] or 0),
            statutes_price,
            int(row["last_updated"] or 0),
            int(row["timestamp"]),
        )
        stats.append(stats_dict)

    return {"stats": stats, "announcers": json_announcers}


@app.get("/protocol/stats")
@app.get("/stats")
def get_stats(
    period: Optional[str] = Query(default="3m"),
    start_date: Optional[datetime] = Query(default=None),
    end_date: Optional[datetime] = Query(default=None),
    sample_interval: str = Query(default="5m"),
):
    global _in_process_state, _max_cache

    now = int(time.time())
    end_timestamp = int(end_date.timestamp()) if end_date else now

    # --- Standard period path (cached) ---
    all_periods = set(STANDARD_PERIODS.keys()) | {"max"}
    if period is not None and period in all_periods and start_date is None:
        db_path = _get_db_path()

        if period == "max":
            if _max_cache is not None and (now - _max_cache["saved_at"]) < _FRESH_THRESHOLD:
                return Response(content=_max_cache["body"], media_type="application/json")
            conn = sqlite3.connect(db_path)
            row = conn.execute("SELECT MIN(timestamp) FROM block_stats_v2").fetchone()
            conn.close()
            min_ts = row[0] if row and row[0] else end_timestamp
            result = _query_stats(db_path, min_ts, end_timestamp, _INTERVAL_SECONDS)
            _max_cache = {"body": json.dumps(result).encode(), "saved_at": now}
            return result

        start_timestamp = end_timestamp - STANDARD_PERIODS[period]

        # Warm path 1: pre-serialized bytes — no filter, no serialization, no DB.
        rc = _response_caches.get(period)
        if rc is not None and (now - rc["saved_at"]) < _FRESH_THRESHOLD:
            return Response(content=rc["body"], media_type="application/json")

        def _filter_state(state: dict) -> dict:
            return {
                "stats": [s for s in state["stats"] if s["timestamp"] >= start_timestamp],
                "announcers": {
                    lid: [e for e in entries if e["timestamp"] >= start_timestamp]
                    for lid, entries in state["announcers"].items()
                    if any(e["timestamp"] >= start_timestamp for e in entries)
                },
            }

        # Warm path 2: in-process state fresh but this period not yet serialized.
        if _in_process_state is not None and (now - _in_process_state["saved_at"]) < _FRESH_THRESHOLD:
            result = _filter_state(_in_process_state)
            body = json.dumps(result).encode()
            _response_caches[period] = {"body": body, "saved_at": now}
            return Response(content=body, media_type="application/json")

        # Cold path: compute full window, populate both caches.
        state_start = end_timestamp - _STATE_MAX_DAYS * SECONDS_PER_DAY
        state = _query_stats(db_path, state_start, end_timestamp, _INTERVAL_SECONDS)
        state["saved_at"] = now
        _in_process_state = state
        result = _filter_state(state)
        body = json.dumps(result).encode()
        _response_caches[period] = {"body": body, "saved_at": now}
        return Response(content=body, media_type="application/json")

    if period is not None and period not in all_periods:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(sorted(all_periods))}",
        )

    # --- Custom date range path (no cache) ---
    if sample_interval not in INTERVAL_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sample_interval. Must be one of: {', '.join(INTERVAL_MAP.keys())}",
        )
    interval_seconds = INTERVAL_MAP[sample_interval]
    start_timestamp = int(start_date.timestamp()) if start_date else end_timestamp - 90 * SECONDS_PER_DAY

    return _query_stats(_get_db_path(), start_timestamp, end_timestamp, interval_seconds)


@app.post("/sync_block_stats")
async def sync_block_stats(max_blocks: int = 1000):
    """Trigger a block scan. Compatible with circuit-cli sync_backend.py -b."""
    client = None
    try:
        client = await _make_client()
        result = await scan_blocks(client, _get_db_path(), max_blocks=max_blocks)
        return {"status": "done", **result}
    except Exception as e:
        log.exception("Error during sync_block_stats")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if client is not None:
            client.close()
            await client.await_closed()
