from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from chia.full_node.full_node_rpc_client import FullNodeRpcClient
from chia.types.blockchain_format.program import Program, uncurry
from chia_rs import CoinSpend
from chia_rs.sized_bytes import bytes32

from circuit_analytics.config import BYC_TAIL_HASH, CRT_TAIL_HASH, STATUTES_LAUNCHER_ID
from circuit_analytics.drivers.protocol_math import PRECISION
from circuit_analytics.drivers.registry import AnnouncerRegistry
from circuit_analytics.mods import (
    ATOM_ANNOUNCER_MOD_HASH,
    CAT_MOD_HASH,
    COLLATERAL_VAULT_MOD_HASH,
    SINGLETON_ISA_MOD_HASH,
    SINGLETON_MOD_HASH,
    STATUTES_LAUNCHER_HASH,
)
from circuit_analytics.scanner.handlers.announcer import AnnouncerHandler
from circuit_analytics.scanner.handlers.base import StatsDelta
from circuit_analytics.scanner.handlers.cat import CatHandler
from circuit_analytics.scanner.handlers.oracle import OracleHandler
from circuit_analytics.scanner.handlers.registry import RegistryHandler
from circuit_analytics.scanner.handlers.singleton_isa import StatutesHandler
from circuit_analytics.scanner.handlers.vault import CollateralVaultHandler
from circuit_analytics.scanner.models import (
    AnnouncerCoin,
    AuctionCoin,
    BlockStatsV2,
    DailyBlockStatsV2,
    GoverningCRT,
    LiveBlockHash,
    SavingsVaultCoin,
    ScannerHeight,
    TreasuryCoin,
    VaultCoin,
    create_tables,
)

log = logging.getLogger(__name__)

SECONDS_PER_DAY = 86400
REORG_CHECK_DEPTH = 10
REORG_SAFETY_BUFFER = 10
BLOCKS_TO_FETCH = 200
SPENDS_FETCH_CONCURRENCY = 20


async def _fetch_block_spends(client: FullNodeRpcClient, header_hash: bytes32, sem: asyncio.Semaphore):
    async with sem:
        return await client.get_block_spends(header_hash)


def get_statutes_struct() -> Program:
    """Compute the statutes struct from config constants."""
    return Program.to((SINGLETON_ISA_MOD_HASH, (STATUTES_LAUNCHER_ID, STATUTES_LAUNCHER_HASH)))


# --- DB helpers ---


def _upsert_vault_coin(conn: sqlite3.Connection, coin: VaultCoin) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO vault_coin VALUES (?,?,?,?,?,?,?,?)",
        (
            coin.name,
            coin.collateral,
            coin.principal,
            coin.discounted_principal,
            coin.auction_state,
            int(coin.in_bad_debt),
            coin.inner_puzzle_hash,
            coin.height,
        ),
    )


def _upsert_savings_coin(conn: sqlite3.Connection, coin: SavingsVaultCoin) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO savings_vault_coin VALUES (?,?,?,?,?)",
        (coin.name, coin.balance, coin.discounted_balance, coin.inner_puzzle_hash, coin.height),
    )


def _upsert_auction_coin(conn: sqlite3.Connection, coin: AuctionCoin) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO auction_coin VALUES (?,?,?,?,?)",
        (coin.name, coin.auction_type, coin.auction_status, coin.parent_name, coin.height),
    )


def _upsert_treasury_coin(conn: sqlite3.Connection, coin: TreasuryCoin) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO treasury_coin VALUES (?,?,?,?,?,?)",
        (coin.name, coin.launcher_id, coin.ring_prev_launcher_id, coin.amount, coin.parent_name, coin.height),
    )


def _upsert_announcer_coin(conn: sqlite3.Connection, coin: AnnouncerCoin) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO announcer_coin VALUES (?,?,?,?,?,?,?,?)",
        (
            coin.name,
            coin.launcher_id,
            coin.timestamp_expires,
            coin.price,
            coin.timestamp,
            int(coin.spent),
            int(coin.approved),
            coin.height,
        ),
    )


def _upsert_governing_crt(conn: sqlite3.Connection, coin: GoverningCRT) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO governing_crt VALUES (?,?,?,?,?,?,?)",
        (coin.name, coin.amount, coin.bill, coin.operation, coin.timestamp, int(coin.spent), coin.height),
    )


def _mark_coins_spent(conn: sqlite3.Connection, coin_names: Set[str]) -> None:
    if not coin_names:
        return
    placeholders = ",".join("?" * len(coin_names))
    names_list = list(coin_names)
    conn.execute(f"DELETE FROM vault_coin WHERE name IN ({placeholders})", names_list)
    conn.execute(f"DELETE FROM savings_vault_coin WHERE name IN ({placeholders})", names_list)
    conn.execute(f"DELETE FROM treasury_coin WHERE name IN ({placeholders})", names_list)
    conn.execute(f"DELETE FROM auction_coin WHERE name IN ({placeholders})", names_list)
    conn.execute(f"UPDATE governing_crt SET spent=1 WHERE name IN ({placeholders})", names_list)
    conn.execute(f"UPDATE announcer_coin SET spent=1 WHERE name IN ({placeholders})", names_list)


def _delete_coins_at_height(conn: sqlite3.Connection, height: int) -> None:
    conn.execute("DELETE FROM vault_coin WHERE height >= ?", (height,))
    conn.execute("DELETE FROM savings_vault_coin WHERE height >= ?", (height,))
    conn.execute("DELETE FROM treasury_coin WHERE height >= ?", (height,))
    conn.execute("DELETE FROM auction_coin WHERE height >= ?", (height,))
    conn.execute("DELETE FROM governing_crt WHERE height >= ?", (height,))
    conn.execute("DELETE FROM announcer_coin WHERE height >= ?", (height,))
    conn.execute("DELETE FROM live_block_hash WHERE height >= ?", (height,))
    conn.execute("DELETE FROM block_stats_v2 WHERE height >= ?", (height,))


def _save_coin(conn: sqlite3.Connection, coin: Any) -> None:
    if isinstance(coin, VaultCoin):
        _upsert_vault_coin(conn, coin)
    elif isinstance(coin, SavingsVaultCoin):
        _upsert_savings_coin(conn, coin)
    elif isinstance(coin, AuctionCoin):
        _upsert_auction_coin(conn, coin)
    elif isinstance(coin, TreasuryCoin):
        _upsert_treasury_coin(conn, coin)
    elif isinstance(coin, AnnouncerCoin):
        _upsert_announcer_coin(conn, coin)
    elif isinstance(coin, GoverningCRT):
        _upsert_governing_crt(conn, coin)


def _write_block_stats(conn: sqlite3.Connection, stats: BlockStatsV2) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO block_stats_v2 VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )""",
        (
            stats.height,
            stats.block_hash,
            stats.timestamp,
            stats.last_updated,
            stats.statutes_price,
            stats.current_stability_fee_df,
            stats.current_interest_rate_df,
            stats.cumulative_stability_fee_df,
            stats.cumulative_interest_rate_df,
            stats.vault_operations_count,
            stats.vault_count_incr,
            stats.vault_count_decr,
            stats.collateral_deposited,
            stats.collateral_withdrawn,
            stats.collateral_sold,
            stats.byc_borrowed,
            stats.byc_repaid,
            stats.sf_repaid,
            stats.sf_transferred,
            stats.discounted_principal_delta,
            stats.liquidation_start_count,
            stats.liquidation_restart_count,
            stats.liquidation_ended_count,
            stats.lp_incurred,
            stats.ii_incurred,
            stats.ii_paid,
            stats.fees_incurred,
            stats.fees_paid,
            stats.principal_incurred,
            stats.principal_paid,
            stats.bad_debt_count_incr,
            stats.bad_debt_count_decr,
            stats.bad_debt_ii_incurred,
            stats.bad_debt_ii_recovered,
            stats.bad_debt_fees_incurred,
            stats.bad_debt_fees_recovered,
            stats.bad_debt_principal_incurred,
            stats.bad_debt_principal_recovered,
            stats.savings_vault_operations_count,
            stats.savings_vault_count_incr,
            stats.savings_vault_count_decr,
            stats.discounted_savings_balance_delta,
            stats.byc_deposited,
            stats.byc_withdrawn,
            stats.interest_paid,
            stats.approved_announcer_count_delta,
            stats.treasury_coin_count_delta,
            stats.treasury_balance_delta,
            stats.recharge_auction_coin_count_delta,
            stats.recharge_auction_count_delta,
            stats.surplus_auction_count_delta,
            stats.governance_operations_count,
            stats.governance_coin_count_delta,
            stats.governance_circulation_delta,
            stats.crt_circulation_delta,
            stats.registry_operations_count,
            stats.registered_announcer_count_delta,
        ),
    )


def _get_last_height(conn: sqlite3.Connection) -> Optional[int]:
    row = conn.execute("SELECT height FROM scanner_height WHERE id=1").fetchone()
    return row[0] if row else None


def _set_last_height(conn: sqlite3.Connection, height: int) -> None:
    conn.execute("INSERT OR REPLACE INTO scanner_height (id, height) VALUES (1, ?)", (height,))


def _get_live_hashes(conn: sqlite3.Connection, start: int, end: int) -> Dict[int, str]:
    rows = conn.execute(
        "SELECT height, block_hash FROM live_block_hash WHERE height >= ? AND height <= ?",
        (start, end),
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def _downsample_old_blockstats(conn: sqlite3.Connection, current_timestamp: int) -> None:
    SEVEN_DAYS_SECONDS = 7 * SECONDS_PER_DAY
    cutoff_timestamp = ((current_timestamp - SEVEN_DAYS_SECONDS) // SECONDS_PER_DAY) * SECONDS_PER_DAY

    max_row = conn.execute("SELECT MAX(timestamp) FROM daily_block_stats_v2").fetchone()
    max_downsampled_ts = max_row[0] if max_row and max_row[0] is not None else None
    start_timestamp = (max_downsampled_ts // SECONDS_PER_DAY) * SECONDS_PER_DAY if max_downsampled_ts is not None else 0

    old_stats_rows = conn.execute(
        "SELECT * FROM block_stats_v2 WHERE timestamp < ? AND timestamp >= ? ORDER BY timestamp",
        (cutoff_timestamp, start_timestamp),
    ).fetchall()

    if not old_stats_rows:
        return
    if max_downsampled_ts is not None and old_stats_rows[-1][2] <= max_downsampled_ts:
        return

    # Column names for block_stats_v2 (positional)
    cols = [desc[0] for desc in conn.execute("SELECT * FROM block_stats_v2 LIMIT 0").description]

    def row_to_dict(row):
        return dict(zip(cols, row))

    daily_groups: Dict[int, list] = {}
    for row in old_stats_rows:
        d = row_to_dict(row)
        day_ts = (d["timestamp"] // SECONDS_PER_DAY) * SECONDS_PER_DAY
        daily_groups.setdefault(day_ts, []).append(d)

    for day_ts, day_stats in daily_groups.items():
        last = day_stats[-1]

        running_gov = 0
        peak_gov_delta = 0
        running_circ = 0
        peak_circ_delta = 0
        for s in day_stats:
            running_gov += s["governance_coin_count_delta"]
            if running_gov > peak_gov_delta:
                peak_gov_delta = running_gov
            running_circ += s["governance_circulation_delta"]
            if running_circ > peak_circ_delta:
                peak_circ_delta = running_circ

        def _sum(field):
            return sum(s[field] for s in day_stats)

        def _avg_int(field):
            vals = [s[field] for s in day_stats if s[field] is not None]
            return int(sum(vals) / len(vals)) if vals else None

        conn.execute(
            """INSERT OR REPLACE INTO daily_block_stats_v2 VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )""",
            (
                day_ts,
                last["timestamp"],
                last["last_updated"],
                last["statutes_price"],
                _avg_int("current_stability_fee_df"),
                _avg_int("current_interest_rate_df"),
                last["cumulative_stability_fee_df"],
                last["cumulative_interest_rate_df"],
                _sum("vault_operations_count"),
                _sum("vault_count_incr"),
                _sum("vault_count_decr"),
                _sum("collateral_deposited"),
                _sum("collateral_withdrawn"),
                _sum("collateral_sold"),
                _sum("byc_borrowed"),
                _sum("byc_repaid"),
                _sum("sf_repaid"),
                _sum("sf_transferred"),
                _sum("discounted_principal_delta"),
                _sum("liquidation_start_count"),
                _sum("liquidation_restart_count"),
                _sum("liquidation_ended_count"),
                _sum("lp_incurred"),
                _sum("ii_incurred"),
                _sum("ii_paid"),
                _sum("fees_incurred"),
                _sum("fees_paid"),
                _sum("principal_incurred"),
                _sum("principal_paid"),
                _sum("bad_debt_count_incr"),
                _sum("bad_debt_count_decr"),
                _sum("bad_debt_ii_incurred"),
                _sum("bad_debt_ii_recovered"),
                _sum("bad_debt_fees_incurred"),
                _sum("bad_debt_fees_recovered"),
                _sum("bad_debt_principal_incurred"),
                _sum("bad_debt_principal_recovered"),
                _sum("savings_vault_operations_count"),
                _sum("savings_vault_count_incr"),
                _sum("savings_vault_count_decr"),
                _sum("discounted_savings_balance_delta"),
                _sum("byc_deposited"),
                _sum("byc_withdrawn"),
                _sum("interest_paid"),
                _sum("approved_announcer_count_delta"),
                _sum("treasury_coin_count_delta"),
                _sum("treasury_balance_delta"),
                _sum("recharge_auction_coin_count_delta"),
                _sum("recharge_auction_count_delta"),
                _sum("surplus_auction_count_delta"),
                _sum("governance_operations_count"),
                _sum("governance_coin_count_delta"),
                peak_gov_delta,
                _sum("governance_circulation_delta"),
                peak_circ_delta,
                _sum("crt_circulation_delta"),
                _sum("registry_operations_count"),
                _sum("registered_announcer_count_delta"),
            ),
        )


async def scan_blocks(
    client: FullNodeRpcClient,
    db_path: str,
    max_blocks: int | None = None,
) -> Dict[str, Any]:
    """
    Scan new blocks from the Chia node and write analytics data to SQLite.

    Args:
        client: Connected FullNodeRpcClient
        db_path: Path to SQLite database file
        max_blocks: Max blocks to process per call (default 1000)

    Returns:
        Dict with blocks_synced, blocks_with_ops, last_height, last_timestamp
    """
    db_exists = Path(db_path).exists()
    conn = sqlite3.connect(db_path)
    create_tables(conn)

    statutes_struct = get_statutes_struct()
    byc_tail_hash = bytes(BYC_TAIL_HASH)
    crt_tail_hash = bytes(CRT_TAIL_HASH)

    collateral_vault_handler = CollateralVaultHandler()
    announcer_handler = AnnouncerHandler()
    statutes_handler = StatutesHandler()
    oracle_handler = OracleHandler()
    cat_handler = CatHandler()
    registry_handler = RegistryHandler()
    registry_mod_hash = AnnouncerRegistry.get_mod_struct()[1]

    blockchain_state = await client.get_blockchain_state()
    current_height = blockchain_state["peak"].height
    current_timestamp = int(time.time())

    last_height = _get_last_height(conn)

    if last_height is None:
        # First run: start from statutes genesis block
        if not db_exists:
            print(f"Creating analytics database at {db_path}, scanning from protocol genesis.")
        coin_record = await client.get_coin_record_by_name(STATUTES_LAUNCHER_ID)
        if not coin_record:
            log.error("No statutes coin found, check network configuration.")
            conn.close()
            return {"blocks_synced": 0, "blocks_with_ops": 0, "last_height": None, "last_timestamp": None}
        last_height = coin_record.confirmed_block_index - 1
        last_updated = coin_record.timestamp
    else:
        # Initialize running state from last block_stats_v2
        row = conn.execute(
            "SELECT timestamp, current_stability_fee_df, current_interest_rate_df, "
            "cumulative_stability_fee_df, cumulative_interest_rate_df, statutes_price, last_updated "
            "FROM block_stats_v2 ORDER BY height DESC LIMIT 1"
        ).fetchone()
        if row:
            last_updated = row[0]
        else:
            last_updated = current_timestamp

    # Initialize running fee/rate state
    row = conn.execute(
        "SELECT current_stability_fee_df, current_interest_rate_df, "
        "cumulative_stability_fee_df, cumulative_interest_rate_df, statutes_price, last_updated "
        "FROM block_stats_v2 ORDER BY height DESC LIMIT 1"
    ).fetchone()
    if row:
        current_stability_fee_df = row[0]
        current_interest_rate_df = row[1]
        cumulative_stability_fee_df = row[2]
        cumulative_interest_rate_df = row[3]
        statutes_price = row[4]
        last_updated = row[5]
    else:
        current_stability_fee_df = PRECISION
        current_interest_rate_df = PRECISION
        cumulative_stability_fee_df = PRECISION
        cumulative_interest_rate_df = PRECISION
        statutes_price = None
        last_updated = current_timestamp

    # --- Reorg detection ---
    if last_height and last_height > 0:
        check_start_height = max(1, last_height - REORG_CHECK_DEPTH + 1)
        reorg_detected = False
        reorg_height = None

        try:
            end_check_height = min(last_height + 1, current_height + 1)
            current_block_records = await client.get_block_records(check_start_height, end_check_height)
            stored_hashes = _get_live_hashes(conn, check_start_height, last_height)

            for block_record in current_block_records:
                height = block_record["height"]
                current_hash = block_record["header_hash"]
                if height in stored_hashes and stored_hashes[height] != current_hash:
                    log.warning("Reorg at height %s", height)
                    reorg_detected = True
                    reorg_height = height
                    break

            if not reorg_detected:
                for height in stored_hashes:
                    if height > current_height:
                        reorg_detected = True
                        reorg_height = current_height + 1
                        break

        except Exception as e:
            log.exception("Error during reorg check: %s", e)
            reorg_detected = True
            reorg_height = check_start_height

        if reorg_detected:
            rollback_height = max(0, reorg_height - REORG_SAFETY_BUFFER)
            log.warning("Reorg at height %s, rolling back to %s", reorg_height, rollback_height)
            _delete_coins_at_height(conn, reorg_height)
            _set_last_height(conn, rollback_height)
            conn.commit()
            last_height = rollback_height

            if last_height >= current_height:
                conn.close()
                return {"blocks_synced": 0, "blocks_with_ops": 0, "last_height": last_height, "last_timestamp": None}

            # Re-read running state after rollback
            row = conn.execute(
                "SELECT current_stability_fee_df, current_interest_rate_df, "
                "cumulative_stability_fee_df, cumulative_interest_rate_df, statutes_price, last_updated "
                "FROM block_stats_v2 ORDER BY height DESC LIMIT 1"
            ).fetchone()
            if row:
                current_stability_fee_df, current_interest_rate_df = row[0], row[1]
                cumulative_stability_fee_df, cumulative_interest_rate_df = row[2], row[3]
                statutes_price = row[4]
                last_updated = row[5]
            else:
                current_stability_fee_df = PRECISION
                current_interest_rate_df = PRECISION
                cumulative_stability_fee_df = PRECISION
                cumulative_interest_rate_df = PRECISION
                statutes_price = None

    if last_height >= current_height:
        conn.close()
        return {"blocks_synced": 0, "blocks_with_ops": 0, "last_height": last_height, "last_timestamp": None}

    if max_blocks is not None:
        current_height = min(last_height + max_blocks, current_height)

    print(f"Scanning blocks {last_height + 1} to {current_height} ({current_height - last_height} blocks)")

    total_blocks_scanned = 0
    blocks_with_ops = 0
    new_last_height = -1
    new_last_timestamp = None
    last_printed_milestone = last_height // 10000

    sem = asyncio.Semaphore(SPENDS_FETCH_CONCURRENCY)

    for end_height in range(last_height + 1, current_height + 1, BLOCKS_TO_FETCH):
        retries = 3
        while retries:
            try:
                block_records = await client.get_block_records(end_height, end_height + BLOCKS_TO_FETCH)
                break
            except asyncio.TimeoutError:
                log.warning("Timeout fetching block records, retrying...")
                await asyncio.sleep(5)
                retries -= 1
        else:
            raise RuntimeError("Failed to fetch block records from RPC")

        if not block_records:
            break

        total_blocks_scanned += len(block_records)

        # Pre-fetch block spends for all transaction blocks in this batch concurrently.
        tx_records = [br for br in block_records if br["timestamp"]]
        if tx_records:
            fetched = await asyncio.gather(
                *[_fetch_block_spends(client, bytes32.from_hexstr(br["header_hash"]), sem) for br in tx_records]
            )
            spends_map = {br["header_hash"]: spends for br, spends in zip(tx_records, fetched)}
        else:
            spends_map = {}

        for block_record in block_records:
            if not block_record["timestamp"]:
                new_last_height = block_record["height"]
                continue

            block_delta = StatsDelta()
            coins_to_add = []
            coins_to_remove: Set[str] = set()
            last_statutes_info = None

            block_spends = spends_map.get(block_record["header_hash"])

            if block_spends:
                log.debug("Processing spends for height: %s", block_record["height"])
                for coin_spend in block_spends:
                    mod, args = uncurry(coin_spend.puzzle_reveal)
                    mod_hash = mod.get_tree_hash()
                    handler_result = None

                    if mod_hash == COLLATERAL_VAULT_MOD_HASH:
                        handler_result = collateral_vault_handler.handle(coin_spend, block_record, statutes_struct)
                    elif mod_hash == ATOM_ANNOUNCER_MOD_HASH:
                        handler_result = announcer_handler.handle(coin_spend, block_record, statutes_struct)
                    elif mod_hash == registry_mod_hash:
                        handler_result = registry_handler.handle(coin_spend, block_record, statutes_struct)
                    elif mod_hash == CAT_MOD_HASH:
                        handler_result = cat_handler.handle(
                            coin_spend, block_record, statutes_struct, byc_tail_hash, crt_tail_hash
                        )
                    elif mod_hash == SINGLETON_ISA_MOD_HASH:
                        handler_result = statutes_handler.handle(
                            coin_spend, block_record, statutes_struct, last_updated
                        )
                        if handler_result and handler_result.stats_delta.last_updated is not None:
                            last_updated = handler_result.stats_delta.last_updated
                    elif mod_hash == SINGLETON_MOD_HASH:
                        handler_result = oracle_handler.handle(coin_spend, block_record, statutes_struct)

                    if handler_result:
                        block_delta = block_delta + handler_result.stats_delta
                        coins_to_add.extend(handler_result.coins_to_add)
                        coins_to_remove.update(handler_result.coins_to_remove)
                        if handler_result.last_statutes_info:
                            last_statutes_info = handler_result.last_statutes_info

            # Update running state
            if block_delta.statutes_price is not None:
                statutes_price = block_delta.statutes_price
            if block_delta.last_updated is not None:
                last_updated = block_delta.last_updated
            if block_delta.current_stability_fee_df is not None:
                current_stability_fee_df = block_delta.current_stability_fee_df
            if block_delta.current_interest_rate_df is not None:
                current_interest_rate_df = block_delta.current_interest_rate_df
            if block_delta.cumulative_stability_fee_df is not None:
                cumulative_stability_fee_df = block_delta.cumulative_stability_fee_df
            if block_delta.cumulative_interest_rate_df is not None:
                cumulative_interest_rate_df = block_delta.cumulative_interest_rate_df

            has_ops = (
                block_delta.vault_operations_count > 0
                or block_delta.savings_vault_operations_count > 0
                or block_delta.recharge_operations_count > 0
                or block_delta.surplus_operations_count > 0
                or block_delta.announcer_operations_count > 0
                or block_delta.registry_operations_count > 0
                or block_delta.governance_operations_count > 0
                or block_delta.governance_coin_count_delta != 0
                or block_delta.treasury_coin_count_delta != 0
                or block_delta.treasury_balance_delta != 0
                or block_delta.statutes_spend_found
            )

            if has_ops:
                block_stat = BlockStatsV2(
                    height=block_record["height"],
                    block_hash=block_record["header_hash"],
                    timestamp=block_record["timestamp"],
                    last_updated=last_updated,
                    statutes_price=statutes_price,
                    current_stability_fee_df=current_stability_fee_df,
                    current_interest_rate_df=current_interest_rate_df,
                    cumulative_stability_fee_df=cumulative_stability_fee_df,
                    cumulative_interest_rate_df=cumulative_interest_rate_df,
                    vault_operations_count=block_delta.vault_operations_count,
                    vault_count_incr=block_delta.vault_count_incr,
                    vault_count_decr=block_delta.vault_count_decr,
                    collateral_deposited=block_delta.collateral_deposited,
                    collateral_withdrawn=block_delta.collateral_withdrawn,
                    collateral_sold=block_delta.collateral_sold,
                    byc_borrowed=block_delta.byc_borrowed,
                    byc_repaid=block_delta.byc_repaid,
                    sf_repaid=block_delta.sf_repaid,
                    sf_transferred=block_delta.sf_transferred,
                    discounted_principal_delta=block_delta.discounted_principal_delta,
                    liquidation_start_count=block_delta.liquidation_start_count,
                    liquidation_restart_count=block_delta.liquidation_restart_count,
                    liquidation_ended_count=block_delta.liquidation_ended_count,
                    lp_incurred=block_delta.lp_incurred,
                    ii_incurred=block_delta.ii_incurred,
                    ii_paid=block_delta.ii_paid,
                    fees_incurred=block_delta.fees_incurred,
                    fees_paid=block_delta.fees_paid,
                    principal_incurred=block_delta.principal_incurred,
                    principal_paid=block_delta.principal_paid,
                    bad_debt_count_incr=block_delta.bad_debt_count_incr,
                    bad_debt_count_decr=block_delta.bad_debt_count_decr,
                    bad_debt_ii_incurred=block_delta.bad_debt_ii_incurred,
                    bad_debt_ii_recovered=block_delta.bad_debt_ii_recovered,
                    bad_debt_fees_incurred=block_delta.bad_debt_fees_incurred,
                    bad_debt_fees_recovered=block_delta.bad_debt_fees_recovered,
                    bad_debt_principal_incurred=block_delta.bad_debt_principal_incurred,
                    bad_debt_principal_recovered=block_delta.bad_debt_principal_recovered,
                    savings_vault_operations_count=block_delta.savings_vault_operations_count,
                    savings_vault_count_incr=block_delta.savings_vault_count_incr,
                    savings_vault_count_decr=block_delta.savings_vault_count_decr,
                    discounted_savings_balance_delta=block_delta.discounted_savings_balance_delta,
                    byc_deposited=block_delta.byc_deposited,
                    byc_withdrawn=block_delta.byc_withdrawn,
                    interest_paid=block_delta.interest_paid,
                    approved_announcer_count_delta=block_delta.approved_announcer_count_delta,
                    treasury_coin_count_delta=block_delta.treasury_coin_count_delta,
                    treasury_balance_delta=block_delta.treasury_balance_delta,
                    recharge_auction_coin_count_delta=block_delta.recharge_auction_coin_count_delta,
                    recharge_auction_count_delta=block_delta.recharge_auction_count_delta,
                    surplus_auction_count_delta=block_delta.surplus_auction_count_delta,
                    governance_operations_count=block_delta.governance_operations_count,
                    governance_coin_count_delta=block_delta.governance_coin_count_delta,
                    governance_circulation_delta=block_delta.governance_circulation_delta,
                    crt_circulation_delta=block_delta.crt_circulation_delta,
                    registry_operations_count=block_delta.registry_operations_count,
                    registered_announcer_count_delta=block_delta.registered_announcer_count_delta,
                )
                _write_block_stats(conn, block_stat)
                blocks_with_ops += 1

            # Write coin changes
            for coin in coins_to_add:
                if isinstance(coin, GoverningCRT) or coin.name not in coins_to_remove:
                    try:
                        _save_coin(conn, coin)
                    except Exception:
                        log.exception("Failed to save coin %s", coin)

            _mark_coins_spent(conn, coins_to_remove)

            conn.execute(
                "INSERT OR REPLACE INTO live_block_hash (height, block_hash) VALUES (?,?)",
                (block_record["height"], block_record["header_hash"]),
            )

            new_last_height = block_record["height"]
            new_last_timestamp = block_record["timestamp"]

            await asyncio.sleep(0)

        if new_last_height != -1:
            _set_last_height(conn, new_last_height)

        _downsample_old_blockstats(conn, current_timestamp)
        conn.commit()

        if new_last_height != -1 and new_last_height // 10000 > last_printed_milestone:
            last_printed_milestone = new_last_height // 10000
            print(
                f"Scanned to block {new_last_height} ({total_blocks_scanned} blocks scanned, {blocks_with_ops} with ops, {current_height - new_last_height} blocks left)"
            )

    final_height = new_last_height if new_last_height != -1 else last_height
    conn.close()

    log.info(
        "Finished scan_blocks: %d blocks scanned (%d with ops), up to height %d",
        total_blocks_scanned,
        blocks_with_ops,
        final_height,
    )
    return {
        "blocks_synced": total_blocks_scanned,
        "blocks_with_ops": blocks_with_ops,
        "last_height": final_height,
        "last_timestamp": new_last_timestamp,
    }
