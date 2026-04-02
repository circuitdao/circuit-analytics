"""
Full on-chain rescan integration test.

Scans all blocks from the protocol launch block to the current chain tip,
writes to a temporary SQLite DB, and asserts protocol invariants on the result.

Run with:
    poetry run pytest tests/test_rescan.py -v -s --timeout=7200

Requires a reachable Chia full node. Configure via env vars:
    CHIA_ROOT   (default: ~/.chia/mainnet-macmini)
    CHIA_NODES  (default: 192.168.1.100:8555)
"""
import logging
import os
import sqlite3
import tempfile
import time
from pathlib import Path

import pytest
from chia.full_node.full_node_rpc_client import FullNodeRpcClient
from chia.util.config import load_config
from chia_rs.sized_ints import uint16

from circuit_analytics.scanner.block_scanner import scan_blocks

log = logging.getLogger(__name__)

PROTOCOL_LAUNCH_HEIGHT = 8_135_347
DEFAULT_CHIA_ROOT = Path.home() / ".chia" / "mainnet-macmini"
DEFAULT_NODE = "192.168.1.100:8555"


@pytest.fixture
async def full_node_client():
    root_path = Path(os.environ.get("CHIA_ROOT", DEFAULT_CHIA_ROOT))
    node_str = os.environ.get("CHIA_NODES", DEFAULT_NODE).split(",")[0].strip()
    host, port = node_str.rsplit(":", 1)

    config = load_config(root_path, "config.yaml")
    client = await FullNodeRpcClient.create(host, uint16(int(port)), root_path, config)
    try:
        await client.healthz()
    except Exception as e:
        pytest.skip(f"Full node not reachable at {host}:{port}: {e}")

    yield client
    client.close()
    await client.await_closed()


@pytest.mark.integration
async def test_full_rescan(full_node_client, max_blocks):
    """
    Scan all blocks from the protocol launch to chain tip and assert
    protocol-level invariants on the accumulated data.

    Pass --max-blocks N to limit the scan to N blocks after launch (useful for smoke testing).
    """
    client = full_node_client

    blockchain_state = await client.get_blockchain_state()
    tip_height = blockchain_state["peak"].height
    full_blocks_available = tip_height - PROTOCOL_LAUNCH_HEIGHT
    blocks_to_scan = min(max_blocks, full_blocks_available) if max_blocks is not None else full_blocks_available
    print(f"\nChain tip: {tip_height}  |  Blocks to scan: ~{blocks_to_scan}", flush=True)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        total_synced = 0
        total_with_ops = 0
        t0 = time.monotonic()

        while True:
            remaining = (blocks_to_scan - total_synced) if max_blocks is not None else 5000
            result = await scan_blocks(client, db_path, max_blocks=min(5000, remaining))
            synced = result["blocks_synced"]
            total_synced += synced
            total_with_ops += result["blocks_with_ops"]

            elapsed = time.monotonic() - t0
            pct = 100.0 * total_synced / blocks_to_scan if blocks_to_scan else 100
            rate = total_synced / elapsed if elapsed > 0 else 0
            eta = (blocks_to_scan - total_synced) / rate if rate > 0 else 0
            print(
                f"[{elapsed:5.0f}s] {total_synced:>7}/{blocks_to_scan} blocks "
                f"({pct:5.1f}%)  height={result['last_height']}  "
                f"ops_blocks={total_with_ops}  rate={rate:.0f} blk/s  ETA={eta:.0f}s",
                flush=True,
            )

            if synced == 0 or (max_blocks is not None and total_synced >= blocks_to_scan):
                break

        elapsed = time.monotonic() - t0
        print(f"\nRescan complete in {elapsed:.1f}s. Total blocks: {total_synced}, with ops: {total_with_ops}", flush=True)

        _assert_invariants(db_path, tip_height if max_blocks is None else None)

    finally:
        os.unlink(db_path)


def _assert_invariants(db_path: str, tip_height: int | None) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # --- Scanner reached near the tip (only when doing a full rescan) ---
    row = conn.execute("SELECT height FROM scanner_height WHERE id=1").fetchone()
    assert row is not None, "scanner_height not written"
    scanner_height = row["height"]
    if tip_height is not None:
        assert scanner_height >= tip_height - 100, (
            f"Scanner stopped at {scanner_height}, expected near tip {tip_height}"
        )
    print(f"Scanner height: {scanner_height}", flush=True)

    # --- At least some blocks with protocol activity ---
    blocks_with_ops = conn.execute("SELECT COUNT(*) FROM block_stats_v2").fetchone()[0]
    assert blocks_with_ops > 0, "No blocks with protocol activity found"
    print(f"Blocks with ops: {blocks_with_ops}", flush=True)

    # --- Statutes price was observed ---
    row = conn.execute(
        "SELECT statutes_price FROM block_stats_v2 WHERE statutes_price IS NOT NULL LIMIT 1"
    ).fetchone()
    assert row is not None, "No statutes price observed in any block"
    print(f"First observed statutes price: {row['statutes_price']}", flush=True)

    # --- Vault operations occurred ---
    totals = conn.execute("""
        SELECT
            SUM(vault_operations_count)         AS vault_ops,
            SUM(vault_count_incr)               AS vaults_opened,
            SUM(vault_count_decr)               AS vaults_closed,
            SUM(byc_borrowed)                   AS total_borrowed,
            SUM(byc_repaid)                     AS total_repaid,
            SUM(collateral_deposited)           AS total_deposited,
            SUM(collateral_withdrawn)           AS total_withdrawn,
            SUM(collateral_sold)                AS total_sold,
            SUM(liquidation_start_count)        AS liquidations_started,
            SUM(bad_debt_count_incr)            AS bad_debt_incr,
            SUM(bad_debt_count_decr)            AS bad_debt_decr,
            SUM(savings_vault_operations_count) AS savings_ops,
            SUM(governance_operations_count)    AS gov_ops,
            SUM(registry_operations_count)      AS registry_ops
        FROM block_stats_v2
    """).fetchone()

    print(
        f"Vault ops={totals['vault_ops'] or 0}  opened={totals['vaults_opened'] or 0}  "
        f"closed={totals['vaults_closed'] or 0}  borrowed={totals['total_borrowed'] or 0}  "
        f"repaid={totals['total_repaid'] or 0}  deposited={totals['total_deposited'] or 0}  "
        f"withdrawn={totals['total_withdrawn'] or 0}  sold={totals['total_sold'] or 0}  "
        f"liquidations={totals['liquidations_started'] or 0}",
        flush=True,
    )

    # Presence assertions: only meaningful over a full rescan (enough history to have activity)
    if tip_height is not None:
        assert (totals["vault_ops"] or 0) > 0, "No vault operations found"
        assert (totals["vaults_opened"] or 0) > 0, "No vaults were ever opened"
        assert (totals["total_borrowed"] or 0) > 0, "No BYC ever borrowed"
        assert (totals["total_deposited"] or 0) > 0, "No collateral ever deposited"
        assert (totals["registry_ops"] or 0) > 0, "No registry operations found"

    # Conservation invariants: always hold regardless of scan window
    assert (totals["total_repaid"] or 0) <= (totals["total_borrowed"] or 0), (
        f"Repaid {totals['total_repaid']} > borrowed {totals['total_borrowed']}"
    )

    total_out = (totals["total_withdrawn"] or 0) + (totals["total_sold"] or 0)
    assert total_out <= (totals["total_deposited"] or 0), (
        f"Collateral out ({total_out}) > collateral in ({totals['total_deposited']})"
    )

    assert (totals["bad_debt_decr"] or 0) <= (totals["bad_debt_incr"] or 0), (
        f"bad_debt_count_decr {totals['bad_debt_decr']} > bad_debt_count_incr {totals['bad_debt_incr']}"
    )

    assert (totals["vaults_closed"] or 0) <= (totals["vaults_opened"] or 0), (
        f"vaults_closed {totals['vaults_closed']} > vaults_opened {totals['vaults_opened']}"
    )

    # --- Active vault coins sanity ---
    active_vaults = conn.execute("SELECT COUNT(*) FROM vault_coin").fetchone()[0]
    negative_collateral = conn.execute(
        "SELECT COUNT(*) FROM vault_coin WHERE collateral < 0"
    ).fetchone()[0]
    assert negative_collateral == 0, f"{negative_collateral} vault(s) have negative collateral"
    print(f"Active vaults: {active_vaults}", flush=True)

    # --- Active savings vaults sanity ---
    negative_savings = conn.execute(
        "SELECT COUNT(*) FROM savings_vault_coin WHERE balance < 0"
    ).fetchone()[0]
    assert negative_savings == 0, f"{negative_savings} savings vault(s) have negative balance"

    # --- Governance: circulation is non-negative ---
    gov_circulation = conn.execute(
        "SELECT SUM(governance_circulation_delta) FROM block_stats_v2"
    ).fetchone()[0] or 0
    assert gov_circulation >= 0, f"Total governance circulation delta is negative: {gov_circulation}"

    conn.close()
