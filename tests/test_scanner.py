"""Smoke tests for the circuit-analytics scanner."""
import dataclasses
import sqlite3

import pytest


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

def test_handler_imports():
    from circuit_analytics.scanner.handlers.vault import CollateralVaultHandler
    from circuit_analytics.scanner.handlers.savings import SavingsHandler
    from circuit_analytics.scanner.handlers.announcer import AnnouncerHandler
    from circuit_analytics.scanner.handlers.treasury import TreasuryHandler
    from circuit_analytics.scanner.handlers.recharge_auction import RechargeAuctionHandler
    from circuit_analytics.scanner.handlers.surplus_auction import SurplusAuctionHandler
    from circuit_analytics.scanner.handlers.registry import RegistryHandler
    from circuit_analytics.scanner.handlers.oracle import OracleHandler
    from circuit_analytics.scanner.handlers.singleton_isa import StatutesHandler
    from circuit_analytics.scanner.handlers.governance import GovernanceHandler, LaunchGovernanceHandler
    from circuit_analytics.scanner.handlers.cat import CatHandler


def test_block_scanner_imports():
    from circuit_analytics.scanner.block_scanner import scan_blocks, get_statutes_struct


# ---------------------------------------------------------------------------
# StatsDelta arithmetic
# ---------------------------------------------------------------------------

def test_stats_delta_add_integers():
    from circuit_analytics.scanner.handlers.base import StatsDelta

    a = StatsDelta(vault_operations_count=3, byc_borrowed=1000, collateral_deposited=500)
    b = StatsDelta(vault_operations_count=2, byc_borrowed=200, collateral_withdrawn=100)
    c = a + b

    assert c.vault_operations_count == 5
    assert c.byc_borrowed == 1200
    assert c.collateral_deposited == 500
    assert c.collateral_withdrawn == 100


def test_stats_delta_add_zero():
    from circuit_analytics.scanner.handlers.base import StatsDelta

    a = StatsDelta(vault_count_incr=7, discounted_principal_delta=-300)
    zero = StatsDelta()
    assert (a + zero).vault_count_incr == 7
    assert (zero + a).vault_count_incr == 7
    assert (a + zero).discounted_principal_delta == -300


def test_stats_delta_add_optional_fields_take_latest():
    from circuit_analytics.scanner.handlers.base import StatsDelta

    a = StatsDelta(statutes_price=100, last_updated=1000, current_stability_fee_df=5)
    b = StatsDelta(statutes_price=200, last_updated=2000)
    c = a + b

    # other's value wins when set
    assert c.statutes_price == 200
    assert c.last_updated == 2000
    # self's value preserved when other is None
    assert c.current_stability_fee_df == 5


def test_stats_delta_add_optional_fields_preserve_when_other_none():
    from circuit_analytics.scanner.handlers.base import StatsDelta

    a = StatsDelta(statutes_price=100, cumulative_stability_fee_df=999)
    b = StatsDelta()  # all optionals are None
    c = a + b

    assert c.statutes_price == 100
    assert c.cumulative_stability_fee_df == 999


def test_stats_delta_add_statutes_spend_found_is_or():
    from circuit_analytics.scanner.handlers.base import StatsDelta

    assert (StatsDelta(statutes_spend_found=False) + StatsDelta(statutes_spend_found=False)).statutes_spend_found is False
    assert (StatsDelta(statutes_spend_found=True) + StatsDelta(statutes_spend_found=False)).statutes_spend_found is True
    assert (StatsDelta(statutes_spend_found=False) + StatsDelta(statutes_spend_found=True)).statutes_spend_found is True


def test_stats_delta_bool_not_summed_as_int():
    # statutes_spend_found is a bool field — verify it is excluded from int summation
    # (if it were included, True + True would give 2 instead of True)
    from circuit_analytics.scanner.handlers.base import StatsDelta

    a = StatsDelta(statutes_spend_found=True)
    b = StatsDelta(statutes_spend_found=True)
    c = a + b
    assert c.statutes_spend_found is True
    assert not isinstance(c.statutes_spend_found, int) or c.statutes_spend_found == True  # noqa: E712


# ---------------------------------------------------------------------------
# HandlerResult
# ---------------------------------------------------------------------------

def test_handler_result_defaults():
    from circuit_analytics.scanner.handlers.base import HandlerResult, StatsDelta

    r = HandlerResult()
    assert r.coins_to_add == []
    assert r.coins_to_remove == set()
    assert isinstance(r.stats_delta, StatsDelta)
    assert r.last_statutes_info is None


# ---------------------------------------------------------------------------
# Governance op name mapping
# ---------------------------------------------------------------------------

def test_plain_op_name():
    from circuit_analytics.scanner.handlers.governance import plain_op_name

    assert plain_op_name(None, with_governance=True) == "enter_governance"
    assert plain_op_name(None, with_governance=False) == "exit_governance"
    assert plain_op_name("PROGRAM_GOVERNANCE_RESET_BILL_MOD") == "reset_bill"
    assert plain_op_name("PROGRAM_GOVERNANCE_PROPOSE_BILL_MOD") == "propose_bill"
    assert plain_op_name("PROGRAM_GOVERNANCE_VETO_ANNOUNCEMENT_MOD") == "announce_veto"
    assert plain_op_name("PROGRAM_GOVERNANCE_VETO_BILL_MOD") == "veto_bill"
    assert plain_op_name("PROGRAM_GOVERNANCE_IMPLEMENT_BILL_MOD") == "implement_bill"

    with pytest.raises(ValueError):
        plain_op_name("UNKNOWN_MOD")


# ---------------------------------------------------------------------------
# SQLite schema
# ---------------------------------------------------------------------------

@pytest.fixture
def mem_db():
    conn = sqlite3.connect(":memory:")
    from circuit_analytics.scanner.models import create_tables
    create_tables(conn)
    yield conn
    conn.close()


def test_create_tables(mem_db):
    tables = {row[0] for row in mem_db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    expected = {
        "vault_coin", "savings_vault_coin", "auction_coin", "treasury_coin",
        "announcer_coin", "governing_crt", "live_block_hash",
        "block_stats_v2", "daily_block_stats_v2", "scanner_height",
    }
    assert expected.issubset(tables)


def test_block_stats_roundtrip(mem_db):
    from circuit_analytics.scanner.models import BlockStatsV2
    from circuit_analytics.scanner.block_scanner import _write_block_stats

    stats = BlockStatsV2(
        height=1000,
        block_hash="abc123",
        timestamp=1700000000,
        vault_operations_count=3,
        byc_borrowed=5000,
        collateral_deposited=100000,
        statutes_price=123456,
        current_stability_fee_df=999,
    )
    _write_block_stats(mem_db, stats)
    mem_db.commit()

    row = mem_db.execute("SELECT * FROM block_stats_v2 WHERE height=1000").fetchone()
    cols = [d[0] for d in mem_db.execute("SELECT * FROM block_stats_v2 LIMIT 0").description]
    d = dict(zip(cols, row))

    assert d["height"] == 1000
    assert d["block_hash"] == "abc123"
    assert d["vault_operations_count"] == 3
    assert d["byc_borrowed"] == 5000
    assert d["statutes_price"] == 123456
    assert d["current_stability_fee_df"] == 999
    assert d["collateral_withdrawn"] == 0  # default


def test_vault_coin_roundtrip(mem_db):
    from circuit_analytics.scanner.models import VaultCoin
    from circuit_analytics.scanner.block_scanner import _upsert_vault_coin, _mark_coins_spent

    coin = VaultCoin(
        name="deadbeef",
        collateral=1000000,
        principal=500,
        discounted_principal=490,
        auction_state="0",
        in_bad_debt=False,
        inner_puzzle_hash="cafebabe",
        height=42,
    )
    _upsert_vault_coin(mem_db, coin)
    mem_db.commit()

    row = mem_db.execute("SELECT * FROM vault_coin WHERE name='deadbeef'").fetchone()
    assert row is not None
    assert row[1] == 1000000  # collateral

    # spending removes vault coins
    _mark_coins_spent(mem_db, {"deadbeef"})
    mem_db.commit()
    assert mem_db.execute("SELECT COUNT(*) FROM vault_coin WHERE name='deadbeef'").fetchone()[0] == 0


def test_governing_crt_spent_flag(mem_db):
    from circuit_analytics.scanner.models import GoverningCRT
    from circuit_analytics.scanner.block_scanner import _upsert_governing_crt, _mark_coins_spent

    coin = GoverningCRT(
        name="crt001",
        amount=1_000_000_000,
        bill=None,
        operation=None,
        timestamp=1700000000,
        spent=False,
        height=100,
    )
    _upsert_governing_crt(mem_db, coin)
    mem_db.commit()

    assert mem_db.execute("SELECT spent FROM governing_crt WHERE name='crt001'").fetchone()[0] == 0

    _mark_coins_spent(mem_db, {"crt001"})
    mem_db.commit()

    assert mem_db.execute("SELECT spent FROM governing_crt WHERE name='crt001'").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Field consistency: StatsDelta int fields map to BlockStatsV2
# ---------------------------------------------------------------------------

def test_stats_delta_fields_covered_by_block_stats():
    """Every int field written from StatsDelta into BlockStatsV2 must exist on both."""
    from circuit_analytics.scanner.handlers.base import StatsDelta
    from circuit_analytics.scanner.models import BlockStatsV2

    # Fields in StatsDelta that are intentionally NOT persisted to block_stats_v2
    # (operation-level counters aggregated differently or not needed in DB)
    excluded = {
        "statutes_spend_found",  # bool, handled separately
        "statutes_price", "last_updated",  # optional scalars, not int-summed
        "cumulative_stability_fee_df", "cumulative_interest_rate_df",  # running state, not from delta
        "current_stability_fee_df", "current_interest_rate_df",  # optional scalars
        "announcer_operations_count",  # not stored in block_stats_v2
        "recharge_operations_count",   # not stored in block_stats_v2
        "surplus_operations_count",    # not stored in block_stats_v2
    }

    delta_int_fields = {
        f.name for f in dataclasses.fields(StatsDelta)
        if f.name not in excluded
    }
    block_stats_fields = {f.name for f in dataclasses.fields(BlockStatsV2)}

    missing = delta_int_fields - block_stats_fields
    assert not missing, f"StatsDelta fields not in BlockStatsV2: {missing}"
