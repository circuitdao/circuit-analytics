from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class VaultCoin:
    name: str
    collateral: int
    principal: int
    discounted_principal: int
    auction_state: str
    in_bad_debt: bool
    inner_puzzle_hash: str
    height: int


@dataclass
class SavingsVaultCoin:
    name: str
    balance: int
    discounted_balance: int
    inner_puzzle_hash: str
    height: int


@dataclass
class AuctionCoin:
    name: str
    auction_type: int
    auction_status: int
    parent_name: str
    height: int


@dataclass
class TreasuryCoin:
    name: str
    launcher_id: str
    ring_prev_launcher_id: str
    amount: int
    parent_name: str
    height: int


@dataclass
class AnnouncerCoin:
    name: str
    launcher_id: str
    timestamp_expires: Optional[int]
    price: Optional[int]
    timestamp: Optional[int]
    spent: bool
    approved: bool
    height: int


@dataclass
class GoverningCRT:
    name: str
    amount: int
    bill: Optional[str]
    operation: Optional[str]
    timestamp: int
    spent: bool
    height: int


@dataclass
class LiveBlockHash:
    height: int
    block_hash: str


@dataclass
class BlockStatsV2:
    height: int
    block_hash: str
    timestamp: int
    last_updated: Optional[int] = None
    statutes_price: Optional[int] = None
    current_stability_fee_df: Optional[int] = None
    current_interest_rate_df: Optional[int] = None
    cumulative_stability_fee_df: int = 0
    cumulative_interest_rate_df: int = 0
    vault_operations_count: int = 0
    vault_count_incr: int = 0
    vault_count_decr: int = 0
    collateral_deposited: int = 0
    collateral_withdrawn: int = 0
    collateral_sold: int = 0
    byc_borrowed: int = 0
    byc_repaid: int = 0
    sf_repaid: int = 0
    sf_transferred: int = 0
    discounted_principal_delta: int = 0
    liquidation_start_count: int = 0
    liquidation_restart_count: int = 0
    liquidation_ended_count: int = 0
    lp_incurred: int = 0
    ii_incurred: int = 0
    ii_paid: int = 0
    fees_incurred: int = 0
    fees_paid: int = 0
    principal_incurred: int = 0
    principal_paid: int = 0
    bad_debt_count_incr: int = 0
    bad_debt_count_decr: int = 0
    bad_debt_ii_incurred: int = 0
    bad_debt_ii_recovered: int = 0
    bad_debt_fees_incurred: int = 0
    bad_debt_fees_recovered: int = 0
    bad_debt_principal_incurred: int = 0
    bad_debt_principal_recovered: int = 0
    savings_vault_operations_count: int = 0
    savings_vault_count_incr: int = 0
    savings_vault_count_decr: int = 0
    discounted_savings_balance_delta: int = 0
    byc_deposited: int = 0
    byc_withdrawn: int = 0
    interest_paid: int = 0
    approved_announcer_count_delta: int = 0
    treasury_coin_count_delta: int = 0
    treasury_balance_delta: int = 0
    recharge_auction_coin_count_delta: int = 0
    recharge_auction_count_delta: int = 0
    surplus_auction_count_delta: int = 0
    governance_operations_count: int = 0
    governance_coin_count_delta: int = 0
    governance_circulation_delta: int = 0
    crt_circulation_delta: int = 0
    registry_operations_count: int = 0
    registered_announcer_count_delta: int = 0


@dataclass
class DailyBlockStatsV2:
    date: int  # Unix timestamp of day start (midnight UTC)
    timestamp: int  # Last timestamp of the day
    last_updated: Optional[int] = None
    statutes_price: Optional[int] = None
    current_stability_fee_df: Optional[int] = None
    current_interest_rate_df: Optional[int] = None
    cumulative_stability_fee_df: int = 0
    cumulative_interest_rate_df: int = 0
    vault_operations_count: int = 0
    vault_count_incr: int = 0
    vault_count_decr: int = 0
    collateral_deposited: int = 0
    collateral_withdrawn: int = 0
    collateral_sold: int = 0
    byc_borrowed: int = 0
    byc_repaid: int = 0
    sf_repaid: int = 0
    sf_transferred: int = 0
    discounted_principal_delta: int = 0
    liquidation_start_count: int = 0
    liquidation_restart_count: int = 0
    liquidation_ended_count: int = 0
    lp_incurred: int = 0
    ii_incurred: int = 0
    ii_paid: int = 0
    fees_incurred: int = 0
    fees_paid: int = 0
    principal_incurred: int = 0
    principal_paid: int = 0
    bad_debt_count_incr: int = 0
    bad_debt_count_decr: int = 0
    bad_debt_ii_incurred: int = 0
    bad_debt_ii_recovered: int = 0
    bad_debt_fees_incurred: int = 0
    bad_debt_fees_recovered: int = 0
    bad_debt_principal_incurred: int = 0
    bad_debt_principal_recovered: int = 0
    savings_vault_operations_count: int = 0
    savings_vault_count_incr: int = 0
    savings_vault_count_decr: int = 0
    discounted_savings_balance_delta: int = 0
    byc_deposited: int = 0
    byc_withdrawn: int = 0
    interest_paid: int = 0
    approved_announcer_count_delta: int = 0
    treasury_coin_count_delta: int = 0
    treasury_balance_delta: int = 0
    recharge_auction_coin_count_delta: int = 0
    recharge_auction_count_delta: int = 0
    surplus_auction_count_delta: int = 0
    governance_operations_count: int = 0
    governance_coin_count_delta: int = 0
    governance_coin_count_peak_delta: int = 0
    governance_circulation_delta: int = 0
    governance_circulation_peak_delta: int = 0
    crt_circulation_delta: int = 0
    registry_operations_count: int = 0
    registered_announcer_count_delta: int = 0


@dataclass
class ScannerHeight:
    id: int  # always 1
    height: int


def create_tables(conn: sqlite3.Connection) -> None:
    """Create all tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS vault_coin (
            name TEXT PRIMARY KEY,
            collateral INTEGER NOT NULL,
            principal INTEGER NOT NULL,
            discounted_principal INTEGER NOT NULL,
            auction_state TEXT NOT NULL,
            in_bad_debt INTEGER NOT NULL,
            inner_puzzle_hash TEXT NOT NULL,
            height INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_vault_coin_height ON vault_coin (height);
        CREATE INDEX IF NOT EXISTS ix_vault_coin_inner_puzzle_hash ON vault_coin (inner_puzzle_hash);

        CREATE TABLE IF NOT EXISTS savings_vault_coin (
            name TEXT PRIMARY KEY,
            balance INTEGER NOT NULL,
            discounted_balance INTEGER NOT NULL,
            inner_puzzle_hash TEXT NOT NULL,
            height INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_savings_vault_coin_height ON savings_vault_coin (height);

        CREATE TABLE IF NOT EXISTS auction_coin (
            name TEXT PRIMARY KEY,
            auction_type INTEGER NOT NULL,
            auction_status INTEGER NOT NULL,
            parent_name TEXT NOT NULL,
            height INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_auction_coin_height ON auction_coin (height);

        CREATE TABLE IF NOT EXISTS treasury_coin (
            name TEXT PRIMARY KEY,
            launcher_id TEXT NOT NULL,
            ring_prev_launcher_id TEXT NOT NULL,
            amount INTEGER NOT NULL,
            parent_name TEXT NOT NULL,
            height INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_treasury_coin_height ON treasury_coin (height);

        CREATE TABLE IF NOT EXISTS announcer_coin (
            name TEXT PRIMARY KEY,
            launcher_id TEXT NOT NULL,
            timestamp_expires INTEGER,
            price INTEGER,
            timestamp INTEGER,
            spent INTEGER NOT NULL DEFAULT 0,
            approved INTEGER NOT NULL DEFAULT 0,
            height INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_announcer_coin_height ON announcer_coin (height);
        CREATE INDEX IF NOT EXISTS ix_announcer_coin_launcher_id ON announcer_coin (launcher_id);

        CREATE TABLE IF NOT EXISTS governing_crt (
            name TEXT PRIMARY KEY,
            amount INTEGER NOT NULL,
            bill TEXT,
            operation TEXT,
            timestamp INTEGER NOT NULL,
            spent INTEGER NOT NULL DEFAULT 0,
            height INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_governing_crt_height ON governing_crt (height);

        CREATE TABLE IF NOT EXISTS live_block_hash (
            height INTEGER PRIMARY KEY,
            block_hash TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS block_stats_v2 (
            height INTEGER PRIMARY KEY,
            block_hash TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            last_updated INTEGER,
            statutes_price INTEGER,
            current_stability_fee_df INTEGER,
            current_interest_rate_df INTEGER,
            cumulative_stability_fee_df INTEGER NOT NULL DEFAULT 0,
            cumulative_interest_rate_df INTEGER NOT NULL DEFAULT 0,
            vault_operations_count INTEGER NOT NULL DEFAULT 0,
            vault_count_incr INTEGER NOT NULL DEFAULT 0,
            vault_count_decr INTEGER NOT NULL DEFAULT 0,
            collateral_deposited INTEGER NOT NULL DEFAULT 0,
            collateral_withdrawn INTEGER NOT NULL DEFAULT 0,
            collateral_sold INTEGER NOT NULL DEFAULT 0,
            byc_borrowed INTEGER NOT NULL DEFAULT 0,
            byc_repaid INTEGER NOT NULL DEFAULT 0,
            sf_repaid INTEGER NOT NULL DEFAULT 0,
            sf_transferred INTEGER NOT NULL DEFAULT 0,
            discounted_principal_delta INTEGER NOT NULL DEFAULT 0,
            liquidation_start_count INTEGER NOT NULL DEFAULT 0,
            liquidation_restart_count INTEGER NOT NULL DEFAULT 0,
            liquidation_ended_count INTEGER NOT NULL DEFAULT 0,
            lp_incurred INTEGER NOT NULL DEFAULT 0,
            ii_incurred INTEGER NOT NULL DEFAULT 0,
            ii_paid INTEGER NOT NULL DEFAULT 0,
            fees_incurred INTEGER NOT NULL DEFAULT 0,
            fees_paid INTEGER NOT NULL DEFAULT 0,
            principal_incurred INTEGER NOT NULL DEFAULT 0,
            principal_paid INTEGER NOT NULL DEFAULT 0,
            bad_debt_count_incr INTEGER NOT NULL DEFAULT 0,
            bad_debt_count_decr INTEGER NOT NULL DEFAULT 0,
            bad_debt_ii_incurred INTEGER NOT NULL DEFAULT 0,
            bad_debt_ii_recovered INTEGER NOT NULL DEFAULT 0,
            bad_debt_fees_incurred INTEGER NOT NULL DEFAULT 0,
            bad_debt_fees_recovered INTEGER NOT NULL DEFAULT 0,
            bad_debt_principal_incurred INTEGER NOT NULL DEFAULT 0,
            bad_debt_principal_recovered INTEGER NOT NULL DEFAULT 0,
            savings_vault_operations_count INTEGER NOT NULL DEFAULT 0,
            savings_vault_count_incr INTEGER NOT NULL DEFAULT 0,
            savings_vault_count_decr INTEGER NOT NULL DEFAULT 0,
            discounted_savings_balance_delta INTEGER NOT NULL DEFAULT 0,
            byc_deposited INTEGER NOT NULL DEFAULT 0,
            byc_withdrawn INTEGER NOT NULL DEFAULT 0,
            interest_paid INTEGER NOT NULL DEFAULT 0,
            approved_announcer_count_delta INTEGER NOT NULL DEFAULT 0,
            treasury_coin_count_delta INTEGER NOT NULL DEFAULT 0,
            treasury_balance_delta INTEGER NOT NULL DEFAULT 0,
            recharge_auction_coin_count_delta INTEGER NOT NULL DEFAULT 0,
            recharge_auction_count_delta INTEGER NOT NULL DEFAULT 0,
            surplus_auction_count_delta INTEGER NOT NULL DEFAULT 0,
            governance_operations_count INTEGER NOT NULL DEFAULT 0,
            governance_coin_count_delta INTEGER NOT NULL DEFAULT 0,
            governance_circulation_delta INTEGER NOT NULL DEFAULT 0,
            crt_circulation_delta INTEGER NOT NULL DEFAULT 0,
            registry_operations_count INTEGER NOT NULL DEFAULT 0,
            registered_announcer_count_delta INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS ix_block_stats_v2_timestamp ON block_stats_v2 (timestamp);

        CREATE TABLE IF NOT EXISTS daily_block_stats_v2 (
            date INTEGER PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            last_updated INTEGER,
            statutes_price INTEGER,
            current_stability_fee_df INTEGER,
            current_interest_rate_df INTEGER,
            cumulative_stability_fee_df INTEGER NOT NULL DEFAULT 0,
            cumulative_interest_rate_df INTEGER NOT NULL DEFAULT 0,
            vault_operations_count INTEGER NOT NULL DEFAULT 0,
            vault_count_incr INTEGER NOT NULL DEFAULT 0,
            vault_count_decr INTEGER NOT NULL DEFAULT 0,
            collateral_deposited INTEGER NOT NULL DEFAULT 0,
            collateral_withdrawn INTEGER NOT NULL DEFAULT 0,
            collateral_sold INTEGER NOT NULL DEFAULT 0,
            byc_borrowed INTEGER NOT NULL DEFAULT 0,
            byc_repaid INTEGER NOT NULL DEFAULT 0,
            sf_repaid INTEGER NOT NULL DEFAULT 0,
            sf_transferred INTEGER NOT NULL DEFAULT 0,
            discounted_principal_delta INTEGER NOT NULL DEFAULT 0,
            liquidation_start_count INTEGER NOT NULL DEFAULT 0,
            liquidation_restart_count INTEGER NOT NULL DEFAULT 0,
            liquidation_ended_count INTEGER NOT NULL DEFAULT 0,
            lp_incurred INTEGER NOT NULL DEFAULT 0,
            ii_incurred INTEGER NOT NULL DEFAULT 0,
            ii_paid INTEGER NOT NULL DEFAULT 0,
            fees_incurred INTEGER NOT NULL DEFAULT 0,
            fees_paid INTEGER NOT NULL DEFAULT 0,
            principal_incurred INTEGER NOT NULL DEFAULT 0,
            principal_paid INTEGER NOT NULL DEFAULT 0,
            bad_debt_count_incr INTEGER NOT NULL DEFAULT 0,
            bad_debt_count_decr INTEGER NOT NULL DEFAULT 0,
            bad_debt_ii_incurred INTEGER NOT NULL DEFAULT 0,
            bad_debt_ii_recovered INTEGER NOT NULL DEFAULT 0,
            bad_debt_fees_incurred INTEGER NOT NULL DEFAULT 0,
            bad_debt_fees_recovered INTEGER NOT NULL DEFAULT 0,
            bad_debt_principal_incurred INTEGER NOT NULL DEFAULT 0,
            bad_debt_principal_recovered INTEGER NOT NULL DEFAULT 0,
            savings_vault_operations_count INTEGER NOT NULL DEFAULT 0,
            savings_vault_count_incr INTEGER NOT NULL DEFAULT 0,
            savings_vault_count_decr INTEGER NOT NULL DEFAULT 0,
            discounted_savings_balance_delta INTEGER NOT NULL DEFAULT 0,
            byc_deposited INTEGER NOT NULL DEFAULT 0,
            byc_withdrawn INTEGER NOT NULL DEFAULT 0,
            interest_paid INTEGER NOT NULL DEFAULT 0,
            approved_announcer_count_delta INTEGER NOT NULL DEFAULT 0,
            treasury_coin_count_delta INTEGER NOT NULL DEFAULT 0,
            treasury_balance_delta INTEGER NOT NULL DEFAULT 0,
            recharge_auction_coin_count_delta INTEGER NOT NULL DEFAULT 0,
            recharge_auction_count_delta INTEGER NOT NULL DEFAULT 0,
            surplus_auction_count_delta INTEGER NOT NULL DEFAULT 0,
            governance_operations_count INTEGER NOT NULL DEFAULT 0,
            governance_coin_count_delta INTEGER NOT NULL DEFAULT 0,
            governance_coin_count_peak_delta INTEGER NOT NULL DEFAULT 0,
            governance_circulation_delta INTEGER NOT NULL DEFAULT 0,
            governance_circulation_peak_delta INTEGER NOT NULL DEFAULT 0,
            crt_circulation_delta INTEGER NOT NULL DEFAULT 0,
            registry_operations_count INTEGER NOT NULL DEFAULT 0,
            registered_announcer_count_delta INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS scanner_height (
            id INTEGER PRIMARY KEY DEFAULT 1,
            height INTEGER NOT NULL
        );
    """)
    conn.commit()
