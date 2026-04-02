"""
Shared stats aggregation logic for Circuit protocol analytics.

Imported by both circuit-analytics (server.py) and circuit (app/routers/protocol.py)
to avoid duplicating calculate_stats and ZERO_RUNNING_TOTALS.
"""
from __future__ import annotations

from circuit_analytics.drivers.protocol_math import (
    MCAT,
    MOJOS,
    calculate_cumulative_discount_factor,
    calculate_interest,
    per_minute_discount_factor_to_annual_rate,
    undiscount_principal,
    undiscount_savings_balance,
)

MAX_TX_BLOCK_TIME = 120  # seconds — max Chia transaction block time

ZERO_RUNNING_TOTALS = {
    ## Collateral vaults ##
    "vault_operations_count": 0,
    "vault_count_incr": 0,
    "vault_count_decr": 0,
    "collateral_deposited": 0,
    "collateral_withdrawn": 0,
    "collateral_sold": 0,
    "byc_borrowed": 0,
    "byc_repaid": 0,
    "sf_repaid": 0,
    "sf_transferred": 0,
    "discounted_principal_delta": 0,
    "liquidation_start_count": 0,
    "liquidation_restart_count": 0,
    "liquidation_ended_count": 0,
    "lp_incurred": 0,
    "ii_incurred": 0,
    "ii_paid": 0,
    "fees_incurred": 0,
    "fees_paid": 0,
    "principal_incurred": 0,
    "principal_paid": 0,
    "bad_debt_count_incr": 0,
    "bad_debt_count_decr": 0,
    "bad_debt_ii_incurred": 0,
    "bad_debt_ii_recovered": 0,
    "bad_debt_fees_incurred": 0,
    "bad_debt_fees_recovered": 0,
    "bad_debt_principal_incurred": 0,
    "bad_debt_principal_recovered": 0,
    ## Savings vaults ##
    "savings_vault_operations_count": 0,
    "savings_vault_count_incr": 0,
    "savings_vault_count_decr": 0,
    "discounted_savings_balance_delta": 0,
    "byc_deposited": 0,
    "byc_withdrawn": 0,
    "interest_paid": 0,
    ## Announcers ##
    "approved_announcer_count_delta": 0,
    ## Treasury ##
    "treasury_coin_count_delta": 0,
    "treasury_balance_delta": 0,
    ## Recharge auctions ##
    "recharge_auction_coin_count_delta": 0,
    "recharge_auction_count_delta": 0,
    ## Surplus auctions ##
    "surplus_auction_count_delta": 0,
    ## Governance ##
    "governance_operations_count": 0,
    "governance_coin_count_delta": 0,
    "governance_circulation_delta": 0,
    ## CRT ##
    "crt_circulation_delta": 0,
    ## Registry ##
    "registry_operations_count": 0,
    "registered_announcer_count_delta": 0,
}


def calculate_stats(
    running_totals: dict,
    current_sf_df: int,
    current_ir_df: int,
    cumulative_sf_df: int,
    cumulative_ir_df: int,
    statutes_price: int,
    last_updated: int,
    timestamp: int,
) -> dict:
    cc_sf_df = calculate_cumulative_discount_factor(
        cumulative_sf_df,
        current_sf_df,
        last_updated,
        timestamp + 3 * MAX_TX_BLOCK_TIME,
    )
    cc_ir_df = calculate_cumulative_discount_factor(
        cumulative_ir_df,
        current_ir_df,
        last_updated,
        timestamp - 3 * MAX_TX_BLOCK_TIME,
    )
    vault_count = running_totals["vault_count_incr"] - running_totals["vault_count_decr"]
    collateral = (
        running_totals["collateral_deposited"]
        - running_totals["collateral_withdrawn"]
        - running_totals["collateral_sold"]
    )
    collateral_usd = int((collateral / MOJOS) * statutes_price) / 100 if statutes_price else None
    principal_repaid = running_totals["byc_repaid"] - running_totals["sf_repaid"]
    principal = (
        running_totals["byc_borrowed"]
        + running_totals["sf_transferred"]
        - principal_repaid
        - running_totals["principal_incurred"]
    )
    byc_in_circulation = (
        running_totals["byc_borrowed"]
        + running_totals["sf_transferred"]
        - principal_repaid
        - running_totals["principal_paid"]
        - running_totals["bad_debt_principal_recovered"]
    )
    undiscounted_principal = undiscount_principal(running_totals["discounted_principal_delta"], cc_sf_df)
    accrued_sf = undiscounted_principal - principal
    collateral_ratio = (
        int(100 * collateral_usd * MCAT / undiscounted_principal) if undiscounted_principal else 0
    )
    sf_incurred = running_totals["fees_incurred"] - (
        running_totals["lp_incurred"] - running_totals["ii_incurred"]
    )
    accrued_sf_alltime = (
        undiscounted_principal
        - principal
        + running_totals["sf_transferred"]
        + running_totals["sf_repaid"]
        + sf_incurred
    )
    fees_received = (
        running_totals["sf_transferred"] + running_totals["sf_repaid"] + running_totals["fees_paid"]
    )
    bad_debt_incurred = (
        running_totals["bad_debt_ii_incurred"]
        + running_totals["bad_debt_fees_incurred"]
        + running_totals["bad_debt_principal_incurred"]
    )
    debt_in_liquidation = (
        running_totals["ii_incurred"]
        + running_totals["fees_incurred"]
        + running_totals["principal_incurred"]
        - running_totals["ii_paid"]
        - running_totals["fees_paid"]
        - running_totals["principal_paid"]
        - bad_debt_incurred
    )
    bad_debt = (
        bad_debt_incurred
        - running_totals["bad_debt_ii_recovered"]
        - running_totals["bad_debt_fees_recovered"]
        - running_totals["bad_debt_principal_recovered"]
    )
    bad_debt_principal = running_totals["bad_debt_principal_incurred"] - running_totals["bad_debt_principal_recovered"]
    debt = undiscounted_principal + debt_in_liquidation + bad_debt_principal
    savings_vault_count = running_totals["savings_vault_count_incr"] - running_totals["savings_vault_count_decr"]
    interest_paid = running_totals["interest_paid"]
    savings_balance = (
        running_totals["byc_deposited"]
        + interest_paid
        - running_totals["byc_withdrawn"]
    )
    discounted_savings_balance = running_totals["discounted_savings_balance_delta"]
    undiscounted_savings_balance = undiscount_savings_balance(discounted_savings_balance, cc_ir_df)
    accrued_interest = calculate_interest(discounted_savings_balance, savings_balance, cc_ir_df)
    accrued_interest_alltime = accrued_interest + interest_paid
    profit = fees_received - interest_paid - running_totals["bad_debt_principal_recovered"]
    projected_revenue = int(undiscounted_principal * per_minute_discount_factor_to_annual_rate(current_sf_df) / 100)
    projected_cost = int(undiscounted_savings_balance * per_minute_discount_factor_to_annual_rate(current_ir_df) / 100)
    projected_profit = projected_revenue - projected_cost

    return {
        "timestamp": timestamp,
        "last_updated": last_updated,
        "statutes_price": statutes_price,
        ## Collateral vaults ##
        "vault_operations_count": running_totals["vault_operations_count"],
        "vault_count": vault_count,
        "collateral": collateral,
        "collateral_usd": collateral_usd,
        "byc_in_circulation": byc_in_circulation,
        "undiscounted_principal": undiscounted_principal,
        "byc_borrowed": running_totals["byc_borrowed"],
        "debt_repaid": running_totals["byc_repaid"],
        "accrued_sf": accrued_sf,
        "accrued_sf_alltime": accrued_sf_alltime,
        "fees_received": fees_received,
        "debt_in_liquidation": debt_in_liquidation,
        "bad_debt": bad_debt,
        "bad_debt_principal": bad_debt_principal,
        "debt": debt,
        "collateral_ratio": collateral_ratio,
        ## Savings vaults ##
        "savings_vault_operations_count": running_totals["savings_vault_operations_count"],
        "savings_vault_count": savings_vault_count,
        "savings_balance": savings_balance,
        "accrued_interest": accrued_interest,
        "accrued_interest_alltime": accrued_interest_alltime,
        "interest_paid": interest_paid,
        ## Announcers ##
        "approved_announcer_count": running_totals["approved_announcer_count_delta"],
        ## Treasury ##
        "treasury_coin_count": running_totals["treasury_coin_count_delta"],
        "treasury_balance": running_totals["treasury_balance_delta"],
        ## Recharge auctions ##
        "recharge_auction_coin_count": running_totals["recharge_auction_coin_count_delta"],
        "recharge_auction_count": running_totals["recharge_auction_count_delta"],
        ## Surplus auctions ##
        "surplus_auction_count": running_totals["surplus_auction_count_delta"],
        ## Governance ##
        "governance_operations_count": running_totals["governance_operations_count"],
        "governance_coin_count": running_totals["governance_coin_count_delta"],
        "governance_in_circulation": running_totals["governance_circulation_delta"],
        ## CRT ##
        "crt_in_circulation": running_totals["crt_circulation_delta"],
        ## Registry ##
        "registered_announcers": running_totals["registered_announcer_count_delta"],
        ## Protocol-wide ##
        "profit": profit,
        "projected_revenue": projected_revenue,
        "projected_cost": projected_cost,
        "projected_profit": projected_profit,
    }
