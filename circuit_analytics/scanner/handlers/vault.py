from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program
from chia.wallet.util.compute_additions import compute_additions
from chia_rs import CoinSpend

from circuit_analytics.drivers.protocol_math import (
    calculate_cumulative_discount_factor,
    calculate_total_fees,
    undiscount_principal,
)
from circuit_analytics.drivers.vault import (
    CollateralVaultState,
    VaultBidInfo,
    VaultBorrowInfo,
    VaultDepositInfo,
    VaultLiquidateInfo,
    VaultRecoverInfo,
    VaultRepayInfo,
    VaultSFTransferInfo,
    VaultTransferInfo,
    VaultWithdrawInfo,
    get_collateral_vault_info,
    get_vault_solution_info,
)
from circuit_analytics.scanner.handlers.base import HandlerResult, SpendHandler
from circuit_analytics.scanner.models import VaultCoin


class CollateralVaultHandler(SpendHandler):
    def handle(
        self, coin_spend: CoinSpend, block_record: Dict[str, Any], statutes_struct: Program
    ) -> Optional[HandlerResult]:
        old_state: CollateralVaultState = get_collateral_vault_info(coin_spend, spend=False)

        if old_state.statutes_struct != statutes_struct:
            return None

        sol_info = get_vault_solution_info(coin_spend)

        new_state: CollateralVaultState = get_collateral_vault_info(
            coin_spend, spend=True, statutes_struct=statutes_struct
        )

        result = HandlerResult()
        delta = result.stats_delta
        delta.vault_operations_count += 1

        if isinstance(sol_info, VaultDepositInfo):
            if old_state.collateral == 0 and old_state.auction_state.nullp():
                delta.vault_count_incr += 1
            delta.collateral_deposited += sol_info.deposit_amount

        elif isinstance(sol_info, VaultWithdrawInfo):
            if new_state.collateral == 0:
                delta.vault_count_decr += 1
            delta.collateral_withdrawn += sol_info.withdraw_amount

        elif isinstance(sol_info, VaultBorrowInfo):
            delta.byc_borrowed += sol_info.borrow_amount
            delta.discounted_principal_delta += new_state.discounted_principal - old_state.discounted_principal

        elif isinstance(sol_info, VaultRepayInfo):
            delta.byc_repaid += sol_info.repay_amount
            delta.sf_repaid += sol_info.sf_transfer_amount
            delta.discounted_principal_delta += new_state.discounted_principal - old_state.discounted_principal

        elif isinstance(sol_info, VaultTransferInfo):
            pass

        elif isinstance(sol_info, VaultLiquidateInfo):
            if sol_info.is_start:
                delta.liquidation_start_count += 1
                delta.discounted_principal_delta += new_state.discounted_principal - old_state.discounted_principal
                delta.ii_incurred += new_state.initiator_incentive_balance or 0
                delta.fees_incurred += new_state.byc_to_treasury_balance or 0
                delta.principal_incurred += new_state.byc_to_melt_balance or 0
            else:
                delta.liquidation_restart_count += 1

        elif isinstance(sol_info, VaultBidInfo):
            byc_bid_amount = sol_info.byc_bid_amount
            ii_balance = old_state.initiator_incentive_balance or 0
            treasury_balance = old_state.byc_to_treasury_balance or 0
            melt_balance = old_state.byc_to_melt_balance or 0
            ii_paid = min(ii_balance, byc_bid_amount)
            remaining = byc_bid_amount - ii_paid
            fees_paid = min(treasury_balance, remaining)
            principal_paid = remaining - fees_paid
            total_debt = ii_balance + treasury_balance + melt_balance
            leftover_debt = total_debt - byc_bid_amount
            collateral_sold = old_state.collateral - new_state.collateral
            fully_liquidated = (leftover_debt == 0)
            bad_debt_incurred = (leftover_debt > 0 and new_state.collateral == 0)
            delta.ii_paid += ii_paid
            delta.fees_paid += fees_paid
            delta.principal_paid += principal_paid
            delta.collateral_sold += collateral_sold
            delta.liquidation_ended_count += 1 if (fully_liquidated or bad_debt_incurred) else 0
            delta.bad_debt_count_incr += 1 if bad_debt_incurred else 0
            if bad_debt_incurred:
                delta.bad_debt_ii_incurred += new_state.initiator_incentive_balance or 0
                delta.bad_debt_fees_incurred += new_state.byc_to_treasury_balance or 0
                delta.bad_debt_principal_incurred += new_state.byc_to_melt_balance or 0

        elif isinstance(sol_info, VaultRecoverInfo):
            recover_amount = sol_info.recover_amount
            fully_recovered = (old_state.byc_to_melt_balance == recover_amount)
            delta.bad_debt_ii_recovered += old_state.initiator_incentive_balance or 0
            delta.bad_debt_fees_recovered += old_state.byc_to_treasury_balance or 0
            delta.bad_debt_principal_recovered += recover_amount
            delta.bad_debt_count_decr += 1 if fully_recovered else 0
            delta.vault_count_decr += 1 if fully_recovered else 0

        elif isinstance(sol_info, VaultSFTransferInfo):
            cc_sf_df = calculate_cumulative_discount_factor(
                sol_info.statutes_cumulative_stability_fee_df,
                sol_info.current_stability_fee_df,
                sol_info.price_info[1],
                sol_info.current_timestamp,
            )
            undiscounted = undiscount_principal(old_state.discounted_principal, cc_sf_df)
            sf_transferred = calculate_total_fees(undiscounted, old_state.principal, 0)
            delta.sf_transferred += sf_transferred

        else:
            raise ValueError(f"Unknown vault solution info type {type(sol_info)} for coin {coin_spend.coin.name().hex()}")

        result.coins_to_remove.add(coin_spend.coin.name().hex())
        name = compute_additions(coin_spend)[0].name()

        vault_coin = VaultCoin(
            name=name.hex(),
            collateral=new_state.collateral,
            principal=new_state.principal,
            discounted_principal=new_state.discounted_principal,
            auction_state=str(new_state.auction_state),
            in_bad_debt=new_state.in_bad_debt,
            inner_puzzle_hash=new_state.inner_puzzle_hash.hex(),
            height=block_record["height"],
        )
        result.coins_to_add.append(vault_coin)
        return result
