import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program
from chia.wallet.util.compute_additions import compute_additions
from chia_rs import CoinSpend

from circuit_analytics.drivers.savings import SavingsVaultInfo, get_savings_operation_info, get_savings_puzzle_hash
from circuit_analytics.mods import CAT_MOD, CAT_MOD_HASH, SAVINGS_VAULT_MOD, SAVINGS_VAULT_MOD_HASH
from circuit_analytics.scanner.handlers.base import HandlerResult
from circuit_analytics.scanner.models import SavingsVaultCoin

log = logging.getLogger(__name__)


class SavingsHandler:
    def handle(
        self,
        coin_spend: CoinSpend,
        inner_args: Program,
        tail_hash: bytes,
        byc_tail_hash: bytes,
        block_record: Dict[str, Any],
        statutes_struct: Program,
    ) -> Optional[HandlerResult]:
        log.debug(
            "Found a BYC spend, processing: %s %s",
            SAVINGS_VAULT_MOD.get_tree_hash().hex(),
            SAVINGS_VAULT_MOD_HASH.hex(),
        )

        parent_savings_info = SavingsVaultInfo.from_coin_spend(coin_spend, from_puzzle=True)
        savings_info = SavingsVaultInfo.from_coin_spend(coin_spend)

        if statutes_struct != inner_args.at("rf"):
            log.warning("Savings vault statutes struct mismatch")
            return None

        result = HandlerResult()
        delta = result.stats_delta
        delta.savings_vault_operations_count += 1

        parent_amount = parent_savings_info.amount if parent_savings_info else 0
        parent_discounted = parent_savings_info.discounted_balance if parent_savings_info else 0

        if parent_amount == 0 and parent_discounted == 0 and savings_info.amount > 0:
            delta.savings_vault_count_incr += 1

        if (
            (parent_amount > 0 or parent_discounted > 0)
            and savings_info.amount == 0
            and savings_info.discounted_balance == 0
        ):
            delta.savings_vault_count_decr += 1

        op = get_savings_operation_info(coin_spend)
        assert op["type"] == "spend"
        delta.discounted_savings_balance_delta += op["discounted_balance_delta"]
        delta.interest_paid += op["interest_payment"]

        balance_delta_net_of_interest_payment = op["balance_delta"] - op["interest_payment"]
        if balance_delta_net_of_interest_payment > 0:
            delta.byc_deposited = balance_delta_net_of_interest_payment
        elif balance_delta_net_of_interest_payment < 0:
            delta.byc_withdrawn = -balance_delta_net_of_interest_payment

        result.coins_to_remove.add(coin_spend.coin.name().hex())

        new_coins = compute_additions(coin_spend)
        savings_puzzle_hash = get_savings_puzzle_hash(
            statutes_struct, savings_info.discounted_balance, savings_info.inner_puzzle_hash
        )

        savings_vault_puzzle_hash = CAT_MOD.curry(
            CAT_MOD_HASH, byc_tail_hash, savings_puzzle_hash
        ).get_tree_hash_precalc(savings_puzzle_hash)

        for new_coin in new_coins:
            if new_coin.puzzle_hash == savings_vault_puzzle_hash:
                break
        else:
            raise ValueError("No new savings coin found in spend")

        savings_vault_coin = SavingsVaultCoin(
            name=new_coin.name().hex(),
            balance=savings_info.amount,
            discounted_balance=savings_info.discounted_balance,
            inner_puzzle_hash=savings_info.inner_puzzle_hash.hex(),
            height=block_record["height"],
        )
        result.coins_to_add.append(savings_vault_coin)
        return result
