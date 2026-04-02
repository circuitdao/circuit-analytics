import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia.wallet.util.compute_additions import compute_additions
from chia_rs import CoinSpend
from chia_rs.sized_bytes import bytes32

from circuit_analytics.drivers.treasury import get_treasury_solution_info
from circuit_analytics.scanner.handlers.base import HandlerResult
from circuit_analytics.scanner.models import TreasuryCoin

log = logging.getLogger(__name__)


class TreasuryHandler:
    def handle(
        self, coin_spend: CoinSpend, inner_args: Program, block_record: Dict[str, Any], statutes_struct: Program
    ) -> Optional[HandlerResult]:
        if statutes_struct != inner_args.at("rf"):
            return None

        _, cat_args = uncurry(coin_spend.puzzle_reveal)
        cat_inner_puzzle = cat_args.at("rrf")
        cat_inner_solution = Program.from_serialized(coin_spend.solution).first()

        treasury_info = get_treasury_solution_info(coin_spend.coin, cat_inner_puzzle, cat_inner_solution)

        if treasury_info is None:
            return None

        launcher_id = bytes32(inner_args.at("rrf").atom)
        ring_prev_launcher_id = bytes32(inner_args.at("rrrf").atom)
        is_eve_spend = treasury_info.parent_parent_id is None

        new_coins = compute_additions(coin_spend)
        new_coin = new_coins[1] if len(new_coins) == 2 else new_coins[0]

        result = HandlerResult()
        delta = result.stats_delta

        if is_eve_spend:
            delta.treasury_coin_count_delta += 1
            balance_delta = new_coin.amount
        else:
            balance_delta = new_coin.amount - coin_spend.coin.amount

        delta.treasury_balance_delta += balance_delta

        result.coins_to_remove.add(coin_spend.coin.name().hex())
        result.coins_to_add.append(
            TreasuryCoin(
                name=new_coin.name().hex(),
                launcher_id=launcher_id.hex(),
                ring_prev_launcher_id=ring_prev_launcher_id.hex(),
                amount=new_coin.amount,
                parent_name=coin_spend.coin.name().hex(),
                height=block_record["height"],
            )
        )
        return result
