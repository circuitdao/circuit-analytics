import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program
from chia.wallet.util.compute_additions import compute_additions
from chia_rs import CoinSpend

from circuit_analytics.drivers import AuctionStatus, AuctionType
from circuit_analytics.drivers.surplus_auction import get_surplus_info
from circuit_analytics.scanner.handlers.base import HandlerResult
from circuit_analytics.scanner.models import AuctionCoin

log = logging.getLogger(__name__)


class SurplusAuctionHandler:
    def handle(
        self, coin_spend: CoinSpend, inner_args: Program, block_record: Dict[str, Any], statutes_struct: Program
    ) -> Optional[HandlerResult]:
        if statutes_struct != inner_args.at("rf"):
            return None

        result = HandlerResult()
        delta = result.stats_delta
        delta.surplus_operations_count += 1

        new_coins = compute_additions(coin_spend)
        surplus_info = get_surplus_info(coin_spend)

        if not surplus_info:
            delta.surplus_auction_count_delta -= 1
            result.coins_to_remove.add(coin_spend.coin.name().hex())
            return result

        if surplus_info.last_bid.nullp():
            delta.surplus_auction_count_delta += 1

        new_coin = new_coins[0]
        result.coins_to_add.append(
            AuctionCoin(
                name=new_coin.name().hex(),
                auction_type=AuctionType.SURPLUS.value,
                auction_status=AuctionStatus.RUNNING.value,
                parent_name=coin_spend.coin.name().hex(),
                height=block_record["height"],
            )
        )
        result.coins_to_remove.add(new_coin.parent_coin_info.hex())
        return result
