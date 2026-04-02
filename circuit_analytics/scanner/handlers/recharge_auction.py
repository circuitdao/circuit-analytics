import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program
from chia.wallet.util.compute_additions import compute_additions
from chia_rs import CoinSpend

from circuit_analytics.drivers import AuctionStatus, AuctionType
from circuit_analytics.drivers.recharge_auction import get_recharge_info
from circuit_analytics.scanner.handlers.base import HandlerResult
from circuit_analytics.scanner.models import AuctionCoin

log = logging.getLogger(__name__)


class RechargeAuctionHandler:
    def handle(
        self,
        coin_spend: CoinSpend,
        inner_args: Program,
        tail_hash: bytes,
        byc_tail_hash: bytes,
        block_record: Dict[str, Any],
        statutes_struct: Program,
    ) -> Optional[HandlerResult]:
        if statutes_struct != inner_args.at("rf"):
            return None

        if tail_hash != byc_tail_hash:
            log.warning(
                "Found a recharge auction with correct statutes struct but wrong tail hash?! (%s)",
                tail_hash,
            )
            return None

        result = HandlerResult()
        delta = result.stats_delta
        delta.recharge_operations_count += 1

        parent_recharge_info = get_recharge_info(coin_spend, spend=False)
        recharge_info = get_recharge_info(coin_spend)

        if Program.to(parent_recharge_info.launcher_id).nullp():
            delta.recharge_auction_coin_count_delta += 1

        if recharge_info.auction_params.nullp():
            auction_status = AuctionStatus.STANDBY.value
            if not parent_recharge_info.launcher_id:
                delta.recharge_auction_count_delta -= 1
        else:
            auction_status = AuctionStatus.RUNNING.value
            if parent_recharge_info.auction_params.nullp():
                delta.recharge_auction_count_delta += 1

        result.coins_to_add.append(
            AuctionCoin(
                name=compute_additions(coin_spend)[0].name().hex(),
                auction_type=AuctionType.RECHARGE.value,
                auction_status=auction_status,
                parent_name=coin_spend.coin.name().hex(),
                height=block_record["height"],
            )
        )
        result.coins_to_remove.add(coin_spend.coin.name().hex())
        return result
