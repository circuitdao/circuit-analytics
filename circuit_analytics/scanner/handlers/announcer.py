import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia.wallet.util.compute_additions import compute_additions
from chia_rs import CoinSpend

from circuit_analytics.drivers.announcer import get_price_announcer_info
from circuit_analytics.mods import ATOM_ANNOUNCER_MOD
from circuit_analytics.scanner.handlers.base import HandlerResult, SpendHandler
from circuit_analytics.scanner.models import AnnouncerCoin

log = logging.getLogger(__name__)


class AnnouncerHandler(SpendHandler):
    def handle(
        self, coin_spend: CoinSpend, block_record: Dict[str, Any], statutes_struct: Program
    ) -> Optional[HandlerResult]:
        mod, args = uncurry(coin_spend.puzzle_reveal)

        if mod != ATOM_ANNOUNCER_MOD:
            return None

        log.debug("Found an ATOM announcer spend")

        if statutes_struct != args.at("rf"):
            return None

        announcer_info = get_price_announcer_info(coin_spend)

        result = HandlerResult()
        result.stats_delta.announcer_operations_count += 1

        if announcer_info is None:
            # announcer exited
            return result

        result.coins_to_remove.add(coin_spend.coin.name().hex())
        new_coins = compute_additions(coin_spend)

        announcer_coin = AnnouncerCoin(
            name=new_coins[0].name().hex(),
            launcher_id=announcer_info.launcher_id.hex(),
            timestamp_expires=announcer_info.timestamp_expires,
            price=announcer_info.value.as_int(),
            timestamp=block_record["timestamp"],
            spent=False,
            approved=announcer_info.approved,
            height=block_record["height"],
        )
        result.coins_to_add.append(announcer_coin)

        if announcer_info.approved:
            result.stats_delta.approved_announcer_count_delta += 1

        return result
