import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia_rs import CoinSpend

from circuit_analytics.drivers.statutes import Statutes, StatutePosition
from circuit_analytics.mods import SINGLETON_ISA_MOD
from circuit_analytics.scanner.handlers.base import HandlerResult, SpendHandler

log = logging.getLogger(__name__)


class StatutesHandler(SpendHandler):
    def handle(
        self,
        coin_spend: CoinSpend,
        block_record: Dict[str, Any],
        statutes_struct: Program,
        last_updated: int = 0,
    ) -> Optional[HandlerResult]:
        mod, args = uncurry(coin_spend.puzzle_reveal)

        if mod != SINGLETON_ISA_MOD:
            return None

        log.debug("Found a singleton ISA spend")

        statutes_info = Statutes.get_statutes_info(coin_spend, skip_mod_hash_verification=True)

        if statutes_info.statutes_struct != statutes_struct:
            return None

        result = HandlerResult()
        delta = result.stats_delta

        result.last_statutes_info = statutes_info

        if statutes_info.price_info[1] == last_updated:
            return result

        delta.statutes_spend_found = True
        delta.statutes_price = statutes_info.price_info[0]
        delta.last_updated = statutes_info.price_info[1]

        if statutes_info.cumulative_stability_fee_df:
            delta.cumulative_stability_fee_df = statutes_info.cumulative_stability_fee_df
        if statutes_info.cumulative_interest_rate_df:
            delta.cumulative_interest_rate_df = statutes_info.cumulative_interest_rate_df

        maybe_stability_fee_df = statutes_info.statutes[StatutePosition.STABILITY_FEE_DF.value].as_int()
        maybe_interest_rate_df = statutes_info.statutes[StatutePosition.INTEREST_DF.value].as_int()

        if maybe_stability_fee_df:
            delta.current_stability_fee_df = maybe_stability_fee_df
        if maybe_interest_rate_df:
            delta.current_interest_rate_df = maybe_interest_rate_df

        return result
