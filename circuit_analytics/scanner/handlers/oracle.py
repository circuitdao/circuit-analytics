import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia_rs import CoinSpend

from circuit_analytics.drivers.oracle import get_oracle_solution_info
from circuit_analytics.mods import ORACLE_MOD
from circuit_analytics.scanner.handlers.base import HandlerResult, SpendHandler

log = logging.getLogger(__name__)


class OracleHandler(SpendHandler):
    def handle(
        self, coin_spend: CoinSpend, block_record: Dict[str, Any], statutes_struct: Program
    ) -> Optional[HandlerResult]:
        mod, args = uncurry(coin_spend.puzzle_reveal)
        inner_puzzle = args.at("rf")
        inner_puz_mod, _ = inner_puzzle.uncurry()

        if inner_puz_mod != ORACLE_MOD:
            return None

        log.debug("Found an oracle spend")
        get_oracle_solution_info(coin_spend)
        return HandlerResult()
