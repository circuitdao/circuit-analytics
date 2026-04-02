import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia_rs import CoinSpend
from chia_rs.sized_bytes import bytes32

from circuit_analytics.drivers.cat import CatTailRevealInfo, CrtTailLaunchInfo
from circuit_analytics.drivers.cat import get_cat_solution_info as get_cat_spend_info
from circuit_analytics.mods import (
    CAT_MOD,
    GOVERNANCE_MOD,
    LAUNCH_GOVERNANCE_MOD,
    RECHARGE_AUCTION_MOD,
    SAVINGS_VAULT_MOD,
    SURPLUS_AUCTION_MOD,
    TREASURY_MOD,
)
from circuit_analytics.scanner.handlers.base import HandlerResult, SpendHandler
from circuit_analytics.scanner.handlers.governance import GovernanceHandler, LaunchGovernanceHandler
from circuit_analytics.scanner.handlers.recharge_auction import RechargeAuctionHandler
from circuit_analytics.scanner.handlers.registry import RegistryHandler
from circuit_analytics.scanner.handlers.savings import SavingsHandler
from circuit_analytics.scanner.handlers.surplus_auction import SurplusAuctionHandler
from circuit_analytics.scanner.handlers.treasury import TreasuryHandler

log = logging.getLogger(__name__)


class CatHandler(SpendHandler):
    def __init__(self):
        self.savings_handler = SavingsHandler()
        self.treasury_handler = TreasuryHandler()
        self.recharge_handler = RechargeAuctionHandler()
        self.surplus_handler = SurplusAuctionHandler()
        self.launch_governance_handler = LaunchGovernanceHandler()
        self.governance_handler = GovernanceHandler()
        self.registry_handler = RegistryHandler()

    def handle(
        self,
        coin_spend: CoinSpend,
        block_record: Dict[str, Any],
        statutes_struct: Program,
        byc_tail_hash: bytes,
        crt_tail_hash: bytes,
    ) -> Optional[HandlerResult]:
        mod, args = uncurry(coin_spend.puzzle_reveal)
        if mod != CAT_MOD:
            return None

        inner_puzzle = args.at("rrf")
        inner_puz_mod, inner_args = inner_puzzle.uncurry()
        tail_hash = args.at("rf").atom

        log.debug("Found a CAT spend with tail hash: %s", tail_hash.hex())

        if tail_hash == byc_tail_hash:
            if inner_puz_mod == SAVINGS_VAULT_MOD:
                return self.savings_handler.handle(
                    coin_spend, inner_args, tail_hash, byc_tail_hash, block_record, statutes_struct
                )
            elif inner_puz_mod == TREASURY_MOD:
                return self.treasury_handler.handle(coin_spend, inner_args, block_record, statutes_struct)
            elif inner_puz_mod == RECHARGE_AUCTION_MOD:
                return self.recharge_handler.handle(
                    coin_spend, inner_args, tail_hash, byc_tail_hash, block_record, statutes_struct
                )

        elif tail_hash == crt_tail_hash:
            # Check CRT spends for a TAIL reveal — CrtTailLaunchInfo means CRT was issued.
            cat_spend_info = get_cat_spend_info(coin_spend, crt_tail_hash=bytes32(crt_tail_hash))
            if isinstance(cat_spend_info, CatTailRevealInfo) and isinstance(
                cat_spend_info.limitations_solution_info, CrtTailLaunchInfo
            ):
                result = HandlerResult()
                result.stats_delta.crt_circulation_delta += cat_spend_info.limitations_solution_info.delta_amount
                return result

            if inner_puz_mod == SURPLUS_AUCTION_MOD:
                return self.surplus_handler.handle(coin_spend, inner_args, block_record, statutes_struct)
            elif inner_puz_mod == LAUNCH_GOVERNANCE_MOD:
                return self.launch_governance_handler.handle(
                    coin_spend, inner_args, block_record, statutes_struct
                )
            elif inner_puz_mod == GOVERNANCE_MOD:
                return self.governance_handler.handle(coin_spend, inner_args, block_record, statutes_struct)

        return None
