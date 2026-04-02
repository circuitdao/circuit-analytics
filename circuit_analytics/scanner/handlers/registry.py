import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia_rs import CoinSpend

from circuit_analytics.drivers.registry import (
    AnnouncerRegistry,
    RegistryLaunchSolution,
    RegistryRegisterSolution,
    RegistryRewardSolution,
    get_registry_solution_info,
)
from circuit_analytics.scanner.handlers.base import HandlerResult

log = logging.getLogger(__name__)


class RegistryHandler:
    def handle(
        self, coin_spend: CoinSpend, block_record: Dict[str, Any], statutes_struct: Program
    ) -> Optional[HandlerResult]:
        mod, args = uncurry(coin_spend.puzzle_reveal)

        if statutes_struct != args.at("rf"):
            return None

        result = HandlerResult()
        delta = result.stats_delta
        delta.registry_operations_count += 1

        solution_info = get_registry_solution_info(coin_spend)

        if isinstance(solution_info, RegistryLaunchSolution):
            pass

        elif isinstance(solution_info, RegistryRegisterSolution):
            delta.registered_announcer_count_delta += 1

        elif isinstance(solution_info, RegistryRewardSolution):
            delta.registered_announcer_count_delta -= args.at("rrf").list_len()
            delta.crt_circulation_delta += solution_info.rewards_per_interval

        return result
