import logging
from typing import Any, Dict, Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia.wallet.util.compute_additions import compute_additions
from chia_rs import CoinSpend

from circuit_analytics.drivers.crt import GovernanceExitInfo, get_governance_solution_info
from circuit_analytics.mods import operation_name
from circuit_analytics.scanner.handlers.base import HandlerResult
from circuit_analytics.scanner.models import GoverningCRT

log = logging.getLogger(__name__)


def plain_op_name(program_name: str | None, with_governance: bool = True) -> str:
    if program_name is None:
        if with_governance:
            return "enter_governance"
        return "exit_governance"
    elif program_name == "PROGRAM_GOVERNANCE_RESET_BILL_MOD":
        return "reset_bill"
    elif program_name == "PROGRAM_GOVERNANCE_PROPOSE_BILL_MOD":
        return "propose_bill"
    elif program_name == "PROGRAM_GOVERNANCE_VETO_ANNOUNCEMENT_MOD":
        return "announce_veto"
    elif program_name == "PROGRAM_GOVERNANCE_VETO_BILL_MOD":
        return "veto_bill"
    elif program_name == "PROGRAM_GOVERNANCE_IMPLEMENT_BILL_MOD":
        return "implement_bill"
    raise ValueError(f"Unknown governance program name {program_name}")


class LaunchGovernanceHandler:
    def handle(
        self, coin_spend: CoinSpend, inner_args: Program, block_record: Dict[str, Any], statutes_struct: Program
    ) -> Optional[HandlerResult]:
        if statutes_struct.get_tree_hash() != inner_args.at("rrrf").atom:
            return None

        result = HandlerResult()
        delta = result.stats_delta

        new_coins = compute_additions(coin_spend)
        new_coin = new_coins[0]
        name = new_coin.name()
        amount = coin_spend.coin.amount

        delta.governance_coin_count_delta += 1
        delta.governance_circulation_delta += amount

        govt_crt = GoverningCRT(
            name=name.hex(),
            amount=amount,
            bill=None,
            operation=None,
            timestamp=block_record["timestamp"],
            spent=False,
            height=block_record["height"],
        )
        result.coins_to_add.append(govt_crt)
        result.coins_to_remove.add(new_coin.parent_coin_info.hex())
        return result


class GovernanceHandler:
    def handle(
        self, coin_spend: CoinSpend, inner_args: Program, block_record: Dict[str, Any], statutes_struct: Program
    ) -> Optional[HandlerResult]:
        if statutes_struct != inner_args.at("rrf"):
            return None

        result = HandlerResult()
        delta = result.stats_delta
        delta.governance_operations_count += 1

        new_coins = compute_additions(coin_spend)
        assert len(new_coins) == 1
        new_coin = new_coins[0]

        _, cat_args = uncurry(coin_spend.puzzle_reveal)
        cat_inner_puzzle = cat_args.at("rrf")
        cat_inner_solution = Program.from_serialized(coin_spend.solution).first()

        gov_info = get_governance_solution_info(
            coin_spend.coin, cat_inner_puzzle, cat_inner_solution, statutes_struct=statutes_struct
        )

        if gov_info is None:
            return None

        with_governance = not isinstance(gov_info, GovernanceExitInfo)
        op = gov_info.operation if not gov_info.operation.nullp() else None
        prog_name = operation_name(op)
        op_name = plain_op_name(prog_name, with_governance=with_governance)  # noqa: F841

        if not with_governance:
            delta.governance_coin_count_delta -= 1
            delta.governance_circulation_delta -= coin_spend.coin.amount
            result.coins_to_remove.add(new_coin.parent_coin_info.hex())
        else:
            name = new_coin.name()
            amount = new_coin.amount
            bill = inner_args.at("rrrrf")

            govt_crt = GoverningCRT(
                name=name.hex(),
                amount=amount,
                bill=bill.as_bin().hex() if not bill.nullp() else None,
                operation=prog_name,
                timestamp=block_record["timestamp"],
                spent=False,
                height=block_record["height"],
            )
            result.coins_to_add.append(govt_crt)
            result.coins_to_remove.add(new_coin.parent_coin_info.hex())

        return result
