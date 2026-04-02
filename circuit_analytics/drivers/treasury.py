from __future__ import annotations

import logging

from dataclasses import dataclass
from typing import Any, List, Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia_rs.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend
from chia.wallet.util.curry_and_treehash import (
    calculate_hash_of_quoted_mod_hash,
    curry_and_treehash,
)
from chia_rs import Coin

from circuit_analytics.drivers.condition_filtering import filter_and_extract_remark_solution
from circuit_analytics.drivers.protocol_math import PRECISION_PCT
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    BYC_TAIL_MOD_HASH,
    CRT_TAIL_MOD_HASH,
    TREASURY_MOD_HASH,
)
from circuit_analytics.utils import (
    to_list,
    to_tuple,
    to_type,
)


log = logging.getLogger(__name__)


@dataclass
class TreasurySolutionInfo:
    inner_puzzle: Program
    inner_solution: Program
    # lineage proof
    parent_parent_id: bytes32 | None
    parent_ring_prev_launcher_id: bytes32 | None
    parent_amount: int | None
    statutes_inner_puzzle_hash: bytes32
    current_amount: int


@dataclass
class TreasuryRebalanceInfo(TreasurySolutionInfo):
    prev_parent_id: bytes32
    prev_curried_args_hash: bytes32
    prev_amount: int
    next_parent_id: bytes32
    next_curried_args_hash: bytes32
    next_amount: int
    min_parent_id: bytes32
    min_curried_args_hash: bytes32
    min_amount: int
    max_parent_id: bytes32
    max_curried_args_hash: bytes32
    max_amount: int
    rebalance_ratio_pct: int
    prev_delta_sum: int
    baseline_amount: int
    new_amount: int
    args: Program

    @property
    def withdraw_amount(self) -> int:
        if self.current_amount > self.new_amount:
            return self.current_amount - self.new_amount
        return 0


@dataclass
class TreasuryChangeOrderingInfo(TreasurySolutionInfo):
    rebalance_args: Program
    new_ring_prev_launcher_id: bytes32
    new_amount: int

    @property
    def withdraw_amount(self) -> int:
        return 0


@dataclass
class TreasuryChangeBalanceInfo(TreasurySolutionInfo):
    rebalance_args: Program
    approver_parent_id: bytes32
    approver_mod_hash: bytes32
    approver_cat_tail_hash: bytes32 | None
    approver_mod_curried_args_hash: bytes32
    approver_amount: int
    approval_mod_hashes: Program
    new_amount: int
    run_tail_mod_hash: bytes32 | None

    @property
    def withdraw_amount(self) -> int:
        if self.current_amount > self.new_amount:
            return self.current_amount - self.new_amount
        return 0


def get_treasury_solution_info(
    coin: Coin, cat_inner_puzzle: Program, cat_inner_solution: Program
) -> TreasurySolutionInfo | None:
    """Takes Treasury coin and CAT layer inner puzzle and inner solution and returns info."""
    mod, curried_args = uncurry(cat_inner_puzzle)
    if mod.get_tree_hash() != TREASURY_MOD_HASH:
        return None
    (
        mod_hash,
        statutes_struct,
        launcher_id,
        ring_prev_launcher_id,
    ) = to_list(curried_args, 4, ["bytes32", None, "bytes32", "bytes32"])
    (
        inner_puzzle,
        inner_solution,
    ) = to_list(cat_inner_solution, 2)
    input_conditions = inner_puzzle.run(inner_solution)
    solution_remark_body, filtered_conditions = filter_and_extract_remark_solution(list(input_conditions.as_iter()))
    (
        lineage_proof,
        statutes_inner_puzzle_hash,
        current_amount,
        rebalance_args,
        args,
    ) = to_tuple(solution_remark_body, 5, [None, "bytes32", "int", None, None])
    if lineage_proof.nullp():
        # eve spend
        parent_parent_id = None
        parent_ring_prev_launcher_id = None
        parent_amount = None
    elif not lineage_proof.listp():
        raise SpendError(f"Treasury lineage proof cannot be an atom (other than nil), got {lineage_proof}")
    elif not lineage_proof.rest().listp():
        # parent spend was a rebalance or change balance
        # (parent parent id . parent amount)
        (
            parent_parent_id,
            parent_amount,
        ) = to_tuple(lineage_proof, 2, ["bytes32", "int"])
        parent_ring_prev_launcher_id = None
    elif lineage_proof.rest().list_len() == 1:
        # parent spend was a change ordering
        # (parent parent id, parent ring_prev_launcher_id)
        (
            parent_parent_id,
            parent_ring_prev_launcher_id,
        ) = to_list(lineage_proof, 2, ["bytes32", "bytes32"])
        parent_amount = None
    elif lineage_proof.rest().list_len() == 2:
        # parent spend was a change ordering (non-eve with amount)
        # (parent parent id, parent ring_prev_launcher_id, parent amount)
        (
            parent_parent_id,
            parent_ring_prev_launcher_id,
            parent_amount,
        ) = to_list(lineage_proof, 3, ["bytes32", "bytes32", "int"])
    else:
        raise SpendError(
            f"Treasury lineage proof must be nil, a 2-element struct, a 2-element list, or a 3-element list, got ({lineage_proof})"
        )
    if not current_amount >= 0:
        raise SpendError(f"Treasury solution current amount must be >= 0 ({current_amount} >= 0)")
    statutes_struct = curried_args.at("rf")
    statutes_struct_hash = statutes_struct.get_tree_hash()
    byc_tail_hash = curry_and_treehash(calculate_hash_of_quoted_mod_hash(BYC_TAIL_MOD_HASH), statutes_struct_hash)
    crt_tail_hash = curry_and_treehash(calculate_hash_of_quoted_mod_hash(CRT_TAIL_MOD_HASH), statutes_struct_hash)
    solution_info = TreasurySolutionInfo(
        inner_puzzle=inner_puzzle,
        inner_solution=inner_solution,
        parent_parent_id=parent_parent_id,
        parent_ring_prev_launcher_id=parent_ring_prev_launcher_id,
        parent_amount=parent_amount,
        statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
        current_amount=current_amount,
    )
    if not rebalance_args.nullp():
        # rebalance
        new_amount = rebalance_args.at("rrrrrrrf").as_int()
    elif not args.listp():
        # change ring ordering: args is a bytes32 atom (new_ring_prev_launcher_id)
        new_amount = current_amount
    else:
        # change balance: args is a 7-element list
        new_amount = args.at("rrrrrf").as_int()
    withdraw_amount = current_amount - new_amount if current_amount > new_amount else 0
    if not new_amount >= 0:
        raise SpendError(f"Treasury operations require new amount >= 0, got {new_amount}")
    if not withdraw_amount >= 0:
        raise SpendError(f"Treasury operations require withdraw amount >= 0, got {withdraw_amount}")
    if not ((current_amount == new_amount + withdraw_amount) or (withdraw_amount == 0 and new_amount > current_amount)):
        raise SpendError(
            f"Treasury operations require that if there is a withdrawal (incl no balance change), "
            f"withdraw amount is exact difference between old and new amounts "
            f"({withdraw_amount} = {current_amount} - {new_amount}), or if not, that new amount is greater than old amount "
            f"({new_amount} > {current_amount})"
        )
    if withdraw_amount > 0 and new_amount > current_amount:
        raise SpendError(
            f"Treasury operations cannot result in withdrawal and deposit at the same time (not ({withdraw_amount} > 0 and {new_amount} > {current_amount}))"
        )
    if not rebalance_args.nullp():
        # rebalance operation
        (
            prev_coin_info,
            next_coin_info,
            min_coin_info,
            max_coin_info,
            rebalance_ratio_pct,
            prev_delta_sum,
            baseline_amount,
            _,  # new_amount
        ) = to_list(rebalance_args, 8, [None, None, None, None, "int", "int", "int", "int"])
        (
            prev_parent_id,
            prev_curried_args_hash,
            prev_amount,
        ) = to_list(prev_coin_info, 3, ["bytes32", "bytes32", "int"])
        (
            next_parent_id,
            next_curried_args_hash,
            next_amount,
        ) = to_list(next_coin_info, 3, ["bytes32", "bytes32", "int"])
        (
            min_parent_id,
            min_curried_args_hash,
            min_amount,
        ) = to_list(min_coin_info, 3, ["bytes32", "bytes32", "int"])
        (
            max_parent_id,
            max_curried_args_hash,
            max_amount,
        ) = to_list(max_coin_info, 3, ["bytes32", "bytes32", "int"])
        if not prev_amount >= 0:
            raise SpendError(f"Treasury rebalance operation requires prev_amount >= 0, got {prev_amount}")
        if not next_amount >= 0:
            raise SpendError(f"Treasury rebalance operation requires next_amount >= 0, got {next_amount}")
        if not min_amount >= 0:
            raise SpendError(f"Treasury rebalance operation requires min_amount >= 0, got {min_amount}")
        if not max_amount >= 0:
            raise SpendError(f"Treasury rebalance operation requires max_amount >= 0, got {max_amount}")
        if not baseline_amount >= 0:
            raise SpendError(f"Treasury rebalance operation requires baseline amount >= 0, got {baseline_amount}")
        if not new_amount >= baseline_amount:
            raise SpendError(
                f"Treasury rebalance operation requires new amount >= baseline amount ({new_amount} >= {baseline_amount})"
            )
        if not new_amount <= baseline_amount + 1:
            raise SpendError(
                f"Treasury rebalance operation requires new amount <= baseline amount + 1 ({new_amount} <= {baseline_amount + 1})"
            )
        if not min_amount > 0:
            raise SpendError(f"Treasury rebalance operation requires min amount > 0 ({min_amount} > 0)")
        if not (max_amount - min_amount) * PRECISION_PCT > min_amount * rebalance_ratio_pct:
            raise SpendError(
                f"Treasury rebalance operation requires sufficiently large difference between min and max coin amounts. max - min amount > min amount * rebalance ratio "
                f"({max_amount}-{min_amount}>{min_amount}*{rebalance_ratio_pct / PRECISION_PCT})"
            )
        return TreasuryRebalanceInfo(
            **solution_info.__dict__,
            # rebalance args
            prev_parent_id=prev_parent_id,
            prev_curried_args_hash=prev_curried_args_hash,
            prev_amount=prev_amount,
            next_parent_id=next_parent_id,
            next_curried_args_hash=next_curried_args_hash,
            next_amount=next_amount,
            min_parent_id=min_parent_id,
            min_curried_args_hash=min_curried_args_hash,
            min_amount=min_amount,
            max_parent_id=max_parent_id,
            max_curried_args_hash=max_curried_args_hash,
            max_amount=max_amount,
            rebalance_ratio_pct=rebalance_ratio_pct,
            prev_delta_sum=prev_delta_sum,
            baseline_amount=baseline_amount,
            new_amount=new_amount,
            # args
            args=args,
        )
    elif not args.listp():
        # change ring ordering operation: args is a bytes32 atom (new_ring_prev_launcher_id)
        new_ring_prev_launcher_id = to_type(args, "bytes32", "new_ring_prev_launcher_id")
        return TreasuryChangeOrderingInfo(
            **solution_info.__dict__,
            # rebalance args
            rebalance_args=rebalance_args,
            # args
            new_ring_prev_launcher_id=new_ring_prev_launcher_id,
            # other
            new_amount=new_amount,
        )
    else:
        # change balance operation
        (
            approver_parent_id,
            approver_mod_hash,
            approver_mod_curried_args_hash_prog,
            approver_amount,
            approval_mod_hashes,
            _,  # new_amount
            run_tail_mod_hash,
        ) = to_list(args, 7, ["bytes32", "bytes32", None, "int", None, "int", "bytes32_or_nil"])
        (
            collateral_vault_mod_hash,
            surplus_auction_mod_hash,
            recharge_auction_mod_hash,
            savings_vault_mod_hash,
            announcer_registry_mod_hash,
        ) = to_list(approval_mod_hashes, 5, ["bytes32", "bytes32", "bytes32", "bytes32", "bytes32"])
        if approver_mod_curried_args_hash_prog.list_len() > 0:
            approver_cat_tail_hash = to_type(
                approver_mod_curried_args_hash_prog.first(), "bytes32", "approver_mod_curried_args_hash.first()"
            )
            approver_mod_curried_args_hash = to_type(
                approver_mod_curried_args_hash_prog.rest(), "bytes32", "approver_mod_curried_args_hash.rest()"
            )
        else:
            approver_cat_tail_hash = None
            approver_mod_curried_args_hash = to_type(
                approver_mod_curried_args_hash_prog, "bytes32", "approver_mod_curried_args_hash"
            )
        if approver_cat_tail_hash:
            # approver is a CAT
            if approver_cat_tail_hash == crt_tail_hash:
                # surplus auction
                if not approver_mod_hash == surplus_auction_mod_hash:
                    raise SpendError(
                        f"Treasury change balance operation requires approval from a surplus auction if approver is a CRT CAT "
                        f"({approver_mod_hash.hex()} = {surplus_auction_mod_hash.hex()})"
                    )
            elif approver_cat_tail_hash == byc_tail_hash:
                if not approver_mod_hash in [recharge_auction_mod_hash, savings_vault_mod_hash]:
                    raise SpendError(
                        f"Treasury change balance operation requires approval from a recharge auction or savings vault if a approver is a BYC CAT "
                        f"({approver_mod_hash.hex()} in [{recharge_auction_mod_hash.hex()}, {savings_vault_mod_hash.hex()}])"
                    )
            else:
                raise SpendError(
                    f"Treasury change balance operation requires approval from a BYC or CRT coin if the approver is a CAT "
                    f"({approver_cat_tail_hash.hex()} in [{byc_tail_hash.hex()}, {crt_tail_hash.hex()}])"
                )
        else:
            # approver is not a CAT. Must be a collateral vault
            if not approver_mod_hash == collateral_vault_mod_hash:
                raise SpendError(
                    f"Treasury change balance operation requires approval from a collateral vault if approver is not a CAT "
                    f"({approver_mod_hash.hex()} = {collateral_vault_mod_hash.hex()})"
                )
        if not approver_amount >= 0:
            raise SpendError(
                f"Treasury change balance operation requires approver amount >= 0 ({approver_amount} >= 0)"
            )
        if not new_amount >= 0:
            raise SpendError(f"Treasury change balance operation requires new amount >= 0 ({new_amount} >= 0)")
        return TreasuryChangeBalanceInfo(
            **solution_info.__dict__,
            # rebalance args
            rebalance_args=rebalance_args,
            # args
            approver_parent_id=approver_parent_id,
            approver_mod_hash=approver_mod_hash,
            approver_cat_tail_hash=approver_cat_tail_hash,
            approver_mod_curried_args_hash=approver_mod_curried_args_hash,
            approver_amount=approver_amount,
            approval_mod_hashes=approval_mod_hashes,
            new_amount=new_amount,
            run_tail_mod_hash=run_tail_mod_hash,
        )


def sort_ring_tuples(tuples: list[tuple[Any, Any]]) -> list[tuple[Any, Any]]:
    """Return list of sorted 2-tuples where second element points to first element of predecessor tuple.

    For Treasury purposes, tuples are (launcher_id, ring_prev_launcher_id).
    Tuple elements can be of any type, eg. bytes32 or (hex) string.
    """

    # Check that tuples provided are unique
    if len(tuples) != len(set(tuples)):
        log.error("Treasury spends are not unique: %d provided, %d unique", len(tuples), len(set(tuples)))
        raise ValueError("Treasury spends are not unique")

    # Create a dictionary mapping launcher id to tuple
    id_to_tuple = {t[0]: t for t in tuples}

    # Choose an arbitrary starting tuple
    start_tuple = tuples[0]
    start_id = start_tuple[0]

    # Reconstruct the sorted list
    sorted_list = []
    current_id = start_id

    ring_size = 0

    while True:
        current_tuple = id_to_tuple[current_id]
        ring_size += 1
        sorted_list.append(current_tuple)
        next_id = None
        for t in tuples:
            if t[1] == current_id:
                next_id = t[0]
                break

        if next_id is None:
            log.error(
                "Treasury coins do not form a ring: break at id=0x%s after %d steps",
                bytes(current_id).hex() if isinstance(current_id, (bytes, bytearray)) else str(current_id),
                ring_size,
            )
            raise ValueError("Treasury coins do not form a ring")

        if next_id == start_id:  # Completed a ring
            if ring_size != len(tuples):
                log.error(
                    "Treasury coins do not form a full ring: sub-ring of size %d detected (expected %d)",
                    ring_size,
                    len(tuples),
                )
                raise ValueError("Treasury coins do not form a ring, but sub-ring detected")
            log.debug("Sorted ring tuples completed: size=%d", ring_size)
            break

        current_id = next_id

    return sorted_list


def is_ring(tuples: list[tuple[Any, Any]]) -> bool:
    try:
        sort_ring_tuples(tuples)
    except Exception:
        return False
    return True
