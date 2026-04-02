from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Any, TypeVar, Type

from chia.types.blockchain_format.program import Program, uncurry
from chia.types.coin_spend import CoinSpend
from chia_rs import Coin
from chia_rs.sized_bytes import bytes32
from chia.wallet.util.curry_and_treehash import calculate_hash_of_quoted_mod_hash, curry_and_treehash

from circuit_analytics.drivers.condition_filtering import filter_and_extract_remark_solution
from circuit_analytics.drivers.protocol_math import PRECISION_BPS, PRICE_PRECISION
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    ATOM_ANNOUNCER_MOD_HASH,
    ORACLE_MOD_HASH,
    ORACLE_MOD_RAW,
    PROGRAM_BACKUP_ORACLE_PRICE_MUTATION_MOD_RAW,
    PROGRAM_ORACLE_PRICE_MUTATION_MOD_HASH,
    PROGRAM_STANDARD_ORACLE_PRICE_MUTATION_MOD_HASH,
    PROGRAM_STANDARD_ORACLE_PRICE_MUTATION_MOD_RAW,
    SINGLETON_MOD_HASH,
)
from circuit_analytics.utils import to_list, to_tuple, to_type, tuple_to_struct


log = logging.getLogger(__name__)


MAX_ORACLE_PRICE = 1_000_000 * PRICE_PRECISION
T = TypeVar("T", bound="OracleMutationProgram")


@dataclass
class OracleMutationProgram(ABC):
    name: str = field(init=False)  # name of raw mutation program mod
    raw_mod: Program = field(init=False)  # raw mutation program mod (without curried fixed args)

    @classmethod
    def from_env(cls: Type[T]) -> T:
        oracle_params = os.getenv("CIRCUIT_ORACLE_MUTATION_PROGRAM_PARAMETERS", None)
        if not oracle_params:
            return StandardOracleMutationProgram()
        try:
            M_OF_N, WHITELIST = oracle_params.split(",")
            m_of_n = int(M_OF_N)
            whitelist = [bytes32(launcher_id.atom) for launcher_id in Program.from_hexstr(WHITELIST).as_iter()]
        except Exception:
            raise ValueError(
                "Failed to extract backup oracle m-of-n or whitelist from CIRCUIT_ORACLE_MUTATION_PROGRAM_PARAMETERS env var"
            )
        return BackupOracleMutationProgram(m_of_n, whitelist)

    @property
    def raw_mod_hash(self) -> bytes32:
        return self.raw_mod.get_tree_hash()

    @property
    @abstractmethod
    def mod(self) -> Program:
        """Return mutation program mod with fixed args curried."""
        ...

    @property
    def mod_hash(self) -> bytes32:
        """Return mutation program mod hash with fixed args curried."""
        return self.mod.get_tree_hash()


@dataclass
class StandardOracleMutationProgram(OracleMutationProgram):
    name: str = "standard"
    raw_mod: Program = field(default_factory=lambda: PROGRAM_STANDARD_ORACLE_PRICE_MUTATION_MOD_RAW)

    @property
    def mod(self) -> Program:
        return self.raw_mod.curry(ATOM_ANNOUNCER_MOD_HASH)


@dataclass
class BackupOracleMutationProgram(OracleMutationProgram):
    name: str = "backup"
    raw_mod: Program = field(default_factory=lambda: PROGRAM_BACKUP_ORACLE_PRICE_MUTATION_MOD_RAW)
    m_of_n: int = field(default=-1)
    whitelist: list[bytes32] = field(default_factory=list)

    def __post_init__(self):
        if self.m_of_n < 0:
            raise ValueError(
                "Must provide a (non-negative) value for m_of_n when instantating BackupOracleMutationProgram"
            )
        if not self.whitelist:
            raise ValueError(
                "Backup Oracle mutation program must contain at least one whitelisted announcer launcher ID"
            )

    @property
    def mod(self) -> Program:
        return self.raw_mod.curry(ATOM_ANNOUNCER_MOD_HASH, self.m_of_n, self.whitelist)


def get_oracle_mutation_program(mutation_mod: Program) -> OracleMutationProgram:
    """Returns info on an oracle mutation program.

    Arguments:
    - mutation_mod: an Oracle mutation program (with fixed args curried)
    """
    mutation_mod_hash = mutation_mod.get_tree_hash()
    outer_mod, fixed_args = uncurry(mutation_mod)
    if mutation_mod_hash == PROGRAM_STANDARD_ORACLE_PRICE_MUTATION_MOD_HASH:
        mutation_program = StandardOracleMutationProgram("standard", outer_mod)
        assert mutation_program.mod_hash == mutation_mod_hash, (
            "Standard oracle mutation program mod hash does not match"
        )
        return mutation_program
    else:
        # non-standard oracle mutation
        if outer_mod.get_tree_hash() == PROGRAM_BACKUP_ORACLE_PRICE_MUTATION_MOD_RAW.get_tree_hash():
            # backup oracle
            (
                atom_announcer_mod_hash,
                m_of_n,
                whitelist,
            ) = to_list(fixed_args, 3, ["bytes32", "int", None])
            launcher_ids = []
            for launcher_id in whitelist.as_iter():
                launcher_ids.append(to_type(launcher_id.atom, "bytes32", "launcher ID whitelisted in backup oracle"))
            mutation_program = BackupOracleMutationProgram("backup", outer_mod, m_of_n, launcher_ids)
            assert mutation_program.mod_hash == mutation_mod_hash, (
                "Backup oracle mutation program mod hash does not match"
            )
            return mutation_program
        else:
            raise ValueError(f"Unknown raw oracle mutation program hash {outer_mod.get_tree_hash().hex()}")


@dataclass
class OracleSolutionInfo:
    inner_puzzle: Program
    inner_solution: Program
    operation: Program

    @property
    def operation_hash(self) -> bytes32 | None:
        if self.operation.nullp():
            return None
        return self.operation.get_tree_hash()

    @property
    def operation_program(self) -> OracleMutationProgram | None:
        if self.operation.nullp():
            return None  # not a mutation operation (announce)
        else:
            return get_oracle_mutation_program(self.operation)


@dataclass
class OracleMutationInfo(OracleSolutionInfo):
    statutes_inner_puzzle_hash: bytes32
    m_of_n: int
    price_updatable_after_seconds: int
    price_updatable_threshold_bps: int
    price_delay: int
    current_timestamp: int
    price_announcers: list[list[bytes32, bytes32, int]]
    # calculated
    median_price: int
    new_price_infos: list[tuple[int, int]]


@dataclass
class OracleAnnounceInfo(OracleSolutionInfo):
    current_timestamp: int
    price_delay: int
    statutes_inner_puzzle_hash: bytes32 | None

    @property
    def is_launch(self) -> bool:
        return True if not self.statutes_inner_puzzle_hash else False


def unique_elements(lst: list[Any]) -> Any:
    seen = set()
    for element in lst:
        if element in seen:
            return element
        seen.add(element)
    return None


def get_oracle_solution_info(spend_info: CoinSpend | Program) -> OracleSolutionInfo:
    # spend_info is either oracle coin spend or a tuple of oracle singleton inner puzzle and solution
    if isinstance(spend_info, CoinSpend):
        coin_spend = spend_info
        (
            launcher_id,
            price_infos,
            last_updated,
            launcher_puzzle_hash,
        ) = get_oracle_puzzle_info(coin_spend.puzzle_reveal)
        singleton_struct = tuple_to_struct((SINGLETON_MOD_HASH, tuple_to_struct((launcher_id, launcher_puzzle_hash))))
        solution = Program.from_serialized(coin_spend.solution)
        (
            lineage_proof,
            _amount,
            singleton_inner_solution,
        ) = to_list(solution, 3)
        # lineage proof
        if lineage_proof.list_len() == 2:
            (
                parent_parent_coin_name,
                parent_amount,
            ) = to_list(lineage_proof, 2, ["bytes32", "int"])
            parent_coin_name = Coin(parent_parent_coin_name, launcher_puzzle_hash, parent_amount).name()
            if parent_coin_name != coin_spend.coin.parent_coin_info:
                raise SpendError(
                    f"Oracle eve lineage proof does not yield correct parent coin ID. "
                    f"Expected {coin_spend.coin.parent_coin_info}, got {parent_coin_name}"
                )
        elif lineage_proof.list_len() == 3:
            (
                parent_parent_coin_name,
                parent_inner_puzzle_hash,
                parent_amount,
            ) = to_list(lineage_proof, 3, ["bytes32", "bytes32", "int"], "Oracle non-eve lineage proof")
            parent_puzzle_hash = curry_and_treehash(
                calculate_hash_of_quoted_mod_hash(SINGLETON_MOD_HASH),
                singleton_struct.get_tree_hash(),
                parent_inner_puzzle_hash,
            )
            parent_coin_name = Coin(parent_parent_coin_name, parent_puzzle_hash, parent_amount).name()
            if parent_coin_name != coin_spend.coin.parent_coin_info:
                raise SpendError(
                    f"Oracle non-eve lineage proof does not yield correct parent coin ID. "
                    f"Expected {coin_spend.coin.parent_coin_info}, got {parent_coin_name}"
                )
        else:
            raise SpendError(
                f"Oracle lineage proof must be list of length 2 (eve spend) or 3, got {lineage_proof.list_len()}"
            )
    elif isinstance(spend_info, tuple) and len(spend_info) == 2:
        singleton_inner_puzzle = spend_info[0]
        singleton_inner_solution = spend_info[1]
        sip_mod, sip_args = uncurry(singleton_inner_puzzle)
        price_infos = [(pi.first().as_int(), pi.rest().as_int()) for pi in sip_args.at("rrf").as_iter()]
    else:
        ValueError("Invalid argument type in get_oracle_solution_info. Must be either CoinSpend or Program")
    # inner solution
    (
        inner_puzzle,
        inner_solution,
        operation,
    ) = to_list(singleton_inner_solution, 3)
    solution_info = OracleSolutionInfo(inner_puzzle, inner_solution, operation)
    mutation_program: OracleMutationProgram | None = solution_info.operation_program
    operation_hash = operation.get_tree_hash() if not operation.nullp() else None
    input_conditions = inner_puzzle.run(inner_solution)
    solution_remark, filtered_conditions = filter_and_extract_remark_solution(list(input_conditions.as_iter()))
    (
        signed_operation_hash,
        op_args,
    ) = to_list(solution_remark, 2, ["bytes32_or_none", None])
    if not signed_operation_hash == operation_hash:
        raise SpendError(
            f"Oracle operation hash extracted from solution REMARK does not match hash "
            f"of operation program provided in solution. "
            f"Expected {operation_hash.hex()}, got {signed_operation_hash.hex()}"
        )
    if isinstance(mutation_program, StandardOracleMutationProgram):
        (
            statutes_inner_puzzle_hash,
            m_of_n,
            price_updatable_after_seconds,
            price_updatable_threshold_bps,
            price_delay,
            current_timestamp,
            price_announcers,  # ; -> ((launcher_id args_hash price))
        ) = to_list(op_args, 7, ["bytes32", "int", "int", "int", "int", "uint64", None])
        announcers = []
        for announcer in price_announcers.as_iter():
            announcers.append(to_list(announcer, 3, ["bytes32", "bytes32", "int"]))
        sorted_prices = sorted([a[2] for a in announcers])  # ascending order
        median_price = sorted_prices[len(sorted_prices) // 2]
        last_matured_price_info, price_infos_cut = cut_price_infos(price_infos, current_timestamp - price_delay)
        if not price_infos_cut:
            raise SpendError(
                "Oracle mutation only possible if there is at least one price info in price info queue. "
                "This is a state the oracle should never reach. Something is wrong."
            )
        new_price_infos = price_infos_cut + [(median_price, current_timestamp)]
        last_matured_price, last_matured_price_timestamp = (
            last_matured_price_info if last_matured_price_info else (0, 0)
        )
        last_price, last_price_timestamp = price_infos_cut[-1]
        if not median_price >= 0:
            raise SpendError(f"Oracle mutation must have non-negative median price ({median_price} >= 0)")
        if not median_price < MAX_ORACLE_PRICE:
            raise SpendError(
                f"Oracle mutation must have median price less than MAX_ORACLE_PRICE ({median_price} < {MAX_ORACLE_PRICE})"
            )
        if not len(announcers) >= m_of_n:
            raise SpendError(
                f"Oracle mutation with too few announcers. Must be passed info for at least {m_of_n} announcers, got {len(announcers)}"
            )
        duplicate = unique_elements([a[0] for a in announcers])
        if duplicate:
            raise SpendError(
                f"Oracle mutation must be passed info for mututally distinct announcers. Duplicate announcer launcher ID: {duplicate.hex()}"
            )
        if not median_price > 0:
            raise SpendError(f"Oracle mutation must result in positive median price ({median_price} > 0)")
        if not current_timestamp > last_price_timestamp:
            raise SpendError(
                f"Oracle mutation requires current timestamp to be greater than most "
                f"recent timestamp in price info queue ({current_timestamp} > {last_price_timestamp})"
            )
        if not (
            current_timestamp - last_price_timestamp > price_updatable_after_seconds
            or abs(((last_price - median_price) * PRECISION_BPS) // last_price) > price_updatable_threshold_bps
        ):
            raise SpendError(
                f"Oracle update thresholds not reached. Can only update if current timestamp minus most recent timestamp in price info queue is "
                f"greater than ORACLE_PRICE_UPDATE_DELAY ({current_timestamp - last_price_timestamp} > {price_updatable_after_seconds}) or "
                f"median price deviates by more than ORACLE_PRICE_UPDATE_RATIO_BPS from most recent price in price info queue "
                f"({abs(((last_price - median_price) * PRECISION_BPS) // last_price)} > {price_updatable_threshold_bps})"
            )
        return OracleMutationInfo(
            **solution_info.__dict__,
            # op args
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            m_of_n=m_of_n,
            price_updatable_after_seconds=price_updatable_after_seconds,
            price_updatable_threshold_bps=price_updatable_threshold_bps,
            price_delay=price_delay,
            current_timestamp=current_timestamp,
            price_announcers=announcers,
            # calculated
            median_price=median_price,
            new_price_infos=new_price_infos,
        )
    elif isinstance(mutation_program, BackupOracleMutationProgram):
        pass
    elif operation_hash is None:
        # announce operation
        (
            current_timestamp,
            price_delay,
            statutes_inner_puzzle_hash,
        ) = to_list(op_args, 3, ["int", "int", "bytes32_or_nil"])
        cutoff = current_timestamp - price_delay if (current_timestamp and price_delay) else 0
        last_matured_price_info, _ = cut_price_infos(price_infos, cutoff)
        last_matured_price, last_matured_price_timestamp = (
            last_matured_price_info if last_matured_price_info else (0, 0)
        )
        if not last_matured_price_timestamp:
            raise SpendError("Oracle announce operation requires a matured price")
        return OracleAnnounceInfo(
            **solution_info.__dict__,
            current_timestamp=current_timestamp,
            price_delay=price_delay,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
        )
    else:
        raise SpendError(
            f"Oracle operation hash invalid. Expected nil or a valid mutation program hash, got {operation_hash.hex()}"
        )


def get_cutoff(
    current_timestamp: int | None,
    price_delay: int | None,  # Statute
) -> int:
    """Returns timestamp at which oracle price infos will be cut off"""
    if current_timestamp and price_delay:
        return current_timestamp - price_delay
    else:
        return 0


def cut_price_infos(
    price_infos: list[tuple[int, int]], cutoff_timestamp: int
) -> tuple[tuple[int, int] | None, list[tuple[int, int]] | None]:
    """Returns last matured price info and the list of last matured price info concatenated with non-matured price infos.

    If there is no matured price info, None will be returned together with the list of non-matured price infos.
    Use function get_cutoff to calculate the cutoff timestamp to pass to this function.

    Equivalent of cut-price-infos function in oracle.clib
    """
    if not price_infos:
        return None, []
    last_matured_price_info = None
    for i in range(len(price_infos)):
        if cutoff_timestamp > price_infos[i][1]:
            last_matured_price_info = price_infos[i]
        elif not last_matured_price_info:
            # no matured price info
            return None, price_infos
        else:
            # matured price info and at least one non-matured one
            return last_matured_price_info, [last_matured_price_info] + price_infos[i:]
    # all price infos matured
    return last_matured_price_info, [last_matured_price_info]


def get_oracle_puzzle_info(oracle_puzzle: Program) -> tuple[bytes32, list[tuple[int, int]], int, bytes32]:
    singleton_mod, singleton_args = uncurry(oracle_puzzle)
    if singleton_mod and singleton_mod.get_tree_hash() == SINGLETON_MOD_HASH:
        # singleton_struct = singleton_args.first()
        launcher_id = bytes32(singleton_args.at("frf").atom)
        launcher_puzzle_hash = bytes32(singleton_args.at("frr").atom)
        inner_puzzle = Program.to(singleton_args.at("rf"))
        try:
            inner_mod, inner_args = uncurry(inner_puzzle)
        except IndexError:
            raise ValueError("Invalid oracle puzzle to uncurry")
        if inner_mod and inner_mod.get_tree_hash() == ORACLE_MOD_HASH:
            price_infos = [(x.first().as_int(), x.rest().as_int()) for x in list(inner_args.at("rrf").as_iter())]
            last_updated = price_infos[-1][1]
        else:
            raise ValueError("Not an oracle puzzle")
    else:
        raise ValueError("Invalid oracle puzzle. Must have singleton layer as outermost layer")

    raw_mod, fixed_args = uncurry(inner_mod)
    assert raw_mod == ORACLE_MOD_RAW, "Raw oracle mod does not match"
    mutation_mod_hash = bytes32(fixed_args.first().atom)

    mutation_program = OracleMutationProgram.from_env()
    assert mutation_program.mod_hash == mutation_mod_hash, (
        "Mutation mod hash of oracle puzzle does not match environment variable"
    )

    return (
        launcher_id,
        price_infos,  # contains last_updated
        last_updated,
        launcher_puzzle_hash,
    )
