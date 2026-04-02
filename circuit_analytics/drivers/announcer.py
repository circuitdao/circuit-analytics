from __future__ import annotations

from enum import Enum
from dataclasses import dataclass, fields
import logging
from typing import Optional

from chia.types.blockchain_format.program import Program, uncurry, run
from chia_rs.sized_bytes import bytes32
from chia.types.condition_opcodes import ConditionOpcode
from chia_rs.sized_ints import uint32, uint64
from chia_rs import CoinSpend

from circuit_analytics.drivers import PROTOCOL_PREFIX
from circuit_analytics.drivers.condition_filtering import filter_and_extract_unique_create_coin
from circuit_analytics.drivers.protocol_math import PRECISION_BPS, PRECISION, find_oracle_median_price
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    PROGRAM_ANNOUNCER_GOVERN_MOD,
    PROGRAM_ANNOUNCER_ANNOUNCE_MOD,
    PROGRAM_ANNOUNCER_CONFIGURE_MOD,
    PROGRAM_ANNOUNCER_MUTATE_MOD,
    PROGRAM_ANNOUNCER_PENALIZE_MOD,
    PROGRAM_ANNOUNCER_REGISTER_MOD,
    ATOM_ANNOUNCER_MOD_HASH,
    ATOM_ANNOUNCER_MOD,
)
from circuit_analytics.utils import to_list, to_tuple, tree_hash_of_apply


log = logging.getLogger(__name__)


class PriceAnnouncerOperations(Enum):
    ANNOUNCE = PROGRAM_ANNOUNCER_ANNOUNCE_MOD
    MUTATE = PROGRAM_ANNOUNCER_MUTATE_MOD
    GOVERN = PROGRAM_ANNOUNCER_GOVERN_MOD
    PENALIZE = PROGRAM_ANNOUNCER_PENALIZE_MOD
    REGISTER = PROGRAM_ANNOUNCER_REGISTER_MOD
    CONFIGURE = PROGRAM_ANNOUNCER_CONFIGURE_MOD

    @classmethod
    def hashes(cls) -> list[bytes32]:
        """Returns list of all operation mod hashes"""
        return [op.value.get_tree_hash() for op in cls]


@dataclass
class PriceAnnouncerInfo:
    launcher_id: bytes32
    prev_announce: bool
    inner_puzzle_hash: bytes32
    prev_deposit: uint64
    deposit: uint64
    approved: bool
    value_ttl: uint64
    value: Program
    min_deposit: uint64
    claim_counter: uint64
    cooldown_start: uint64
    penalizable_at: uint64
    timestamp_expires: uint64
    name: bytes32 = None

    @property
    def is_approved(self):
        return bool(self.approved)

    def is_valid(self, current_timestamp: int) -> bool:
        log.debug(
            f"is_valid: {self.launcher_id.hex()} {self.approved}; {self.timestamp_expires} >= {current_timestamp} "
            f"= {self.timestamp_expires >= current_timestamp}"
        )
        return self.approved and self.timestamp_expires >= current_timestamp

    def is_penalizable(self, statutes_info, current_timestamp: int) -> bool:
        from circuit_analytics.drivers.statutes import StatutePosition
        penalty_per_interval = statutes_info.statutes[StatutePosition.ANNOUNCER_PENALTY_PER_INTERVAL_BPS.value].as_int()
        announcer_min_deposit = statutes_info.statutes[StatutePosition.ANNOUNCER_MINIMUM_DEPOSIT_MOJOS.value].as_int()
        announcer_max_value_ttl = statutes_info.statutes[StatutePosition.ANNOUNCER_MAXIMUM_VALUE_TTL.value].as_int()
        penalized_deposit = (self.deposit * (PRECISION_BPS - penalty_per_interval)) // PRECISION_BPS
        return (
            current_timestamp > self.timestamp_expires
            or self.min_deposit > self.deposit
            or announcer_min_deposit > self.min_deposit
            or self.value_ttl > announcer_max_value_ttl
        ) and (
            self.launcher_id
            and self.approved
            and current_timestamp > self.penalizable_at
            and self.deposit > penalized_deposit >= 0
        )

    def is_registered(self, registry_claim_counter: int) -> bool:
        return self.claim_counter == registry_claim_counter

    @classmethod
    def from_program(cls, program: Program):
        (
            launcher_id,
            prev_announce,
            inner_puzzle_hash,
            prev_deposit,
            deposit,
            approved,
            value_ttl,
            value,
            min_deposit,
            claim_counter,
            cooldown_start,
            penalizable_at,
            timestamp_expires,
        ) = program.as_iter()
        return cls(
            bytes32(launcher_id.as_atom()),
            bool(prev_announce.as_int()),
            bytes32(inner_puzzle_hash.as_atom()),
            uint64(prev_deposit.as_int()),
            uint64(deposit.as_int()),
            bool(approved.as_int()),
            uint64(value_ttl.as_int()),
            value,
            uint64(min_deposit.as_int()),
            uint64(claim_counter.as_int()),
            uint64(cooldown_start.as_int()),
            uint64(penalizable_at.as_int()),
            uint64(timestamp_expires.as_int()),
        )


def select_announcers_for_oracle_update(
    announcers: list[PriceAnnouncerInfo],
    m_of_n: int,
    lowest_or_highest: bool = None,  # aim for lowest (True) or highest (False) median price possible
) -> tuple[int, list[PriceAnnouncerInfo]]:
    """Returns median price and list of Announcers to use to update Oracle price.

    If m_of_n <= number of Announcers, then a subset of m_of_n Announcers is returned, o/w all of them will be used.

    If lowest_or_highest is provided, Announcers will be selected to get lower (True) or highest (False) median price.
    """
    sorted_announcers = sorted(announcers, key=lambda ann: ann.value.as_int())
    if len(announcers) < m_of_n:
        median_price, median_index = find_oracle_median_price([ann.value.as_int() for ann in sorted_announcers])
        return median_price, sorted_announcers

    if lowest_or_highest is None:
        # we use all announcers to determine median price
        median_price, median_index = find_oracle_median_price([ann.value.as_int() for ann in sorted_announcers])
        if m_of_n % 2 == 0:
            selected_announcers = sorted_announcers[
                slice(median_index - int(m_of_n / 2), median_index + int(m_of_n / 2))
            ]
        else:
            selected_announcers = sorted_announcers[
                slice(median_index - int((m_of_n - 1) / 2), median_index + int((m_of_n - 1) / 2) + 1)
            ]
    else:
        # we select a subset of announcers to get lower/highest median price possible
        if lowest_or_highest:
            # going for lowest price
            selected_announcers = sorted_announcers[:m_of_n]
        else:
            # going for highest price
            selected_announcers = sorted_announcers[-m_of_n:]
        median_price, median_index = find_oracle_median_price(selected_announcers)

    return median_price, selected_announcers


def get_price_announcer_info(spend: CoinSpend, via_puzzle=False) -> PriceAnnouncerInfo | None:
    mod, args = uncurry(spend.puzzle_reveal)
    assert mod == ATOM_ANNOUNCER_MOD, "Parent coin was not an announcer"
    if via_puzzle:
        (
            _,
            _,
            launcher_id,
            prev_announce,
            inner_puzzle_hash,
            approved,
            prev_deposit,
            deposit,
            value_ttl,
            value,
            min_deposit,
            claim_counter,
            cooldown_start,
            penalizable_at,
            timestamp_expires,
        ) = args.as_iter()

        return PriceAnnouncerInfo.from_program(
            Program.to(
                [
                    launcher_id,
                    prev_announce,
                    inner_puzzle_hash,
                    prev_deposit,
                    deposit,
                    approved,
                    value_ttl,
                    value,
                    min_deposit,
                    claim_counter,
                    cooldown_start,
                    penalizable_at,
                    timestamp_expires,
                ]
            )
        )
    conditions = run(spend.puzzle_reveal, spend.solution)
    # find first protocol REMARK condition
    for condition in conditions.as_iter():
        if not condition.listp():
            continue
        if condition.first().atom == ConditionOpcode.REMARK:
            if condition.rest().nullp():
                continue
            if not condition.rest().listp():
                continue
            if condition.rest().first().as_atom() == PROTOCOL_PREFIX:
                try:
                    return PriceAnnouncerInfo.from_program(condition.rest().rest())
                except ValueError:
                    continue
    # no protocol REMARK condition found. announcer layer was exited
    return None


@dataclass
class AnnouncerSolutionInfo:
    lineage_proof: Program
    inner_puzzle: Program
    solution_or_conditions: Program
    input_conditions: list[Program]
    new_puzzle_hash: bytes32 | None
    new_deposit: uint64
    operation: Program
    signed_operation_hash: Program
    args: Program
    rest_of_condition: Program
    inner_puzzle_hash: bytes32 | None

    @property
    def operation_hash(self) -> bytes32:
        return self.operation.get_tree_hash()

    def is_eve_solution(self) -> bool:
        return self.lineage_proof.nullp()


@dataclass
class AnnouncerMutateInfo(AnnouncerSolutionInfo):
    current_timestamp: uint64
    new_value: Program

    @property
    def new_price(self) -> uint64 | None:
        if self.new_value.list_len() > 0:
            return None
        return self.new_value.as_int()


@dataclass
class AnnouncerGovernInfo(AnnouncerSolutionInfo):
    statutes_inner_puzzle_hash: bytes32
    input_toggle_activation: bool
    max_disapproval_penalty_factor_or_min_deposit: uint64  # both statutes


@dataclass
class AnnouncerPenalizeInfo(AnnouncerSolutionInfo):
    statute_penalty_per_interval: uint64
    statutes_inner_puzzle_hash: bytes32
    statute_penalty_interval_in_minutes: uint64
    statute_min_deposit: uint64
    statute_max_value_ttl: uint64
    current_timestamp: uint64


@dataclass
class AnnouncerRegisterInfo(AnnouncerSolutionInfo):
    registry_mod_hash: bytes32
    registry_args_hash: bytes32
    registry_claim_counter: uint64
    target_puzzle_hash: bytes32
    statutes_inner_puzzle_hash: bytes32
    statute_min_deposit: uint64


@dataclass
class AnnouncerConfigureInfo(AnnouncerSolutionInfo):
    current_timestamp: Optional[uint64] = None
    statutes_inner_puzzle_hash: Optional[uint64] = None
    input_toggle_activation: Optional[uint64] = None
    deactivation_cooldown_interval: Optional[uint64] = None
    statute_min_deposit: Optional[uint64] = None  # min_deposit in puzzle
    new_value_ttl: Optional[uint64] = None
    statute_max_value_ttl: Optional[uint64] = None  # max_value_ttl in puzzle
    new_value: Optional[uint64] = None
    new_min_deposit: Optional[uint64] = None

    def __post_init__(self):
        """Raise an error if some but not all derived fields are None or if all derived fields are None but not args is not nil."""
        base_class_field_names = {field.name for field in fields(AnnouncerSolutionInfo)}
        derived_class_fields = [field for field in fields(self) if field.name not in base_class_field_names]
        is_none = [getattr(self, field.name) is None for field in derived_class_fields]
        none_fields = [field.name for field in derived_class_fields if getattr(self, field.name) is None]
        non_none_fields = [field.name for field in derived_class_fields if getattr(self, field.name) is not None]
        assert not (any(is_none) and self.args.nullp()), (
            f"All fields defined in AnnouncerConfigureInfo should be None when exiting announcer ({non_none_fields})"
        )
        assert not (any(is_none) and not all(is_none)), (
            f"Some fields defined in AnnouncerConfigureInfo are None ({none_fields}), "
            f"but others are not ({non_none_fields}). All or none must be None"
        )

    @property
    def is_exit(self) -> bool:
        """Return True if all fields defined in AnnouncerConfigureInfo (ie not inherited from AnnouncerSolutionInfo) are None."""
        return self.args.nullp()


@dataclass
class AnnouncerAnnounceInfo(AnnouncerSolutionInfo):
    pass


def get_announcer_solution_info(coin_spend: CoinSpend) -> AnnouncerSolutionInfo:
    announcer_info = get_price_announcer_info(coin_spend, via_puzzle=True)
    solution = Program.from_serialized(coin_spend.solution)
    (
        lineage_proof,
        inner_puzzle,
        operation,
        solution_or_conditions,
    ) = to_tuple(solution, 4)
    raw_conditions: Program
    if not inner_puzzle.nullp():
        # non-announce spend
        raw_conditions = inner_puzzle.run(solution_or_conditions)
    else:
        # announce spend
        raw_conditions = solution_or_conditions
    create_coin_body, input_conditions = filter_and_extract_unique_create_coin(list(raw_conditions.as_iter()))
    operation_hash = operation.get_tree_hash()
    (
        new_puzzle_hash,
        new_deposit,
        op,
        rest_of_condition,
    ) = to_tuple(create_coin_body, 4, ["bytes32_or_none", "uint64", None, None])
    (
        signed_operation_hash,
        args,
    ) = to_tuple(op, 2)
    if not rest_of_condition.nullp():
        raise SpendError("Announcer create coin input condition has excess memo fields")
    if operation_hash != signed_operation_hash:
        raise SpendError(
            f"In Announcer spend, hash of operation must match signed operation hash ({operation_hash.hex()} != {signed_operation_hash})"
        )
    if operation_hash == PriceAnnouncerOperations.ANNOUNCE.value.get_tree_hash():
        if not inner_puzzle.nullp():
            raise SpendError("Announcer announce operation must have inner puzzle solution arg equal to nil")
    else:
        # non-announce operation
        if operation_hash not in PriceAnnouncerOperations.hashes():
            raise SpendError(f"Non-announce Announcer operation is invalid. Operation hash: {operation_hash.hex()}")
        if inner_puzzle.nullp():
            raise SpendError(
                f"Non-announce Announcer operation must have non-nil inner puzzle solution arg, got {inner_puzzle}"
            )
    if inner_puzzle.get_tree_hash() == announcer_info.inner_puzzle_hash:
        # owner spend
        inner_puzzle_hash = announcer_info.inner_puzzle_hash
    else:
        # keeper spend
        inner_puzzle_hash = None
    if lineage_proof.nullp():
        # eve lineage proof
        if not all(
            [
                announcer_info.prev_announce == False,
                announcer_info.approved == False,
                announcer_info.prev_deposit == 0,
                announcer_info.deposit == 0,
                announcer_info.value_ttl == 0,
                announcer_info.value == 0,
                announcer_info.min_deposit == 0,
                announcer_info.claim_counter == 0,
                announcer_info.cooldown_start == 0,
                announcer_info.penalizable_at == 0,
                announcer_info.timestamp_expires == 0,
            ]
        ):
            raise SpendError("Eve lineage proof requires announcer to be in eve state")
        if announcer_info.launcher_id != coin_spend.coin.parent_coin_info:
            raise SpendError(
                "Either trying to spend non-eve announcer with eve lineage proof, or eve announcer has wrong LAUNCHER_ID curried"
            )
    else:
        # non-eve lineage proof
        (
            parent_parent_id,
            parent_curried_args_hash,
        ) = to_tuple(lineage_proof, 2, ["bytes32", "bytes32"])
        from chia_rs import Coin
        parent_coin_id = Coin(
            parent_parent_id,
            tree_hash_of_apply(ATOM_ANNOUNCER_MOD_HASH, parent_curried_args_hash),
            announcer_info.prev_deposit,
        ).name()
        if parent_coin_id != coin_spend.coin.parent_coin_info:
            raise SpendError("Invalid non-eve lineage proof for announcer provided")
    # now check operations
    if operation_hash == PriceAnnouncerOperations.MUTATE.value.get_tree_hash():
        # mutate
        (
            current_timestamp,
            new_value,
        ) = to_list(args, 2, ["int", None])
        if new_puzzle_hash is not None:
            raise SpendError(f"Cannot mutate announcer if new_puzzle_hash is set ({new_puzzle_hash})")
        if new_value.listp():
            raise SpendError(f"Cannot mutate announcer with a non-atom value ({new_value})")
        if len(new_value.as_atom()) >= 1024:
            raise SpendError(f"Cannot mutate announcer with a value >= 1024 bytes ({len(new_value.as_atom())} bytes)")
        if not announcer_info.launcher_id:
            raise SpendError("Cannot mutate announcer if LAUNCHER_ID is not set")
        if not inner_puzzle_hash:
            raise SpendError("Only owner can mutate Announcer")
        if new_deposit < announcer_info.min_deposit:
            raise SpendError(
                f"Cannot mutate announcer so that new deposit is less than MIN_DEPOSIT ({new_deposit} < {announcer_info.min_deposit})"
            )
        return AnnouncerMutateInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution_or_conditions=solution_or_conditions,
            input_conditions=input_conditions,
            new_puzzle_hash=new_puzzle_hash,
            new_deposit=new_deposit,
            operation=operation,
            signed_operation_hash=signed_operation_hash,
            args=args,
            rest_of_condition=rest_of_condition,
            inner_puzzle_hash=inner_puzzle_hash,
            # args
            current_timestamp=current_timestamp,
            new_value=new_value,
        )
    elif operation_hash == PriceAnnouncerOperations.GOVERN.value.get_tree_hash():
        # govern
        if new_puzzle_hash is not None:
            raise SpendError(f"Cannot govern announcer if new_puzzle_hash is set ({new_puzzle_hash})")
        (
            statutes_inner_puzzle_hash,
            input_toggle_activation,
            statute_max_disapproval_penalty_factor_or_min_deposit,  # max_disapproval_penalty_factor_or_min_deposit in puzzle. penalty factor when disapproving, min deposit when approving
        ) = to_list(args, 3, ["bytes32", "int", "int"])
        if input_toggle_activation not in [0, 1]:
            raise SpendError(
                f"When governing announcer, input_toggle_activation must be 0 or 1. Cannot be {input_toggle_activation}"
            )
        toggle_activation = bool(input_toggle_activation)
        if not (
            (not toggle_activation and announcer_info.approved) or (toggle_activation and not announcer_info.approved)
        ):
            if not toggle_activation:
                raise SpendError("Governance can only deactivate an approved announcer")
            if toggle_activation:
                raise SpendError("Governance can only approve a non-approved announcer")
        if not toggle_activation:
            # deactivating
            if new_deposit > announcer_info.deposit:
                raise SpendError(
                    f"Governance cannot increase announcer deposit when deactivating ({new_deposit} > {announcer_info.deposit})"
                )
            min_penalized_deposit = announcer_info.deposit - (
                (
                    (announcer_info.deposit * PRECISION * statute_max_disapproval_penalty_factor_or_min_deposit)
                    // PRECISION
                )
                // PRECISION_BPS
            )
            if new_deposit < min_penalized_deposit:
                raise SpendError(
                    f"Governance cannot reduce deposit to below min penalized deposit ({new_deposit} < {min_penalized_deposit}). {statute_max_disapproval_penalty_factor_or_min_deposit=}"
                )
        else:
            # approving
            if new_deposit != announcer_info.deposit:
                raise SpendError(
                    f"Governance cannot change deposit when approving announcer ({new_deposit} != {announcer_info.deposit})"
                )
            if new_deposit < announcer_info.min_deposit:
                raise SpendError(
                    f"Governance cannot approve an announcer whose deposit is less than MIN_DEPOSIT ({new_deposit} < {announcer_info.min_deposit})"
                )
            if announcer_info.min_deposit < statute_max_disapproval_penalty_factor_or_min_deposit:
                raise SpendError(
                    f"Governance cannot approve an announcer whose MIN_DEPOSIT is less than Statute ANNOUNCER_MINIMUM_DEPOSIT_MOJOS ({announcer_info.min_deposit} < {statute_max_disapproval_penalty_factor_or_min_deposit})"
                )
        return AnnouncerGovernInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution_or_conditions=solution_or_conditions,
            input_conditions=input_conditions,
            new_puzzle_hash=new_puzzle_hash,
            new_deposit=new_deposit,
            operation=operation,
            signed_operation_hash=signed_operation_hash,
            args=args,
            rest_of_condition=rest_of_condition,
            inner_puzzle_hash=inner_puzzle_hash,
            # args
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            input_toggle_activation=toggle_activation,
            statute_max_disapproval_penalty_factor_or_min_deposit=statute_max_disapproval_penalty_factor_or_min_deposit,
        )
    elif operation_hash == PriceAnnouncerOperations.PENALIZE.value.get_tree_hash():
        # penalize
        if new_puzzle_hash is not None:
            raise SpendError(f"Cannot penalize announcer if new_puzzle_hash is set ({new_puzzle_hash})")
        (
            statute_penalty_per_interval,  # penalty_per_interval_bps in puzzle
            statutes_inner_puzzle_hash,
            statute_penalty_interval_in_minutes,  # penalty_interval_in_minutes in puzzle
            statute_min_deposit,  # min_deposit in puzzle
            statute_max_value_ttl,  # max_value_ttl in puzzle
            current_timestamp,
        ) = to_list(args, 6, ["int", "bytes32", "int", "int", "int", "uint64"])
        penalized_deposit = (announcer_info.deposit * (PRECISION_BPS - statute_penalty_per_interval)) // PRECISION_BPS
        if current_timestamp <= announcer_info.penalizable_at:
            raise SpendError(
                f"Cannot penalize an announcer at or before PENALIZABLE_AT ({current_timestamp} <= {announcer_info.penalizable_at})"
            )
        if not (
            current_timestamp > announcer_info.timestamp_expires
            or announcer_info.min_deposit > announcer_info.deposit
            or statute_min_deposit > announcer_info.min_deposit
            or announcer_info.value_ttl > statute_max_value_ttl
        ):
            raise SpendError("Cannot penalize announcer that is not penalizable")
        if not announcer_info.approved:
            raise SpendError("Cannot penalize announcer that is not approved")
        if not announcer_info.launcher_id:
            raise SpendError("Cannot penalize eve announcer")
        if not announcer_info.deposit > penalized_deposit:
            raise SpendError("Cannot penalize announcer if penalty would not reduce deposit")
        if penalized_deposit < 0:
            raise SpendError(
                f"Canot penalize announcer for more than its deposit. ({announcer_info.deposit - penalized_deposit} > {announcer_info.deposit})"
            )
        return AnnouncerPenalizeInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution_or_conditions=solution_or_conditions,
            input_conditions=input_conditions,
            new_puzzle_hash=new_puzzle_hash,
            new_deposit=new_deposit,
            operation=operation,
            signed_operation_hash=signed_operation_hash,
            args=args,
            rest_of_condition=rest_of_condition,
            inner_puzzle_hash=inner_puzzle_hash,
            # args
            statute_penalty_per_interval=statute_penalty_per_interval,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            statute_penalty_interval_in_minutes=statute_penalty_interval_in_minutes,
            statute_min_deposit=statute_min_deposit,  # statute min deposit
            statute_max_value_ttl=statute_max_value_ttl,
            current_timestamp=current_timestamp,
        )
    elif operation_hash == PriceAnnouncerOperations.REGISTER.value.get_tree_hash():
        # register
        if new_puzzle_hash is not None:
            raise SpendError(f"Cannot register announcer if new_puzzle_hash is set ({new_puzzle_hash})")
        (
            registry_mod_hash,
            registry_args_hash,
            registry_claim_counter,
            target_puzzle_hash,
            statutes_inner_puzzle_hash,
            statute_min_deposit,  # min_deposit in puzzle
        ) = to_list(args, 6, ["bytes32", "bytes32", "uint64", "bytes32", "bytes32", "int"])
        if inner_puzzle.nullp():
            raise SpendError("Cannot register announcer without running inner puzzle. Must be owner")
        if not announcer_info.approved:
            raise SpendError("Cannot register announcer that is not approved")
        if registry_claim_counter <= announcer_info.claim_counter:
            raise SpendError(
                f"Cannot register announcer twice in same Rewards period ({registry_claim_counter} <= {announcer_info.claim_counter})"
            )
        if new_deposit < announcer_info.min_deposit:
            raise SpendError(
                f"Cannot register an announcer that is left with deposit less than MIN_DEPOSIT ({new_deposit} < {announcer_info.min_deposit})"
            )
        if announcer_info.min_deposit < statute_min_deposit:
            raise SpendError(
                f"Cannot register an announcer with MIN_DEPOSIT less than Statute ANNOUNCER_MIN_DEPOSIT ({announcer_info.min_deposit} <= {statute_min_deposit})"
            )
        return AnnouncerRegisterInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution_or_conditions=solution_or_conditions,
            input_conditions=input_conditions,
            new_puzzle_hash=new_puzzle_hash,
            new_deposit=new_deposit,
            operation=operation,
            signed_operation_hash=signed_operation_hash,
            args=args,
            rest_of_condition=rest_of_condition,
            inner_puzzle_hash=inner_puzzle_hash,
            # args
            registry_mod_hash=registry_mod_hash,
            registry_args_hash=registry_args_hash,
            registry_claim_counter=registry_claim_counter,
            target_puzzle_hash=target_puzzle_hash,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            statute_min_deposit=statute_min_deposit,
        )
    elif operation_hash == PriceAnnouncerOperations.ANNOUNCE.value.get_tree_hash():
        # announce
        if new_deposit:
            raise SpendError(f"Cannot announce announcer if new_deposit is set ({new_deposit})")
        if new_puzzle_hash is not None:
            raise SpendError(f"Cannot announce announcer if new_puzzle_hash is set ({new_puzzle_hash})")
        if inner_puzzle_hash is not None:
            raise SpendError(f"Cannot announce announcer if inner_puzzle_hash is set ({inner_puzzle_hash})")
        if input_conditions:
            raise SpendError(f"Cannot announce announcer if input_conditions are provided ({input_conditions})")
        if not args.nullp():
            raise SpendError(f"Cannot announce announcer if args are provided ({args})")
        if not rest_of_condition.nullp():
            raise SpendError(f"Cannot announce announcer if rest_of_condition is set ({rest_of_condition})")
        return AnnouncerAnnounceInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution_or_conditions=solution_or_conditions,
            input_conditions=input_conditions,
            new_puzzle_hash=new_puzzle_hash,
            new_deposit=new_deposit,
            operation=operation,
            signed_operation_hash=signed_operation_hash,
            args=args,
            rest_of_condition=rest_of_condition,
            inner_puzzle_hash=inner_puzzle_hash,
        )
    elif operation_hash == PriceAnnouncerOperations.CONFIGURE.value.get_tree_hash():
        # configure
        if inner_puzzle.nullp():
            raise SpendError("Cannot configure announcer without running inner puzzle. Must be owner")
        if args.nullp():
            # exiting announcer
            if announcer_info.approved:
                raise SpendError("Cannot exit an approved announcer")
            return AnnouncerConfigureInfo(
                lineage_proof=lineage_proof,
                inner_puzzle=inner_puzzle,
                solution_or_conditions=solution_or_conditions,
                input_conditions=input_conditions,
                new_puzzle_hash=new_puzzle_hash,
                new_deposit=new_deposit,
                operation=operation,
                signed_operation_hash=signed_operation_hash,
                args=args,
                rest_of_condition=rest_of_condition,
                inner_puzzle_hash=inner_puzzle_hash,
                # args
                current_timestamp=None,
                statutes_inner_puzzle_hash=None,
                input_toggle_activation=None,
                deactivation_cooldown_interval=None,  # deactivation_cooldown_interval in puzzle
                statute_min_deposit=None,  # min_deposit in puzzle
                new_value_ttl=None,
                statute_max_value_ttl=None,  # max_value_ttl in puzzle
                new_value=None,
                new_min_deposit=None,
            )
        else:
            # configuring announcer
            (
                current_timestamp,
                statutes_inner_puzzle_hash,
                input_toggle_activation,
                statute_deactivation_cooldown_interval,  # deactivation_colldown_interval in puzzle
                statute_min_deposit,  # min_deposit in puzzle
                new_value_ttl,
                statute_max_value_ttl,  # max_value_ttl in puzzle
                new_value,
                new_min_deposit,
            ) = to_list(args, 9, ["uint64", "bytes32", "int", "int", "int", "uint64", "int", "uint64", "uint64"])
            toggle_activation = bool(input_toggle_activation)
            if toggle_activation and not announcer_info.approved:
                raise SpendError("Only governance can approve an announcer")
            if not toggle_activation and announcer_info.approved:
                if (
                    announcer_info.cooldown_start > 0
                    and current_timestamp - announcer_info.cooldown_start > statute_deactivation_cooldown_interval
                ):
                    new_approved = False
                    new_cooldown_start = 0
                elif announcer_info.cooldown_start == 0:
                    new_approved = True
                    new_cooldown_start = current_timestamp
                else:
                    new_approved = True
                    new_cooldown_start = announcer_info.cooldown_start
            elif announcer_info.cooldown_start == 0:
                new_approved = announcer_info.approved
                new_cooldown_start = announcer_info.cooldown_start
            else:
                new_appoved = announcer_info.approved
                new_cooldown_start = 0
            if new_deposit < new_min_deposit:
                raise SpendError(
                    f"Cannot configure an announcer that is left with deposit less than MIN_DEPOSIT ({new_deposit} < {new_min_deposit})"
                )
            if new_value_ttl > statute_max_value_ttl:
                raise SpendError(
                    f"Cannot configure an announcer that is left with VALUE_TTL greater than Statute ANNOUNCER_MAXIMUM_VALUE_TTL ({new_value_ttl} > {statute_max_value_ttl})"
                )
            if new_value_ttl < 0:
                raise SpendError(f"Cannot configure an announcer to have negative VALUE_TTL ({new_value_ttl})")
            if new_approved:
                if new_min_deposit < statute_min_deposit:
                    raise SpendError(
                        f"Cannot configure an approved announcer to be left with MIN_DEPOSIT less than statute ANNOUNCER_MINIMUM_DEPOSIT_MOJOS ({new_min_deposit} < {statute_min_deposit})"
                    )
            else:
                if new_min_deposit < 0:
                    raise SpendError(
                        f"Cannot configure a deactivated announcer to have negative MIN_DEPOSIT ({new_min_deposit})"
                    )
            return AnnouncerConfigureInfo(
                lineage_proof=lineage_proof,
                inner_puzzle=inner_puzzle,
                solution_or_conditions=solution_or_conditions,
                input_conditions=input_conditions,
                new_puzzle_hash=new_puzzle_hash,
                new_deposit=new_deposit,
                operation=operation,
                signed_operation_hash=signed_operation_hash,
                args=args,
                rest_of_condition=rest_of_condition,
                inner_puzzle_hash=inner_puzzle_hash,
                # args
                current_timestamp=current_timestamp,
                statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
                input_toggle_activation=input_toggle_activation,
                deactivation_cooldown_interval=statute_deactivation_cooldown_interval,
                statute_min_deposit=statute_min_deposit,
                new_value_ttl=new_value_ttl,
                statute_max_value_ttl=statute_max_value_ttl,
                new_value=new_value,
                new_min_deposit=new_min_deposit,
            )
    else:
        raise SpendError(f"Unkown announcer operation. Operation hash: {operation_hash}")
