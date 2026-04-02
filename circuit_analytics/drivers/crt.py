from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia.types.condition_opcodes import ConditionOpcode
from chia_rs import Coin
from chia_rs.sized_bytes import bytes32

from circuit_analytics.drivers import SOLUTION_PREFIX
from circuit_analytics.drivers.condition_filtering import is_valid_ann_cond, is_valid_msg_cond
from circuit_analytics.drivers.protocol_math import PRECISION
from circuit_analytics.drivers.statutes import StatutePosition, statute_value_to_str_or_int
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    CAT_MOD,
    CAT_MOD_HASH,
    GOVERNANCE_MOD,
    GOVERNANCE_MOD_HASH,
    LAUNCH_GOVERNANCE_MOD,
    PROGRAM_GOVERNANCE_IMPLEMENT_BILL_MOD,
    PROGRAM_GOVERNANCE_PROPOSE_BILL_MOD,
    PROGRAM_GOVERNANCE_RESET_BILL_MOD,
    PROGRAM_GOVERNANCE_VETO_ANNOUNCEMENT_MOD,
    PROGRAM_GOVERNANCE_VETO_BILL_MOD,
)
from circuit_analytics.utils import to_list, to_tuple, to_type

log = logging.getLogger(__name__)


INTERVAL_LOWER_HARD_LIMIT = 1800  # 30 mins in seconds
INTERVAL_UPPER_HARD_LIMIT = 604800  # 7 days in seconds
MAINTAINER_MIN_VOTE_AMOUNT = 350000000000  # min 350bn mCRT in a single vote to enable override
MAINTAINER_END_PERIOD = 1798761600  # 2027-01-01


class GovernanceOperations(Enum):
    VETO = PROGRAM_GOVERNANCE_VETO_BILL_MOD
    RESET = PROGRAM_GOVERNANCE_RESET_BILL_MOD
    PROPOSE = PROGRAM_GOVERNANCE_PROPOSE_BILL_MOD
    IMPLEMENT = PROGRAM_GOVERNANCE_IMPLEMENT_BILL_MOD
    VETO_ANNOUNCE = PROGRAM_GOVERNANCE_VETO_ANNOUNCEMENT_MOD
    TRANSFER = 0
    EXIT = 1

    @classmethod
    def hashes(cls) -> list[bytes32]:
        """Returns list of all operation mod hashes"""
        return [op.value.get_tree_hash() for op in cls if isinstance(op.value, Program)]


@dataclass
class GovernanceSolutionInfo:
    lineage_proof: Program
    inner_puzzle: Program
    inner_solution: Program
    inner_conditions: Program
    statutes_inner_puzzle_hash: bytes32 | Program  # not bytes32 because some ops don't use it (ie can be anything)
    operation: Program  # bill_operation in puzzle
    args: Program
    raw_puzzle_hash: Program  # puzzle hash of unique CREATE_COIN input condition
    new_puzzle_hash: bytes32  # new (inner) puzzle hash (of governance or exit coin)
    amount: int  # current and new amount. new_coin_amount in puzzle


@dataclass
class GovernanceVetoInfo(GovernanceSolutionInfo):
    parent_veto_id: bytes32
    veto_amount: int
    veto_inner_puzzle_hash: bytes32
    veto_bill_hash: bytes32
    current_timestamp: int


@dataclass
class GovernanceResetInfo(GovernanceSolutionInfo):
    pass


@dataclass
class GovernanceProposeInfo(GovernanceSolutionInfo):
    bill_proposal_fee_mojos: int
    # new_bill
    statute_index: int
    new_statute_value: Program | bytes32 | int  # Program if custom conditions, bytes32 if oracle launcher ID, int o/w
    new_threshold_amount_to_propose: int
    new_veto_interval: int
    new_implementation_delay: int
    new_max_delta: int
    # new_bill end
    # current_statute
    statute_value: Program
    threshold_amount_to_propose: int
    veto_interval: int
    implementation_delay: int
    max_delta: int
    # current_statute end
    current_timestamp: int
    implementation_interval: int

    def statute(self) -> StatutePosition | bool:
        """Returns Statute that is being changed, True is custom condition(s), False if invalid statute_index."""
        if self.statute_index in range(StatutePosition.max_statutes_idx() + 1):
            return StatutePosition(self.statute_index)
        elif self.statute_index == -1:
            return True
        else:
            return False

    def custom_conditions(self) -> list[Program] | None:
        """If bill proposes to pass custom condition(s), return them as a list, or else None."""
        if self.statute_index == -1:
            return list(self.new_statue_value.as_iter())
        return None


@dataclass
class GovernanceImplementInfo(GovernanceSolutionInfo):
    current_timestamp: int


@dataclass
class GovernanceVetoAnnounceInfo(GovernanceSolutionInfo):
    target_bill_hash: bytes32
    target_coin_id: bytes32


@dataclass
class GovernanceTransferInfo(GovernanceSolutionInfo):
    pass


@dataclass
class GovernanceExitInfo(GovernanceSolutionInfo):
    pass


def find_bill_condition(conditions: Program) -> tuple[bytes32 | None, Program, Program, int]:
    create_coin_amount = -1
    bill_operation: Program = None
    for cond in conditions.as_iter():
        if cond.first().atom == ConditionOpcode.REMARK and cond.rest().first().atom == SOLUTION_PREFIX:
            if not bill_operation:
                bill_operation = cond.rest().rest().first()
            continue  # ignore all but first solution REMARK condition
        elif cond.first().atom == ConditionOpcode.CREATE_COIN and create_coin_amount == -1:
            create_coin_amount = cond.at("rrf").as_int()

    if bill_operation:
        bill_operation_tuple = to_tuple(bill_operation, 3, [None, None, None])
    else:
        bill_operation_tuple = tuple([None, Program.to(0), Program.to(0)])

    return *bill_operation_tuple, create_coin_amount


def validate_veto_conditions(inner_puzzle_hash: bytes32, veto_conditions: list[Program]) -> bool:
    for cond in veto_conditions:
        if cond.first().atom == ConditionOpcode.CREATE_COIN:
            if cond.at("rf").atom != inner_puzzle_hash:
                raise SpendError("Governance keeper operation must not change inner puzzle hash")
    return True


def filter_inner_conditions(conditions: Program) -> tuple[bytes32 | None, bool]:
    """Returns puzzle hash of unique CREATE_COIN condition and None/new (inner) puzzle hash if it is/is not an exit.

    This function checks for all possible errors that could arise in filter-conditions function defined in governance.clsp.
    """

    found_create_coin = False

    for cond in conditions.as_iter():
        if cond.first().atom == ConditionOpcode.CREATE_COIN:
            if found_create_coin:
                raise ValueError("Encountered more than one CREATE_COIN condition in governance coin spend")
            if not cond.at("rrf").as_int() >= 0:
                raise ValueError("CREATE_COIN condition in governance coin spend must have non-negative amount")
            found_create_coin = True
            raw_puzzle_hash = cond.at("rf")
            if raw_puzzle_hash.list_len() > 0:
                raise SpendError(
                    f"CREATE_COIN input condition of governance spend must have an atom in puzzle hash position {raw_puzzle_hash}"
                )
            new_puzzle_hash = (
                bytes32(raw_puzzle_hash.atom) if len(raw_puzzle_hash.atom) == 32 else None
            )  # None means governance layer is being exited
        elif cond.first().atom in [ConditionOpcode.SEND_MESSAGE, ConditionOpcode.RECEIVE_MESSAGE]:
            if not is_valid_msg_cond(cond.rest()):
                raise ValueError("Encountered invalid MESSAGE condition in governance coin spend")
        elif cond.first().atom in [
            ConditionOpcode.CREATE_COIN_ANNOUNCEMENT,
            ConditionOpcode.CREATE_PUZZLE_ANNOUNCEMENT,
        ]:
            if not is_valid_ann_cond(cond.rest()):
                raise ValueError("Encountered invalid ANNOUNCEMENT condition in governance coin spend")

    if not found_create_coin:
        raise ValueError("No CREATE_COIN input condition encountered in governance coin spend")

    return raw_puzzle_hash, new_puzzle_hash


def get_governance_solution_info(
    coin: Coin,
    cat_inner_puzzle: Program,
    cat_inner_solution: Program,
    crt_tail_hash: bytes32 = None,
    statutes_struct: Program = None,
) -> GovernanceSolutionInfo | None:
    """Takes governance puzzle and solution (CAT layer inner puzzle and inner solution) and returns info."""
    inner_mod, inner_args = uncurry(cat_inner_puzzle)
    if inner_mod.get_tree_hash() != GOVERNANCE_MOD_HASH:
        return None
    # inner_args: [GOVERNANCE_MOD_HASH, crt_tail_hash, statutes_struct, inner_puzzle_hash, bill]
    if crt_tail_hash is None:
        crt_tail_hash = bytes32(inner_args.at("rf").atom)
    if statutes_struct is None:
        statutes_struct = inner_args.at("rrf")
    inner_puzzle_hash = bytes32(inner_args.at("rrrf").atom)
    bill = inner_args.at("rrrrf")
    (lineage_proof, inner_puzzle, inner_solution, bill_operation) = to_list(cat_inner_solution, 4)
    inner_conditions = inner_puzzle.run(inner_solution)
    (
        statutes_inner_puzzle_hash,  # can be anything if operation does not assert statutes (reset, veto announcement, transfer)
        signed_bill_operation_hash,
        args,
        new_coin_amount,
    ) = find_bill_condition(inner_conditions)
    to_type(Program.to(new_coin_amount), "uint64", "governance coin CREATE_COIN amount")
    bill_operation_hash = bill_operation.get_tree_hash() if not bill_operation.nullp() else Program.to(None)
    if signed_bill_operation_hash != bill_operation_hash:
        raise SpendError("In governance spend solution, hash of bill operation must match signed bill operation hash")
    if signed_bill_operation_hash != Program.to(None):
        # operation other than transfer or exit
        if not (
            (inner_puzzle.get_tree_hash() == inner_puzzle_hash and bill_operation_hash in GovernanceOperations.hashes())
            or (
                bill_operation_hash == GovernanceOperations.VETO.value.get_tree_hash()
                and validate_veto_conditions(inner_puzzle_hash, inner_conditions)
            )
        ):
            if inner_puzzle.get_tree_hash() == inner_puzzle_hash:
                raise SpendError(
                    f"Governance owner spend is using unknown operation. Operation hash: {bill_operation_hash}"
                )
            elif bill_operation_hash == GovernanceOperations.VETO.value.get_tree_hash():
                raise SpendError("Failed to validate veto conditions of governance spend")
            else:
                raise SpendError(
                    "Invalid governance operation. Either operation is misspecified or keeper is trying to perform an owner operation"
                )
    else:
        # this is a transfer or exit
        if not new_coin_amount >= 0:
            raise SpendError(f"New amount of governance coin must be non-negative, got {new_coin_amount}")
        if inner_puzzle.get_tree_hash() != inner_puzzle_hash:
            raise SpendError(
                f"Governance transfer and exit operations are owner spends. Incorrect inner puzzle was revealed "
                f"({inner_puzzle.get_tree_hash().hex()} = {inner_puzzle_hash.hex()})"
            )
        if not bill.nullp():
            raise SpendError("Governance transfer and exit operations are only possible when no bill is set")
    raw_puzzle_hash, new_puzzle_hash = filter_inner_conditions(inner_conditions)
    if new_puzzle_hash is None and not bill.nullp():
        raise ValueError("Cannot exit governance layer if BILL is set")
    if new_puzzle_hash is None:
        # we are exiting
        new_puzzle_hash = inner_puzzle_hash
        assert new_puzzle_hash
        exit = True
    else:
        exit = False
    if not new_coin_amount == coin.amount:
        raise SpendError(
            f"New governance coin amount does not match current amount ({new_coin_amount} != {new_coin_amount})"
        )
    if lineage_proof.rest().nullp():
        # eve
        parent_parent_id = bytes32(lineage_proof.first().atom)
        parent_coin_id = Coin(
            parent_parent_id,
            CAT_MOD.curry(
                CAT_MOD_HASH,
                crt_tail_hash,
                LAUNCH_GOVERNANCE_MOD.curry(
                    GOVERNANCE_MOD_HASH,
                    CAT_MOD_HASH,
                    crt_tail_hash,
                    statutes_struct.get_tree_hash(),
                ),
            ).get_tree_hash(),
            new_coin_amount,
        ).name()
        if parent_coin_id != coin.parent_coin_info:
            raise SpendError("Invalid eve lineage proof for governance coin provided")
    else:
        # non-eve
        (
            parent_parent_id,
            parent_bill_hash,
        ) = to_list(lineage_proof, 2, ["bytes32", "bytes32"])
        # get_tree_hash_precalc fails when parent_bill_hash is the hash of nil (Program.to(0))
        # because it would curry a bytes32 atom but precalc expects the program's hash.
        # In that case, fall back to currying Program.to(0) and computing the hash directly.
        _nil_prog_hash = Program.to(0).get_tree_hash()
        if parent_bill_hash == _nil_prog_hash:
            gov_inner_puzzle_hash = GOVERNANCE_MOD.curry(
                GOVERNANCE_MOD_HASH,
                crt_tail_hash,
                statutes_struct,
                inner_puzzle_hash,
                Program.to(0),
            ).get_tree_hash()
        else:
            gov_inner_puzzle_hash = GOVERNANCE_MOD.curry(
                GOVERNANCE_MOD_HASH,
                crt_tail_hash,
                statutes_struct,
                inner_puzzle_hash,
                parent_bill_hash,
            ).get_tree_hash_precalc(parent_bill_hash)
        parent_ph = CAT_MOD.curry(
            CAT_MOD_HASH,
            crt_tail_hash,
            gov_inner_puzzle_hash,
        ).get_tree_hash_precalc(gov_inner_puzzle_hash)
        parent_coin_id = Coin(
            parent_parent_id,
            parent_ph,
            new_coin_amount,  # coin amount of governance coin may not change
        ).name()
        if parent_coin_id != coin.parent_coin_info:
            raise SpendError("Invalid non-eve lineage proof for governance coin provided")
    # operations
    if not bill_operation.nullp():
        if bill_operation_hash == GovernanceOperations.VETO.value.get_tree_hash():
            if bill.nullp():
                raise SpendError("Cannot veto a coin that has no bill set")
            veto_period_expiry = bill.first().first().as_int()
            (
                parent_veto_id,
                veto_amount,
                veto_inner_puzzle_hash,
                veto_bill_hash,
                current_timestamp,
            ) = to_list(args, 5, ["bytes32", "uint64", "bytes32", "bytes32", "uint64"])
            if veto_amount <= new_coin_amount:
                raise SpendError(
                    f"Failed to veto bill due to insufficient amount of vetoing coin ({veto_amount} <= {new_coin_amount})"
                )
            if veto_period_expiry <= current_timestamp:
                raise SpendError(
                    f"Failed to veto bill as veto period expired ({veto_period_expiry} <= {current_timestamp})"
                )
            return GovernanceVetoInfo(
                lineage_proof=lineage_proof,
                inner_puzzle=inner_puzzle,
                inner_solution=inner_solution,
                inner_conditions=inner_conditions,
                statutes_inner_puzzle_hash=bytes32(statutes_inner_puzzle_hash.atom),
                operation=bill_operation,
                args=args,
                raw_puzzle_hash=raw_puzzle_hash,
                new_puzzle_hash=new_puzzle_hash,
                amount=new_coin_amount,
                # args
                parent_veto_id=parent_veto_id,
                veto_amount=veto_amount,
                veto_inner_puzzle_hash=veto_inner_puzzle_hash,
                veto_bill_hash=veto_bill_hash,
                current_timestamp=current_timestamp,
            )
        elif bill_operation_hash == GovernanceOperations.RESET.value.get_tree_hash():
            if bill.nullp():
                raise SpendError("Can only reset bill if a bill is set")
            return GovernanceResetInfo(
                lineage_proof=lineage_proof,
                inner_puzzle=inner_puzzle,
                inner_solution=inner_solution,
                inner_conditions=inner_conditions,
                statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
                operation=bill_operation,
                args=args,
                raw_puzzle_hash=raw_puzzle_hash,
                new_puzzle_hash=new_puzzle_hash,
                amount=new_coin_amount,
                # no args
            )
        elif bill_operation_hash == GovernanceOperations.PROPOSE.value.get_tree_hash():
            if not bill.nullp():
                raise SpendError("Can only propose bill if no bill set")
            (
                bill_proposal_fee_mojos,
                new_bill,
                current_statute,
                current_timestamp,
                implementation_interval,
            ) = to_list(args, 5, ["uint64", None, None, "uint64", "uint64"])
            (
                statute_index,
                new_statute_value,
                new_threshold_amount_to_propose,
                new_veto_interval,
                new_implementation_delay,
                new_max_delta,
            ) = to_list(new_bill, 6, ["int64", None, "uint64", "uint64", "uint64", "uint64"])
            (
                statute_value,
                threshold_amount_to_propose,
                veto_interval,
                implementation_delay,
                max_delta,
            ) = to_list(current_statute, 5, [None, "int", "int", "int", "int"])
            if not bill_proposal_fee_mojos >= 0:
                raise SpendError(f"Governance bill proposal fee must be non-negative, got {bill_proposal_fee_mojos}")
            if not new_threshold_amount_to_propose >= 1:
                raise SpendError(
                    f"Cannot propose a proposal threshold less than 1 mCRT ({new_threshold_amount_to_propose} >= 1)"
                )
            if not new_veto_interval >= 1:
                raise SpendError(f"Cannot propose a veto interval less than 1 second ({new_veto_interval} >= 1)")
            if not new_implementation_delay >= 1:
                raise SpendError(
                    f"Cannot propose an implementation delay less than 1 second ({new_implementation_delay} >= 1)"
                )
            if not new_max_delta >= 0:
                raise SpendError(f"Cannot propose a negative max delta ({new_max_delta} >= 0)")
            if not current_timestamp >= 0:
                raise SpendError(
                    f"Governance propose bill operation must be passed non-negative current timestamp ({current_timestamp} >= 0)"
                )
            if not implementation_interval >= 0:
                raise SpendError(
                    f"Governance propose bill operation must be passed non-negative implementation interval ({implementation_interval} >= 0)"
                )
            # validate Statute value
            if statute_index == -1:
                # custom conditions
                if not new_statute_value.listp():
                    raise SpendError(f"Proposed custom conditions must be a list, got {new_statute_value}")
            elif statute_index == StatutePosition.ORACLE_LAUNCHER_ID.value:
                # oracle launcher ID
                new_statute_value = to_type(new_statute_value, "bytes32", "new value for Statute ORACLE_LAUNCHER_ID")
            else:
                # statutes of index >= 1
                new_statute_value = to_type(new_statute_value, "int", f"new value for Statute at index {statute_index}")

                if (
                    statute_index == StatutePosition.GOVERNANCE_IMPLEMENTATION_INTERVAL.value
                    and not new_statute_value > INTERVAL_LOWER_HARD_LIMIT
                ):
                    raise SpendError(
                        f"Proposed value for Statute GOVERNANCE_IMPLEMENTATION_INTERVAL must be greater than lower hard limit "
                        f"({new_statute_value} > {INTERVAL_LOWER_HARD_LIMIT})"
                    )
                elif (
                    statute_index in [StatutePosition.STABILITY_FEE_DF.value, StatutePosition.INTEREST_DF.value]
                    and not new_statute_value >= PRECISION
                ):
                    raise SpendError(
                        f"Proposed value for Statute {StatutePosition(statute_index).name} must not be smaller than {PRECISION}, got {new_statute_value.as_int()}"
                    )
                elif (
                    statute_index
                    in [
                        StatutePosition.ORACLE_M_OF_N,
                        StatutePosition.VAULT_LIQUIDATION_RATIO_PCT,
                        StatutePosition.VAULT_AUCTION_STARTING_PRICE_FACTOR_BPS,
                        StatutePosition.VAULT_AUCTION_PRICE_TTL,
                        StatutePosition.VAULT_AUCTION_MINIMUM_PRICE_FACTOR_BPS,
                    ]
                    and not new_statute_value >= 1
                ):
                    raise SpendError(
                        f"Proposed value for Statute {StatutePosition(statute_index).name} must be positive, got {new_statute_value.as_int()}"
                    )
                elif not new_statute_value >= 0:
                    raise SpendError(
                        f"Proposed value for Statute {StatutePosition(statute_index).name} must be non-negative, got {new_statute_value.as_int()}"
                    )
            if not new_veto_interval <= INTERVAL_UPPER_HARD_LIMIT:
                raise SpendError(
                    f"Proposed new veto interval must not exceed upper hard limit ({new_veto_interval} <= {INTERVAL_UPPER_HARD_LIMIT})"
                )
            if not new_veto_interval >= INTERVAL_LOWER_HARD_LIMIT:
                raise SpendError(
                    f"Proposed new veto interval must not be less than lower hard limit ({new_veto_interval} >= {INTERVAL_LOWER_HARD_LIMIT})"
                )
            if not new_implementation_delay <= INTERVAL_UPPER_HARD_LIMIT:
                raise SpendError(
                    f"Proposed new implementation delay must not exceed upper hard limit ({new_implementation_delay} <= {INTERVAL_UPPER_HARD_LIMIT})"
                )
            if new_coin_amount <= threshold_amount_to_propose:
                raise SpendError(
                    f"Cannot propose a bill if governance coin amount is not greater than proposal threshold ({new_coin_amount} <= {threshold_amount_to_propose})"
                )
            if (not (max_delta == 0 or statute_index <= 0)) and abs(new_statute_value - statute_value) >= max_delta:
                raise SpendError(
                    f"Proposed value for Statute {StatutePosition(statute_index).name} must deviate from current value by less than max delta "
                    f"(abs({new_statute_value}-{statute_value}) < {max_delta})"
                )
            return GovernanceProposeInfo(
                lineage_proof=lineage_proof,
                inner_puzzle=inner_puzzle,
                inner_solution=inner_solution,
                inner_conditions=inner_conditions,
                statutes_inner_puzzle_hash=bytes32(statutes_inner_puzzle_hash.atom),
                operation=bill_operation,
                args=args,
                raw_puzzle_hash=raw_puzzle_hash,
                new_puzzle_hash=new_puzzle_hash,
                amount=new_coin_amount,
                # args
                bill_proposal_fee_mojos=bill_proposal_fee_mojos,
                # new_bill
                statute_index=statute_index,
                new_statute_value=new_statute_value,
                new_threshold_amount_to_propose=new_threshold_amount_to_propose,
                new_veto_interval=new_veto_interval,
                new_implementation_delay=new_implementation_delay,
                new_max_delta=new_max_delta,
                # new_bill end
                # current_statute
                statute_value=statute_value,
                threshold_amount_to_propose=threshold_amount_to_propose,
                veto_interval=veto_interval,
                implementation_delay=implementation_delay,
                max_delta=max_delta,
                # current_statute end
                current_timestamp=current_timestamp,
                implementation_interval=implementation_interval,
            )
        elif bill_operation_hash == GovernanceOperations.IMPLEMENT.value.get_tree_hash():
            [current_timestamp] = to_list(args, 1, ["uint"])
            (
                proposal_times,  # (veto_period_expiry implementation_delay_expiry . implementation_interval_expiry)
                new_statute_index,
                new_statute_value,
                new_threshold_amount_to_propose,
                new_veto_seconds,
                new_delay_seconds,
                new_max_delta,
            ) = to_list(bill, 7, [None, "int", None, "int", "int", "int", "int"])
            (
                veto_period_expiry,
                implementation_delay_expiry,
                implementation_interval_expiry,
            ) = to_tuple(proposal_times, 3, ["int", "int", "int"])
            if (
                new_coin_amount <= MAINTAINER_MIN_VOTE_AMOUNT or MAINTAINER_END_PERIOD <= current_timestamp
            ) and current_timestamp <= implementation_delay_expiry:
                raise SpendError(
                    f"If not maintainer, cannot implement bill before implementation delay has passed ({current_timestamp} <= {implementation_delay_expiry})"
                )
            if implementation_interval_expiry <= current_timestamp:
                raise SpendError(
                    f"Cannot implement bill after implementation interval has ended ({implementation_delay_expiry} <= {current_timestamp})"
                )
            return GovernanceImplementInfo(
                lineage_proof=lineage_proof,
                inner_puzzle=inner_puzzle,
                inner_solution=inner_solution,
                inner_conditions=inner_conditions,
                statutes_inner_puzzle_hash=bytes32(statutes_inner_puzzle_hash.atom),
                operation=bill_operation,
                args=args,
                raw_puzzle_hash=raw_puzzle_hash,
                new_puzzle_hash=new_puzzle_hash,
                amount=new_coin_amount,
                # args
                current_timestamp=current_timestamp,
            )
        elif bill_operation_hash == GovernanceOperations.VETO_ANNOUNCE.value.get_tree_hash():
            (
                target_bill_hash,
                target_coin_id,
            ) = to_list(args, 2, ["bytes32", "bytes32"])
            return GovernanceVetoAnnounceInfo(
                lineage_proof=lineage_proof,
                inner_puzzle=inner_puzzle,
                inner_solution=inner_solution,
                inner_conditions=inner_conditions,
                statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
                operation=bill_operation,
                args=args,
                raw_puzzle_hash=raw_puzzle_hash,
                new_puzzle_hash=new_puzzle_hash,
                amount=new_coin_amount,
                # args
                target_bill_hash=target_bill_hash,
                target_coin_id=target_coin_id,
            )
    else:
        if exit:
            # exit
            if inner_solution.nullp():
                raise SpendError("Governance exit operation can only be performed if inner puzzle is revealed")
            if not bill.nullp():
                raise SpendError("Governance exit operation can only be performed if no bill is set")
            return GovernanceExitInfo(
                lineage_proof=lineage_proof,
                inner_puzzle=inner_puzzle,
                inner_solution=inner_solution,
                inner_conditions=inner_conditions,
                statutes_inner_puzzle_hash=bytes32(statutes_inner_puzzle_hash.atom),
                operation=bill_operation,
                args=args,
                raw_puzzle_hash=raw_puzzle_hash,
                new_puzzle_hash=new_puzzle_hash,
                amount=new_coin_amount,
                # args
            )
        else:
            # transfer
            if inner_solution.nullp():
                raise SpendError("Governance transfer operation can only be performed if inner puzzle is revealed")
            if not bill.nullp():
                raise SpendError("Governance transfer operation can only be performed if no bill is set")
            return GovernanceTransferInfo(
                lineage_proof=lineage_proof,
                inner_puzzle=inner_puzzle,
                inner_solution=inner_solution,
                inner_conditions=inner_conditions,
                statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
                operation=bill_operation,
                args=args,
                raw_puzzle_hash=raw_puzzle_hash,
                new_puzzle_hash=new_puzzle_hash,
                amount=new_coin_amount,
                # args
            )


@dataclass
class Statute:
    value: Program
    threshold_amount_to_propose: int
    veto_interval: int
    implementation_delay: int
    max_delta: int

    def to_program(self):
        values = [
            self.value,
            self.threshold_amount_to_propose,
            self.veto_interval,
            self.implementation_delay,
            self.max_delta,
        ]
        return Program.to(values)


class BillStatus(Enum):
    VETOABLE = 0
    IN_IMPLEMENTATION_DELAY = 1
    IMPLEMENTABLE = 2
    LAPSED = 3


@dataclass
class Bill(Statute):
    statute_index: int = None
    proposal_times: Program = None

    def to_program(self):
        values = [
            self.value,
            self.threshold_amount_to_propose,
            self.veto_interval,
            self.implementation_delay,
            self.max_delta,
        ]
        if self.statute_index is not None:
            # prepend statute index to values
            values = [self.statute_index] + values
            if self.proposal_times is not None:
                # prepend proposal times to values
                values = [self.proposal_times] + values
        return Program.to(values)

    def get_status_info(self, current_timestamp: int, human_readable: bool = False) -> Optional[dict]:
        if self.proposal_times is not None and self.proposal_times != Program.to(0):
            veto_interval_expiry = self.proposal_times.at("f").as_int()
            implementation_delay_expiry = self.proposal_times.at("rf").as_int()
            implementation_interval_expiry = self.proposal_times.at("rr").as_int()
            if current_timestamp < veto_interval_expiry:
                status = BillStatus.VETOABLE.name
                status_expires_at = veto_interval_expiry
                status_expires_in = veto_interval_expiry - current_timestamp
            elif current_timestamp < implementation_delay_expiry:
                status = BillStatus.IN_IMPLEMENTATION_DELAY.name
                status_expires_at = implementation_delay_expiry
                status_expires_in = implementation_delay_expiry - current_timestamp
            elif current_timestamp < implementation_interval_expiry:
                status = BillStatus.IMPLEMENTABLE.name
                status_expires_at = implementation_interval_expiry
                status_expires_in = implementation_interval_expiry - current_timestamp
            else:
                status = BillStatus.LAPSED.name
                status_expires_at = None
                status_expires_in = None
            if human_readable:
                return {
                    "status": status,
                    "status_expires_at": f"{datetime.fromtimestamp(status_expires_at).strftime('%Y-%m-%d, %H:%M:%S')}",
                    "status_expires_in": f"{status_expires_in} seconds",
                    "enacts_at": f"{datetime.fromtimestamp(veto_interval_expiry).strftime('%Y-%m-%d, %H:%M:%S')}",
                    "enacts_in": f"{veto_interval_expiry - current_timestamp} seconds",
                    "implementable_at": f"{datetime.fromtimestamp(implementation_delay_expiry).strftime('%Y-%m-%d, %H:%M:%S')}",
                    "implementable_in": f"{implementation_delay_expiry - current_timestamp} seconds",
                    "lapses_at": f"{datetime.fromtimestamp(implementation_interval_expiry).strftime('%Y-%m-%d, %H:%M:%S')}",
                    "lapses_in": f"{implementation_interval_expiry - current_timestamp} seconds",
                }
            return {
                "status": status,
                "status_expires_at": status_expires_at,
                "status_expires_in": status_expires_in,
                "enacts_at": veto_interval_expiry,
                "enacts_in": veto_interval_expiry - current_timestamp,
                "implementable_at": implementation_delay_expiry,
                "implementable_in": implementation_delay_expiry - current_timestamp,
                "lapses_at": implementation_interval_expiry,
                "lapses_in": implementation_interval_expiry - current_timestamp,
            }
        return None

    @staticmethod
    def from_program(program):
        if program.list_len() == 0:
            return None
        values = list(program.as_iter())
        statute_index = None
        proposal_times = None
        if len(values) == 7:
            proposal_times = values.pop(0)
            statute_index = values.pop(0).as_int()
        elif len(values) == 6:
            statute_index = values.pop(0)
        values = [x.as_int() if idx > 0 else x for idx, x in enumerate(values)]
        return Bill(*values, statute_index=statute_index, proposal_times=proposal_times)

    def to_json_dict(self):
        return {
            "value": bytes(self.value).hex(),
            "threshold_amount_to_propose": self.threshold_amount_to_propose,
            "veto_interval": self.veto_interval,
            "implementation_delay": self.implementation_delay,
            "max_delta": self.max_delta,
            "statute_index": self.statute_index,
            "proposal_times": self.proposal_times.to_json_dict() if self.proposal_times else None,
        }

    def get_bill_info(self) -> dict:
        assert self.statute_index is not None, "get_bill_info requires bill to have statute index set"
        return {
            "bill_as_hex": self.to_program().as_bin().hex(),
            "value": statute_value_to_str_or_int(
                self.value, StatutePosition(self.statute_index) if self.statute_index != -1 else None
            ),
            "threshold_amount_to_propose": self.threshold_amount_to_propose,
            "veto_interval": self.veto_interval,
            "implementation_delay": self.implementation_delay,
            "max_delta": self.max_delta,
            "statute_index": self.statute_index,
            "statute_name": StatutePosition(self.statute_index).name,
            "proposal_times": {
                "veto_interval_expiry": self.proposal_times.at("f").as_int(),
                "implementation_delay_expiry": self.proposal_times.at("rf").as_int(),
                "implementation_interval_expiry": self.proposal_times.at("rr").as_int(),
            }
            if self.proposal_times is not None
            else None,
        }
