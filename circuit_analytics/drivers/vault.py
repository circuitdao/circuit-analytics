from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from chia.types.blockchain_format.program import Program, run, uncurry
from chia.types.coin_spend import CoinSpend
from chia.types.condition_opcodes import ConditionOpcode
from chia.wallet.util.compute_additions import compute_additions
from chia_rs import Coin
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint64

from circuit_analytics.drivers import get_driver_info, PROTOCOL_PREFIX, SOLUTION_PREFIX
from circuit_analytics.drivers.condition_filtering import filter_and_extract_remark_solution
from circuit_analytics.drivers.protocol_math import (
    LR_BUFFER,
    MOJOS,
    PRECISION,
    PRECISION_BPS,
    PRICE_PRECISION,
    calculate_collateral_ratio,
    calculate_cumulative_discount_factor,
    calculate_current_auction_price_bps,
    calculate_discounted_principal_for_mint,
    calculate_discounted_principal_for_repay,
    calculate_expected_collateral,
    calculate_fees_to_pay,
    calculate_max_borrow_amount,
    calculate_min_collateral_amount,
    calculate_required_byc_bid_amount,
    calculate_total_fees,
    undiscount_principal,
)
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    COLLATERAL_VAULT_MOD,
    COLLATERAL_VAULT_MOD_HASH,
    PROGRAM_VAULT_BORROW_MOD,
    PROGRAM_VAULT_DEPOSIT_MOD,
    PROGRAM_VAULT_KEEPER_BID_MOD,
    PROGRAM_VAULT_KEEPER_RECOVER_BAD_DEBT_MOD,
    PROGRAM_VAULT_KEEPER_START_AUCTION_MOD,
    PROGRAM_VAULT_KEEPER_TRANSFER_SF_TO_TREASURY_MOD,
    PROGRAM_VAULT_REPAY_MOD,
    PROGRAM_VAULT_TRANSFER_MOD,
    PROGRAM_VAULT_WITHDRAW_MOD,
)
from circuit_analytics.utils import (
    MAX_TX_BLOCK_TIME,
    to_list,
    to_tuple,
    tree_hash_of_apply,
)


log = logging.getLogger(__name__)


class CollateralVaultOperation(Enum):
    # owner operations
    DEPOSIT = PROGRAM_VAULT_DEPOSIT_MOD.get_tree_hash()
    WITHDRAW = PROGRAM_VAULT_WITHDRAW_MOD.get_tree_hash()
    BORROW = PROGRAM_VAULT_BORROW_MOD.get_tree_hash()
    REPAY = PROGRAM_VAULT_REPAY_MOD.get_tree_hash()
    TRANSFER = PROGRAM_VAULT_TRANSFER_MOD.get_tree_hash()
    # keeper operations
    START_AUCTION = PROGRAM_VAULT_KEEPER_START_AUCTION_MOD.get_tree_hash()
    BID = PROGRAM_VAULT_KEEPER_BID_MOD.get_tree_hash()
    RECOVER_BAD_DEBT = PROGRAM_VAULT_KEEPER_RECOVER_BAD_DEBT_MOD.get_tree_hash()
    TRANSFER_SF_TO_TREASURY = PROGRAM_VAULT_KEEPER_TRANSFER_SF_TO_TREASURY_MOD.get_tree_hash()


def print_auction_state(auction_state: Program, indent: str = ""):
    if auction_state == Program.to(0):
        print(f"{indent}auction state: nil")
    else:
        print(f"{indent}auction state:")
        print(f"{indent}  auction_start_time={auction_state.at('f').as_int()}")
        print(f"{indent}  start_price={auction_state.at('rf').as_int()}")
        print(f"{indent}  step_price_decrease_factor={auction_state.at('rrf').as_int()}")
        print(f"{indent}  step_time_interval={auction_state.at('rrrf').as_int()}")
        print(f"{indent}  initiator_puzzle_hash={auction_state.at('rrrrf').as_atom().hex()}")
        print(f"{indent}  initiator_incentive_balance={auction_state.at('rrrrrf').as_int()}")
        print(f"{indent}  auction_timeout={auction_state.at('rrrrrrf').as_int()}")
        print(f"{indent}  byc_to_treasury_balance={auction_state.at('rrrrrrrf').as_int()}")
        print(f"{indent}  byc_to_melt_balance={auction_state.at('rrrrrrrrf').as_int()}")
        print(f"{indent}  minimum_bid_amount={auction_state.at('rrrrrrrrrf').as_int()}")
        print(f"{indent}  minimum_auction_price={auction_state.at('rrrrrrrrrrf').as_int()}")


def singleton_struct_to_python(singleton_struct: Program) -> tuple[bytes32, tuple[bytes32, bytes32]]:
    return (
        bytes32(singleton_struct.first().atom),
        (bytes32(singleton_struct.at("rf").atom), bytes32(singleton_struct.at("rr").atom)),
    )


@dataclass
class VaultSolutionInfo:
    lineage_proof: Program
    inner_puzzle: Program
    solution: Program
    operation: Program
    statutes_inner_puzzle_hash: bytes32
    vault_operation_hash: bytes32

    @property
    def operation_hash(self) -> bytes32:
        return self.operation.get_tree_hash()

    @staticmethod
    def get_treasury_coin_info(
        treasury_coin_info: Program,
    ) -> list[Program, Program, Program, int] | list[bytes32, bytes32, bytes32, int]:
        """Returns treasury coin info fields converted to appropirate types.

        Treassury coin info fields are:
        - treasury_parent_id
        - treasury_launcher_id
        - treasury_prev_launcher_id
        - treasury_amount
        """
        if treasury_coin_info.nullp():
            return to_list(Program.to([0, 0, 0, 0]), 4, [None, None, None, "uint64"])
        else:
            return to_list(treasury_coin_info, 4, ["bytes32", "bytes32", "bytes32", "uint64"])


@dataclass
class VaultDepositInfo(VaultSolutionInfo):
    deposit_amount: int
    current_timestamp: int
    price_info: tuple[int, int]
    liquidation_ratio: int
    statutes_cumulative_stability_fee_df: int
    current_stability_fee_df: int


@dataclass
class VaultWithdrawInfo(VaultSolutionInfo):
    withdraw_amount: int
    price_info: tuple[int, int]
    liquidation_ratio: int
    current_timestamp: int
    statutes_cumulative_stability_fee_df: int
    current_stability_fee_df: int


@dataclass
class VaultBorrowInfo(VaultSolutionInfo):
    borrow_amount: int
    minimum_debt_amount: int
    liquidation_ratio: int
    price_info: tuple[int, int]
    byc_issuing_coin_parent_id: bytes32 | None
    statutes_cumulative_stability_fee_df: int
    current_stability_fee_df: int
    current_timestamp: int


@dataclass
class VaultRepayInfo(VaultSolutionInfo):
    repay_amount: int
    sf_transfer_amount: int
    statutes_cumulative_stability_fee_df: int
    byc_melting_coin_parent_id: bytes32 | None
    minimum_debt_amount: int
    byc_treasury_coin_info: Program
    min_treasury_delta: int
    price_info: tuple[int, int]
    current_stability_fee_df: int
    current_timestamp: int
    liquidation_ratio: int

    @property
    def treasury_coin(self) -> list[bytes32, bytes32, bytes32, int] | list[Program, Program, Program, int]:
        """Returns self.byc_treasury_coin_info destructured and converted to appropriate types."""
        return self.get_treasury_coin_info(self.byc_treasury_coin_info)


@dataclass
class VaultTransferInfo(VaultSolutionInfo):
    target_puzzle_hash: bytes32
    current_timestamp: int
    price_info: tuple[int, int]
    liquidation_ratio: int
    statutes_cumulative_stability_fee_df: int
    current_stability_fee_df: int


@dataclass
class VaultSFTransferInfo(VaultSolutionInfo):
    byc_issuing_coin_parent_id: bytes32
    statutes_cumulative_stability_fee_df: int
    byc_treasury_coin_info: Program
    min_treasury_delta: int
    current_timestamp: int
    current_stability_fee_df: int
    price_info: tuple[int, int]
    liquidation_ratio: int

    @property
    def treasury_coin(self) -> list[bytes32, bytes32, bytes32, int] | list[Program, Program, Program, int]:
        """Returns self.byc_treasury_coin_info destructured and converted to appropriate types."""
        return self.get_treasury_coin_info(self.byc_treasury_coin_info)


@dataclass
class VaultLiquidateInfo(VaultSolutionInfo):  # or separate into start and restart?
    start_only: Program  # -> () or (initiator_incentive_flat_fee initiator_incentive_relative_fee_bps liquidation_ratio statutes_cumulative_stability_fee_df current_stability_fee_df liquidation_penalty_bps)
    current_timestamp: int
    step_time_interval: int
    step_price_decrease_factor: int
    price_info: tuple[int, int]
    starting_price_factor_bps: int
    initiator_puzzle_hash: bytes32
    auction_ttl: int
    minimum_bid_amount_flat: int
    minimum_bid_amount_bps: int
    minimum_price_factor_bps: int

    @property
    def is_start(self) -> bool:
        if self.start_only.nullp():
            return False
        return True

    @property
    def is_restart(self) -> bool:
        if self.start_only.nullp():
            return True
        return False

    @property
    def initiator_incentive_flat_fee(self) -> int:
        if self.start_only.nullp():
            return 0
        return self.start_only.at("f").as_int()

    @property
    def initiator_incentive_relative_fee_bps(self) -> int:
        if self.start_only.nullp():
            return 0
        return self.start_only.at("rf").as_int()

    @property
    def liquidation_ratio(self) -> int:
        if self.start_only.nullp():
            return 1
        return self.start_only.at("rrf").as_int()

    @property
    def statutes_cumulative_stability_fee_df(self) -> int:
        if self.start_only.nullp():
            return 1
        return self.start_only.at("rrrf").as_int()

    @property
    def current_stability_fee_df(self) -> int:
        if self.start_only.nullp():
            return 1
        return self.start_only.at("rrrrf").as_int()

    @property
    def liquidation_penalty_bps(self) -> int:
        if self.start_only.nullp():
            return 0
        return self.start_only.at("rrrrrf").as_int()


@dataclass
class VaultBidInfo(VaultSolutionInfo):
    current_timestamp: int
    byc_bid_amount: int
    byc_melting_coin_parent_id: bytes32 | None
    byc_treasury_coin_info: Program
    min_treasury_delta: int
    target_puzzle_hash: bytes32
    my_coin_id: bytes32

    @property
    def treasury_coin(self) -> list[bytes32, bytes32, bytes32, int] | list[Program, Program, Program, int]:
        """Returns self.byc_treasury_coin_info destructured and converted to appropriate types."""
        return self.get_treasury_coin_info(self.byc_treasury_coin_info)


@dataclass
class VaultRecoverInfo(VaultSolutionInfo):
    recover_amount: int
    current_timestamp: int
    byc_treasury_coin_info: Program
    min_treasury_delta: int

    @property
    def treasury_coin(self) -> list[bytes32, bytes32, bytes32, int] | list[Program, Program, Program, int]:
        """Returns self.byc_treasury_coin_info destructured and converted to appropriate types."""
        return self.get_treasury_coin_info(self.byc_treasury_coin_info)


def get_vault_solution_info(coin_spend: CoinSpend) -> VaultSolutionInfo:
    vault_info = get_collateral_vault_info(coin_spend, spend=False)
    # vault_info = get_next_puzzle_state(coin_spend, ignore_solution=True)
    solution = Program.from_serialized(coin_spend.solution)
    (
        lineage_proof,
        inner_puzzle,
        inner_solution,
        operation,
    ) = to_list(solution.first(), 4)
    # lineage proof
    if lineage_proof.nullp():
        # spend of empty vault (no collateral, no debt). includes eve spend
        if not (
            vault_info.collateral == 0
            and vault_info.principal == 0
            and vault_info.auction_state.nullp()
            and vault_info.discounted_principal == 0
        ):
            raise SpendError("Cannot spend non-empty vault with eve lineage proof (nil)")
    else:
        # spend of non-empty vault
        (
            parent_parent_coin_name,
            parent_curried_args_hash,
            parent_amount,
        ) = to_list(lineage_proof, 3, ["bytes32", "bytes32", "int"])
        parent_coin_name = Coin(
            parent_parent_coin_name,
            tree_hash_of_apply(COLLATERAL_VAULT_MOD_HASH, parent_curried_args_hash),
            parent_amount,
        ).name()
        if parent_coin_name != coin_spend.coin.parent_coin_info:
            raise SpendError(
                f"Collateral vault non-eve lineage proof does not yield correct parent coin ID. "
                f"Expected {coin_spend.coin.parent_coin_info.hex()}, got {parent_coin_name.hex()}"
            )
    input_conditions = inner_puzzle.run(inner_solution)
    vault_operation_condition, filtered_conditions = filter_and_extract_remark_solution(
        list(input_conditions.as_iter())
    )
    inner_puzzle_hash = inner_puzzle.get_tree_hash()
    (statutes_inner_puzzle_hash, vault_operation_hash, args) = to_tuple(
        vault_operation_condition, 3, ["bytes32", "bytes32", None]
    )
    # statutes_puzzle_hash = calculate_statutes_puzzle_hash(vault_info.statutes_struct, statutes_inner_puzzle_hash)
    operation_hash = operation.get_tree_hash()
    # first check main puzzle
    if not vault_operation_hash == operation_hash:
        raise SpendError(
            f"Collateral vault operation hash extracted from solution REMARK does not match hash "
            f"of operation program provided in solution. "
            f"Expected {operation_hash.hex()}, got {vault_operation_hash.hex()}"
        )
    if operation_hash in [
        PROGRAM_VAULT_DEPOSIT_MOD.get_tree_hash(),
        PROGRAM_VAULT_WITHDRAW_MOD.get_tree_hash(),
        PROGRAM_VAULT_BORROW_MOD.get_tree_hash(),
        PROGRAM_VAULT_REPAY_MOD.get_tree_hash(),
        PROGRAM_VAULT_TRANSFER_MOD.get_tree_hash(),
    ]:
        if not vault_info.auction_state.nullp():
            raise SpendError("Collateral vault owner spend not possible if liquidation auction is active")
        if inner_puzzle_hash != vault_info.inner_puzzle_hash:
            raise SpendError(
                f"Collateral vault hash of inner puzzle does not match inner puzzle hash. "
                f"Expected {vault_info.inner_puzzle_hash.hex()}, got {inner_puzzle_hash.hex()}"
            )
    # now check operation program
    if operation_hash == PROGRAM_VAULT_DEPOSIT_MOD.get_tree_hash():
        (
            deposit_amount,
            current_timestamp,
            price_info,
            liquidation_ratio,
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
        ) = to_list(args, 6, ["uint64", "uint64", None, "int", "int", "int"])
        (price, price_timestamp) = to_tuple(price_info, 2, ["int", "int"])
        new_collateral = vault_info.collateral + deposit_amount
        cc_sf_df = calculate_cumulative_discount_factor(
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
            price_timestamp,
            current_timestamp + 3 * MAX_TX_BLOCK_TIME,
        )
        undiscounted_principal = undiscount_principal(vault_info.discounted_principal, cc_sf_df)
        min_collateral_amount = calculate_min_collateral_amount(undiscounted_principal, liquidation_ratio, price)
        if not deposit_amount > 0:
            raise SpendError(f"Collateral vault deposit amount must be greater than 0, got {deposit_amount}")
        if not new_collateral >= min_collateral_amount:
            raise SpendError(
                f"Collateral vault deposit must result in new collateral no less than "
                f"min collateral amount ({new_collateral} > {min_collateral_amount})"
            )
        return VaultDepositInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution=solution,
            operation=operation,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            vault_operation_hash=vault_operation_hash,
            # args
            deposit_amount=deposit_amount,
            current_timestamp=current_timestamp,
            price_info=(price, price_timestamp),
            liquidation_ratio=liquidation_ratio,
            statutes_cumulative_stability_fee_df=statutes_cumulative_stability_fee_df,
            current_stability_fee_df=current_stability_fee_df,
        )
    elif operation_hash == PROGRAM_VAULT_WITHDRAW_MOD.get_tree_hash():
        (
            withdraw_amount,
            price_info,
            liquidation_ratio,
            current_timestamp,
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
        ) = to_list(args, 6, ["uint64", None, "int", "uint64", "int", "int"])
        (price, price_timestamp) = to_tuple(price_info, 2, ["int", "int"])
        new_collateral = vault_info.collateral - withdraw_amount
        cc_sf_df = calculate_cumulative_discount_factor(
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
            price_timestamp,
            current_timestamp + 3 * MAX_TX_BLOCK_TIME,
        )
        undiscounted_principal = undiscount_principal(vault_info.discounted_principal, cc_sf_df)
        min_collateral_amount = calculate_min_collateral_amount(
            undiscounted_principal, liquidation_ratio + LR_BUFFER, price
        )
        if not new_collateral >= 0:
            raise SpendError(
                f"Collateral vault withdraw operation cannot result in "
                f"negative collateral amount ({new_collateral} >= 0)"
            )
        if not vault_info.collateral > new_collateral:
            raise SpendError(
                f"Collateral vault withdraw operation must result in "
                f"less collateral in vault ({new_collateral} < {vault_info.collateral})"
            )
        if not new_collateral >= min_collateral_amount:
            raise SpendError(
                f"Collateral vault withdraw operation must result in new collateral no less than "
                f"min collateral amount ({new_collateral} >= {min_collateral_amount})"
            )
        return VaultWithdrawInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution=solution,
            operation=operation,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            vault_operation_hash=vault_operation_hash,
            # args
            withdraw_amount=withdraw_amount,
            price_info=(price, price_timestamp),
            liquidation_ratio=liquidation_ratio,
            current_timestamp=current_timestamp,
            statutes_cumulative_stability_fee_df=statutes_cumulative_stability_fee_df,
            current_stability_fee_df=current_stability_fee_df,
        )
    elif operation_hash == PROGRAM_VAULT_BORROW_MOD.get_tree_hash():
        (
            borrow_amount,
            minimum_debt_amount,
            liquidation_ratio,
            price_info,
            byc_issuing_coin_parent_id,
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
            current_timestamp,
        ) = to_list(args, 8, ["uint64", "int", "int", None, "bytes32_or_none", "int", "int", "uint64"])
        (
            price,
            price_timestamp,
        ) = to_tuple(price_info, 2, ["int", "int"])
        new_principal = vault_info.principal + borrow_amount
        cc_sf_df = calculate_cumulative_discount_factor(
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
            price_timestamp,
            current_timestamp,
        )
        discounted_borrow_amount = -1 * ((-1 * borrow_amount * PRECISION) // cc_sf_df)
        new_discounted_principal = vault_info.discounted_principal + discounted_borrow_amount
        cc_sf_df_adj = calculate_cumulative_discount_factor(
            cc_sf_df,
            current_stability_fee_df,
            1,
            3 * MAX_TX_BLOCK_TIME,
        )
        new_undiscounted_principal = undiscount_principal(new_discounted_principal, cc_sf_df_adj)
        min_collateral_amount = calculate_min_collateral_amount(
            new_undiscounted_principal, liquidation_ratio + LR_BUFFER, price
        )
        if not borrow_amount > 0:
            raise SpendError(f"Collateral vault borrow amount must be great than 0 ({borrow_amount} > 0)")
        if not new_undiscounted_principal > minimum_debt_amount:
            raise SpendError(
                f"Collateral vault borrow operation must result in new debt greater than "
                f"min debt amount ({new_undiscounted_principal} >= {minimum_debt_amount})"
            )
        if not vault_info.collateral >= min_collateral_amount:
            raise SpendError(
                f"Collateral vault borrow operation must result in collateral no less than "
                f"min collateral amount ({vault_info.collateral} >= {min_collateral_amount})"
            )
        return VaultBorrowInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution=solution,
            operation=operation,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            vault_operation_hash=vault_operation_hash,
            # args
            borrow_amount=borrow_amount,
            minimum_debt_amount=minimum_debt_amount,
            liquidation_ratio=liquidation_ratio,
            price_info=(price, price_timestamp),
            byc_issuing_coin_parent_id=byc_issuing_coin_parent_id,
            statutes_cumulative_stability_fee_df=statutes_cumulative_stability_fee_df,
            current_stability_fee_df=current_stability_fee_df,
            current_timestamp=current_timestamp,
        )
    elif operation_hash == PROGRAM_VAULT_REPAY_MOD.get_tree_hash():
        (
            repay_amount,  # amount of byc to repay
            sf_transfer_amount,  # SFs to transfer to treasury (part of repay amount)
            statutes_cumulative_stability_fee_df,
            byc_melting_coin_parent_id,
            minimum_debt_amount,
            byc_treasury_coin_info,
            min_treasury_delta,
            price_info,
            current_stability_fee_df,
            current_timestamp,
            liquidation_ratio,
        ) = to_list(
            args, 11, ["uint64", "uint64", "int", "bytes32_or_none", "int", None, "int", None, "int", "uint64", "int"]
        )
        (
            price,
            price_timestamp,
        ) = to_tuple(price_info, 2, ["int", "int"])
        (
            treasury_parent_id,
            treasury_launcher_id,
            treasury_prev_launcher_id,
            treasury_amount,
        ) = VaultRepayInfo.get_treasury_coin_info(byc_treasury_coin_info)
        cc_sf_df = calculate_cumulative_discount_factor(
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
            price_timestamp,
            current_timestamp + 3 * MAX_TX_BLOCK_TIME,
        )
        undiscounted_principal = undiscount_principal(vault_info.discounted_principal, cc_sf_df)
        accrued_sf = calculate_total_fees(undiscounted_principal, vault_info.principal, 0)
        negative_principal_to_repay = sf_transfer_amount - repay_amount
        new_principal = vault_info.principal + negative_principal_to_repay
        if repay_amount == undiscounted_principal:
            new_discounted_principal = 0
        else:
            new_discounted_principal = vault_info.discounted_principal - ((repay_amount * PRECISION) // cc_sf_df)
        new_undiscounted_principal = undiscount_principal(new_discounted_principal, cc_sf_df)
        min_collateral_amount = calculate_min_collateral_amount(new_undiscounted_principal, liquidation_ratio, price)
        new_treasury_amount = sf_transfer_amount + treasury_amount
        # note: repay_amount cannot be 0. either repay_amount >= sf_transfer_amount > 0 or no-op guard prevents it
        if not repay_amount > 0:
            raise SpendError(f"Collateral vault repay amount must be greater than 0 ({repay_amount} > 0)")
        if not undiscounted_principal >= repay_amount:
            raise SpendError(
                f"Collateral vault repay amount must not be greater than debt "
                f"({repay_amount} <= {undiscounted_principal})"
            )
        if not sf_transfer_amount >= 0:
            raise SpendError(
                f"Collateral vault repay operation must not have a "
                f"negative SF transfer amount ({sf_transfer_amount} >= 0)"
            )
        # The puzzle's no-op guard (any repay_amount>0 sf_transfer_amount>0) is implicitly
        # satisfied: repay_amount > 0 is already enforced above.
        if not repay_amount >= sf_transfer_amount:
            raise SpendError(
                f"Collateral vault repay operation cannot transfer more SFs "
                f"than repay amount ({sf_transfer_amount} <= {repay_amount})"
            )
        if not accrued_sf >= sf_transfer_amount:
            raise SpendError(
                f"Collateral vault repay operation cannot transfer more SFs "
                f"than accrued SFs ({sf_transfer_amount} <= {accrued_sf})"
            )
        if not new_principal >= 0:
            raise SpendError(
                f"Collateral vault repay operation must not result in negative principal ({new_principal} >= 0)"
            )
        if not (new_undiscounted_principal == 0 or new_undiscounted_principal > minimum_debt_amount):
            raise SpendError(
                f"Collateral vault repay operation must either clear all debt or result in remaining debt "
                f"exceeding minimum debt amount ({new_undiscounted_principal} = 0 "
                f"or {new_undiscounted_principal} > {minimum_debt_amount})"
            )
        if not (sf_transfer_amount == 0 or sf_transfer_amount > min_treasury_delta):
            raise SpendError(
                f"Collateral vault repay operation must either not transfer any SFs "
                f"or transfer more than min treasury delta ({sf_transfer_amount} = 0 "
                f"or {sf_transfer_amount} > {min_treasury_delta})"
            )
        if not vault_info.collateral >= min_collateral_amount:
            raise SpendError(
                f"Collateral vault repay operation must result in collateral no less than "
                f"min collateral amount ({vault_info.collateral} >= {min_collateral_amount})"
            )
        return VaultRepayInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution=solution,
            operation=operation,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            vault_operation_hash=vault_operation_hash,
            # args
            repay_amount=repay_amount,
            sf_transfer_amount=sf_transfer_amount,
            statutes_cumulative_stability_fee_df=statutes_cumulative_stability_fee_df,
            byc_melting_coin_parent_id=byc_melting_coin_parent_id,
            minimum_debt_amount=minimum_debt_amount,
            byc_treasury_coin_info=byc_treasury_coin_info,
            min_treasury_delta=min_treasury_delta,
            price_info=(price, price_timestamp),
            current_stability_fee_df=current_stability_fee_df,
            current_timestamp=current_timestamp,
            liquidation_ratio=liquidation_ratio,
        )
    elif operation_hash == PROGRAM_VAULT_TRANSFER_MOD.get_tree_hash():
        (
            target_puzzle_hash,
            current_timestamp,
            price_info,
            liquidation_ratio,
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
        ) = to_list(args, 6, ["bytes32", "uint64", None, "int", "int", "int"])
        (
            price,
            price_timestamp,
        ) = to_tuple(price_info, 2, ["int", "int"])
        cc_sf_df = calculate_cumulative_discount_factor(
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
            price_timestamp,
            current_timestamp + 3 * MAX_TX_BLOCK_TIME,
        )
        undiscounted_principal = undiscount_principal(vault_info.discounted_principal, cc_sf_df)
        min_collateral_amount = calculate_min_collateral_amount(undiscounted_principal, liquidation_ratio, price)
        if not vault_info.collateral >= min_collateral_amount:
            raise SpendError(
                f"Collateral vault transfer operation must result in collateral no less than "
                f"min collateral amount ({vault_info.collateral} >= {min_collateral_amount})"
            )
        return VaultTransferInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution=solution,
            operation=operation,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            vault_operation_hash=vault_operation_hash,
            # args
            target_puzzle_hash=target_puzzle_hash,
            current_timestamp=current_timestamp,
            price_info=(price, price_timestamp),
            liquidation_ratio=liquidation_ratio,
            statutes_cumulative_stability_fee_df=statutes_cumulative_stability_fee_df,
            current_stability_fee_df=current_stability_fee_df,
        )
    elif operation_hash == PROGRAM_VAULT_KEEPER_TRANSFER_SF_TO_TREASURY_MOD.get_tree_hash():
        (
            byc_melting_coin_parent_id,
            statutes_cumulative_stability_fee_df,
            treasury_coin_info,
            min_treasury_delta,
            current_timestamp,
            current_stability_fee_df,
            price_info,
            liquidation_ratio,
        ) = to_list(args, 8, ["bytes32", "int", None, "int", "uint64", "int", None, "int"])
        (
            treasury_parent_id,
            treasury_launcher_id,
            treasury_prev_launcher_id,
            treasury_amount,
        ) = to_list(treasury_coin_info, 4, ["bytes32", "bytes32", "bytes32", "uint64"])
        (
            price,
            price_timestamp,
        ) = to_tuple(price_info, 2, ["int", "int"])
        cc_sf_df = calculate_cumulative_discount_factor(
            statutes_cumulative_stability_fee_df,
            current_stability_fee_df,
            price_timestamp,
            current_timestamp,  # + 3 * MAX_TX_BLOCK_TIME,
        )
        undiscounted_principal = undiscount_principal(vault_info.discounted_principal, cc_sf_df)
        min_collateral_amount = calculate_min_collateral_amount(undiscounted_principal, liquidation_ratio, price)
        fees_to_treasury = calculate_total_fees(undiscounted_principal, vault_info.principal, 0)
        # new_treasury_amount = treasury_amount + fees_to_treasury
        if not vault_info.auction_state.nullp():
            raise SpendError("Collateral vault SF transfer only permitted if vault not seized.")
        if not fees_to_treasury > 0:
            raise SpendError(
                f"Collateral vault SF transfer only permitted if there are SFs to transfer ({fees_to_treasury} > 0)"
            )
        if not fees_to_treasury > min_treasury_delta:
            raise SpendError(
                f"Collateral vault SF transfer only permitted if transfer amount "
                f"exceeds min treasury delta ({fees_to_treasury} > {min_treasury_delta})"
            )
        if not vault_info.collateral >= min_collateral_amount:
            raise SpendError(
                f"Collateral vault SF transfer operation must result in collateral no less "
                f"than min collateral amount ({vault_info.collateral} >= {min_collateral_amount})"
            )
        return VaultSFTransferInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution=solution,
            operation=operation,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            vault_operation_hash=vault_operation_hash,
            # args
            byc_issuing_coin_parent_id=byc_melting_coin_parent_id,
            statutes_cumulative_stability_fee_df=statutes_cumulative_stability_fee_df,
            min_treasury_delta=min_treasury_delta,
            current_timestamp=current_timestamp,
            current_stability_fee_df=current_stability_fee_df,
            price_info=(price, price_timestamp),
            liquidation_ratio=liquidation_ratio,
            byc_treasury_coin_info=treasury_coin_info,
        )
    elif operation_hash == PROGRAM_VAULT_KEEPER_START_AUCTION_MOD.get_tree_hash():
        (
            start_only,
            current_timestamp,
            step_time_interval,
            step_price_decrease_factor,
            price_info,
            starting_price_factor_bps,
            initiator_puzzle_hash,
            auction_ttl,
            minimum_bid_amount_flat,
            minimum_bid_amount_bps,
            minimum_price_factor_bps,
        ) = to_list(args, 11, [None, "uint64", "int", "int", None, "int", "bytes32", "int", "int", "int", "int"])
        (
            price,
            price_timestamp,
        ) = to_tuple(price_info, 2, ["int", "int"])
        if not start_only.nullp():
            # start auction
            (
                initiator_incentive_flat_fee,
                initiator_incentive_relative_fee_bps,
                liquidation_ratio,
                statutes_cumulative_stability_fee_df,
                current_stability_fee_df,
                liquidation_penalty_bps,
            ) = to_list(start_only, 6, ["int", "int", "int", "int", "int", "int"])
            if not vault_info.auction_state.nullp():
                raise SpendError(
                    "Cannot start collateral vault auction that was started previously (auction state is not nil)"
                )
        else:
            # restart auction
            liquidation_ratio = 1
            statutes_cumulative_stability_fee_df = 1
            current_stability_fee_df = 1
            liquidation_penalty_bps = 0
            if vault_info.auction_state.nullp():
                raise SpendError(
                    "Cannot restart collateral vault auction that was not previously started (auction state is nil)"
                )
        if vault_info.auction_state.nullp():
            # start auction
            cc_sf_df = calculate_cumulative_discount_factor(
                statutes_cumulative_stability_fee_df,
                current_stability_fee_df,
                price_timestamp,
                current_timestamp + 3 * MAX_TX_BLOCK_TIME,
            )
            undiscounted_principal = undiscount_principal(vault_info.discounted_principal, cc_sf_df)
            min_collateral_amount = calculate_min_collateral_amount(undiscounted_principal, liquidation_ratio, price)
            if not min_collateral_amount > vault_info.collateral:
                raise SpendError(
                    f"Collateral vault auction can only be started if collateral "
                    f"is less than min collateral amount ({vault_info.collateral} < {min_collateral_amount})"
                )
        else:
            # restart auction
            if not current_timestamp - vault_info.start_time > auction_ttl:
                raise SpendError(
                    f"Collateral vault auction can only be restarted if auction has "
                    f"expired ({current_timestamp - vault_info.start_time} > {auction_ttl})"
                )
            if not vault_info.collateral > 0:
                raise SpendError(
                    f"Collateral vault auction can not be restarted "
                    f"if there is no collateral left ({vault_info.collateral} > 0)"
                )
        return VaultLiquidateInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution=solution,
            operation=operation,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            vault_operation_hash=vault_operation_hash,
            # args
            start_only=start_only,
            current_timestamp=current_timestamp,
            step_time_interval=step_time_interval,
            step_price_decrease_factor=step_price_decrease_factor,
            price_info=(price, price_timestamp),
            starting_price_factor_bps=starting_price_factor_bps,
            initiator_puzzle_hash=initiator_puzzle_hash,
            auction_ttl=auction_ttl,
            minimum_bid_amount_flat=minimum_bid_amount_flat,
            minimum_bid_amount_bps=minimum_bid_amount_bps,
            minimum_price_factor_bps=minimum_price_factor_bps,
        )
    elif operation_hash == PROGRAM_VAULT_KEEPER_BID_MOD.get_tree_hash():
        (
            current_timestamp,
            byc_bid_amount,
            byc_melting_coin_parent_id,
            byc_treasury_coin_info,
            min_treasury_delta,
            target_puzzle_hash,
            my_coin_id,
        ) = to_list(args, 7, ["uint64", "uint64", "bytes32_or_none", None, "int", "bytes32", "bytes32"])
        if byc_bid_amount > vault_info.initiator_incentive_balance:
            initiator_incentive = vault_info.initiator_incentive_balance
        else:
            initiator_incentive = byc_bid_amount
        remaining_byc_bid_amount = byc_bid_amount - initiator_incentive
        if remaining_byc_bid_amount > vault_info.byc_to_treasury_balance:
            byc_to_treasury = vault_info.byc_to_treasury_balance
        else:
            byc_to_treasury = remaining_byc_bid_amount
        byc_to_melt = remaining_byc_bid_amount - byc_to_treasury
        debt = (
            vault_info.initiator_incentive_balance + vault_info.byc_to_treasury_balance + vault_info.byc_to_melt_balance
        )
        leftover_debt = debt - byc_bid_amount
        auction_price = calculate_current_auction_price_bps(
            vault_info.start_price,
            current_timestamp,
            vault_info.start_time,
            vault_info.step_price_decrease_factor,
            vault_info.step_time_interval,
        )
        bid_xch_collateral_amount_pre = (
            (byc_bid_amount * PRECISION_BPS * PRICE_PRECISION * MOJOS) // auction_price
        ) // 1000
        if bid_xch_collateral_amount_pre > vault_info.collateral:
            bid_xch_collateral_amount = vault_info.collateral
        else:
            bid_xch_collateral_amount = bid_xch_collateral_amount_pre
        leftover_collateral = vault_info.collateral - bid_xch_collateral_amount
        if not leftover_debt >= 0:
            raise SpendError(f"Collateral vault auction bid must not result in negative debt ({leftover_debt} >= 0)")
        if not byc_bid_amount > 0:
            raise SpendError(f"Collateral vault auction bid amount must be positive ({byc_bid_amount} > 0)")
        if not (
            byc_bid_amount > vault_info.minimum_bid_amount
            or (vault_info.minimum_bid_amount >= debt and byc_bid_amount == debt)
            or leftover_collateral == 0
        ):
            raise SpendError(
                f"Collateral vault auction bid amount ({byc_bid_amount}) must "
                f"exceed min bid amount ({vault_info.minimum_bid_amount}) or pay off all debt ({debt}) "
                f"or leave no collateral in vault ({leftover_collateral})"
            )
        if not current_timestamp > vault_info.start_time:
            raise SpendError(
                f"Collateral vault auction bid can only be placed if auction start time "
                f"has passed ({current_timestamp} > {vault_info.start_time})"
            )
        if not vault_info.auction_ttl > current_timestamp - vault_info.start_time:
            raise SpendError(
                f"Collateral vault auction bid can only be placed if auction "
                f"has not expired ({vault_info.auction_ttl} > {current_timestamp - vault_info.start_time})"
            )
        if not auction_price // PRECISION_BPS > vault_info.min_price:
            raise SpendError(
                f"Collateral vault auction bid can only be placed if auction price "
                f"exceeds min auction price ({auction_price // PRECISION_BPS} > {vault_info.min_price})"
            )
        if not vault_info.collateral > 0:
            raise SpendError(
                f"Collateral vault auction bid can only be placed "
                f"if there is collateral left ({vault_info.collateral} > 0)"
            )
        if not leftover_collateral >= 0:
            raise SpendError(
                f"Collateral vault auction bid can only be placed "
                f"if resulting collateral is non-negative ({leftover_collateral} >= 0)"
            )
        if not (
            byc_to_treasury > min_treasury_delta
            or byc_to_treasury == vault_info.byc_to_treasury_balance
            or byc_to_treasury == 0
            or leftover_collateral == 0
        ):
            raise SpendError(
                f"Collateral vault auction bid can only be placed if amount transferred to treasury ({byc_to_treasury}) "
                f"is greater than min treasury delta ({min_treasury_delta}) or equal to "
                f"remaining balance to be transferred to treasury ({vault_info.byc_to_treasury_balance}) "
                f"or 0 or if it results in no collateral being left ({leftover_collateral})"
            )
        return VaultBidInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution=solution,
            operation=operation,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            vault_operation_hash=vault_operation_hash,
            # args
            current_timestamp=current_timestamp,
            byc_bid_amount=byc_bid_amount,
            byc_melting_coin_parent_id=byc_melting_coin_parent_id,
            byc_treasury_coin_info=byc_treasury_coin_info,
            min_treasury_delta=min_treasury_delta,
            target_puzzle_hash=target_puzzle_hash,
            my_coin_id=my_coin_id,
        )
    elif operation_hash == PROGRAM_VAULT_KEEPER_RECOVER_BAD_DEBT_MOD.get_tree_hash():
        (
            recover_amount,
            current_timestamp,
            treasury_coin_info,
            min_treasury_delta,
        ) = to_list(args, 4, ["uint64", "uint64", None, "int"])
        (
            treasury_parent_id,
            treasury_launcher_id,
            treasury_prev_launcher_id,
            treasury_amount,
        ) = to_list(treasury_coin_info, 4, ["bytes32", "bytes32", "bytes32", "uint64"])
        leftover_byc_to_melt_balance = vault_info.byc_to_melt_balance - recover_amount
        if not vault_info.collateral == 0:
            raise SpendError(
                f"Collateral vault bad debt cannot be recovered if there is collateral left ({vault_info.collateral})"
            )
        if not leftover_byc_to_melt_balance >= 0:
            raise SpendError(
                f"Collateral vault bad debt recover amount must not exceed "
                f"BYC to melt balance ({recover_amount} <= {vault_info.byc_to_melt_balance})"
            )
        if not recover_amount > 0:
            raise SpendError(f"Collateral vault bad debt recover amount must be positive ({recover_amount} > 0)")
        if not (recover_amount > min_treasury_delta or recover_amount == vault_info.byc_to_melt_balance):
            raise SpendError(
                f"Collateral vault bad debt recover amount must exceed min treasury delta or recover all debt "
                f"({recover_amount} > {min_treasury_delta} or {recover_amount} == {vault_info.byc_to_melt_balance})"
            )
        return VaultRecoverInfo(
            lineage_proof=lineage_proof,
            inner_puzzle=inner_puzzle,
            solution=solution,
            operation=operation,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            vault_operation_hash=vault_operation_hash,
            # args
            recover_amount=recover_amount,
            current_timestamp=current_timestamp,
            byc_treasury_coin_info=treasury_coin_info,
            min_treasury_delta=min_treasury_delta,
        )
    else:
        raise SpendError(f"Unknown collateral vault operation. Operation hash: {operation_hash.hex()}")


@dataclass
class CollateralVaultState:
    vault_mod_hash: bytes32
    statutes_struct: Program
    collateral: uint64 | Program
    principal: uint64 | Program
    auction_state: Program
    inner_puzzle_hash: bytes32
    discounted_principal: int | Program
    parent_name: Optional[bytes32] = None
    last_spend: Optional[CoinSpend] = None
    operation: Optional[Dict] = None
    byc_tail_hash: bytes32 = None
    fees: int = None
    coin_name: bytes32 = None

    @property
    def start_time(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("f").as_int()

    @property
    def start_price(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("rf").as_int()

    @property
    def step_price_decrease_factor(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("rrf").as_int()

    @property
    def step_time_interval(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("rrrf").as_int()

    @property
    def initiator_puzzle_hash(self) -> bytes32 | None:
        if self.auction_state.nullp():
            return None
        return bytes32(self.auction_state.at("rrrrf").atom)

    @property
    def initiator_incentive_balance(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("rrrrrf").as_int()

    @property
    def auction_ttl(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("rrrrrrf").as_int()

    @property
    def byc_to_treasury_balance(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("rrrrrrrf").as_int()

    @property
    def byc_to_melt_balance(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("rrrrrrrrf").as_int()

    @property
    def minimum_bid_amount(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("rrrrrrrrrf").as_int()

    @property
    def min_price(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.auction_state.at("rrrrrrrrrrf").as_int()

    @property
    def seized_debt(self) -> int | None:
        if self.auction_state.nullp():
            return None
        return self.initiator_incentive_balance + self.byc_to_treasury_balance + self.byc_to_melt_balance

    def min_byc_amount_to_bid(self, current_timestamp: int) -> int | None:
        if self.auction_state.nullp():
            return None
        required_byc_bid_amount = calculate_required_byc_bid_amount(
            self.collateral,
            self.start_price,
            self.step_price_decrease_factor,
            self.step_time_interval,
            self.start_time,
            current_timestamp,
        )  # minimum bid amount required to receive all collateral
        return min(
            self.minimum_bid_amount + 1,
            self.seized_debt,
            required_byc_bid_amount,
        )

    def max_byc_amount_to_bid(self, current_timestamp: int) -> int | None:
        if self.auction_state.nullp():
            return None
        required_byc_bid_amount = calculate_required_byc_bid_amount(
            self.collateral,
            self.start_price,
            self.step_price_decrease_factor,
            self.step_time_interval,
            self.start_time,
            current_timestamp,
        )  # minimum bid amount required to receive all collateral
        return min(
            self.seized_debt,
            required_byc_bid_amount,  # prevent overspending even though allowed by puzzle
        )

    def print(self, text: str = "", indent: str = ""):
        text = f" ({text})" if text else ""
        print(f"{indent}Collateral vault state{text}:")
        print(f"{indent}  {self.vault_mod_hash=}")
        print(f"{indent}  statutes_struct={singleton_struct_to_python(self.statutes_struct)}")
        print(f"{indent}  {self.collateral=}")
        print(f"{indent}  {self.principal=}")
        print_auction_state(self.auction_state, f"{indent}  ")
        print(f"{indent}  {self.inner_puzzle_hash=}")
        print(f"{indent}  {self.discounted_principal=}")
        print(f"{indent}  {self.parent_name=}")
        print(f"{indent}  {self.coin_name=}")

    def __post_init__(self):
        if isinstance(self.discounted_principal, Program):
            # LATER: similarly convert collateral and principal if they are Programs?
            self.discounted_principal = self.discounted_principal.as_int()  # convert to uint64 instead of int?

    def to_puzzle(self) -> Program:
        return COLLATERAL_VAULT_MOD.curry(
            COLLATERAL_VAULT_MOD.get_tree_hash(),
            self.statutes_struct,
            self.collateral,
            self.principal,
            self.auction_state,
            self.inner_puzzle_hash,
            self.discounted_principal,
        )

    def to_curried_values(self):
        return [
            self.collateral,
            self.principal,
            self.auction_state,
            self.inner_puzzle_hash,
            self.discounted_principal,
        ]

    @staticmethod
    def calculate_minimum_bid_amount(debt: int, minimum_bid_amount_flat: int, minimum_bid_amount_bps: int) -> int:
        """Return minimum bid amount for a liquidation auction that is being started.

        Minimum bid amount is (re-)calculated when a liquidation auction is started or restarted, and then kept fixed
        until the end of the auction.

        Debt is the frozen debt at start of auction, or, when restarting, the debt remaining after previous auction.
        """
        minimum_bid_amount_relative = (debt * minimum_bid_amount_bps) // PRECISION_BPS
        minimum_bid_amount = max(minimum_bid_amount_flat, minimum_bid_amount_relative)
        return minimum_bid_amount

    def get_stability_fees(
        self, current_cumulative_stability_fee_df: int, liquidation_penalty_percent: int = None
    ) -> int:
        """Calculate accrued Stability Fees of vault. Includes Liquidation Penalty if specified."""
        log.debug(
            "Calculating accrued SFs: principal=%s discounted_principal=%s cc_sf_df=%s lp=%s",
            self.principal,
            self.discounted_principal,
            current_cumulative_stability_fee_df,
            liquidation_penalty_percent,
        )
        return calculate_fees_to_pay(
            None,
            current_cumulative_stability_fee_df,
            self.principal,
            uint64(self.discounted_principal),
            liquidation_penalty_percent,
        )

    def balance_deltas(self, bid_amount: int, current_time: int = None) -> tuple[int, int, int, int | None] | None:
        """Returns a tuple (of non-negative values) indicating by how much a given bid will recude each debt component
        as well as collateral.

        No error is thrown if bid_amount is greater than vault debt.
        """
        if self.auction_state.nullp():
            return None
        byc_to_initiator = min(self.initiator_incentive_balance, bid_amount)
        amount_left = bid_amount - byc_to_initiator
        byc_to_treasury = min(self.byc_to_treasury_balance, amount_left)
        amount_left = amount_left - byc_to_treasury
        byc_to_melt = min(self.byc_to_melt_balance, amount_left)
        amount_left = amount_left - byc_to_melt
        if current_time:
            bid_xch_collateral_amount_pre = (
                (bid_amount * PRECISION_BPS * PRICE_PRECISION * MOJOS)
                // self.get_auction_price(current_time, full_precision=True)  # , as_float=False)
            ) // 1000
            bid_xch_collateral_amount = (
                self.collateral if bid_xch_collateral_amount_pre > self.collateral else bid_xch_collateral_amount_pre
            )
        return byc_to_initiator, byc_to_treasury, byc_to_melt, bid_xch_collateral_amount

    def get_debt(self, cumulative_stability_fee_df: int = None) -> int:
        if not self.seized:
            assert cumulative_stability_fee_df >= PRECISION
            return self.principal + self.get_stability_fees(
                cumulative_stability_fee_df,
                0,  # liquidation_penalty_percent
            )
        return self.seized_debt

    def get_min_deposit(self, cumulative_stability_fee_df, liquidation_ratio, current_price) -> int:
        """Minimum amount of collateral that can be deposited to vault given current state.

        cumulative_stability_fee_df must have been calculated with 3 * MAX_TX_BLOCK_TIME added to current_timestamp.
        """
        if self.seized:
            return 0
        debt = self.get_debt(cumulative_stability_fee_df)
        min_collateral_required = calculate_min_collateral_amount(debt, liquidation_ratio, current_price)
        log.debug(
            f"calc min deposit: {debt=} {current_price=} "
            f"{liquidation_ratio=} {self.collateral=} {min_collateral_required=}"
        )
        return max(1, min_collateral_required - self.collateral)

    def get_max_withdraw(self, cumulative_stability_fee_df, liquidation_ratio, current_price) -> int:
        """Maximum amount of collateral that can be withdrawn from vault given current state.

        cumulative_stability_fee_df must have been calculated with 3 * MAX_TX_BLOCK_TIME added to current_timestamp.
        """
        if self.seized:
            return 0
        debt = self.get_debt(cumulative_stability_fee_df)
        min_collateral_required = calculate_min_collateral_amount(debt, liquidation_ratio + LR_BUFFER, current_price)
        log.debug(
            f"calc max withdraw: {debt=} {current_price=} "
            f"{liquidation_ratio+LR_BUFFER=} {self.collateral=} {min_collateral_required=}"
        )
        return max(0, self.collateral - min_collateral_required)

    def get_max_borrow(
        self,
        cumulative_stability_fee_df: int,  # current cumulative SF DF (excl 3 * MAX_TX_BLOCK_TIME add-on)
        liquidation_ratio: int,
        current_price: int,
        current_sf_df: int,  # as used to calculate cumulative_stability_fee_df
    ) -> int:
        """Maximum amount of BYC that can be borrowed from vault given current state.

        cumulative_stability_fee_df (current cumulative SF DF) must have been calculated
        without 3 * MAX_TX_BLOCK_TIME added to current_timestamp.
        """
        if self.seized:
            return 0
        return calculate_max_borrow_amount(
            self.collateral,
            self.discounted_principal,
            liquidation_ratio,
            current_price,
            cumulative_stability_fee_df,
            current_sf_df,
        )

    def _repay_amount_valid(
        self,
        repay_amount: int,
        cumulative_stability_fee_df: int,  # incl 3 * MAX_TX_BLOCK_TIME add-on
        liquidation_ratio: int,
        current_price: int,
    ) -> int:  # -1: too small, 0: too large, 1: valid
        if repay_amount < 1:
            return -1  # too small
        debt = self.get_debt(cumulative_stability_fee_df)
        if repay_amount > debt:
            return 0  # too large
        if repay_amount == debt:
            new_discounted_principal = 0
        else:
            new_discounted_principal = self.discounted_principal - (
                (repay_amount * PRECISION) // cumulative_stability_fee_df
            )
        new_undiscounted_principal = -((-new_discounted_principal * cumulative_stability_fee_df) // PRECISION)
        min_collateral_required = calculate_min_collateral_amount(
            new_undiscounted_principal, liquidation_ratio, current_price
        )
        # log.debug(f"{min_collateral_required/MOJOS=} {self.collateral/MOJOS=}")
        if self.collateral >= min_collateral_required:
            return 1  # valid
        return -1  # too small

    def _approximate_repay_amount(
        self,
        repay_amount: int,
        cumulative_stability_fee_df: int,
        liquidation_ratio: int,
        current_price: int,
        delta: int,
    ) -> tuple[int, int]:
        """Returns repay amount approximation"""
        log.debug(f"approximating repay amount with {repay_amount=} {delta=}")
        while (
            self._repay_amount_valid(repay_amount, cumulative_stability_fee_df, liquidation_ratio, current_price) >= 0
        ):
            repay_amount -= delta
            log.debug(f"decreased repay_amount to: {repay_amount}")
        while (
            self._repay_amount_valid(repay_amount, cumulative_stability_fee_df, liquidation_ratio, current_price) == -1
        ):
            repay_amount += delta
            log.debug(f"increased repay_amount to: {repay_amount}")
        log.debug(f"returning repay amount: {repay_amount}")
        return repay_amount

    def get_repay_ranges(
        self,
        cumulative_stability_fee_df: int,  # incl 3 * MAX_TX_BLOCK_TIME add-on
        liquidation_ratio: int,
        current_price: int,
        min_treasury_delta: int = None,  # Statute value. If None, don't check if repay amount will result in valid SF transfer amount
    ) -> list[tuple[int, int]]:
        """Returns a list of repay amount ranges (each as a tuple (min, max)) for vault given current state.

        There can be 0, 1 2, or 3 ranges. They are sorted in ascending order.
        The ranges represent all possible repay amounts. In most cases exactly one range is returned.

        Returns an empty list if repayment is impossible. This can happen eg when there is no debt or accrued SFs <= min treasury delta.
        Two ranges can occur when
          0 < principal < min treasury delta < accrued SFs.
        Three ranges occur when
          min prepay amount <= principal < min treasury delta < accrued SFs and min repay amount + min treasury delta > accrued SFs

        cumulative_stability_fee_df must have been calculated with 3 * MAX_TX_BLOCK_TIME added to current_timestamp.
        """
        if self.seized:
            return 0
        debt = self.get_debt(cumulative_stability_fee_df)
        if debt < 1:
            return []  # no debt to repay
        min_collateral_required = calculate_min_collateral_amount(debt, liquidation_ratio, current_price)
        log.debug(f"{min_collateral_required/MOJOS=} {self.collateral/MOJOS=}")
        # estimate min repay amount
        repay_amount = max(1, int((min_collateral_required - self.collateral) * (current_price * 10 / MOJOS)))
        log.debug(f"{debt/1000=} {(debt-repay_amount)/1000=}")
        log.debug(f"initial min repay amount estimate: {repay_amount}")
        delta = max(1, int(repay_amount / 100))  # start with something reasonable
        while delta > 0:
            repay_amount = self._approximate_repay_amount(
                repay_amount,
                cumulative_stability_fee_df,
                liquidation_ratio,
                current_price,
                delta,
            )
            delta = delta // 2
        min_repay_amount = repay_amount

        # account for min treasury delta if desired
        if min_treasury_delta is not None:
            accrued_sf = calculate_fees_to_pay(
                None,
                cumulative_stability_fee_df,
                self.principal,
                self.discounted_principal,
            )
            principal_repay_range = [1, self.principal]
            sf_repay_range = [min_treasury_delta + 1, accrued_sf]
            if accrued_sf <= min_treasury_delta:
                # cannot withdraw any SFs
                if min_repay_amount <= self.principal:
                    return [(min_repay_amount, self.principal)]
                return []
            if self.principal < min_treasury_delta:
                # there may be a repay amount gap
                if min_repay_amount <= self.principal:
                    repay_ranges = [(min_repay_amount, self.principal)]
                    # note that we have: accrued_sf > min_treasury_delta
                    if min_repay_amount + min_treasury_delta <= accrued_sf:
                        repay_ranges.append((min_treasury_delta + 1, debt))
                    else:
                        repay_ranges.append((min_treasury_delta + 1, accrued_sf))
                        repay_ranges.append((min_repay_amount + min_treasury_delta + 1, debt))
                    return repay_ranges
                # this is case where we cannot repay principal on its own
                if accrued_sf <= min_treasury_delta:  # never true here
                    return []  # will never get here
                if self.principal + accrued_sf < min_repay_amount:
                    return []
                return [(max(min_treasury_delta + 1, min_repay_amount), debt)]
            # there's exactly one range
            #  because accrued_sf > min_treasury_delta and principal >= min_treasury_delta,
            #  and min_repay_amount <= debt always holds
            return [(min_repay_amount, debt)]

        return [(min_repay_amount, debt)]

    def get_operation_amount_ranges(
        self,
        cumulative_stability_fee_df: int,
        liquidation_ratio: int,
        current_price: int,
        current_sf_df: int,
        min_treasury_delta: int,
    ) -> dict:
        # TODO: check that calculating adj cumulative SF DF as below is still correct with new, O(log min) methodology
        cumulative_stability_fee_df_adj = cumulative_stability_fee_df
        for i in range(6):
            cumulative_stability_fee_df_adj = (cumulative_stability_fee_df_adj * current_sf_df) // PRECISION
        repay_ranges = self.get_repay_ranges(
            cumulative_stability_fee_df_adj,  # incl 3 * MAX_TX_BLOCK_TIME add-on
            liquidation_ratio,
            current_price,
            min_treasury_delta,
        )
        if self.seized:
            return {
                "min_deposit": None,
                "max_withdraw": None,
                "max_borrow": None,
                "min_repay": None,
                "max_repay": None,
            }
        if not repay_ranges:
            min_repay = 0
            max_repay = 0
        elif len(repay_ranges) > 1:
            min_repay = repay_ranges[0][0]
            max_repay = repay_ranges[1][1]
        else:
            min_repay = repay_ranges[0][0]
            max_repay = repay_ranges[0][1]
        ranges = {
            "min_deposit": self.get_min_deposit(cumulative_stability_fee_df_adj, liquidation_ratio, current_price),
            "max_withdraw": self.get_max_withdraw(cumulative_stability_fee_df_adj, liquidation_ratio, current_price),
            "max_borrow": self.get_max_borrow(
                cumulative_stability_fee_df,  # current cumulative SF DF (excl 3 * MAX_TX_BLOCK_TIME add-on)
                liquidation_ratio,
                current_price,
                current_sf_df,  # as used to calculate cumulative_stability_fee_df
            ),
            "min_repay": min_repay,
            "max_repay": max_repay,
        }
        return ranges

    def get_collateral_ratio(
        self, cumulative_stability_fee_df: int, xch_price: int, additional_debt: int = 0
    ) -> float | None:
        """Return collateral ratio as a floating point number.

        For example 1.3645 = 136.45%.

        Returns float('inf') if debt + additional_debt is 0, returns None if debt + additional_debt is negative.

        Arguments:
        - cumulative_stability_fee_df: should be calculated with + 3 * MAX_TX_BLOCK_TIME offset for current_timestamp
        - xch_price: XCH price [cBYC per XCH]
        - additional_debt: an expected change in debt, e.g. because of a borrow or repay [mBYC]

        This function is for information only. It has no equivalent in the protocol.
        """
        # LATER: verify that this results in the actual collateral ratio being greater than the one calculated (to avoid unexpected liquidations).
        debt = self.get_debt(cumulative_stability_fee_df)
        return calculate_collateral_ratio(debt + additional_debt, self.collateral, xch_price)

    ## Vault terminology ##
    # liquidatable: auction can be started (and owner can still perform actions)
    # seized: auction was started and debt > 0 (ie vault hasn't been returned to owner).
    #     a vault is seized iff an owner cannot perform any actions on it
    # in liquidation: seized, debt > 0 and collateral > 0
    #     a seized vault is in liquidation iff it is not in bad debt
    # in bad debt: seized, debt > 0 and collateral = 0
    # biddable: in liquidation, auction price > min price and auction TTL has not passed
    # restartable: in liquidation and auction TTL has passed.
    #     it's theoretically possible for a vault to be neither biddable nor restartable.
    #     this happens if auction price < min price but auction TTL has not passed.
    #     governance should set auction parameters so that this doesn't happen.

    @property
    def in_bad_debt(self) -> bool:
        """Indicates whether a vault has incurred bad debt."""
        if self.auction_state.listp():
            (
                auction_start_time,
                start_price,
                step_price_decrease_factor,
                step_time_interval,
                initiator_puzzle_hash,
                initiator_incentive_balance,
                auction_timeout,
                byc_to_treasury_balance,
                byc_to_melt_balance,
                minimum_bid_amount,
                _,
            ) = self.auction_state.as_iter()
            debt = (
                initiator_incentive_balance.as_int() + byc_to_treasury_balance.as_int() + byc_to_melt_balance.as_int()
            )
            if debt > 0 and self.collateral == 0:
                return True
        return False

    @property
    def in_liquidation(self) -> bool:
        """Indicates whether a vault is in liquidation, ie liquidation was started, vault still has debt, but is not in bad debt."""
        return not self.auction_state.nullp() and not self.in_bad_debt

    @property
    def seized(self) -> bool:
        """Vault is seized if it's in liquidation or bad debt.

        A seized vault is one that is no longer controlled by its owner.
        A vault is seized iff auction state != nil.
        """
        return self.in_liquidation or self.in_bad_debt

    def is_startable(
        self, current_cumulative_stability_fee_df: int, liquidation_ratio_pct: int, statutes_price: int
    ) -> bool:
        """Indicates whether liquidation auction can be started.

        If the vault is already in liquidation or has incurred bad debt it is not considered startable.
        """
        if self.seized:
            return False
        debt = self.get_debt(current_cumulative_stability_fee_df)
        min_collateral_amount = calculate_min_collateral_amount(debt, liquidation_ratio_pct, statutes_price)
        return min_collateral_amount > self.collateral

    def get_auction_ttl(self, current_time: int) -> Optional[int]:
        """If auction is underway, returns number of seconds left before timeout.
        If auction has timed out, return 0. If not in liquidation, returns None.
        """
        if not self.in_liquidation:
            return None
        auction_start_time = self.auction_state.first().as_int()
        auction_timeout = self.auction_state.at("rrrrrrf").as_int()
        return max(0, auction_timeout - (current_time - auction_start_time) + 1)

    def is_restartable(self, current_time: int) -> bool:
        """Indicates whether a liquidation auction can be restarted."""
        return self.get_auction_ttl(current_time) == 0

    def is_liquidatable(
        self,
        current_cumulative_stability_fee_df: int,
        liquidation_ratio_pct: int,
        statutes_price: int,
        current_time: int,
    ):
        """Indicates whether a liquidation auction can be started or restarted."""
        startable = self.is_startable(
            current_cumulative_stability_fee_df,
            liquidation_ratio_pct,
            statutes_price,
        )
        restartable = self.is_restartable(
            current_time,
        )
        return startable or restartable

    def is_biddable(self, current_time: int) -> bool:
        """Indicates whether bids can be placed in auction."""
        if not self.in_liquidation:
            return False
        min_price = self.auction_state.at("rrrrrrrrrrf").as_int()
        return not self.is_restartable(current_time) and self.get_auction_price(current_time) > min_price

    def get_auction_price(
        self,
        current_time: int,
        full_precision=False,
    ) -> Optional[int | float]:
        """Returns current liquidation auction price (in XCH/cBYC)"

        Auction price is returned as an int with precision PRICE_PRECISION, unless full_precision flag is set to True,
        in which case precision is PRICE_PRECISION * PRECISION_BPS.

        Returns None if no auction is underway, ie vault is not in liquidation or liquidation auction can be restarted.
        """
        if not self.in_liquidation or self.is_restartable(current_time):
            return None
        # liquidation auction is underway. calculate auction price
        (
            auction_start_time,
            start_price,
            step_price_decrease_factor,
            step_time_interval,
            initiator_puzzle_hash,
            initiator_incentive_balance_atom,
            auction_timeout,
            byc_to_treasury_balance_atom,
            byc_to_melt_balance_atom,
            minimum_bid_amount,
            min_price,
        ) = self.auction_state.as_iter()
        auction_start_time = auction_start_time.as_int()
        start_price = start_price.as_int()
        start_price_decrease_factor = step_price_decrease_factor.as_int()
        step_time_interval = step_time_interval.as_int()
        auction_price_bps = calculate_current_auction_price_bps(
            start_price,
            current_time,
            auction_start_time,
            start_price_decrease_factor,
            step_time_interval,
        )
        if full_precision:
            return auction_price_bps
        return auction_price_bps // PRECISION_BPS


def find_vault_operation(solution: Program) -> Optional[Program]:
    for condition in solution.as_iter():
        if condition.first().as_atom() == ConditionOpcode.REMARK.value:
            if condition.rest().first().as_atom() == SOLUTION_PREFIX:
                return condition.rest().rest().first()
    return None


def get_collateral_vault_info(
    vault_spend: CoinSpend,  # parent spend
    spend: bool = True,  # if False, return info for parent coin
    statutes_struct: Program = None,
    inner_puzzle_hash: Optional[bytes32] = None,
) -> CollateralVaultState:
    vault_puzzle = vault_spend.puzzle_reveal
    if spend:
        if statutes_struct is None:
            raise SpendError("statutes_struct must be provided")
        try:
            conditions = run(vault_puzzle, vault_spend.solution)
        except Exception as err:
            vault_solution_info = get_vault_solution_info(vault_spend)
            raise SpendError(
                f"Failed to get collateral vault info due to failed spend for unknown reason: {str(err)}. "
                f"{vault_puzzle=} {vault_spend.solution=} {vault_solution_info=}"
            )
        driver_info = get_driver_info(conditions, must_find_driver_info=False)
        if isinstance(driver_info, list):
            # driver info found
            collateral, principal, auction_state, inner_puzzle_hash, discounted_principal = driver_info
        else:
            # driver info not found. must have been vault launch spend
            assert driver_info is None
            assert inner_puzzle_hash is not None, (
                "must provide inner puzzle hash when attempting to get collateral vault info from vault launch spend"
            )
            collateral, principal, auction_state, inner_puzzle_hash, discounted_principal = Program.to(
                [0, 0, 0, inner_puzzle_hash, 0]
            ).as_iter()
    else:
        mod, args = uncurry(vault_puzzle)
        if mod.get_tree_hash() != COLLATERAL_VAULT_MOD_HASH:
            raise ValueError(
                f"Coin is not a collateral vault. Expected mod hash {COLLATERAL_VAULT_MOD_HASH}, got {mod.get_tree_hash().hex()}"
            )
        mod_hash, statutes_struct, collateral, principal, auction_state, inner_puzzle_hash, discounted_principal = (
            args.as_iter()
        )
        if mod_hash.atom != mod.get_tree_hash():
            raise ValueError(
                f"Collateral vault has incorrect mod hash curried. "
                f"Expected {COLLATERAL_VAULT_MOD_HASH.hex()}, got {mod_hash.hex()}"
            )

    state = CollateralVaultState(
        vault_mod_hash=COLLATERAL_VAULT_MOD_HASH,
        statutes_struct=statutes_struct,
        collateral=collateral.as_int(),
        principal=principal.as_int(),
        auction_state=auction_state,
        inner_puzzle_hash=bytes32(inner_puzzle_hash.atom),
        discounted_principal=discounted_principal.as_int(),
    )

    if spend:
        state.parent_name = vault_spend.coin.name()
        state.last_spend = vault_spend
        state.coin_name = compute_additions(vault_spend)[0].name()
    else:
        state.coin_name = vault_spend.coin.name()

    return state
