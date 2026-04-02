from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Optional

from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.types.blockchain_format.program import Program, run_with_cost, uncurry
from chia_rs.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend
from chia.types.condition_opcodes import ConditionOpcode
from chia_rs.sized_ints import uint64
from chia.wallet.util.curry_and_treehash import calculate_hash_of_quoted_mod_hash, curry_and_treehash, shatree_atom
from clvm.casts import int_to_bytes

from circuit_analytics.drivers import PROTOCOL_PREFIX
from circuit_analytics.drivers.condition_filtering import filter_and_extract_unique_create_coin
from circuit_analytics.drivers.protocol_math import PRECISION, calculate_accrued_interest, calculate_cumulative_discount_factor
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import SAVINGS_VAULT_MOD, SAVINGS_VAULT_MOD_HASH
from circuit_analytics.utils import MAX_TX_BLOCK_TIME, to_list, to_tuple


log = logging.getLogger(__name__)


@dataclass
class SavingsSolutionInfo:
    inner_solution: Program
    new_inner_puzzle_hash: bytes32
    new_amount: int
    args_and_memos: Program  # memos in puzzle
    # args
    lineage_proof: Program
    statutes_inner_puzzle_hash: bytes32
    current_amount: int
    statutes_cumulative_interest_df: int
    current_timestamp: int
    current_interest_df: int
    price_info: tuple[int, int]
    min_treasury_delta: int
    treasury_coin_info: Program
    memos: Program

    def lineage_proof_info(self) -> list[bytes32, int, int, bytes32] | Program:
        if self.lineage_proof.nullp():
            return self.lineage_proof
        return to_list(self.lineage_proof, 4, ["bytes32", "int", "int", "bytes32"])

    def treasury_coin(self) -> list["bytes32", "bytes32", "bytes32", "int", "int"]:
        if self.treasury_coin_info.nullp():
            return self.treasury_coin_info
        return to_list(self.treasury_coin_info, 5, ["bytes32", "bytes32", "bytes32", "uint64", "uint64"])

    def interest_payment(self) -> int:
        if self.treasury_coin_info.nullp():
            return 0
        return self.treasury_coin()[4]

    def is_deposit(self) -> bool:
        if self.new_amount - self.interest_payment() > self.current_amount:
            return True
        return False

    def is_withdrawal(self) -> bool:
        if self.new_amount - self.interest_payment() < self.current_amount:
            return True
        return False


def calculate_interest(discounted_balance: int, principal: int, cumulative_interest_df: int) -> int:
    """Equivalent of calculate-interest function in savings_vault.clsp"""
    return (discounted_balance * cumulative_interest_df) // PRECISION - principal


def get_savings_solution_info(
    coin, cat_inner_puzzle: Program, cat_inner_solution: Program
) -> SavingsSolutionInfo | None:
    """Takes saving vault coin, CAT layer inner puzzle and inner solution and returns info."""
    mod, args = uncurry(cat_inner_puzzle)
    if mod.get_tree_hash() != SAVINGS_VAULT_MOD_HASH:
        return None
    (
        mod_hash,
        statutes_struct,
        discounted_balance,
        inner_puzzle,
    ) = to_list(args, 4, ["bytes32", None, "int", None])
    savings_info = SavingsVaultInfo(
        discounted_balance,
        inner_puzzle.get_tree_hash(),
        coin.amount,
    )
    inner_solution = cat_inner_solution.first()
    input_conditions = inner_puzzle.run(inner_solution)
    create_coin_body, conditions = filter_and_extract_unique_create_coin(list(input_conditions.as_iter()))
    (
        new_inner_puzzle_hash,
        new_amount,
        args_and_memos,
    ) = to_list(create_coin_body, 3, ["bytes32", "uint64", None])
    if new_amount < 0:
        raise SpendError(f"Savings vault spend must result in non-negative savings balance ({new_amount} >= 0)")
    (
        lineage_proof,
        statutes_inner_puzzle_hash,
        current_amount,
        statutes_cumulative_interest_df,
        current_timestamp,
        current_interest_df,
        price_info,
        min_treasury_delta,
        treasury_coin_info,
        memos,  # memos for CREATE_COIN condition
    ) = to_list(args_and_memos, 10, [None, "bytes32", "uint64", "int", "uint64", "int", None, "int", None, None])
    if lineage_proof.nullp():
        # eve lineage proof
        if current_amount:
            raise SpendError(f"Savings vault eve coin must have 0 amount ({current_amount} = 0)")
        if savings_info.discounted_balance:
            raise SpendError(
                f"Savings vault eve coin must have 0 discounted balance ({savings_info.discounted_balance} = 0)"
            )
    else:
        # non-eve lineage proof
        (
            parent_parent_id,
            parent_amount,
            parent_discounted_balance,
            parent_inner_puzzle_hash,
        ) = to_list(lineage_proof, 4, ["bytes32", "int", "int", "bytes32"])
    (
        price,
        price_timestamp,
    ) = to_tuple(price_info, 2, ["int", "int"])
    if not treasury_coin_info.nullp():
        (
            treasury_coin_parent_id,
            treasury_coin_launcher_id,
            treasury_coin_prev_launcher_id,
            treasury_coin_amount,
            treasury_withdraw_amount,  # amount of interest to be paid
        ) = to_list(treasury_coin_info, 5, ["bytes32", "bytes32", "bytes32", "uint64", "uint64"])
        cc_ir_df = calculate_cumulative_discount_factor(
            statutes_cumulative_interest_df,
            current_interest_df,
            price_timestamp,
            current_timestamp - 3 * MAX_TX_BLOCK_TIME,
        )
        accrued_interest = calculate_interest(savings_info.discounted_balance, current_amount, cc_ir_df)
        interest_payment = treasury_withdraw_amount
    else:
        accrued_interest = 0
        interest_payment = 0
    if not interest_payment >= 0:
        raise SpendError(f"Savings vault interest payment must be non-negative ({interest_payment} >= 0)")
    if not treasury_coin_info.nullp():
        if not accrued_interest >= interest_payment:
            raise SpendError(
                f"Savings vault interest payment must not be larger than accrued interest ({interest_payment} <= {accrued_interest})"
            )
        if not treasury_coin_amount >= interest_payment:
            raise SpendError(
                f"Savings vault interest payment must not be larger than treasury coin amount ({interest_payment} <= {treasury_coin_amount})"
            )
        if not interest_payment > min_treasury_delta:
            raise SpendError(
                f"Savings vault interest payment must be greater than min treasury delta ({interest_payment} > {min_treasury_delta})"
            )
    return SavingsSolutionInfo(
        inner_solution=inner_solution,
        new_inner_puzzle_hash=new_inner_puzzle_hash,
        new_amount=new_amount,
        args_and_memos=args_and_memos,
        # args
        lineage_proof=lineage_proof,
        statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
        current_amount=current_amount,
        statutes_cumulative_interest_df=statutes_cumulative_interest_df,
        current_timestamp=current_timestamp,
        current_interest_df=current_interest_df,
        price_info=(price, price_timestamp),
        min_treasury_delta=min_treasury_delta,
        treasury_coin_info=treasury_coin_info,
        memos=memos,
    )


@dataclass
class SavingsVaultInfo:
    discounted_balance: uint64
    inner_puzzle_hash: bytes32
    amount: uint64

    @classmethod
    def from_coin_spend(cls, coin_spend: CoinSpend, from_puzzle: bool = False) -> Optional[SavingsVaultInfo]:
        if not from_puzzle:
            _, conditions = run_with_cost(
                coin_spend.puzzle_reveal,
                DEFAULT_CONSTANTS.MAX_BLOCK_COST_CLVM,
                coin_spend.solution,
            )
            for condition in conditions.as_iter():
                if (
                    condition.first() == ConditionOpcode.REMARK
                    and condition.rest().first().as_atom() == PROTOCOL_PREFIX
                ):
                    discounted_balance, amount, inner_puzzle_hash, memos = list(condition.rest().rest().as_iter())
                    # TODO: check that the savings vault has the correct puzzle hash (in particular that we are on correct protocol),
                    #  ie that it wasn't faked, eg by a malicious parent coin that created a protocol remark condition to fake a genuine driver hint.
                    break
            else:
                raise ValueError("No protocol REMARK condition found, can't reveal")
        else:
            _, cat_args = uncurry(coin_spend.puzzle_reveal)
            cat_inner_puzzle = cat_args.at("rrf")
            mod, args = uncurry(cat_inner_puzzle)
            if mod == SAVINGS_VAULT_MOD:
                # TODO: check statutes_struct so that we know we are on right protocol
                _, _, discounted_balance, inner_puzzle = args.as_iter()
                inner_puzzle_hash = Program.to(inner_puzzle.get_tree_hash())
                amount = Program.to(coin_spend.coin.amount)
            else:
                # this was not a savings vault spend (at best a savings vault launch spend, or not related to savings vaults at all)
                return None  # TODO: raise instead?
        return cls(
            discounted_balance.as_int(), bytes32(inner_puzzle_hash.as_atom()), uint64(amount.as_int())
        )

    def accrued_interest(self, cumulative_interest_rate_df: int) -> int:
        """Calculate accrued interest of vault.

        Here, cumulative_interest_rate_df should have been computed using calculate_cumulative_discount_factor with 3x MAX_TX_BLOCK_TIME
        deducted from timestamp_current.
        """
        return calculate_accrued_interest(self.discounted_balance, self.amount, cumulative_interest_rate_df)


def get_savings_operation_info(coin_spend: CoinSpend) -> dict:
    cat_mod, cat_args = uncurry(coin_spend.puzzle_reveal)
    savings_puzzle = cat_args.at("rrf")
    mod, args = uncurry(savings_puzzle)
    assert mod == SAVINGS_VAULT_MOD
    discounted_balance = args.at("rrf").as_int()
    inner_puzzle = args.at("rrrf")
    inner_solution = Program.from_serialized(coin_spend.solution).at("ff")
    inner_conditions = inner_puzzle.run(inner_solution)
    for cond in inner_conditions.as_iter():
        if cond.first().atom == ConditionOpcode.CREATE_COIN:
            new_savings_puzzle_hash = bytes32(cond.at("rf").atom)
            new_savings_balance = cond.at("rrf").as_int()
            op_args = cond.at("rrrf")
            # savings spends must output excatly one CREATE_COIN condition
            # from inner puzzle, so we're done here
            break
    (
        lineage_proof,
        statutes_inner_puzzle_hash,
        current_amount,
        statutes_cumulative_interest_df,
        current_timestamp,
        current_interest_df,
        price_info,
        min_treasury_delta,
        treasury_coin_info,
        memos,
    ) = [arg.as_int() if idx not in [0, 1, 6, 8, 9] else arg for idx, arg in enumerate(op_args.as_iter())]
    current_cumulative_ir_df = calculate_cumulative_discount_factor(
        statutes_cumulative_interest_df,
        current_interest_df,
        price_info.rest().as_int(),
        current_timestamp - 3 * MAX_TX_BLOCK_TIME,
    )
    if not treasury_coin_info.nullp():
        (
            treasury_coin_parent_id,
            treasury_coin_launcher_id,
            treasury_coin_prev_launcher_id,
            treasury_coin_amount,
            treasury_withdrawal_amount,
        ) = [bytes32(arg.atom) if idx < 3 else arg.as_int() for idx, arg in enumerate(treasury_coin_info.as_iter())]
        accrued_interest = calculate_accrued_interest(
            discounted_balance, coin_spend.coin.amount, current_cumulative_ir_df
        )
        interest_payment = treasury_withdrawal_amount
        assert treasury_coin_amount >= interest_payment, (
            "Something is wrong. Treasury coin amount must be larger than interest payment"
        )
        assert accrued_interest >= interest_payment, (
            "Something is wrong. Accrued interest must be larger than interest payment"
        )
    else:
        interest_payment = 0
    savings_info = SavingsVaultInfo.from_coin_spend(coin_spend)
    balance_delta = new_savings_balance - coin_spend.coin.amount
    discounted_balance_delta = savings_info.discounted_balance - discounted_balance
    return dict(
        type="spend",
        balance_delta=balance_delta,
        discounted_balance_delta=discounted_balance_delta,
        interest_payment=interest_payment,
    )


def get_savings_puzzle_hash(statutes_struct: Program, discounted_balance: int, inner_puzzle_hash: bytes32) -> bytes32:
    return curry_and_treehash(
        calculate_hash_of_quoted_mod_hash(SAVINGS_VAULT_MOD_HASH),
        shatree_atom(SAVINGS_VAULT_MOD_HASH),
        statutes_struct.get_tree_hash(),
        shatree_atom(int_to_bytes(discounted_balance)),
        inner_puzzle_hash,
    )
