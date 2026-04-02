from __future__ import annotations

import logging
from dataclasses import dataclass

from chia.types.blockchain_format.program import Program, uncurry, run
from chia.types.coin_spend import CoinSpend
from chia_rs import Coin
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint64

from circuit_analytics.drivers import AuctionStatus, get_driver_info
from circuit_analytics.drivers.condition_filtering import filter_and_extract_remark_solution
from circuit_analytics.drivers.protocol_math import PRECISION_BPS
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    CAT_MOD,
    CAT_MOD_HASH,
    PAYOUT_MOD_HASH,
    PROGRAM_SURPLUS_BID_MOD,
    PROGRAM_SURPLUS_SETTLE_MOD,
    PROGRAM_SURPLUS_START_AUCTION_MOD,
    SURPLUS_AUCTION_MOD_HASH,
)
from circuit_analytics.utils import to_list, to_tuple, to_type, tree_hash_of_apply, unique_launcher_ids

log = logging.getLogger(__name__)


@dataclass
class PayoutSolutionInfo:
    inner_puzzle: Program
    inner_solution: Program
    surplus_auction_parent_id: bytes32
    surplus_auction_curried_args_hash: bytes32
    surplus_auction_amount: int
    puzzle_hash: bytes32 | None
    amount: int
    inner_conditions: Program

    def surplus_auction_coin_id(self, crt_tail_hash: bytes32) -> bytes32:
        surplus_auction_inner_puzzle_hash = tree_hash_of_apply(
            SURPLUS_AUCTION_MOD_HASH, self.surplus_auction_curried_args_hash
        )
        surplus_auction_puzzle_hash = CAT_MOD.curry(
            CAT_MOD_HASH, crt_tail_hash, surplus_auction_inner_puzzle_hash
        ).get_tree_hash_precalc(surplus_auction_inner_puzzle_hash)
        return Coin(self.surplus_auction_parent_id, surplus_auction_puzzle_hash, self.surplus_auction_amount).name()


@dataclass
class PayoutFundInfo(PayoutSolutionInfo):
    pass


@dataclass
class PayoutPayOutInfo(PayoutSolutionInfo):
    pass


def get_payout_solution_info(
    coin: Coin, cat_inner_puzzle: Program, cat_inner_solution: Program
) -> PayoutSolutionInfo | None:
    """Takes Payout coin and CAT layer inner puzzle and inner solution and returns info."""
    mod, curried_args = uncurry(cat_inner_puzzle)
    if mod.get_tree_hash() != PAYOUT_MOD_HASH:
        return None
    (
        mod_hash,
        crt_tail_hash,
        surplus_auction_mod_hash,
        surplus_auction_launcher_id,
    ) = to_list(curried_args, 4, ["bytes32", "bytes32", "bytes32", "bytes32"])
    (
        inner_puzzle,
        inner_solution,
    ) = to_list(cat_inner_solution, 2)
    input_conditions = inner_puzzle.run(inner_solution)
    solution_remark_body, filtered_conditions = filter_and_extract_remark_solution(list(input_conditions.as_iter()))
    (
        surplus_auction_parent_id,
        surplus_auction_curried_args_hash,
        surplus_auction_amount,
        puzzle_hash,
        amount,
    ) = to_list(solution_remark_body, 5, ["bytes32", "bytes32", "uint", "bytes32_or_none", "uint"])
    solution_info = PayoutSolutionInfo(
        surplus_auction_parent_id=surplus_auction_parent_id,
        surplus_auction_curried_args_hash=surplus_auction_curried_args_hash,
        surplus_auction_amount=surplus_auction_amount,
        puzzle_hash=puzzle_hash,
        amount=amount,
        inner_conditions=filtered_conditions,
    )
    surplus_auction_coin_id = solution_info.surplus_auction_coin_id(crt_tail_hash)
    if surplus_auction_coin_id == surplus_auction_launcher_id and not puzzle_hash:
        return PayoutFundInfo(**solution_info.__dict__)
    else:
        return PayoutPayOutInfo(**solution_info.__dict__)


@dataclass
class SurplusSolutionInfo:
    inner_puzzle: Program
    inner_solution: Program
    operation: Program

    @property
    def operation_hash(self) -> bytes32:
        return self.operation.get_tree_hash()


@dataclass
class TreasuryCoinInfoSurplus:
    parent_id: bytes32
    launcher_id: bytes32
    ring_prev_launcher_id: bytes32
    current_amount: int
    withdraw_amount: int

    @property
    def new_amount(self) -> int:
        return self.current_amount - self.withdraw_amount


@dataclass
class SurplusStartInfo(SurplusSolutionInfo):
    statutes_inner_puzzle_hash: bytes32
    payout_coin_parent_id: bytes32
    lot_amount: int
    my_coin_id: bytes32
    bid_ttl: int
    min_price_increase_bps: int
    treasury_coins: list[TreasuryCoinInfoSurplus]
    treasury_maximum: int


@dataclass
class SurplusBidInfo(SurplusSolutionInfo):
    crt_bid_amount: int
    target_puzzle_hash: bytes32
    current_timestamp: int
    my_amount: int
    my_coin_id: bytes32


@dataclass
class SurplusSettleInfo(SurplusSolutionInfo):
    statutes_inner_puzzle_hash: bytes32
    payout_coin_parent_id: bytes32
    my_amount: int
    my_coin_id: bytes


def get_surplus_solution_info(
    coin: Coin, cat_inner_puzzle: Program, cat_inner_solution: Program
) -> SurplusSolutionInfo | None:
    """Takes Surplus Auction coin and CAT layer inner puzzle and inner solution and returns info."""
    mod, args = uncurry(cat_inner_puzzle)
    if mod.get_tree_hash() != SURPLUS_AUCTION_MOD_HASH:
        return None
    (
        mod_hash,
        statutes_struct,
        launcher_id,
        bid_ttl,
        min_price_increase_bps,
        byc_lot_amount,
        last_bid,
    ) = to_list(args, 7, ["bytes32", None, None, "int", "int", "int", None])
    to_type(launcher_id, "bytes32_or_nil")
    surplus_info = SurplusAuctionInfo(
        launcher_id=None if launcher_id.nullp() else bytes32(launcher_id.atom),
        bid_ttl=bid_ttl,
        min_price_increase_bps=min_price_increase_bps,
        lot_amount=byc_lot_amount,
        bid_crt_amount=coin.amount,
        last_bid=last_bid,
    )
    (
        inner_puzzle,
        inner_solution,
        operation,
    ) = to_list(cat_inner_solution, 3)
    input_conditions = inner_puzzle.run(inner_solution)
    solution_remark_body, filtered_conditions = filter_and_extract_remark_solution(list(input_conditions.as_iter()))
    (
        lineage_proof,
        signed_operation_hash,
        op_args,
    ) = to_list(solution_remark_body, 3, [None, "bytes32", None])
    operation_hash = operation.get_tree_hash()
    if not operation_hash == signed_operation_hash:
        raise SpendError(
            f"Surplus auction hash of operation does not match signed operation hash. "
            f"Expected {signed_operation_hash.hex()}, got {operation_hash.hex()}"
        )
    if operation_hash == PROGRAM_SURPLUS_START_AUCTION_MOD.get_tree_hash():
        (
            statutes_inner_puzzle_hash,
            payout_coin_parent_id,
            lot_amount,
            my_coin_id,
            bid_ttl,
            min_price_increase_bps,
            treasury_coins,  # -> ((parent_id treasury_launcher_id ring_prev_launcher_id current_amount withdraw_amount))
            treasury_maximum,
        ) = to_list(op_args, 8, ["bytes32", "bytes32", "int", "bytes32", "int", "int", None, "int"])
        if not treasury_coins.list_len() > 0:
            raise SpendError("Surplus auction start needs to be passed at least one treasury coin")
        treasury_coin_infos: list[TreasuryCoinInfoSurplus] = []
        total_balance = 0  # total balance of provided treasury coins
        total_withdraw_amount = 0
        for treasury_coin in treasury_coins.as_iter():
            (
                parent_id,
                treasury_launcher_id,
                ring_prev_launcher_id,
                current_amount,
                withdraw_amount,
            ) = to_list(treasury_coin, 5, ["bytes32", "bytes32", "bytes32", "int", "int"])
            total_balance += current_amount
            total_withdraw_amount += withdraw_amount
            treasury_coin_infos.append(
                TreasuryCoinInfoSurplus(
                    parent_id=parent_id,
                    launcher_id=treasury_launcher_id,
                    ring_prev_launcher_id=ring_prev_launcher_id,
                    current_amount=current_amount,
                    withdraw_amount=withdraw_amount,
                )
            )
        duplicate = unique_launcher_ids(treasury_coins)
        if duplicate:
            raise SpendError(
                f"Surplus auction start must be passed unique treasury coins. Duplicate launcher ID: {duplicate.hex()}"
            )
        new_total_balance = (
            total_balance - total_withdraw_amount
        )  # total post-withdraw balance of provided treasury coins
        if not total_withdraw_amount == lot_amount:
            raise SpendError(
                f"Surplus auction start must withdraw an amount from Treasury "
                f"equal to the Surplus Auction Lot Amount ({total_withdraw_amount} == {lot_amount})"
            )
        if not new_total_balance > treasury_maximum:
            raise SpendError(
                f"Surplus auction start must leave total balance of post-withdrawal "
                f"treasury coins above Treasury Maximum ({new_total_balance} > {treasury_maximum})"
            )
        return SurplusStartInfo(
            inner_puzzle=inner_puzzle,
            inner_solution=inner_solution,
            operation=operation,
            # args
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            payout_coin_parent_id=payout_coin_parent_id,
            lot_amount=lot_amount,
            my_coin_id=my_coin_id,
            bid_ttl=bid_ttl,
            min_price_increase_bps=min_price_increase_bps,
            treasury_coins=treasury_coin_infos,
            treasury_maximum=treasury_maximum,
        )
    elif operation_hash == PROGRAM_SURPLUS_BID_MOD.get_tree_hash():
        to_list(lineage_proof, 3, ["bytes32", "int", "bytes32"])
        (
            crt_bid_amount,
            target_puzzle_hash,
            current_timestamp,
            my_amount,
            my_coin_id,
        ) = to_list(op_args, 5, ["int", "bytes32", "int", "int", "bytes32"])
        if surplus_info.last_bid.nullp():
            last_target_puzzle_hash = Program.to(None)
            last_bid_timestamp = 0
        else:
            (
                last_target_puzzle_hash,
                last_bid_timestamp,
            ) = to_tuple(surplus_info.last_bid, 2, ["bytes32", "int"])
        if not any(
            [
                all([my_amount == 0, crt_bid_amount > 0]),
                crt_bid_amount > my_amount,
            ]
        ):
            raise SpendError(
                f"Surplus auction bid amount must be larger than pervious bid amount (if any) or otherwise positive ({crt_bid_amount} > {my_amount} >= 0)"
            )
        if not any(
            [
                not last_bid_timestamp,
                surplus_info.bid_ttl > current_timestamp - last_bid_timestamp,
            ]
        ):
            raise SpendError(
                f"Surplus auction bid has expired ({surplus_info.bid_ttl} > {current_timestamp - last_bid_timestamp}). "
            )
        min_crt_bid_amount = (my_amount * (PRECISION_BPS + surplus_info.min_price_increase_bps)) // PRECISION_BPS
        if not crt_bid_amount > min_crt_bid_amount:
            raise SpendError(
                f"Surplus auction bid must increase bid mount by at least "
                f"the Auction Minimum Price Increase ({crt_bid_amount} > {min_crt_bid_amount})"
            )
        return SurplusBidInfo(
            inner_puzzle=inner_puzzle,
            inner_solution=inner_solution,
            operation=operation,
            # args
            crt_bid_amount=crt_bid_amount,
            target_puzzle_hash=target_puzzle_hash,
            current_timestamp=current_timestamp,
            my_amount=my_amount,
            my_coin_id=my_coin_id,
        )
    elif operation_hash == PROGRAM_SURPLUS_SETTLE_MOD.get_tree_hash():
        to_list(lineage_proof, 3, ["bytes32", "int", "bytes32"])
        (
            statutes_inner_puzzle_hash,
            payout_coin_parent_id,
            my_amount,
            my_coin_id,
        ) = to_list(op_args, 4, ["bytes32", "bytes32", "int", "bytes32"])
        return SurplusSettleInfo(
            inner_puzzle=inner_puzzle,
            inner_solution=inner_solution,
            operation=operation,
            # args
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            payout_coin_parent_id=payout_coin_parent_id,
            my_amount=my_amount,
            my_coin_id=my_coin_id,
        )
    else:
        raise SpendError(f"Unknown surplus auction operation. Operation hash: {operation_hash.hex()}")


@dataclass
class SurplusAuctionInfo:
    launcher_id: bytes32 | None
    bid_ttl: int
    min_price_increase_bps: int
    lot_amount: uint64
    bid_crt_amount: uint64
    last_bid: Program

    @property
    def last_target_puzzle_hash(self) -> str | None:
        if self.last_bid.nullp():
            return None
        return bytes32(self.last_bid.first().atom)

    @property
    def last_timestamp(self) -> int | None:
        if self.last_bid.nullp():
            return None
        return self.last_bid.rest().as_int()

    @property
    def bid_expires_at(self) -> int | None:
        if self.last_bid.nullp():
            return None
        return self.last_timestamp + self.bid_ttl

    def bid_expires_in(self, current_timestamp: int) -> int | None:
        if self.last_bid.nullp():
            return None
        return self.bid_expires_at - current_timestamp

    @property
    def status(self) -> AuctionStatus | None:
        if self.launcher_id is None:
            return None
        else:
            return AuctionStatus.RUNNING

    def expired(self, current_timestamp: int) -> bool:
        """A running auction (ie auction was started) has expired if it is no longer possible to place a new bid"""
        if self.last_bid.nullp():
            return False  # Auction never expires if no bid placed
        return current_timestamp >= self.last_timestamp + self.bid_ttl

    def can_be_settled(self, current_timestamp: int) -> bool:
        """An auction can be settled if at least one bid was placed and it has expired"""
        if self.last_bid.nullp():
            return False
        return self.expired(current_timestamp)

    def get_min_crt_amount_to_bid(self) -> int | None:
        """The minimum amount of CRT that must be bid.

        The value returned depends on the previous bid (if one).
        """
        if self.last_bid.nullp():
            return 1
        return (self.bid_crt_amount * (PRECISION_BPS + self.min_price_increase_bps)) // PRECISION_BPS + 1


def get_surplus_info(
    surplus_spend: CoinSpend,  # parent spend
    spend=True,  # if False, get state by uncurrying parent coin
):
    puzzle = surplus_spend.puzzle_reveal
    if spend:
        conditions = run(puzzle, surplus_spend.solution)
        try:
            launcher_id, bid_ttl, min_price_increase_bps, lot_amount, bid_crt_amount, last_bid = get_driver_info(
                conditions
            )
        except ValueError:
            return None

    else:
        cat_mod, args = uncurry(puzzle)
        assert cat_mod == CAT_MOD
        cat_inner_puzzle = args.at("rrf")
        (_, _, launcher_id, bid_ttl, min_price_increase_bps, lot_amount, last_bid) = uncurry(cat_inner_puzzle)[
            1
        ].as_iter()
        bid_crt_amount = Program.to(surplus_spend.coin.amount)

    return SurplusAuctionInfo(
        launcher_id.atom,
        bid_ttl.as_int(),
        min_price_increase_bps.as_int(),
        lot_amount.as_int(),
        uint64(bid_crt_amount.as_int()),
        last_bid,
    )
