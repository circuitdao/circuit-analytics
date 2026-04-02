from __future__ import annotations

import logging
from dataclasses import dataclass

from chia.types.blockchain_format.program import Program, uncurry, run
from chia.types.coin_spend import CoinSpend
from chia_rs import Coin
from chia_rs.sized_bytes import bytes32

from circuit_analytics.drivers import AuctionStatus, get_driver_info
from circuit_analytics.drivers.condition_filtering import filter_and_extract_remark_solution
from circuit_analytics.drivers.protocol_math import PRECISION, PRECISION_BPS
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    CAT_MOD,
    PROGRAM_RECHARGE_BID_MOD,
    PROGRAM_RECHARGE_LAUNCH_MOD,
    PROGRAM_RECHARGE_SETTLE_MOD,
    PROGRAM_RECHARGE_START_AUCTION_MOD,
    RECHARGE_AUCTION_MOD_HASH,
)
from circuit_analytics.utils import to_list, to_tuple, to_type, unique_launcher_ids

log = logging.getLogger(__name__)


@dataclass
class RechargeSolutionInfo:
    inner_puzzle: Program
    inner_solution: Program
    operation: Program


@dataclass
class TreasuryCoinInfoRecharge:
    parent_id: bytes32
    launcher_id: bytes32
    ring_prev_launcher_id: bytes32
    current_amount: int


@dataclass
class RechargeLaunchInfo(RechargeSolutionInfo):
    statutes_inner_puzzle_hash: bytes32
    my_coin_id: bytes32


@dataclass
class RechargeStartInfo(RechargeSolutionInfo):
    statutes_inner_puzzle_hash: bytes32
    current_time: int
    auction_ttl: int
    bid_ttl: int
    min_crt_price: int
    min_byc_bid_amount: int
    min_price_increase_bps: int
    max_byc_bid_amount: int
    treasury_minimum: int
    treasury_coins: list[TreasuryCoinInfoRecharge]


@dataclass
class RechargeBidInfo(RechargeSolutionInfo):
    byc_bid_amount: int
    crt_bid_amount: int
    target_puzzle_hash: bytes32
    current_timestamp: int
    my_coin_id: bytes32


@dataclass
class RechargeSettleInfo(RechargeSolutionInfo):
    treasury_coins: list[TreasuryCoinInfoRecharge]
    funding_coin_parent_id: bytes32
    funding_coin_amount: int


@dataclass
class RechargeAuctionInfo:
    launcher_id: bytes32 | None
    auction_params: Program
    last_bid: Program

    # Auction params
    @property
    def start_time(self) -> int | None:
        return self.auction_params.first().as_int() if not self.auction_params.nullp() else None

    @property
    def auction_ttl(self) -> int | None:
        return self.auction_params.at("rf").as_int() if not self.auction_params.nullp() else None

    @property
    def bid_ttl(self) -> int | None:
        return self.auction_params.at("rrf").as_int() if not self.auction_params.nullp() else None

    @property
    def min_crt_price(self) -> int | None:
        return self.auction_params.at("rrrf").as_int() if not self.auction_params.nullp() else None

    @property
    def min_byc_bid_amount(self) -> int | None:
        return self.auction_params.at("rrrrf").as_int() if not self.auction_params.nullp() else None

    @property
    def min_price_increase_bps(self) -> int | None:
        return self.auction_params.at("rrrrrf").as_int() if not self.auction_params.nullp() else None

    @property
    def max_byc_bid_amount(self) -> int | None:
        return self.auction_params.at("rrrrrrf").as_int() if not self.auction_params.nullp() else None

    # Last bid
    @property
    def last_byc_bid_amount(self) -> int | None:
        if self.last_bid.nullp():
            return None
        return self.last_bid.at("ff").as_int()

    @property
    def last_crt_bid_amount(self) -> int | None:
        if self.last_bid.nullp():
            return None
        return self.last_bid.at("fr").as_int()

    @property
    def last_target_puzzle_hash(self) -> str | None:
        if self.last_bid.nullp():
            return None
        return bytes32(self.last_bid.at("rf").atom)

    @property
    def last_timestamp(self) -> int | None:
        if self.last_bid.nullp():
            return None
        return self.last_bid.at("rrf").as_int()

    @property
    def last_crt_price(self) -> int | None:
        """CRT price is an integer with unit 1/PRECISION BYC per CRT"""
        if self.last_bid.nullp():
            return None
        return (self.last_byc_bid_amount * PRECISION) // self.last_crt_bid_amount

    # Other
    @property
    def status(self) -> AuctionStatus | None:
        if self.launcher_id is None:
            return None
        if self.auction_params.nullp():
            return AuctionStatus.STANDBY
        else:
            return AuctionStatus.RUNNING

    def expired(self, current_timestamp: int) -> bool | None:
        """A running auction (ie auction was started) has expired if it is no longer possible to place a new bid"""
        if self.auction_params.nullp():
            log.debug("Auction not started yet.")
            return None  # Auction not started (coin in stand-by mode)
        log.debug(
            f"Auction started at {self.start_time}. Is expired: {current_timestamp > self.start_time + self.auction_ttl}"
        )
        return current_timestamp > self.start_time + self.auction_ttl or (
            not self.last_bid.nullp() and current_timestamp >= self.last_timestamp + self.bid_ttl
        )

    def can_be_settled(self, current_timestamp: int) -> bool | None:
        """An auction can be settled if at least one bid was placed and it has expired"""
        if self.auction_params.nullp():
            return None
        if self.last_bid.nullp():
            return False
        return self.expired(current_timestamp)

    def get_min_crt_price(self) -> int | None:
        """The minimum implicit CRT price required for a bid to be valid

        The value returned depends on the previous bid (if one).

        CRT price is an integer with unit 1/PRECISION BYC per CRT.
        """
        if self.auction_params.nullp():
            return None
        if self.last_bid.nullp():
            return self.min_crt_price + 1
        return (self.last_crt_price * (PRECISION_BPS + self.min_price_increase_bps)) // PRECISION_BPS + 1

    def get_min_byc_amount_to_bid(self) -> int | None:
        """The minimum amount of BYC that must be offered in a bid"""
        if self.auction_params.nullp():
            return None
        return self.min_byc_bid_amount + 1

    def get_max_crt_amount_to_request(self, byc_bid_amount: int | None = None) -> int | None:
        """The maximum amount of CRT that can be requested in a bid.

        This number depends on the amount of BYC bid. If set to None, min BYC amount to bid is assumed.
        """
        if self.auction_params.nullp():
            return None
        if byc_bid_amount is None:
            byc_bid_amount = self.get_min_byc_amount_to_bid()
        min_crt_price = self.get_min_crt_price()
        crt_amount = int(PRECISION * byc_bid_amount / min_crt_price)
        if (byc_bid_amount * PRECISION) // crt_amount > min_crt_price:
            while (byc_bid_amount * PRECISION) // crt_amount > min_crt_price:
                crt_amount += 1
            if (byc_bid_amount * PRECISION) // crt_amount < min_crt_price:
                crt_amount -= 1
        elif (byc_bid_amount * PRECISION) // crt_amount < min_crt_price:
            while (byc_bid_amount * PRECISION) // crt_amount < min_crt_price:
                crt_amount -= 1
        assert (byc_bid_amount * PRECISION) // crt_amount >= min_crt_price
        return crt_amount

    def get_max_byc_amount_to_bid(self) -> int | None:
        """The maximum amount of BYC that can be offered in a bid"""
        if self.auction_params.nullp():
            return None
        return self.max_byc_bid_amount - 1


def get_recharge_solution_info(
    coin: Coin, cat_inner_puzzle: Program, cat_inner_solution: Program
) -> RechargeSolutionInfo | None:
    """Takes Recharge Auction coin and CAT layer inner puzzle and inner solution and returns info."""
    mod, args = uncurry(cat_inner_puzzle)
    if mod.get_tree_hash() != RECHARGE_AUCTION_MOD_HASH:
        return None
    (
        mod_hash,
        statutes_struct,
        launcher_id,
        auction_params,
        last_bid,
    ) = to_list(args, 5, ["bytes32", None, None, None, None])
    to_type(launcher_id, "bytes32_or_nil")
    recharge_info = RechargeAuctionInfo(
        launcher_id=None if launcher_id.nullp() else bytes32(launcher_id.atom),
        auction_params=auction_params,
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
            f"Recharge auction hash of operation does not match signed operation hash. "
            f"Expected {signed_operation_hash.hex()}, got {operation_hash.hex()}"
        )
    if operation_hash == PROGRAM_RECHARGE_LAUNCH_MOD.get_tree_hash():
        (
            statutes_inner_puzzle_hash,
            my_coin_id,
        ) = to_list(op_args, 2, ["bytes32", "bytes32"])
        if recharge_info.launcher_id:
            raise SpendError(
                f"Recharge auction cannot be launched if launcher ID is set ({recharge_info.launcher_id.hex()})"
            )
        if not recharge_info.auction_params.nullp():
            raise SpendError(
                f"Recharge auction cannot be launched if auction params is set ({recharge_info.auction_params.as_bin().hex()})"
            )
        if not recharge_info.last_bid.nullp():
            raise SpendError(
                f"Recharge auction cannot be launched if last bid is set ({recharge_info.last_bid.as_bin().hex()})"
            )
        return RechargeLaunchInfo(
            inner_puzzle=inner_puzzle,
            inner_solution=inner_solution,
            operation=operation,
            # args
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            my_coin_id=my_coin_id,
        )
    elif operation_hash == PROGRAM_RECHARGE_START_AUCTION_MOD.get_tree_hash():
        to_list(lineage_proof, 3, ["bytes32", "int", "bytes32"])
        (
            statutes_inner_puzzle_hash,
            current_time,
            auction_ttl,
            bid_ttl,
            min_crt_price,
            min_byc_bid_amount,
            min_price_increase_bps,
            max_byc_bid_amount,
            treasury_minimum,
            treasury_coins,  # -> ((parent_id treasury_launcher_id ring_prev_launcher_id current_amount))
        ) = to_list(op_args, 10, ["bytes32", "int", "int", "int", "int", "int", "int", "int", "int", None])
        if not treasury_coins.list_len() > 0:
            raise SpendError("Recharge action start needs to be passed at least one treasury coin")
        if not recharge_info.launcher_id:
            raise SpendError("Recharge auction cannot be started if launcher ID is not set")
        if not recharge_info.last_bid.nullp():
            raise SpendError(
                f"Recharge auction cannot be started if last bid is set ({recharge_info.last_bid.as_bin().hex()})"
            )
        if not recharge_info.auction_params.nullp():
            if not (current_time - recharge_info.start_time > recharge_info.auction_ttl):
                raise SpendError(
                    f"Recharge auction that has been started but not yet received a bid can only be restarted "
                    f"if it has expired ({current_time - recharge_info.start_time} > {recharge_info.auction_ttl})"
                )
        treasury_coin_infos: list[TreasuryCoinInfoRecharge] = []
        total_balance = 0  # total balance of provided treasury coins
        for treasury_coin in treasury_coins.as_iter():
            (
                parent_id,
                treasury_launcher_id,
                ring_prev_launcher_id,
                current_amount,
            ) = to_list(treasury_coin, 4, ["bytes32", "bytes32", "bytes32", "int"])
            total_balance += current_amount
            treasury_coin_infos.append(
                TreasuryCoinInfoRecharge(
                    parent_id=parent_id,
                    launcher_id=treasury_launcher_id,
                    ring_prev_launcher_id=ring_prev_launcher_id,
                    current_amount=current_amount,
                )
            )
        duplicate = unique_launcher_ids(treasury_coins)
        if duplicate:
            raise SpendError(
                f"Recharge auction start must be passed unique treasury coins. Duplicate launcher ID: {duplicate.hex()}"
            )
        if not treasury_minimum > total_balance:
            raise SpendError(
                f"Recharge auction can only be started if treasury balance is below Treasury Minimum ({total_balance} < {treasury_minimum})"
            )
        return RechargeStartInfo(
            inner_puzzle=inner_puzzle,
            inner_solution=inner_solution,
            operation=operation,
            # args
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            current_time=current_time,
            auction_ttl=auction_ttl,
            bid_ttl=bid_ttl,
            min_crt_price=min_crt_price,
            min_byc_bid_amount=min_byc_bid_amount,
            min_price_increase_bps=min_price_increase_bps,
            max_byc_bid_amount=max_byc_bid_amount,
            treasury_minimum=treasury_minimum,
            treasury_coins=treasury_coin_infos,
        )
    elif operation_hash == PROGRAM_RECHARGE_BID_MOD.get_tree_hash():
        to_list(lineage_proof, 3, ["bytes32", "int", "bytes32"])
        (
            bid,
            target_puzzle_hash,
            current_timestamp,
            my_coin_id,
        ) = to_list(op_args, 4, [None, "bytes32", "int", "bytes32"])
        (
            byc_bid_amount,
            crt_bid_amount,
        ) = to_tuple(bid, 2, ["int", "int"])
        crt_price = (byc_bid_amount * PRECISION) // crt_bid_amount
        if recharge_info.auction_params.nullp():
            # this check is implicit in puzzle because AUCTION_PARAMS gets destructured
            raise SpendError(
                "Recharge auction bid can only be placed if auction has been started (ie AUCTION_PARAMS is not nil)"
            )
        if not recharge_info.launcher_id:
            raise SpendError("Recharge auction bid can only be placed if launcher ID is set")
        if not byc_bid_amount > 0:
            raise SpendError(f"Recharge auction bid must have positive BYC bid amount ({byc_bid_amount} > 0)")
        if not crt_bid_amount > 0:
            raise SpendError(f"Recharge auction bid must have positive CRT bid amount ({crt_bid_amount} > 0)")
        if not crt_price > recharge_info.min_crt_price:
            raise SpendError(
                f"Recharge auction bid must have CRT price greater than MIN_CRT_PRICE ({crt_price} > {recharge_info.min_crt_price})"
            )
        if recharge_info.last_crt_price is not None:
            # there's been a previous bid
            min_increased_crt_price = (
                recharge_info.last_crt_price * (PRECISION_BPS + recharge_info.min_price_increase_bps)
            ) // PRECISION_BPS
            if not crt_price > min_increased_crt_price:
                raise SpendError(
                    f"Recharge auction bid must have CRT price greater than min increased CRT price ({crt_price} > {min_increased_crt_price})"
                )
        if (recharge_info.min_byc_bid_amount is not None) and not byc_bid_amount > recharge_info.min_byc_bid_amount:
            raise SpendError(
                f"Recharge auction bid must have BYC bid amount greater than min BYC bid amount curried "
                f"in auction state ({byc_bid_amount} > {recharge_info.min_byc_bid_amount})"
            )
        if (recharge_info.max_byc_bid_amount is not None) and not byc_bid_amount < recharge_info.max_byc_bid_amount:
            raise SpendError(
                f"Recharge auction bid must have BYC bid amount less than max BYC bid amount curried"
                f"in auction state ({byc_bid_amount} < {recharge_info.max_byc_bid_amount})"
            )
        if not recharge_info.auction_ttl > current_timestamp - recharge_info.start_time:
            raise SpendError(
                f"Recharge auction bid cannot be placed if auction has timed out "
                f"({recharge_info.auction_ttl} > {current_timestamp - recharge_info.start_time})"
            )
        if (
            not recharge_info.last_bid.nullp()
        ) and not recharge_info.bid_ttl > current_timestamp - recharge_info.last_timestamp:
            raise SpendError(
                f"Recharge auction bid cannot be placed if previous bid has timed out "
                f"({recharge_info.bid_ttl} > {current_timestamp - recharge_info.last_timestamp})"
            )
        return RechargeBidInfo(
            inner_puzzle=inner_puzzle,
            inner_solution=inner_solution,
            operation=operation,
            # args
            byc_bid_amount=byc_bid_amount,
            crt_bid_amount=crt_bid_amount,
            target_puzzle_hash=target_puzzle_hash,
            current_timestamp=current_timestamp,
            my_coin_id=my_coin_id,
        )
    elif operation_hash == PROGRAM_RECHARGE_SETTLE_MOD.get_tree_hash():
        to_list(lineage_proof, 3, ["bytes32", "int", "bytes32"])
        (
            treasury_coins,  # -> ((parent_id treasury_launcher_id ring_prev_launcher_id current_amount))
            funding_coin_info,
        ) = to_list(op_args, 2, [None, None])
        (
            funding_coin_parent_id,
            funding_coin_amount,
        ) = to_tuple(funding_coin_info, 2, ["bytes32", "int"])
        if not treasury_coins.list_len() > 0:
            raise SpendError("Recharge action settle operation needs to be passed at least one treasury coin")
        treasury_coin_infos: list[TreasuryCoinInfoRecharge] = []
        for treasury_coin in treasury_coins.as_iter():
            (
                parent_id,
                treasury_launcher_id,
                ring_prev_launcher_id,
                current_amount,
            ) = to_list(treasury_coin, 4, ["bytes32", "bytes32", "bytes32", "int"])
            treasury_coin_infos.append(
                TreasuryCoinInfoRecharge(
                    parent_id=parent_id,
                    launcher_id=treasury_launcher_id,
                    ring_prev_launcher_id=ring_prev_launcher_id,
                    current_amount=current_amount,
                )
            )
        duplicate = unique_launcher_ids(treasury_coins)
        if duplicate:
            raise SpendError(
                f"Recharge auction settle operation must be passed unique treasury coins. Duplicate launcher ID: {duplicate.hex()}"
            )
        if recharge_info.last_bid.nullp():
            raise SpendError("Rechange auction cannot be settled since no bid was placed")
        return RechargeBidInfo(
            inner_puzzle=inner_puzzle,
            inner_solution=inner_solution,
            operation=operation,
            # args
            treasury_coins=treasury_coin_infos,
            funding_coin_parent_id=funding_coin_parent_id,
            funding_coin_amount=funding_coin_amount,
        )
    else:
        raise SpendError(f"Unknown recharge auction operation. Operation hash: {operation_hash.hex()}")


def get_recharge_info(
    recharge_spend: CoinSpend,  # parent spend
    spend: bool = True,  # if False, return info for parent coin
) -> RechargeAuctionInfo:
    puzzle = recharge_spend.puzzle_reveal
    if spend:
        conditions = run(puzzle, Program.from_serialized(recharge_spend.solution))
        launcher_id, auction_params, last_bid = get_driver_info(conditions)
    else:
        cat_mod, args = uncurry(puzzle)
        assert cat_mod == CAT_MOD
        cat_inner_puzzle = args.at("rrf")
        _, _, launcher_id, auction_params, last_bid = uncurry(cat_inner_puzzle)[1].as_iter()

    launcher_id = bytes32(launcher_id.atom) if not launcher_id.nullp() else None
    return RechargeAuctionInfo(launcher_id, auction_params, last_bid)
