from __future__ import annotations

import os
import logging
from dataclasses import dataclass

from chia.types.blockchain_format.program import Program, uncurry, run
from chia.types.condition_opcodes import ConditionOpcode
from chia_rs.sized_bytes import bytes32
from chia_rs.sized_ints import uint32
from chia.wallet.util.curry_and_treehash import curried_values_tree_hash
from chia_rs import Coin, CoinSpend

from circuit_analytics.drivers import PROTOCOL_PREFIX, SOLUTION_PREFIX
from circuit_analytics.drivers.condition_filtering import extract_solution_from_remark
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    ANNOUNCER_REGISTRY_MOD_RAW,
    ATOM_ANNOUNCER_MOD_HASH,
    CAT_MOD_HASH,
    CRT_TAIL_MOD_HASH,
    RUN_TAIL_MOD_HASH,
    OFFER_MOD_HASH,
)
from circuit_analytics.utils import (
    to_list,
    to_tuple,
    to_type,
    tree_hash_of_apply,
)


REGISTRY_REGISTER_OPCODE = b"r"  # Register
REGISTRY_REWARD_OPCODE = b"m"  # reward (Mint)
REGISTRY_REWARD_DELAY = 180_000  # seconds

log = logging.getLogger(__name__)


def get_registry_constraints() -> tuple[int, int]:
    try:
        constraints = os.environ["CIRCUIT_ANNOUNCER_REGISTRY_CONSTRAINTS"]
        MAXIMUM_REWARDS_PER_INTERVAL, MINIMUM_REWARDS_INTERVAL = constraints.split(",")
    except (KeyError, ValueError):
        raise ValueError("CIRCUIT_ANNOUNCER_REGISTRY_CONSTRAINTS env variable not set")
    return int(MAXIMUM_REWARDS_PER_INTERVAL), int(MINIMUM_REWARDS_INTERVAL)


@dataclass
class RegistryInfo:
    spend: CoinSpend
    registry: list[bytes32]
    claim_counter: uint32
    rewards_claimable_at: uint32

    def __post_init__(self):
        self.registry = [bytes32(x.atom) for x in self.registry.as_iter()]  # if not x.nullp()]
        self.claim_counter = uint32(self.claim_counter.as_int())
        self.rewards_claimable_at = uint32(self.rewards_claimable_at.as_int())

    @property
    def rewards_distributable_at(self):
        """Statutes price update counter value that must have been reached for
        Announcer Registry reward operation to be executable.
        """
        return self.rewards_claimable_at + 1

    @property
    def max_crt_rewards_per_interval(self):
        return get_registry_constraints()[0] - 1

    @property
    def min_rewards_interval(self):
        return get_registry_constraints()[1] + 1

    def get_lineage_proof(self) -> Program:
        mod, args = uncurry(self.spend.puzzle_reveal)
        assert mod == AnnouncerRegistry.get_mod_struct()[0]
        hashed_args = [x.get_tree_hash() for x in args.as_iter()]
        return Program.to((self.spend.coin.parent_coin_info, curried_values_tree_hash(hashed_args)))


def get_registry_info(registry_spend: CoinSpend, spend=True) -> RegistryInfo:
    if not spend:
        mod, args = uncurry(registry_spend.puzzle_reveal)
        registry = args.at("rrf")
        claim_counter = args.at("rrrf")
        rewards_claimable_at = args.at("rrrrf")
        return RegistryInfo(
            registry_spend,
            registry,
            claim_counter,
            rewards_claimable_at,
        )

    conditions = run(registry_spend.puzzle_reveal, registry_spend.solution)
    for condition in conditions.as_iter():
        if condition.first().atom == ConditionOpcode.REMARK:
            if condition.rest().first().as_atom() == PROTOCOL_PREFIX:
                try:
                    return RegistryInfo(registry_spend, *list(condition.rest().rest().as_iter()))
                except ValueError:
                    continue
    raise ValueError("No protocol REMARK condition found")


@dataclass
class RegistrySolutionInfo:
    lineage_proof: Program
    reward_or_register: bytes
    args: Program


@dataclass
class RegistryLaunchSolution(RegistrySolutionInfo):
    statutes_inner_puzzle_hash: bytes32
    rewards_interval: int
    statutes_launcher_puzzle_hash: bytes32


@dataclass
class RegistryRegisterSolution(RegistrySolutionInfo):
    target_puzzle_hash: bytes32
    announcer_curried_args_hash: bytes32


@dataclass
class RegistryRewardSolution(RegistrySolutionInfo):
    statutes_inner_puzzle_hash: bytes32
    rewards_interval: int
    statutes_price_update_counter: int
    rewards_per_interval: int
    issuance_coin_parent_id: bytes32
    issuance_coin_amount: int
    change_receiver_hash: bytes32
    my_coin_id: bytes32


def get_registry_solution_info(coin_spend: CoinSpend) -> RegistrySolutionInfo:
    registry_info = get_registry_info(coin_spend, spend=False)
    inner_puzzle, inner_solution = Program.from_serialized(coin_spend.solution).as_iter()
    inner_conditions = run(inner_puzzle, inner_solution)
    solution = extract_solution_from_remark(inner_conditions)
    (lineage_proof, reward_or_register, args) = to_list(solution, 3, [None, "bytes", None])
    if reward_or_register in [REGISTRY_REGISTER_OPCODE, REGISTRY_REWARD_OPCODE]:
        # non-eve lineage proof
        (
            parent_parent_id,
            parent_curried_args_hash,
        ) = to_tuple(lineage_proof, 2, ["bytes32", "bytes32"])
        parent_coin_id = Coin(
            parent_parent_id,
            tree_hash_of_apply(AnnouncerRegistry.get_mod_struct()[1], parent_curried_args_hash),
            0,
        ).name()
        if parent_coin_id != coin_spend.coin.parent_coin_info:
            raise SpendError(
                f"Invalid non-eve lineage proof for announcer ({lineage_proof}). Expected parent coin ID {coin_spend.coin.parent_coin_info.hex()}, got {parent_coin_id.hex()}"
            )
        # operations (other than launch)
        if reward_or_register == REGISTRY_REGISTER_OPCODE:
            # register
            (
                target_puzzle_hash,
                announcer_curried_args_hash,
            ) = to_list(args, 2, ["bytes32", "bytes32"])
            return RegistryRegisterSolution(
                lineage_proof=lineage_proof,
                reward_or_register=reward_or_register,
                args=args,
                target_puzzle_hash=target_puzzle_hash,
                announcer_curried_args_hash=announcer_curried_args_hash,
            )
        elif reward_or_register == REGISTRY_REWARD_OPCODE:
            # reward
            (
                statutes_inner_puzzle_hash,
                rewards_interval,
                statutes_price_update_counter,
                rewards_per_interval,
                issuance_coin_info,
                change_receiver_hash,
                my_coin_id,
            ) = to_list(args, 7, ["bytes32", "int", "int", "int", None, "bytes32", "bytes32"])
            (
                issuance_coin_parent_id,
                issuance_coin_amount,
            ) = to_tuple(issuance_coin_info, 2, ["bytes32", "int"])
            if statutes_price_update_counter <= registry_info.rewards_claimable_at:
                raise SpendError(
                    f"Cannot distribute CRT rewards. Statutes price update counter does not exceed REWARDS_CLAIMABLE_AT ({statutes_price_update_counter} <= {registry_info.rewards_claimable_at})"
                )
            MAXIMUM_REWARDS_PER_INTERVAL, MINIMUM_REWARDS_INTERVAL = get_registry_constraints()
            if rewards_interval <= MINIMUM_REWARDS_INTERVAL:
                raise SpendError(
                    f"Cannot distribute CRT rewards. Rewards interval is less than registry constraint MINIMUM_REWARDS_INTERVAL ({rewards_interval} < {MINIMUM_REWARDS_INTERVAL})"
                )
            if not registry_info.registry:
                raise SpendError("Cannot distribute CRT rewards. No registered announcers")
            if rewards_per_interval >= MAXIMUM_REWARDS_PER_INTERVAL:
                raise SpendError(
                    f"Cannot distribute CRT rewards. Rewards per interval is not less than registry constraint MAXIMUM_REWARDS_PER_INTERVAL ({rewards_per_interval} >= {MAXIMUM_REWARDS_PER_INTERVAL})"
                )
            if my_coin_id != coin_spend.coin.name():
                raise SpendError(
                    f"Cannot distribute CRT rewards. Incorrect coin ID for registry coin being spent provided. Expected {coin_spend.coin.name().hex()}, got {my_coin_id.hex()}"
                )
            return RegistryRewardSolution(
                lineage_proof=lineage_proof,
                reward_or_register=reward_or_register,
                args=args,
                statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
                rewards_interval=rewards_interval,
                statutes_price_update_counter=statutes_price_update_counter,
                rewards_per_interval=rewards_per_interval,
                issuance_coin_parent_id=issuance_coin_parent_id,
                issuance_coin_amount=issuance_coin_amount,
                change_receiver_hash=change_receiver_hash,
                my_coin_id=my_coin_id,
            )
    else:
        # launch
        (statutes_inner_puzzle_hash, rewards_interval, statutes_launcher_puzzle_hash) = to_list(
            args, 3, ["bytes32", "int", "bytes32"]
        )
        # eve lineage proof
        statutes_launcher_parent_id = to_type(lineage_proof, "bytes32", "registry launch lineage proof")
        constructed_statutes_launcher_id = Coin(
            statutes_launcher_parent_id,
            statutes_launcher_puzzle_hash,
            1,
        ).name()
        registry_puzzle = Program.from_serialized(coin_spend.puzzle_reveal)
        mod, curried_args = uncurry(registry_puzzle)
        statutes_struct = curried_args.at("rf")
        statutes_launcher_id = to_type(statutes_struct.at("rf"), "bytes32", "launcher ID of registry STATUTES_STRUCT")
        if statutes_launcher_id != constructed_statutes_launcher_id:
            raise SpendError(
                f"Invalid non-eve lineage proof for registry ({lineage_proof}). Expected statutes launcher ID {statutes_launcher_id.hex()}, got {constructed_statutes_launcher_id.hex()}"
            )
        return RegistryLaunchSolution(
            lineage_proof=lineage_proof,
            reward_or_register=reward_or_register,
            args=args,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            rewards_interval=rewards_interval,
            statutes_launcher_puzzle_hash=statutes_launcher_puzzle_hash,
        )


class AnnouncerRegistry:
    _mod = None
    _mod_hash = None

    @classmethod
    def get_mod_struct(cls) -> tuple[Program, bytes32]:
        if cls._mod is None:
            MAXIMUM_REWARDS_PER_INTERVAL, MINIMUM_REWARDS_INTERVAL = get_registry_constraints()
            announcer_registry_mod = ANNOUNCER_REGISTRY_MOD_RAW.curry(
                MAXIMUM_REWARDS_PER_INTERVAL,  # hard limit of CRTs that can be issued per rewards interval
                MINIMUM_REWARDS_INTERVAL,  # hard minimum number of statutes price updates that need to occur before rewards can be distributed again
                ATOM_ANNOUNCER_MOD_HASH,
                CAT_MOD_HASH,
                CRT_TAIL_MOD_HASH,
                RUN_TAIL_MOD_HASH,
                OFFER_MOD_HASH,
            )
            cls._mod = announcer_registry_mod
            cls._mod_hash = announcer_registry_mod.get_tree_hash()
        return cls._mod, cls._mod_hash

    @staticmethod
    def get_eve_coin_name():
        try:
            val = os.environ["CIRCUIT_ANNOUNCER_REGISTRY_EVE_COIN_NAME"]
            return bytes32.from_hexstr(val)
        except KeyError:
            raise ValueError("CIRCUIT_ANNOUNCER_REGISTRY_EVE_COIN_NAME env variable not set")
