import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple

from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program, uncurry
from chia.wallet.util.curry_and_treehash import calculate_hash_of_quoted_mod_hash, curry_and_treehash
from chia_rs.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend
from chia_rs.sized_ints import uint64
from clvm_rs.casts import int_from_bytes

from circuit_analytics.drivers import get_driver_info
from circuit_analytics.drivers.condition_filtering import fail_on_protocol_condition_or_create_coin
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    PROGRAM_STATUTES_MUTATION_MOD_RAW,
    CAT_MOD_HASH,
    GOVERNANCE_MOD_HASH,
    PROGRAM_STATUTES_UPDATE_PRICE_MOD_HASH,
    TREASURY_MOD_HASH,
    OFFER_MOD_HASH,
    PAYOUT_MOD_HASH,
    RUN_TAIL_MOD_HASH,
)
from circuit_analytics.utils import (
    tuple_to_struct,
    to_list,
    to_tuple,
    to_type,
    tree_hash_of_apply,
)

log = logging.getLogger(__name__)


class StatutePosition(Enum):
    # Internal tracking statutes (negative indices, not governable via standard proposals)

    PRICE_UPDATE_COUNTER = -4  # Counter tracking the number of price updates from the oracle.
    # Used internally to coordinate price feed updates and trigger announcer rewards.

    APPROVAL_MOD_HASHES_HASH = -3  # Hash of approved module hashes for protocol security.
    # Defines which puzzle modules are authorized to interact with the protocol,
    # preventing unauthorized code execution.

    CUMULATIVE_INTEREST_DF = -2  # Cumulative interest discount factor applied to savings vaults.
    # Tracks accumulated interest over time using a discount factor approach,
    # allowing precise calculation of accrued interest on deposited CRT.

    CUMULATIVE_STABILITY_FEE_DF = -1  # Cumulative stability fee discount factor applied to collateral vaults.
    # Tracks accumulated fees over time, ensuring debt increases according
    # to the stability fee rate, maintaining protocol sustainability.

    # Core protocol statutes (indices 0-43, governable via proposal system)

    ORACLE_LAUNCHER_ID = 0  # Launcher ID of the price oracle singleton.
    # Identifies the authorized oracle that provides XCH/USD price feeds,
    # which is critical for calculating collateralization ratios and liquidations.

    STABILITY_FEE_DF = 1  # Per-minute discount factor for the stability fee on collateral vaults.
    # Determines the rate at which debt grows in collateral vaults, providing
    # revenue to the protocol and incentivizing responsible borrowing.

    INTEREST_DF = 2  # Per-minute discount factor for interest on savings vaults.
    # Determines the rate at which CRT deposits in savings vaults earn interest,
    # incentivizing CRT holders to lock up their tokens.

    CUSTOM_CONDITIONS = 3  # Program defining custom conditions for protocol operations.
    # Allows governance to add specialized validation logic or constraints
    # for advanced protocol features without modifying core puzzle code.

    ORACLE_M_OF_N = 4  # M-of-N threshold for oracle consensus (e.g., 1 means single oracle).
    # Defines how many oracle signatures are required out of N total oracles
    # to accept a price update, enabling multi-oracle redundancy.

    ORACLE_PRICE_UPDATE_DELAY = 5  # Minimum delay in seconds between oracle price updates.
    # Prevents excessive price update frequency, reducing chain spam
    # and ensuring price stability for protocol calculations.

    ORACLE_PRICE_UPDATE_RATIO_BPS = 6  # Minimum price change in basis points (BPS) to allow oracle update.
    # Prevents trivial price updates, only allowing updates when price
    # changes exceed this threshold (e.g., 500 BPS = 5% change).

    PRICE_DELAY = 7  # Number of price updates to delay before using new price in protocol.
    # Provides time for users to react to price changes before liquidations occur,
    # reducing the risk of immediate liquidation due to oracle manipulation.

    VAULT_MINIMUM_DEBT = 8  # Minimum debt amount (in CRT microtokens) required for a collateral vault.
    # Prevents dust vaults that would be uneconomical to liquidate,
    # ensuring vaults are substantial enough to warrant protocol overhead.

    VAULT_LIQUIDATION_RATIO_PCT = 9  # Collateralization ratio threshold (percentage) triggering liquidation.
    # When vault collateral value falls below this percentage of debt
    # (e.g., 150% means $150 collateral per $100 debt), liquidation begins.

    VAULT_LIQUIDATION_PENALTY_BPS = 10  # Penalty in basis points applied to liquidated vault debt.
    # Adds a fee to the debt during liquidation (e.g., 1200 BPS = 12%),
    # discouraging risky under-collateralization and covering protocol costs.

    VAULT_INITIATOR_INCENTIVE_FLAT = 11  # Flat incentive (in CRT milli tokens) paid to liquidation initiator.
    # Rewards the user who triggers a liquidation, covering their
    # transaction costs and incentivizing timely liquidation detection.

    VAULT_INITIATOR_INCENTIVE_BPS = (
        12  # Proportional incentive in basis points of collateral for liquidation initiator.
    )
    # Additional reward scaled to collateral value, further incentivizing
    # liquidation of larger vaults.

    VAULT_AUCTION_TTL = 13  # Time-to-live in seconds for vault liquidation auctions.
    # Maximum duration a liquidation auction can remain active before expiring,
    # ensuring liquidations don't remain open indefinitely.

    VAULT_AUCTION_STARTING_PRICE_FACTOR_BPS = 14  # Starting price factor in BPS for liquidation auctions.
    # Initial auction price as percentage of oracle price
    # (e.g., 12000 BPS = 120%), starting above market to maximize recovery.

    VAULT_AUCTION_PRICE_TTL = 15  # Time interval in seconds for each price step in liquidation auction.
    # How often the auction price decreases during a Dutch auction,
    # creating urgency for bidders while allowing price discovery.

    VAULT_AUCTION_PRICE_DECREASE_BPS = 16  # Price decrease in BPS per step in liquidation auction.
    # Amount the auction price drops each interval (e.g., 100 BPS = 1%),
    # gradually making the auction more attractive to bidders.

    VAULT_AUCTION_MINIMUM_PRICE_FACTOR_BPS = 17  # Minimum price factor in BPS for liquidation auctions.
    # Floor price as percentage of oracle price (e.g., 4000 BPS = 40%),
    # preventing collateral from being sold at extreme discounts.

    VAULT_AUCTION_MINIMUM_BID_FLAT = 18  # Minimum flat bid amount (in CRT milli tokens) for liquidation auctions.
    # Ensures bids are substantial enough to be economically viable,
    # preventing dust bids that waste blockchain resources.

    VAULT_AUCTION_MINIMUM_BID_BPS = 19  # Minimum bid increase in BPS for subsequent bids in auction.
    # Requires each new bid to exceed previous by this percentage,
    # ensuring meaningful competition and efficient price discovery.

    TREASURY_MINIMUM = 20  # Minimum CRT balance (in milli tokens) the treasury must maintain.
    # Ensures treasury has sufficient funds to operate and provide stability,
    # triggering recharge auctions when balance falls below this threshold.

    TREASURY_MAXIMUM = 21  # Maximum CRT balance (in milli tokens) the treasury should hold.
    # Prevents excessive CRT accumulation, triggering surplus auctions
    # when balance exceeds this limit to redistribute value to governance token holders.

    TREASURY_MINIMUM_DELTA = 22  # Minimum CRT amount (in milli tokens) for treasury operations.
    # Minimum change required for treasury rebalancing operations,
    # preventing inefficient small transactions.

    TREASURY_REBALANCE_RATIO_PCT = 23  # Percentage of treasury balance deviation to correct during rebalancing.
    # Determines how aggressively treasury moves toward target range
    # (e.g., 400% means rebalance 4x the deviation), enabling gradual adjustment.

    AUCTIONS_MINIMUM_PRICE_INCREASE_BPS = 24  # Minimum price increase in BPS for competitive bids across all auctions.
    # Ensures bid increments are meaningful (e.g., 100 BPS = 1% increase),
    # promoting efficient price discovery in recharge and surplus auctions.

    RECHARGE_AUCTION_TTL = 25  # Time-to-live in seconds for recharge auctions (treasury funding).
    # Maximum duration for auctions that sell governance tokens to raise CRT,
    # ensuring auctions don't remain open indefinitely when treasury needs funds.

    RECHARGE_AUCTION_MINIMUM_CRT_PRICE = (
        26  # Minimum acceptable price (in CRT per governance token) for recharge auctions.
    )
    # Floor price preventing governance tokens from being sold too cheaply
    # when replenishing treasury, protecting token holder value.

    RECHARGE_AUCTION_BID_TTL = 27  # Time-to-live in seconds for each bid in recharge auctions.
    # How long bidders have to submit competing bids before auction finalizes,
    # balancing urgency with adequate time for participation.

    RECHARGE_AUCTION_MINIMUM_BID = 28  # Minimum bid amount (in CRT milli tokens) for recharge auctions.
    # Ensures recharge auction bids are substantial enough to meaningfully
    # replenish treasury reserves.

    RECHARGE_AUCTION_MAXIMUM_BID = 29  # Maximum bid amount (in CRT milli tokens) for recharge auctions.
    # Caps individual recharge auction size to prevent excessive
    # governance token dilution in a single auction.

    SURPLUS_AUCTION_LOT = 30  # Fixed CRT amount (in milli tokens) offered per surplus auction.
    # Standard lot size when treasury sells excess CRT for governance tokens,
    # providing predictable auction sizes for participants.

    SURPLUS_AUCTION_BID_TTL = 31  # Time-to-live in seconds for bids in surplus auctions.
    # Duration bidders have to compete for surplus CRT with governance tokens,
    # allowing adequate participation while maintaining auction momentum.

    ANNOUNCER_REWARDS_INTERVAL_PRICE_UPDATES = 32  # Number of price updates between announcer reward distributions.
    # Determines reward frequency for oracle announcers who submit
    # valid price data, incentivizing consistent oracle participation.

    ANNOUNCER_REWARDS_PER_INTERVAL = 33  # Reward amount (in CRT milli tokens) paid to announcers per interval.
    # Compensation for oracle announcers who provide price feed data,
    # ensuring adequate incentive for reliable price oracle operation.

    ANNOUNCER_MINIMUM_DEPOSIT_MOJOS = 34  # Minimum deposit (in mojos/XCH) required to become an announcer.
    # Stake required to participate as price oracle announcer,
    # ensuring announcers have skin in the game and deterring bad actors.

    ANNOUNCER_MAXIMUM_VALUE_TTL = 35  # Maximum time-to-live in seconds for announcer price values.
    # How long an announcer's submitted price remains valid before expiring,
    # ensuring price data stays fresh and relevant.

    ANNOUNCER_PENALTY_INTERVAL_MINUTES = 36  # Time interval in minutes for applying announcer penalties.
    # How frequently penalties are assessed for announcer inactivity
    # or invalid submissions, maintaining oracle data quality.

    ANNOUNCER_PENALTY_PER_INTERVAL_BPS = 37  # Penalty in BPS applied to announcer deposit per penalty interval.
    # Percentage of deposit slashed for poor performance (e.g., 500 BPS = 5%),
    # incentivizing consistent, accurate oracle data submission.

    ANNOUNCER_DISAPPROVAL_MAXIMUM_PENALTY_BPS = 38  # Maximum cumulative penalty in BPS for announcer disapproval.
    # Cap on total deposit that can be slashed (e.g., 2000 BPS = 20%),
    # preventing complete loss while still punishing bad behavior.

    ANNOUNCER_DISAPPROVAL_COOLDOWN_INTERVAL = 39  # Cooldown period in seconds after announcer disapproval.
    # Time an announcer must wait after being penalized before
    # rejoining as oracle, preventing immediate re-entry after punishment.

    GOVERNANCE_BILL_PROPOSAL_FEE_MOJOS = 40  # Fee in mojos (XCH) required to submit a governance proposal.
    # Cost to propose statute changes, preventing spam proposals
    # while keeping governance accessible to serious participants.

    GOVERNANCE_IMPLEMENTATION_INTERVAL = 41  # Delay in seconds between bill approval and implementation.
    # Mandatory waiting period before approved governance changes take effect,
    # allowing users time to react to upcoming protocol modifications.

    GOVERNANCE_COOLDOWN_INTERVAL = 42  # Minimum time in seconds between governance proposal implementations.
    # Rate-limits governance changes to prevent rapid destabilizing modifications,
    # ensuring protocol stability and predictability.

    BLOCK_ISSUANCE = 43  # CRT issuance amount per block for inflation/rewards (0 = disabled).
    # Defines automatic CRT token creation per block if protocol implements
    # inflationary rewards (currently disabled with value 0).

    @classmethod
    def max_statutes_idx(cls) -> int:
        return max(member.value for member in cls)


statute_types = {sp.name: "int" for sp in StatutePosition}
statute_types[StatutePosition.APPROVAL_MOD_HASHES_HASH.name] = "bytes32"
statute_types[StatutePosition.ORACLE_LAUNCHER_ID.name] = "bytes32"
statute_types[StatutePosition.CUSTOM_CONDITIONS.name] = "Program"

# Full Statute is list of 7 elements. this is what's curried in as BILL:
#   (proposal_times statute_index statute_value proposal_threshold veto_interval implementation_delay max_delta)
# last 5 of which are the (plain) Statute. this is what's stored in STATUTES in Statutes singleton:
#   (value proposal_threshold veto_interval implementation_delay max_delta)
# last 4 of which are referred to as Constraints:
#   (proposal_threshold veto_interval implementation_delay max_delta)


def statute_str_to_program(value: str, index: int) -> Program:
    assert index >= -1, "Can only convert Statutes with non-negative index from string to Program"
    if statute_types[StatutePosition(index).name] == "int" and index >= 0:
        prog = Program.to(int(value))
        return prog
    return Program.fromhex(value)


def statute_value_to_str(value: Program, position: StatutePosition | None) -> Optional[str]:
    if position is None:
        # this is a custom announcement
        assert isinstance(value, Program), "Custom announcement value should be of type Program"
        data_type = "Program"
    else:
        data_type = statute_types[position.name]
    if data_type == "Program":
        return value.as_bin().hex()
    elif data_type == "bytes32":
        return bytes32(value.atom).hex()
    elif data_type == "int":
        return str(value.as_int())
    else:
        raise ValueError(f"Unknown Statute data type {data_type}")


def statute_value_to_str_or_int(value: Program, position: StatutePosition | None) -> Optional[str | int]:
    if position is None:
        assert isinstance(value, Program), "Custom announcement value should be of type Program"
        data_type = "Program"
    else:
        data_type = statute_types[position.name]
    if data_type == "Program":
        return value.as_bin().hex()
    elif data_type == "bytes32":
        return bytes32(value.atom).hex()
    elif data_type == "int":
        return value.as_int()
    else:
        raise ValueError(f"Unknown Statute data type {data_type}")


def convert_full_statute(full_statute: Program, statute_index: int = None) -> tuple[list]:
    """Converts a full Statute given as a Program to a list of Python data types and list of strings/ints"""
    if full_statute is None or full_statute == Program.to(0):
        return [], "()"

    # Cache list length to avoid repeated calls
    list_len = full_statute.list_len()

    if statute_index is None:
        assert list_len >= 6, "Full statute must contain statute index"
        statute_index = full_statute.at("f").as_int() if list_len == 6 else full_statute.at("rf").as_int()
    else:
        assert list_len == 5, "Full statute provided does not consist of five elements"

    if list_len == 5:
        value_loc = ""
    elif list_len == 6:
        value_loc = "r"
    elif list_len == 7:
        value_loc = "rr"
    else:
        raise ValueError("Full statute must have between 5 and 7 elements")

    assert statute_index in [sp.value for sp in StatutePosition], f"Invalid statute index {statute_index}"

    # Pre-compute statute position and data type to avoid repeated lookups
    statute_position = StatutePosition(statute_index)
    data_type = statute_types[statute_position.name]

    if data_type == "Program":
        statute_value = full_statute.at(value_loc + "f")
        # Use efficient hex representation instead of expensive disassemble
        statute_value_str = statute_value.as_bin().hex()
    elif data_type == "bytes32":
        statute_value = bytes32(full_statute.at(value_loc + "f").atom)
        statute_value_str = statute_value.hex()
    elif data_type == "int":
        statute_value = full_statute.at(value_loc + "f").as_int()
        statute_value_str = str(statute_value)
    else:
        raise ValueError(f"Unknown statute value data type {data_type}")

    # Cache the last 4 elements conversion to avoid duplicate list creation
    last_four_elements = [e.as_int() for e in list(full_statute.as_iter())[-4:]]
    full_statute_py = [statute_value] + last_four_elements
    full_statute_str = [statute_value_str] + last_four_elements

    if list_len >= 6:
        full_statute_py = [statute_index] + full_statute_py
        full_statute_str = [statute_index] + full_statute_str
    if list_len == 7:
        first_element = full_statute.first()
        if first_element == Program.to(0):
            full_statute_py = [first_element] + full_statute_py
            # Use efficient hex representation instead of expensive disassemble
            full_statute_str = [first_element.as_bin().hex()] + full_statute_str
        else:
            proposal_times = (full_statute.at("ff").as_int(), full_statute.at("fr").as_int())
            full_statute_py = [proposal_times] + full_statute_py
            full_statute_str = [proposal_times] + full_statute_str

    return full_statute_py, "[" + ", ".join(str(e) for e in full_statute_str) + "]"


def calculate_statutes_puzzle_hash(statutes_struct: Program, inner_puzzle_hash: bytes32) -> bytes32:
    """Calculates puzzle hash of statutes singleton.

    Equivalent of calculate-statutes-puzzle-hash function in statutes_utils.clib.
    """
    return curry_and_treehash(
        statutes_struct.first(),
        statutes_struct.get_tree_hash() if statutes_struct.rest().list_len() > 0 else statutes_struct.rest(),
        inner_puzzle_hash,
    )


@dataclass
class StatutesSolutionInfo:
    governance_curried_args_hash: Program
    mutation: Program

    def __post_init__(self):
        # if mutation is not nil, it must be a proper list of 3 elements
        if not self.mutation.nullp():
            to_list(self.mutation, 3)

    @property
    def operation(self) -> Program:
        """Returns operation_mod of statutes.clsp puzzle.

        This is either an actual operation mod or nil in case of announce operation.
        """
        if self.mutation.nullp():
            return Program.to(None)
        (
            operation,
            _,
            _,
        ) = to_list(self.mutation, 3)
        if operation.nullp():
            return Program.to(None)
        return operation

    @property
    def operation_hash(self) -> bytes32 | None:
        if self.operation.nullp():
            return None
        return self.operation.get_tree_hash()

    @property
    def mutation_index(self) -> int:
        if self.mutation.nullp():
            return Program.to(0).as_int()
        (
            _,
            mutation_index,
            _,
        ) = to_list(self.mutation, [None, "int", None])  # TODO: check canonical representation of mutation_index (puzzle rejects non-canonical encodings)
        return mutation_index

    @property
    def mutation_value(self) -> Program:
        if self.mutation.nullp():
            return Program.to(0)
        (
            _,
            _,
            mutation_value,
        ) = to_list(self.mutation, 3)
        return mutation_value


class StatutesMutationInfo(StatutesSolutionInfo):
    mutation_index: int
    mutation_value: Program

    def __post_init__(self):
        # we're overwriting a base class method, so make sure it's returning the same value
        assert self.mutation_value == super().mutation_value
        # we overwrite base class method in order to log warnings if an unexpected Statute value were passed
        if self.mutation_index == -1:
            # should be one or more conditions, ie not an atom
            try:
                self.mutation_value.list_len() <= 1
            except Exception:
                log.warning(
                    f"Statute mutation at index {self.mutation_index} was passed "
                    f"an atom (incl nil) as Statute value: {self.mutation_value}"
                )
        if self.mutation_index == 0:
            # should be convertible to bytes32
            try:
                to_type(self.mutation_value, "bytes32", f"Statute value at index {self.mutation_index}")
            except Exception:
                log.warning(
                    f"Statute mutation at index {self.mutation_index} was passed "
                    f"a non-bytes32-convertible Statute value: {self.mutation_value}"
                )
        elif self.mutation_index >= 1:
            # should be convertible to int
            try:
                to_type(self.mutation_value, "int", f"Statute value at index {self.mutation_index}")
            except Exception:
                log.warning(
                    f"Statute mutation at index {self.mutation_index} was passed "
                    f"a non-int-convertible Statute value: {self.mutation_value}"
                )
        # check that governance_curried_args_hash is bytes32 convertible
        to_type(
            self.governance_curried_args_hash,
            "bytes32",
            "solution arg governance_curried_args_hash in Statutes mutation operation",
        )

    @property
    def governance_puzze_hash(self) -> bytes32:
        return tree_hash_of_apply(GOVERNANCE_MOD_HASH, self.governance_curried_args_hash)


class StatutesCustomConditionsInfo(StatutesMutationInfo):
    custom_conditions: list[Program]  # from mutation_value.first()


class StatutesUpdateStatuteInfo(StatutesMutationInfo):
    value: Program  # mutation_value.first()
    proposal_threshold: Program
    veto_interval: Program
    implementation_delay: Program
    max_delta: Program

    def __post_init__(self):
        try:
            to_type(self.proposal_threshold, "int", f"proposal threshold at index {self.mutation_index}")
        except Exception:
            log.warning(
                f"Statute mutation at index {self.mutation_index} was passed "
                f"a non-int-convertible proposal threshold: {self.proposal_threshold}"
            )
        try:
            to_type(self.veto_interval, "int", f"veto interval at index {self.mutation_index}")
        except Exception:
            log.warning(
                f"Statute mutation at index {self.mutation_index} was passed "
                f"a non-int-convertible veto interval: {self.veto_interval}"
            )
        try:
            to_type(self.implementation_delay, "int", f"implementation delay at index {self.mutation_index}")
        except Exception:
            log.warning(
                f"Statute mutation at index {self.mutation_index} was passed "
                f"a non-int-convertible implementation delay: {self.implementation_delay}"
            )
        try:
            to_type(self.max_delta, "int", f"max delta at index {self.mutation_index}")
        except Exception:
            log.warning(
                f"Statute mutation at index {self.mutation_index} was passed "
                f"a non-int-convertible max delta: {self.max_delta}"
            )


class StatutesUpdatePriceInfo(StatutesSolutionInfo):
    oracle_inner_puzzle_hash: bytes32
    oracle_price: int
    oracle_price_timestamp: int


class StatutesAnnounceInfo(StatutesSolutionInfo):
    pass


def get_statutes_solution_info(coin_spend: CoinSpend) -> StatutesSolutionInfo:
    statutes_info = Statutes.get_statutes_info(coin_spend)
    solution = Program.from_serialized(coin_spend.solution)
    (
        lineage_proof,
        inner_solution,
    ) = to_tuple(solution, 2)
    # lineage proof
    if lineage_proof.atom:
        # eve lineage proof
        parent_parent_coin_name = to_type(lineage_proof, "bytes32", "Statutes eve lineage proof")
        (
            _,
            launcher_id,
            launcher_puzzle_hash,
        ) = to_tuple(statutes_info.statutes_struct, 3, ["bytes32", "bytes32", "bytes32"])
        parent_coin_name = Coin(parent_parent_coin_name, launcher_puzzle_hash, 1).name()
        if parent_coin_name != coin_spend.coin.parent_coin_info:
            raise SpendError(
                f"Statutes eve lineage proof does not yield correct parent coin ID. "
                f"Expected {coin_spend.coin.parent_coin_info}, got {parent_coin_name}"
            )
    else:
        # non-eve lineage proof
        (
            parent_parent_coin_name,
            parent_inner_puzzle_hash,
        ) = to_tuple(lineage_proof, 2, ["bytes32", "bytes32"], "Statutes non-eve lineage proof")
        parent_puzzle_hash = curry_and_treehash(
            calculate_hash_of_quoted_mod_hash(bytes32(statutes_info.statutes_struct.first().atom)),
            statutes_info.statutes_struct.get_tree_hash(),
            parent_inner_puzzle_hash,
        )
        parent_coin_name = Coin(parent_parent_coin_name, parent_puzzle_hash, 1).name()
        if parent_coin_name != coin_spend.coin.parent_coin_info:
            raise SpendError(
                f"Statutes non-eve lineage proof does not yield correct parent coin ID. "
                f"Expected {coin_spend.coin.parent_coin_info}, got {parent_coin_name}"
            )
    # inner solution
    (
        governance_curried_args_hash,
        mutation,
    ) = to_tuple(inner_solution, 2)
    solution_info = StatutesSolutionInfo(
        governance_curried_args_hash=governance_curried_args_hash,
        mutation=mutation,
    )
    if not solution_info.operation_hash:
        # announce operation
        if not solution_info.governance_curried_args_hash.nullp():
            raise SpendError("Statutes announce operation requires governance_curried_args_hash solution arg to be nil")
        if solution_info.mutation != Program.to(None):
            raise SpendError("Statutes announce operation requires mutation solution arg to be nil")
        return StatutesAnnounceInfo(**solution_info.__dict__)
    # non-announce operations
    if not statutes_info.prev_announce:
        raise SpendError("Statutes non-announce operation failed because previous operation was not an announce")
    from circuit_analytics.config import CRT_TAIL_HASH

    PROGRAM_STATUTES_MUTATION_MOD_HASH = PROGRAM_STATUTES_MUTATION_MOD_RAW.curry(
        CAT_MOD_HASH,
        CRT_TAIL_HASH,
        GOVERNANCE_MOD_HASH,
    ).get_tree_hash()
    if solution_info.operation_hash == PROGRAM_STATUTES_MUTATION_MOD_HASH:
        # mutate operation
        if solution_info.mutation_index < -1 or solution_info.mutation_index > StatutePosition.max_statutes_idx():
            raise SpendError(
                f"Statutes mutation operation must have mutation index between -1 and "
                f"{StatutePosition.max_statutes_idx()} included, got {solution_info.mutation_index}"
            )
        solution_info = StatutesMutationInfo(
            **solution_info.__dict__,
            mutation_index=solution_info.mutation_index,
            mutation_value=solution_info.mutation_value,
        )
        if solution_info.mutation_index == -1:
            # custom conditions announcement
            try:
                custom_conditions = list(solution_info.mutation_value.first().as_iter())
                fail_on_protocol_condition_or_create_coin(custom_conditions)
            except ValueError:
                raise SpendError(
                    "Statutes custom conditions announcement failed due to conditions containing a protocol or create coin condition"
                )
            return StatutesCustomConditionsInfo(
                **solution_info.__dict__,
                custom_conditions=custom_conditions,
            )
        else:
            # statute update
            (
                statute_value,
                proposal_threshold,
                veto_interval,
                implementation_delay,
                max_delta,
                element_six,
            ) = to_tuple(solution_info.mutation_value, 6)
            if not element_six.nullp():
                raise SpendError(
                    "Statute mutation operation must have mutation value that is a proper list of exactly 5 elements"
                )
            return StatutesUpdateStatuteInfo(
                **solution_info.__dict__,
                value=statute_value,
                proposal_threshold=proposal_threshold,
                veto_interval=veto_interval,
                implementation_delay=implementation_delay,
                max_delta=max_delta,
            )
    elif solution_info.operation_hash == PROGRAM_STATUTES_UPDATE_PRICE_MOD_HASH:
        # update price operation
        (
            oracle_inner_puzzle_hash,
            oracle_price,
            oracle_price_timestamp,
        ) = to_list(solution_info.mutation_value, 3, ["bytes32", "uint64", "uint64"])
        if oracle_price_timestamp <= statutes_info.price_info[1]:
            raise SpendError(
                f"Statutes Price cannot be updated unless Oracle Price is newer than current Statutes Price ({oracle_price_timestamp} <= {statutes_info.price_info[1]})"
            )
        return StatutesUpdatePriceInfo(
            **solution_info.__dict__,
            oracle_inner_puzzle_hash=oracle_inner_puzzle_hash,
            oracle_price=oracle_price,
            oracle_price_timestamp=oracle_price_timestamp,
        )
    else:
        raise SpendError(
            f"Invalid operation provided for Statutes spend. Operation hash: {solution_info.operation_hash.hex()}"
        )


@dataclass
class StatutesInfo:
    inner_puzzle_hash: bytes32
    statutes: List[Program]
    full_statutes: List[Program]
    treasury_mod_hash: bytes32
    cumulative_stability_fee_df: uint64
    cumulative_interest_rate_df: uint64
    approved_mod_hashes: List[bytes]
    statutes_struct: Program
    price_info: Tuple[int, int]
    oracle_launcher_id: bytes32 | None
    offer_mod_hash: bytes32
    payout_mod_hash: bytes32
    price_update_counter: uint64
    prev_announce: bool
    run_tail_mod_hash: bytes32


class Statutes:
    @staticmethod
    def get_approved_mod_hashes() -> List[bytes]:
        try:
            val = os.environ["CIRCUIT_APPROVED_MOD_HASHES"]
            hashes_prog = Program.fromhex(val).as_iter()
            hashes = [x.as_atom() for x in hashes_prog]
            return hashes
        except KeyError:
            raise ValueError("CIRCUIT_APPROVED_MOD_HASHES env variable not set")

    @staticmethod
    def get_statutes_info(
        coin_spend: CoinSpend,
        force_approved_mod_hashes=None,
        skip_mod_hash_verification: bool = False,
    ) -> StatutesInfo:
        log.debug(f"getting statutes info for coin {coin_spend.coin.name().hex()}")
        puzzle = coin_spend.puzzle_reveal
        _, args = uncurry(puzzle)
        inner_puzzle = args.at("rf")
        inner_mod, inner_args = uncurry(inner_puzzle)
        prev_announce = bool(inner_args.at("rf").as_int())
        statutes = list(inner_args.at("rrf").as_iter())
        price_info = inner_args.at("rrrf").as_python()
        cumulative_stability_fee_rate = uint64(inner_args.at("rrrrf").as_int())
        cumulative_interest_rate = uint64(inner_args.at("rrrrrf").as_int())
        price_update_counter = uint64(inner_args.at("rrrrrrf").as_int())
        raw_inner_mod, fixed_args = uncurry(inner_mod)
        approved_mod_hashes_hash = fixed_args.first().atom
        if not skip_mod_hash_verification:
            if force_approved_mod_hashes is not None:
                approved_mod_hashes = force_approved_mod_hashes
            else:
                approved_mod_hashes = Statutes.get_approved_mod_hashes()
            assert approved_mod_hashes_hash == Program.to(approved_mod_hashes).get_tree_hash()
        else:
            approved_mod_hashes = []
        statutes_struct = args.first()
        oracle_launcher_id = statutes[0].first()
        try:
            return StatutesInfo(
                inner_puzzle.get_tree_hash(),
                [x.first() for x in statutes],
                statutes,
                TREASURY_MOD_HASH,
                cumulative_stability_fee_rate,
                cumulative_interest_rate,
                approved_mod_hashes,
                statutes_struct,
                (int_from_bytes(price_info[0]), int_from_bytes(price_info[1])),
                bytes32(oracle_launcher_id.atom) if not oracle_launcher_id.nullp() else None,
                OFFER_MOD_HASH,
                PAYOUT_MOD_HASH,
                price_update_counter,
                prev_announce,
                RUN_TAIL_MOD_HASH,
            )
        finally:
            log.debug("finished getting statutes info")
