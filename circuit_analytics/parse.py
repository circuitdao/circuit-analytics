"""Parse a single coin spend into the driver's solution info, without a database.

The block scanner routes each spend to a handler that both parses it and writes rows. This
routes on the same puzzle hashes but stops at the parse, so a spend bundle can be inspected
directly. That is what turns "the indexer stalled at height N" into a one-line reproduction:
a parser whose expected solution shape has drifted from the puzzle raises here, on the exact
spend, instead of somewhere inside a scan.

The routing is deliberately a copy of the scanner's rather than a shared helper: the scanner
dispatches to handlers, this dispatches to drivers, and tying them together would mean the
handlers' database concerns leaking in here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from chia.types.blockchain_format.program import Program, uncurry
from chia_rs import CoinSpend

from circuit_analytics.config import (
    ANNOUNCER_REGISTRY_LAUNCHER_ID,
    BYC_TAIL_HASH,
    CRT_TAIL_HASH,
    STATUTES_LAUNCHER_ID,
)
from circuit_analytics.drivers.announcer import get_announcer_solution_info
from circuit_analytics.drivers.cat import get_cat_solution_info
from circuit_analytics.drivers.oracle import get_oracle_solution_info
from circuit_analytics.drivers.recharge_auction import get_recharge_solution_info
from circuit_analytics.drivers.registry import AnnouncerRegistry, get_registry_solution_info
from circuit_analytics.drivers.savings import get_savings_solution_info
from circuit_analytics.drivers.statutes import get_statutes_solution_info
from circuit_analytics.drivers.surplus_auction import (
    get_payout_solution_info,
    get_surplus_solution_info,
)
from circuit_analytics.drivers.treasury import get_treasury_solution_info
from circuit_analytics.drivers.vault import get_vault_solution_info
from circuit_analytics.mods import (
    ATOM_ANNOUNCER_MOD_HASH,
    OFFER_MOD,
    CAT_MOD,
    CAT_MOD_HASH,
    COLLATERAL_VAULT_MOD_HASH,
    GOVERNANCE_MOD,
    LAUNCH_GOVERNANCE_MOD,
    PAYOUT_MOD,
    RECHARGE_AUCTION_MOD,
    SAVINGS_VAULT_MOD,
    SINGLETON_ISA_MOD_HASH,
    SINGLETON_MOD_HASH,
    SURPLUS_AUCTION_MOD,
    TREASURY_MOD,
    ZERO_FAUCET,
)
from chia.wallet.puzzles.p2_delegated_puzzle_or_hidden_puzzle import MOD as P2_DELEGATED_MOD
from circuit_analytics.scanner.block_scanner import get_statutes_struct

log = logging.getLogger(__name__)


@dataclass
class ParsedSpend:
    """One spend from a bundle, and whatever the matching driver made of it."""

    index: int
    coin_id: str
    puzzle_hash: str
    amount: int
    coin_type: str
    """The coin's role: treasury, savings vault, standard tx, statutes, unrecognised..."""
    layer: Optional[str] = None
    """The outer puzzle wrapping the inner one: CAT, singleton, ISA singleton.

    None for puzzles that are not wrapped -- a collateral vault, announcer or registry coin
    is its own top-level puzzle.
    """
    asset: Optional[str] = None
    """For CAT-wrapped coins: BYC, CRT, or the asset ID of some other CAT."""
    launcher_id: Optional[str] = None
    """For singletons: the launcher this one was minted from."""
    info: Optional[Any] = None
    """The driver's *SolutionInfo, or None if nothing claimed the spend."""
    error: Optional[str] = None
    """Set when the driver raised. This is the case worth looking for."""
    note: Optional[str] = None
    """Set when the result is less than it looks, e.g. only the CAT layer was parsed."""

    @property
    def failed(self) -> bool:
        return self.error is not None


def _abbrev(value: bytes) -> str:
    h = bytes(value).hex()
    return h if len(h) <= 20 else f"{h[:10]}\u2026{h[-6:]}"


def _singleton_launcher_id(coin_spend: CoinSpend) -> Optional[bytes]:
    """Launcher ID out of a singleton's curried SINGLETON_STRUCT."""
    try:
        _, args = uncurry(coin_spend.puzzle_reveal)
        atom = args.at("frf").atom
        return bytes(atom) if atom is not None else None
    except Exception:  # noqa: BLE001
        return None


def _singleton_coin_type(coin_spend: CoinSpend, is_isa: bool) -> str:
    """Name a singleton by its launcher, so it is clear which one this is.

    The ISA layer is Circuit's own, so any ISA singleton belongs to the protocol; a standard
    singleton might be anyone's, and the launcher is the only thing that says which.
    """
    launcher = _singleton_launcher_id(coin_spend)
    if is_isa:
        return "statutes" if launcher == bytes(STATUTES_LAUNCHER_ID) else "unknown launcher"
    if launcher == bytes(ANNOUNCER_REGISTRY_LAUNCHER_ID):
        return "announcer registry"
    return "unknown launcher"


def _cat_inner(coin_spend: CoinSpend) -> tuple[Program, Program]:
    """The CAT layer's inner puzzle and inner solution."""
    _, cat_args = uncurry(coin_spend.puzzle_reveal)
    return cat_args.at("rrf"), Program.from_serialized(coin_spend.solution).first()


CAT_INNER_MODS = (
    (SAVINGS_VAULT_MOD, "savings vault", get_savings_solution_info),
    (TREASURY_MOD, "treasury", get_treasury_solution_info),
    (RECHARGE_AUCTION_MOD, "recharge auction", get_recharge_solution_info),
    (SURPLUS_AUCTION_MOD, "surplus auction", get_surplus_solution_info),
    (PAYOUT_MOD, "payout", get_payout_solution_info),
)


# Inner puzzles that legitimately hold a protocol CAT without being a protocol state coin,
# and which no driver parses. Naming them keeps the "unrecognised" note meaning what it is
# for: a puzzle set that does not match the chain. A BYC coin sitting in someone's wallet is
# not a mystery.
KNOWN_NON_DRIVER_INNER_MODS = (
    (P2_DELEGATED_MOD, "standard tx"),
    (OFFER_MOD, "offer"),
    (ZERO_FAUCET, "zero faucet"),
)

# Roles that merely hold a protocol asset rather than being protocol state. A BYC coin in
# someone's wallet or locked in an offer is a balance; two people trading BYC is not protocol
# activity. These are not seeds for the block walk, so they appear only when something ties
# them to a protocol spend -- and then the link says what that was.
#
# The zero faucet is deliberately absent: it is protocol machinery, used to mint the
# zero-amount BYC coins that savings withdrawals need.
NON_PROTOCOL_ROLES = frozenset({"standard tx", "offer"})


def _cat_asset(coin_spend: CoinSpend) -> Optional[str]:
    """Which asset a CAT holds: BYC, CRT, or another asset named by its ID."""
    try:
        _, cat_args = uncurry(coin_spend.puzzle_reveal)
        tail_hash = cat_args.at("rf").atom
    except Exception:  # noqa: BLE001
        return None
    if tail_hash is None:
        return None
    tail_hash = bytes(tail_hash)
    if tail_hash == bytes(BYC_TAIL_HASH):
        return "BYC"
    if tail_hash == bytes(CRT_TAIL_HASH):
        return "CRT"
    return _abbrev(tail_hash)


def _cat_coin_type(coin_spend: CoinSpend) -> tuple[str, Optional[str], Optional[str]]:
    """A CAT spend's role and asset, without parsing it, plus a note if the role is thin.

    Role and asset are separate because they are independent: a treasury coin and a coin
    someone is holding are both BYC, and both facts are worth seeing. Naming the role before
    parsing also means a parse failure is attributed to the right puzzle, which is the whole
    point when the failure is what you are chasing.

    A CAT whose inner puzzle matches no known puzzle is reported as such rather than passing
    quietly: the usual cause is a local puzzle set that does not match the chain being read,
    and silently returning CAT layer info hides that completely.
    """
    asset = _cat_asset(coin_spend)
    try:
        inner_puzzle, _ = _cat_inner(coin_spend)
        inner_mod, _ = uncurry(inner_puzzle)
        for mod, name, _parse in CAT_INNER_MODS:
            if inner_mod == mod:
                return name, asset, None
        if inner_mod in (GOVERNANCE_MOD, LAUNCH_GOVERNANCE_MOD):
            return "governance", asset, None
        for mod, name in KNOWN_NON_DRIVER_INNER_MODS:
            # Match the uncurried mod or the puzzle itself: the settlement puzzle carries no
            # curried args, so uncurrying it does not yield the mod to compare against.
            if inner_mod == mod or inner_puzzle == mod:
                # No driver parses these, so the CAT layer really is the whole story.
                return name, asset, None
        if asset in ("BYC", "CRT"):
            note = (
                f"inner puzzle {inner_mod.get_tree_hash().hex()} is not a puzzle this build "
                "knows, so only the CAT layer was parsed. If this coin should be a savings "
                "vault, treasury, auction or payout coin, the local puzzle set does not match "
                "the chain -- run `circuit-scan verify-config`."
            )
            return "unknown inner puzzle", asset, note
        return "unknown inner puzzle", asset, None
    except Exception:  # noqa: BLE001 - naming is best-effort; the parse reports the real error
        return "CAT", asset, None


def _parse_cat(coin_spend: CoinSpend, statutes_struct: Program) -> Any:
    """Route a CAT spend on its inner puzzle, the way CatHandler does."""
    inner_puzzle, inner_solution = _cat_inner(coin_spend)
    inner_mod, _ = uncurry(inner_puzzle)

    for mod, _name, parse in CAT_INNER_MODS:
        if inner_mod == mod:
            return parse(coin_spend.coin, inner_puzzle, inner_solution)

    # Everything else -- governance coins and plain BYC/CRT -- is parsed from the whole
    # spend, where a tail reveal is what carries protocol meaning.
    return get_cat_solution_info(
        coin_spend, byc_tail_hash=bytes(BYC_TAIL_HASH), crt_tail_hash=bytes(CRT_TAIL_HASH)
    )


def _routes(coin_spend: CoinSpend) -> dict:
    """Top-level puzzle hash -> (coin type, parse thunk), mirroring the scanner's dispatch."""
    return {
        COLLATERAL_VAULT_MOD_HASH: ("collateral vault", lambda: get_vault_solution_info(coin_spend)),
        ATOM_ANNOUNCER_MOD_HASH: ("announcer", lambda: get_announcer_solution_info(coin_spend)),
        AnnouncerRegistry.get_mod_struct()[1]: ("registry", lambda: get_registry_solution_info(coin_spend)),
        SINGLETON_ISA_MOD_HASH: ("statutes", lambda: get_statutes_solution_info(coin_spend)),
        SINGLETON_MOD_HASH: ("oracle", lambda: get_oracle_solution_info(coin_spend)),
    }


def protocol_coin_type(coin_spend: CoinSpend) -> Optional[str]:
    """Name the protocol coin type of a spend, or None if no driver would claim it.

    Deliberately does not parse: this answers "is this one of ours" for spends that may
    later turn out to be unparseable, which is exactly the case worth reporting.
    """
    try:
        mod, _ = uncurry(coin_spend.puzzle_reveal)
    except Exception:  # noqa: BLE001
        return None
    mod_hash = mod.get_tree_hash()
    if mod_hash == CAT_MOD_HASH or mod == CAT_MOD:
        coin_type, asset, _note = _cat_coin_type(coin_spend)
        # Holding a protocol asset is necessary but not sufficient: a wallet or offer coin is
        # someone's balance, not protocol state.
        if asset in ("BYC", "CRT") and coin_type not in NON_PROTOCOL_ROLES:
            return coin_type
        return None
    if mod_hash == SINGLETON_MOD_HASH:
        # The standard singleton layer is not Circuit's: NFTs and DIDs use it too. Only the
        # oracle is ours, and its launcher is not in the config, so the honest test is whether
        # the oracle driver can read it. Without this every NFT spend in a block would seed
        # the linkage walk and drag in unrelated transactions.
        try:
            get_oracle_solution_info(coin_spend)
        except Exception:  # noqa: BLE001
            return None
        return "oracle"
    if mod_hash == SINGLETON_ISA_MOD_HASH:
        return _singleton_coin_type(coin_spend, is_isa=True)
    route = _routes(coin_spend).get(mod_hash)
    return route[0] if route else None


def parse_coin_spend(coin_spend: CoinSpend, statutes_struct: Optional[Program] = None) -> ParsedSpend:
    """Route one spend to its driver and return what came back, or why it failed."""
    if statutes_struct is None:
        statutes_struct = get_statutes_struct()

    coin = coin_spend.coin
    base = dict(
        coin_id=coin.name().hex(),
        puzzle_hash=bytes(coin.puzzle_hash).hex(),
        amount=coin.amount,
    )
    mod, _ = uncurry(coin_spend.puzzle_reveal)
    mod_hash = mod.get_tree_hash()
    routes = _routes(coin_spend)

    is_cat = mod_hash == CAT_MOD_HASH or mod == CAT_MOD
    if not is_cat and mod_hash not in routes:
        return ParsedSpend(index=0, coin_type="unrecognised", **base)

    note = None
    layer = None
    asset = None
    launcher_id = None
    if is_cat:
        layer = "CAT"
        coin_type, asset, note = _cat_coin_type(coin_spend)
    elif mod_hash in (SINGLETON_MOD_HASH, SINGLETON_ISA_MOD_HASH):
        is_isa = mod_hash == SINGLETON_ISA_MOD_HASH
        layer = "ISA singleton" if is_isa else "singleton"
        coin_type = _singleton_coin_type(coin_spend, is_isa=is_isa)
        launcher = _singleton_launcher_id(coin_spend)
        launcher_id = launcher.hex() if launcher else None
    else:
        coin_type = routes[mod_hash][0]
    base.update(layer=layer, asset=asset, launcher_id=launcher_id)
    try:
        info = _parse_cat(coin_spend, statutes_struct) if is_cat else routes[mod_hash][1]()
    except Exception as err:  # noqa: BLE001 - any driver failure is a result, not a crash
        return ParsedSpend(
            index=0, coin_type=coin_type, error=f"{type(err).__name__}: {err}", note=note, **base
        )
    return ParsedSpend(index=0, coin_type=coin_type, info=info, note=note, **base)


def parse_spend_bundle(coin_spends: list[CoinSpend], statutes_struct: Optional[Program] = None) -> list[ParsedSpend]:
    """Parse every spend in a bundle, keeping going past the ones that fail."""
    if statutes_struct is None:
        statutes_struct = get_statutes_struct()
    parsed = []
    for i, coin_spend in enumerate(coin_spends):
        result = parse_coin_spend(coin_spend, statutes_struct)
        result.index = i
        parsed.append(result)
    return parsed
