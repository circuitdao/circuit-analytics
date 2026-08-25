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

from circuit_analytics.config import BYC_TAIL_HASH, CRT_TAIL_HASH
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
)
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
    """Which puzzle the spend was routed to, or "unrecognised"."""
    info: Optional[Any] = None
    """The driver's *SolutionInfo, or None if nothing claimed the spend."""
    error: Optional[str] = None
    """Set when the driver raised. This is the case worth looking for."""
    note: Optional[str] = None
    """Set when the result is less than it looks, e.g. only the CAT layer was parsed."""

    @property
    def failed(self) -> bool:
        return self.error is not None


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


def _cat_coin_type(coin_spend: CoinSpend) -> tuple[str, Optional[str]]:
    """Name a CAT spend's coin type without parsing it, plus a note if that name is thin.

    Naming the type first means a parse failure is still attributed to the right puzzle --
    which is the whole point when the failure is what you are chasing.

    A CAT whose inner puzzle matches no protocol mod is reported as such rather than passing
    quietly as a plain BYC or CRT coin: the usual cause is a local puzzle set that does not
    match the chain being read, and silently returning CAT layer info hides that completely.
    """
    try:
        inner_puzzle, _ = _cat_inner(coin_spend)
        inner_mod, _ = uncurry(inner_puzzle)
        for mod, name, _parse in CAT_INNER_MODS:
            if inner_mod == mod:
                return name, None
        if inner_mod in (GOVERNANCE_MOD, LAUNCH_GOVERNANCE_MOD):
            return "governance", None
        _, cat_args = uncurry(coin_spend.puzzle_reveal)
        tail_hash = cat_args.at("rf").atom
        label = (
            "BYC" if tail_hash == bytes(BYC_TAIL_HASH)
            else "CRT" if tail_hash == bytes(CRT_TAIL_HASH)
            else "CAT"
        )
        if label in ("BYC", "CRT"):
            note = (
                f"inner puzzle {inner_mod.get_tree_hash().hex()} is not a protocol puzzle "
                "this build knows, so only the CAT layer was parsed. If this coin should be a "
                "savings vault, treasury, auction or payout coin, the local puzzle set "
                "does not match the chain -- run `circuit-scan verify-config`."
            )
            return label, note
        return label, None
    except Exception:  # noqa: BLE001 - naming is best-effort; the parse reports the real error
        return "CAT", None


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
        coin_type, _note = _cat_coin_type(coin_spend)
        # A plain BYC or CRT coin is a protocol coin; an unrelated CAT is not.
        return None if coin_type == "CAT" else coin_type
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
    if is_cat:
        coin_type, note = _cat_coin_type(coin_spend)
    else:
        coin_type = routes[mod_hash][0]
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
