"""Check that the local puzzle set matches the network being read.

Every protocol puzzle hash is derived from the compiled puzzles in the installed
`circuit_puzzles`. If that build differs from what is deployed -- an unreleased branch, or a
wheel older than the chain -- then no protocol coin will be recognised by its puzzle, and
tools quietly degrade instead of failing: a savings vault spend parses as a plain BYC CAT,
and a block full of protocol activity looks almost empty.

`CIRCUIT_APPROVED_MOD_HASHES` is the reference. It holds the approval mod hashes the deployed
Statutes coin actually authorises, so comparing the locally computed hashes against it detects
the mismatch without needing a node.

Usage:
    circuit-scan verify-config
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class ModComparison:
    name: str
    deployed: bytes
    local: bytes

    @property
    def matches(self) -> bool:
        return bytes(self.deployed) == bytes(self.local)


def compare_puzzle_set() -> list:
    """Locally computed approval mod hashes against the deployed ones, in statute order."""
    from circuit_analytics import mods
    from circuit_analytics.drivers.registry import AnnouncerRegistry
    from circuit_analytics.drivers.statutes import Statutes

    deployed = Statutes.get_approved_mod_hashes()
    local = [
        ("collateral vault", mods.COLLATERAL_VAULT_MOD_HASH),
        ("surplus auction", mods.SURPLUS_AUCTION_MOD_HASH),
        ("recharge auction", mods.RECHARGE_AUCTION_MOD_HASH),
        ("savings vault", mods.SAVINGS_VAULT_MOD_HASH),
        ("announcer registry", AnnouncerRegistry.get_mod_struct()[1]),
    ]
    return [
        ModComparison(name=name, deployed=bytes(d), local=bytes(value))
        for (name, value), d in zip(local, deployed)
    ]


def puzzle_set_warning() -> Optional[str]:
    """A warning if the local puzzles do not match the deployed ones, else None."""
    try:
        comparisons = compare_puzzle_set()
    except Exception as err:  # noqa: BLE001 - never let the check break the command
        return f"could not compare the local puzzle set against CIRCUIT_APPROVED_MOD_HASHES: {err}"

    mismatched = [c for c in comparisons if not c.matches]
    if not mismatched:
        return None
    lines = [
        "the installed circuit_puzzles does not match the deployed protocol, so protocol",
        "coins will not be recognised by their puzzle and results will be incomplete:",
        "",
    ]
    for c in mismatched:
        lines.append(f"  {c.name:<20} deployed {c.deployed.hex()}")
        lines.append(f"  {'':<20} local    {c.local.hex()}")
    lines += [
        "",
        "Install a circuit_puzzles build matching the chain you are reading, or point",
        "CIRCUIT_APPROVED_MOD_HASHES at the network these puzzles belong to.",
    ]
    return "\n".join(lines)


def verify() -> None:
    from circuit_analytics import config, mods

    print("Config:")
    print(f"  BYC_TAIL_HASH                   {config.BYC_TAIL_HASH.hex()}")
    print(f"  CRT_TAIL_HASH                   {config.CRT_TAIL_HASH.hex()}")
    print(f"  STATUTES_LAUNCHER_ID            {config.STATUTES_LAUNCHER_ID.hex()}")
    print(f"  ANNOUNCER_REGISTRY_LAUNCHER_ID  {config.ANNOUNCER_REGISTRY_LAUNCHER_ID.hex()}")
    print(f"  GENESIS_CHALLENGE               {config.GENESIS_CHALLENGE.hex()}")
    print()
    print("Puzzle mod hashes (from the installed circuit_puzzles):")
    print(f"  CAT_MOD_HASH                    {mods.CAT_MOD_HASH.hex()}")
    print(f"  TREASURY_MOD_HASH               {mods.TREASURY_MOD_HASH.hex()}")
    print()

    comparisons = compare_puzzle_set()
    print("Approval mod hashes vs CIRCUIT_APPROVED_MOD_HASHES:")
    for c in comparisons:
        print(f"  {'ok ' if c.matches else 'DIFFERS'}  {c.name:<20} {c.local.hex()}")
        if not c.matches:
            print(f"           {'deployed':<20} {c.deployed.hex()}")
    print()

    warning = puzzle_set_warning()
    if warning:
        print(warning)
        raise SystemExit(1)
    print("OK: the local puzzle set matches the deployed protocol.")


if __name__ == "__main__":
    verify()
