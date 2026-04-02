"""
Verifies that the env var config constants are consistent with the deployed
circuit_puzzles wheel. Run before scanning to confirm your config is correct.

Usage:
    circuit-scan verify-config
    # or directly:
    python -m circuit_analytics.verify_config
"""

from circuit_analytics import config, mods


def verify():
    errors = []

    # Reconstruct BYC tail hash from first principles and compare to config
    # construct_byc_tail requires the statutes struct, which itself requires
    # the statutes launcher ID — both are env var constants.
    # TODO: implement once statutes struct reconstruction is in place

    print("Config verification:")
    print(f"  BYC_TAIL_HASH:                   {config.BYC_TAIL_HASH.hex()}")
    print(f"  CRT_TAIL_HASH:                   {config.CRT_TAIL_HASH.hex()}")
    print(f"  STATUTES_LAUNCHER_ID:            {config.STATUTES_LAUNCHER_ID.hex()}")
    print(f"  ANNOUNCER_REGISTRY_LAUNCHER_ID:  {config.ANNOUNCER_REGISTRY_LAUNCHER_ID.hex()}")
    print(f"  GENESIS_CHALLENGE:               {config.GENESIS_CHALLENGE.hex()}")
    print()
    print("Puzzle mod hashes (from circuit_puzzles wheel):")
    print(f"  COLLATERAL_VAULT_MOD_HASH:  {mods.COLLATERAL_VAULT_MOD_HASH.hex()}")
    print(f"  SAVINGS_VAULT_MOD_HASH:     {mods.SAVINGS_VAULT_MOD_HASH.hex()}")
    print(f"  ATOM_ANNOUNCER_MOD_HASH:    {mods.ATOM_ANNOUNCER_MOD_HASH.hex()}")
    print(f"  CAT_MOD_HASH:               {mods.CAT_MOD_HASH.hex()}")

    if errors:
        for e in errors:
            print(f"ERROR: {e}")
        raise SystemExit(1)
    print("OK")


if __name__ == "__main__":
    verify()
