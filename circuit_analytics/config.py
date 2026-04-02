"""
Network configuration loaded from environment variables.

These values are deterministic per network — they are computed once from
the deployed puzzle set and never change. Set them for your target network:

  Mainnet env.sh:
    # Protocol launched at block 8135347 (2026-01-06 11:29:45 UTC, timestamp 1767698985)
    export BYC_TAIL_HASH=ae1536f56760e471ad85ead45f00d680ff9cca73b8cc3407be778f1c0c606eac
    export CRT_TAIL_HASH=ea3ace5525d6aaf6d921b66052afc67da11c820b676de91d61ae1a766c8ce615
    export STATUTES_LAUNCHER_ID=101d3e673757782c8f8ac1eb3d531c543df899022bf81a427db4199108d4cdb1
    export ANNOUNCER_REGISTRY_LAUNCHER_ID=01734254bdfb1ec3abfde934bd9ea3b8b19645ee43eff9fea0b8ff51c39a8ae7
    export GENESIS_CHALLENGE=ccd5bb71183532bff220ba46c268991a3ff07eb358e8255a65c30a2dce0e5fbb
    export CIRCUIT_ANNOUNCER_REGISTRY_CONSTRAINTS=300000001,99
    # CLVM-encoded list of 5 approved mod hashes (vault, surplus auction, recharge auction,
    # savings vault, announcer registry) — read by circuit_analytics/drivers/statutes.py
    export CIRCUIT_APPROVED_MOD_HASHES=ffa0c092cc686dad5f31cd3c008d2daa3b1bae044bd50c1fd01ca0af96660dc8e391ffa06253104cf7de1bcbbd34cd10897794737db32fcf9d57bfa9bec13c741fb4c8d2ffa081e0cc376e53e97da0ee154992a3554aa679b8818c88dc54b1fb4c2463c0c786ffa02a3922ea385178c37687a958ab9b51d698888ba6bd8782e7b6c97a771b130aa3ffa0faa2ed871f9b4f5f679cf7d4d306d8b13c9ecceb46c7b1d4f9dd8d3e86c0fc7280

  Testnet env.sh:
    export GENESIS_CHALLENGE=37a90eb5185a9c4439a91ddc98bbadce7b4feba060d50116a067de66bf236615
    ...

Run `circuit-scan verify-config` to confirm these values are consistent
with the deployed puzzles.
"""

import os
from chia_rs.sized_bytes import bytes32


def _require(name: str) -> bytes32:
    val = os.environ.get(name)
    if not val:
        raise EnvironmentError(f"Required environment variable {name} is not set")
    return bytes32.fromhex(val)


# Token tail hashes — replaces construct_byc_tail / construct_crt_tail calls
BYC_TAIL_HASH: bytes32 = _require("BYC_TAIL_HASH")
CRT_TAIL_HASH: bytes32 = _require("CRT_TAIL_HASH")

# Protocol singleton launcher IDs
STATUTES_LAUNCHER_ID: bytes32 = _require("STATUTES_LAUNCHER_ID")
ANNOUNCER_REGISTRY_LAUNCHER_ID: bytes32 = _require("ANNOUNCER_REGISTRY_LAUNCHER_ID")

# Chia network genesis challenge (AGG_SIG_ME additional data)
GENESIS_CHALLENGE: bytes32 = _require("GENESIS_CHALLENGE")
