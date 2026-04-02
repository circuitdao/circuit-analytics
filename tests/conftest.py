import os
import pytest

# Set required env vars before any module-level config.py evaluation.
# Using mainnet values so tests reflect real network constants.
os.environ.setdefault("BYC_TAIL_HASH", "ae1536f56760e471ad85ead45f00d680ff9cca73b8cc3407be778f1c0c606eac")
os.environ.setdefault("CRT_TAIL_HASH", "ea3ace5525d6aaf6d921b66052afc67da11c820b676de91d61ae1a766c8ce615")
os.environ.setdefault("STATUTES_LAUNCHER_ID", "101d3e673757782c8f8ac1eb3d531c543df899022bf81a427db4199108d4cdb1")
os.environ.setdefault("ANNOUNCER_REGISTRY_LAUNCHER_ID", "01734254bdfb1ec3abfde934bd9ea3b8b19645ee43eff9fea0b8ff51c39a8ae7")
os.environ.setdefault("GENESIS_CHALLENGE", "ccd5bb71183532bff220ba46c268991a3ff07eb358e8255a65c30a2dce0e5fbb")
os.environ.setdefault("CIRCUIT_ANNOUNCER_REGISTRY_CONSTRAINTS", "300000001,99")
# CLVM-encoded list of 5 approved mod hashes (collateral vault, surplus auction, recharge auction,
# savings vault, announcer registry). Value = Program.to([bytes.fromhex(h) for h in hashes]).as_bin().hex()
os.environ.setdefault(
    "CIRCUIT_APPROVED_MOD_HASHES",
    "ffa0c092cc686dad5f31cd3c008d2daa3b1bae044bd50c1fd01ca0af96660dc8e391"
    "ffa06253104cf7de1bcbbd34cd10897794737db32fcf9d57bfa9bec13c741fb4c8d2"
    "ffa081e0cc376e53e97da0ee154992a3554aa679b8818c88dc54b1fb4c2463c0c786"
    "ffa02a3922ea385178c37687a958ab9b51d698888ba6bd8782e7b6c97a771b130aa3"
    "ffa0faa2ed871f9b4f5f679cf7d4d306d8b13c9ecceb46c7b1d4f9dd8d3e86c0fc7280",
)


def pytest_addoption(parser):
    parser.addoption(
        "--max-blocks",
        type=int,
        default=None,
        help="Limit rescan test to this many blocks after the protocol launch block (default: scan all)",
    )


@pytest.fixture
def max_blocks(request):
    return request.config.getoption("--max-blocks")
