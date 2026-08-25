"""The spend parser used by `circuit-scan parse`.

Its job is to route a spend to the same driver the block scanner would, and to report a
driver failure as a result rather than crashing. That failure is the interesting case: it is
what a parser drifted from its puzzle looks like, and it is what stalls the scanner, because
the scanner calls the drivers unguarded and has no per-spend try/except above them.
"""

from chia.types.blockchain_format.program import Program
from chia.types.coin_spend import make_spend
from chia.types.condition_opcodes import ConditionOpcode
from chia_rs import Coin

from circuit_analytics.config import BYC_TAIL_HASH
from circuit_analytics.drivers import SOLUTION_PREFIX
from circuit_analytics.drivers.treasury import TreasuryChangeBalanceInfo
from circuit_analytics.mods import BYC_TAIL_MOD_HASH, CAT_MOD, TREASURY_MOD
from circuit_analytics.parse import _cat_coin_type, parse_coin_spend, parse_spend_bundle

STATUTES_STRUCT = Program.to((b"t" * 32, (b"u" * 32, b"v" * 32)))
STATUTES_INNER_PUZZLE_HASH = b"s" * 32
LAUNCHER_ID = b"l" * 32
RING_PREV_LAUNCHER_ID = b"r" * 32
COLLATERAL_VAULT_MOD_HASH = b"1" * 32
APPROVAL_MOD_HASHES = [COLLATERAL_VAULT_MOD_HASH, b"2" * 32, b"3" * 32, b"4" * 32, b"5" * 32]


def _treasury_spend():
    """A treasury change-balance spend, wrapped in its CAT layer as it appears on chain."""
    inner = TREASURY_MOD.curry(
        TREASURY_MOD.get_tree_hash(), STATUTES_STRUCT, LAUNCHER_ID, RING_PREV_LAUNCHER_ID
    )
    # A collateral vault approving a deposit: not a CAT, so the approver args are a bare hash.
    args = [
        b"p" * 32,  # approver_parent_id
        COLLATERAL_VAULT_MOD_HASH,
        b"a" * 32,  # approver_mod_curried_args_hash
        1_000,  # approver_amount
        APPROVAL_MOD_HASHES,
        6_000,  # new_amount
        None,  # run_tail_mod_hash
    ]
    remark_body = (None, (STATUTES_INNER_PUZZLE_HASH, (5_000, (None, args))))
    conditions = [[ConditionOpcode.REMARK, SOLUTION_PREFIX, remark_body]]
    owner_puzzle = Program.to(2)  # returns its solution
    cat_inner_solution = Program.to([owner_puzzle, [conditions]])

    byc_tail = Program.to(BYC_TAIL_MOD_HASH).get_tree_hash()
    cat_puzzle = CAT_MOD.curry(CAT_MOD.get_tree_hash(), byc_tail, inner)
    return make_spend(
        Coin(b"c" * 32, cat_puzzle.get_tree_hash(), 5_000), cat_puzzle, Program.to([cat_inner_solution])
    )


def test_routes_treasury():
    parsed = parse_coin_spend(_treasury_spend())
    assert parsed.coin_type == "treasury"
    assert isinstance(parsed.info, TreasuryChangeBalanceInfo)
    assert not parsed.failed


def test_parses_a_whole_bundle_and_indexes_it():
    parsed = parse_spend_bundle([_treasury_spend(), _treasury_spend()])
    assert [p.index for p in parsed] == [0, 1]
    assert all(p.coin_type == "treasury" for p in parsed)
    assert not any(p.failed for p in parsed)


def test_unrecognised_puzzle_is_reported_not_raised():
    """A spend of some unrelated puzzle is not an error; nothing simply claims it."""
    puzzle = Program.to(1)
    spend = make_spend(Coin(b"c" * 32, puzzle.get_tree_hash(), 1), puzzle, Program.to([]))
    parsed = parse_coin_spend(spend)
    assert parsed.coin_type == "unrecognised"
    assert parsed.info is None
    assert not parsed.failed


def _break_treasury(monkeypatch):
    import circuit_analytics.parse as parse_mod

    def _boom(*_args, **_kwargs):
        raise ValueError("simulated shape drift")

    monkeypatch.setattr(
        parse_mod,
        "CAT_INNER_MODS",
        tuple(
            (mod, name, _boom if name == "treasury" else fn)
            for mod, name, fn in parse_mod.CAT_INNER_MODS
        ),
    )


def test_driver_failure_is_captured_with_the_right_coin_type(monkeypatch):
    """A drifted driver must be reported against the puzzle it belongs to.

    The coin type is resolved before the driver runs precisely so this holds: attributing a
    failure to a generic "CAT" would send whoever is debugging to the wrong parser.
    """
    _break_treasury(monkeypatch)
    parsed = parse_coin_spend(_treasury_spend())
    assert parsed.failed
    assert parsed.coin_type == "treasury"  # not "CAT"
    assert "simulated shape drift" in parsed.error
    assert parsed.info is None


def test_bundle_keeps_going_past_a_failure(monkeypatch):
    """One bad spend must not hide the rest of the bundle."""
    _break_treasury(monkeypatch)
    puzzle = Program.to(1)
    ordinary = make_spend(Coin(b"c" * 32, puzzle.get_tree_hash(), 1), puzzle, Program.to([]))
    parsed = parse_spend_bundle([_treasury_spend(), ordinary])
    assert parsed[0].failed
    assert not parsed[1].failed


def test_unknown_cat_inner_puzzle_is_flagged_not_passed_off_as_a_plain_coin():
    """A BYC CAT whose inner puzzle this build does not know must say so.

    This is what a protocol coin looks like when the installed circuit_puzzles differs from
    what is deployed: the tail still identifies it as BYC, so without the note it reads as an
    ordinary BYC coin and the CAT layer info looks like a complete answer.
    """
    stranger = Program.to(2).curry(b"a" * 32, b"b" * 32)
    cat = CAT_MOD.curry(CAT_MOD.get_tree_hash(), BYC_TAIL_HASH, stranger)
    solution = Program.to(
        [[[]], None, b"p" * 32, [b"c" * 32, b"i" * 32, 100], [b"n" * 32, b"j" * 32, 100], 0, 0]
    )
    spend = make_spend(Coin(b"c" * 32, cat.get_tree_hash(), 12_345), cat, solution)

    label, note = _cat_coin_type(spend)
    assert label == "BYC"
    assert note is not None
    assert "not a protocol puzzle this build knows" in note
    assert "verify-config" in note


def test_known_inner_puzzle_carries_no_note():
    label, note = _cat_coin_type(_treasury_spend())
    assert label == "treasury"
    assert note is None
