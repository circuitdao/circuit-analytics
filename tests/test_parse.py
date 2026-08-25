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
from circuit_analytics.mods import CAT_MOD, TREASURY_MOD, ZERO_FAUCET
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

    cat_puzzle = CAT_MOD.curry(CAT_MOD.get_tree_hash(), BYC_TAIL_HASH, inner)
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

    role, asset, note = _cat_coin_type(spend)
    assert role == "unknown inner puzzle"
    assert asset == "BYC"
    assert note is not None
    assert "not a puzzle this build knows" in note
    assert "verify-config" in note


def test_known_inner_puzzle_carries_no_note():
    role, asset, note = _cat_coin_type(_treasury_spend())
    assert role == "treasury"
    assert asset == "BYC"  # protocol coins carry their asset too, not just the ordinary ones
    assert note is None


def _byc_cat(inner_puzzle, tail_hash=None):
    """A BYC CAT holding the given inner puzzle."""
    cat = CAT_MOD.curry(CAT_MOD.get_tree_hash(), tail_hash or BYC_TAIL_HASH, inner_puzzle)
    solution = Program.to(
        [[[]], None, b"p" * 32, [b"c" * 32, b"i" * 32, 100], [b"n" * 32, b"j" * 32, 100], 0, 0]
    )
    return make_spend(Coin(b"c" * 32, cat.get_tree_hash(), 0), cat, solution)


def test_standard_tx_held_byc_is_named_not_flagged():
    """A key-controlled BYC coin is ordinary; the CAT layer really is the whole story."""
    from chia.wallet.puzzles.p2_delegated_puzzle_or_hidden_puzzle import MOD as P2_DELEGATED_MOD

    role, asset, note = _cat_coin_type(_byc_cat(P2_DELEGATED_MOD.curry(b"\xc0" + b"\x00" * 47)))
    assert role == "standard tx"
    assert asset == "BYC"
    assert note is None


def test_zero_faucet_byc_is_named_not_flagged():
    """Zero-amount BYC coins used for savings withdrawal vacuuming."""
    role, asset, note = _cat_coin_type(_byc_cat(ZERO_FAUCET.curry(ZERO_FAUCET.get_tree_hash())))
    # Named like any other protocol puzzle, with the asset alongside rather than wrapped in.
    assert role == "zero faucet"
    assert asset == "BYC"
    assert note is None


def test_offer_held_byc_is_named_not_flagged():
    """The settlement puzzle carries no curried args, so it is matched as the puzzle itself."""
    from chia.wallet.trading.offer import OFFER_MOD

    role, asset, note = _cat_coin_type(_byc_cat(OFFER_MOD))
    assert role == "offer"
    assert asset == "BYC"
    assert note is None


def test_qualified_label_on_an_unrelated_cat_is_still_not_a_protocol_coin():
    """A familiar role on an unrelated asset must not count as a protocol coin."""
    from chia.wallet.puzzles.p2_delegated_puzzle_or_hidden_puzzle import MOD as P2_DELEGATED_MOD

    from circuit_analytics.parse import protocol_coin_type

    spend = _byc_cat(P2_DELEGATED_MOD.curry(b"\xc0" + b"\x00" * 47), tail_hash=b"z" * 32)
    assert _cat_coin_type(spend)[0] == "standard tx"
    assert protocol_coin_type(spend) is None


def test_crt_is_distinguished_from_byc():
    from chia.wallet.puzzles.p2_delegated_puzzle_or_hidden_puzzle import MOD as P2_DELEGATED_MOD

    from circuit_analytics.config import CRT_TAIL_HASH

    spend = _byc_cat(P2_DELEGATED_MOD.curry(b"\xc0" + b"\x00" * 47), tail_hash=CRT_TAIL_HASH)
    role, asset, _note = _cat_coin_type(spend)
    assert (role, asset) == ("standard tx", "CRT")


def test_other_assets_are_named_by_asset_id():
    """Saying only "CAT" leaves no way to tell which asset was involved."""
    from chia.wallet.puzzles.p2_delegated_puzzle_or_hidden_puzzle import MOD as P2_DELEGATED_MOD

    asset_id = bytes.fromhex("ab" * 32)
    spend = _byc_cat(P2_DELEGATED_MOD.curry(b"\xc0" + b"\x00" * 47), tail_hash=asset_id)
    role, asset, _note = _cat_coin_type(spend)
    assert role == "standard tx"
    assert asset is not None and "ababababab" in asset


# --- singletons ---------------------------------------------------------------------


def _singleton(mod, launcher_id):
    from circuit_analytics.mods import SINGLETON_LAUNCHER_HASH

    struct = Program.to((mod.get_tree_hash(), (launcher_id, SINGLETON_LAUNCHER_HASH)))
    puzzle = mod.curry(struct, Program.to(1))
    return make_spend(Coin(b"c" * 32, puzzle.get_tree_hash(), 1), puzzle, Program.to([]))


def test_isa_singleton_with_the_statutes_launcher_is_named_statutes():
    from circuit_analytics.config import STATUTES_LAUNCHER_ID
    from circuit_analytics.mods import SINGLETON_ISA_MOD
    from circuit_analytics.parse import _singleton_coin_type

    spend = _singleton(SINGLETON_ISA_MOD, STATUTES_LAUNCHER_ID)
    assert _singleton_coin_type(spend, is_isa=True) == "statutes"
    parsed = parse_coin_spend(spend)
    assert parsed.layer == "ISA singleton"
    assert parsed.launcher_id == bytes(STATUTES_LAUNCHER_ID).hex()


def test_other_isa_singletons_are_named_generically():
    from circuit_analytics.mods import SINGLETON_ISA_MOD
    from circuit_analytics.parse import _singleton_coin_type

    spend = _singleton(SINGLETON_ISA_MOD, b"\x11" * 32)
    # The layer field already says "ISA singleton", so the role names what is unknown.
    assert _singleton_coin_type(spend, is_isa=True) == "unknown launcher"
    assert parse_coin_spend(spend).layer == "ISA singleton"


def test_standard_singletons_are_named_generically():
    from circuit_analytics.mods import SINGLETON_MOD
    from circuit_analytics.parse import _singleton_coin_type

    spend = _singleton(SINGLETON_MOD, b"\x22" * 32)
    assert _singleton_coin_type(spend, is_isa=False) == "unknown launcher"
    assert parse_coin_spend(spend).layer == "singleton"


def test_a_foreign_standard_singleton_is_not_a_protocol_coin():
    """NFTs and DIDs use the standard singleton layer too.

    Treating every standard singleton as the oracle would make each of them seed the block
    linkage walk and drag unrelated transactions into the output.
    """
    from circuit_analytics.mods import SINGLETON_MOD
    from circuit_analytics.parse import protocol_coin_type

    assert protocol_coin_type(_singleton(SINGLETON_MOD, b"\x22" * 32)) is None


def test_every_wrapped_coin_reports_its_layer():
    """The layer is what wraps the inner puzzle, and it is orthogonal to the role.

    A treasury coin is a CAT holding BYC; the statutes coin is an ISA singleton. Neither fact
    is derivable from the role alone, and both are what you reach for first when reading an
    unfamiliar spend.
    """
    from circuit_analytics.mods import SINGLETON_ISA_MOD

    from circuit_analytics.config import STATUTES_LAUNCHER_ID

    treasury = parse_coin_spend(_treasury_spend())
    assert (treasury.coin_type, treasury.layer, treasury.asset) == ("treasury", "CAT", "BYC")

    statutes = parse_coin_spend(_singleton(SINGLETON_ISA_MOD, STATUTES_LAUNCHER_ID))
    assert (statutes.coin_type, statutes.layer) == ("statutes", "ISA singleton")
    assert statutes.asset is None  # a singleton holds no asset


def test_unwrapped_puzzles_report_no_layer():
    """A collateral vault, announcer or registry coin is its own top-level puzzle."""
    puzzle = Program.to(1)
    spend = make_spend(Coin(b"c" * 32, puzzle.get_tree_hash(), 1), puzzle, Program.to([]))
    parsed = parse_coin_spend(spend)
    assert parsed.layer is None
    assert parsed.asset is None


def test_wallet_and_offer_coins_are_not_protocol_coins():
    """Holding BYC or CRT is not the same as being protocol state.

    Two people trading BYC is not protocol activity, so these must not seed the block walk.
    They still appear when something ties them to a protocol spend -- see test_linkage, where
    a linked spend is pulled in whether or not it is a protocol coin -- and then the link
    line says what tied it, which is the useful part.
    """
    from chia.wallet.puzzles.p2_delegated_puzzle_or_hidden_puzzle import MOD as P2_DELEGATED_MOD
    from chia.wallet.trading.offer import OFFER_MOD

    from circuit_analytics.config import CRT_TAIL_HASH
    from circuit_analytics.parse import protocol_coin_type

    standard_tx = P2_DELEGATED_MOD.curry(b"\xc0" + b"\x00" * 47)
    assert protocol_coin_type(_byc_cat(standard_tx)) is None
    assert protocol_coin_type(_byc_cat(standard_tx, tail_hash=CRT_TAIL_HASH)) is None
    assert protocol_coin_type(_byc_cat(OFFER_MOD)) is None


def test_zero_faucet_is_still_a_protocol_coin():
    """It is protocol machinery: it mints the zero-amount BYC coins savings withdrawals need."""
    from circuit_analytics.parse import protocol_coin_type

    spend = _byc_cat(ZERO_FAUCET.curry(ZERO_FAUCET.get_tree_hash()))
    assert protocol_coin_type(spend) == "zero faucet"


def test_an_unrecognised_inner_puzzle_still_counts_as_a_protocol_coin():
    """If the puzzle set does not match the chain, the coin may well be protocol state.

    Dropping it would hide the very spends the mismatch warning is about.
    """
    from circuit_analytics.parse import protocol_coin_type

    stranger = Program.to(2).curry(b"a" * 32, b"b" * 32)
    assert protocol_coin_type(_byc_cat(stranger)) == "unknown inner puzzle"
