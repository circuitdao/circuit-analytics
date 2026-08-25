"""Rendering of parsed values.

The formatting exists so that a spend can be read against a puzzle, so the cases worth
pinning are the ones where a raw number misleads.
"""

from circuit_analytics.render import _fmt_value


def test_large_values_are_rendered_as_the_positive_integers_they_are():
    """BYC amounts legitimately reach Chia's MAX_COIN_AMOUNT during a flashloan.

    Reading anything near 2**64 as a negative serialised unsigned would misreport those, and
    CAT ring subtotals follow the amounts, so they reach the same magnitudes.
    """
    from chia.consensus.default_constants import DEFAULT_CONSTANTS

    max_coin_amount = DEFAULT_CONSTANTS.MAX_COIN_AMOUNT
    assert max_coin_amount == 2**64 - 1
    assert _fmt_value("prev_subtotal", max_coin_amount, False) == "18,446,744,073,709,551,615"
    assert "18,446,744,073,709,551,615" in _fmt_value("amount", max_coin_amount, False)


def test_unit_conversion_is_exact_at_the_largest_amounts():
    """Float division loses precision long before Chia's MAX_COIN_AMOUNT.

    18446744073709551615 / 1000 as a float rounds to 18446744073709552.0, misreporting a
    flashloan by hundreds of milli-BYC. The conversion is integer arithmetic for that reason.
    """
    from chia.consensus.default_constants import DEFAULT_CONSTANTS

    from circuit_analytics.render import MCAT, _scaled

    amount = DEFAULT_CONSTANTS.MAX_COIN_AMOUNT
    assert _scaled(amount, MCAT, 3) == "18,446,744,073,709,551.615"
    # The float route, for contrast, does not even end in the right digits.
    assert f"{amount / MCAT:.3f}" != "18446744073709551.615"


def test_scaled_handles_zero_negatives_and_the_smallest_unit():
    from circuit_analytics.render import MCAT, MOJOS, _scaled

    assert _scaled(0, MCAT, 3) == "0.000"
    assert _scaled(-4_950, MCAT, 3) == "-4.950"
    assert _scaled(1, MOJOS, 12, strip=True) == "0.000000000001"
    assert _scaled(1_500_000_000_000, MOJOS, 12, strip=True) == "1.5"


def test_genuinely_negative_values_are_shown_negative():
    """CLVM reads atoms as two's complement, so a negative subtotal arrives negative."""
    assert _fmt_value("prev_subtotal", -1, False) == "-1"
    assert _fmt_value("prev_subtotal", 0, False) == "0"


def test_amounts_are_converted_to_their_units():
    assert "2.500 BYC" in _fmt_value("amount", 2_500, False)
    assert "0.050 CRT" in _fmt_value("crt_bid_amount", 50, False)
    assert "1.5 XCH" in _fmt_value("collateral", 1_500_000_000_000, False)


def test_unknown_integer_fields_are_not_guessed_at():
    """Guessing a unit wrong is worse than showing the raw value."""
    assert _fmt_value("some_counter", 1_234, False) == "1,234"


# --- value layout ---------------------------------------------------------------------


def test_hash_sized_values_are_shown_whole():
    """A 32-byte hash is the thing you want to copy, so it is never truncated."""
    h = bytes.fromhex("ab" * 32)
    assert _fmt_value("x", h, False) == "ab" * 32


def test_values_up_to_the_inline_limit_are_shown_whole():
    """A little headroom above 32 keeps values that carry a hash readable too."""
    from circuit_analytics.render import INLINE_BYTES

    assert INLINE_BYTES == 34
    at_limit = bytes.fromhex("cd" * INLINE_BYTES)
    assert _fmt_value("x", at_limit, False) == at_limit.hex()


def test_values_past_the_inline_limit_are_truncated_with_their_size():
    from circuit_analytics.render import INLINE_BYTES

    over = bytes.fromhex("cd" * (INLINE_BYTES + 1))
    rendered = _fmt_value("x", over, False)
    assert rendered.startswith("cd" * INLINE_BYTES)
    assert "…" in rendered
    assert f"({INLINE_BYTES + 1} bytes)" in rendered


def test_full_shows_everything_on_one_line():
    """--full exists so a value can be pasted somewhere else."""
    big = bytes(range(256)) * 2
    rendered = _fmt_value("x", big, False, full=True)
    assert rendered == big.hex()
    assert "\n" not in rendered
    assert "…" not in rendered


def test_full_does_not_expand_sequences():
    """One copyable line per arg is the whole point, so a list stays on its line.

    The serialised form is what CLVM tools accept, which is what makes it worth copying.
    """
    from chia.types.blockchain_format.program import Program

    from circuit_analytics.render import _render_field

    prog = Program.to([bytes.fromhex("ab" * 32), bytes.fromhex("cd" * 32)])
    lines = _render_field("args", "args", prog, colour=False, full=True, indent="  ")
    assert len(lines) == 1
    assert lines[0].strip() == f"args {prog.as_bin().hex()}"


def test_full_renders_python_sequences_on_one_line_too():
    from circuit_analytics.render import _render_field

    hashes = [bytes.fromhex(c * 64) for c in "ab"]
    lines = _render_field("mods", "mods", hashes, colour=False, full=True, indent="  ")
    assert len(lines) == 1
    assert lines[0].strip() == f"mods [{'a' * 64}, {'b' * 64}]"


def test_lists_put_each_element_on_its_own_line():
    from circuit_analytics.render import _render_field

    hashes = [bytes.fromhex(c * 64) for c in "abc"]
    lines = _render_field("approval_mod_hashes", "approval_mod_hashes", hashes,
                          colour=False, full=False, indent="  ", details=True)
    assert lines[0].strip() == "approval_mod_hashes"
    assert len(lines) == 4
    for i, char in enumerate("abc"):
        assert lines[i + 1].strip() == f"[{i}] {char * 64}"


def test_programs_that_are_lists_are_broken_apart_too():
    """Solution args arrive as a Program, and reading them on one line is what made the
    old output unusable."""
    from chia.types.blockchain_format.program import Program

    from circuit_analytics.render import _render_field

    prog = Program.to([bytes.fromhex("ab" * 32), 42])
    lines = _render_field("args", "args", prog, colour=False, full=False, indent="  ", details=True)
    assert lines[0].strip() == "args"
    assert lines[1].strip() == f"[0] {'ab' * 32}"


def test_empty_sequences_are_not_expanded():
    from chia.types.blockchain_format.program import Program

    from circuit_analytics.render import _render_field

    lines = _render_field("args", "args", Program.to([]), colour=False, full=False, indent="  ", details=True)
    assert len(lines) == 1
    assert lines[0].strip() == "args ()"


def test_coin_amounts_carry_their_units():
    """The coin's amount is the first number you read; raw mojos or milli-units are not it."""
    from types import SimpleNamespace

    from circuit_analytics.render import _fmt_coin_amount

    byc = SimpleNamespace(amount=5_000, layer="CAT", asset="BYC")
    assert _fmt_coin_amount(byc, False) == "5,000 (5.000 BYC)"

    crt = SimpleNamespace(amount=50, layer="CAT", asset="CRT")
    assert _fmt_coin_amount(crt, False) == "50 (0.050 CRT)"

    # An unrecognised asset still gets the standard three CAT decimals, labelled CAT.
    other = SimpleNamespace(amount=2_500, layer="CAT", asset="ababababab…ababab")
    assert _fmt_coin_amount(other, False) == "2,500 (2.500 CAT)"

    # Anything not wrapped in a CAT is denominated in mojos.
    vault = SimpleNamespace(amount=1_500_000_000_000, layer=None, asset=None)
    assert _fmt_coin_amount(vault, False) == "1,500,000,000,000 (1.5 XCH)"


def test_a_flashloan_sized_coin_amount_converts_exactly():
    from types import SimpleNamespace

    from chia.consensus.default_constants import DEFAULT_CONSTANTS

    from circuit_analytics.render import _fmt_coin_amount

    coin = SimpleNamespace(amount=DEFAULT_CONSTANTS.MAX_COIN_AMOUNT, layer="CAT", asset="BYC")
    assert _fmt_coin_amount(coin, False) == (
        "18,446,744,073,709,551,615 (18,446,744,073,709,551.615 BYC)"
    )


# --- the three rendering modes -------------------------------------------------------


def _lines(prog, **kwargs):
    from circuit_analytics.render import _render_field

    return _render_field("args", "args", prog, colour=False, indent="  ", **kwargs)


def _sample():
    from chia.types.blockchain_format.program import Program

    return Program.to([bytes.fromhex("ab" * 32), bytes.fromhex("cd" * 32)])


def test_default_is_compact():
    """One line per field, truncated. This is what you want when scanning a whole block."""
    lines = _lines(_sample(), full=False, details=False)
    assert len(lines) == 1
    assert "…" in lines[0]
    assert "bytes)" in lines[0]


def test_details_expands_sequences_but_still_truncates():
    lines = _lines(_sample(), full=False, details=True)
    assert len(lines) == 3
    assert lines[1].strip() == f"[0] {'ab' * 32}"


def test_full_stops_truncating_but_does_not_expand():
    lines = _lines(_sample(), full=True, details=False)
    assert len(lines) == 1
    assert "…" not in lines[0]
    assert lines[0].strip() == f"args {_sample().as_bin().hex()}"


def test_details_and_full_together_expand_without_truncating():
    """The two switches are independent, so asking for both is coherent."""
    big = [bytes(range(64)), bytes(range(64))]
    lines = _lines(big, full=True, details=True)
    assert len(lines) == 3
    assert "…" not in "".join(lines)
    assert lines[1].strip() == f"[0] {bytes(range(64)).hex()}"


def test_list_elements_get_a_shorter_budget_than_a_field_of_their_own():
    """Several values sharing one line each need far less room than a standalone field.

    Enough to recognise a value and tell it from its neighbours; --details reads one properly.
    """
    from circuit_analytics.render import ELEMENT_BYTES, INLINE_BYTES

    assert ELEMENT_BYTES < INLINE_BYTES
    conditions = [bytes.fromhex("ff3cffa1" + "cb" * 34), bytes.fromhex("ff3dffa0" + "1c" * 33)]
    rendered = _fmt_value("inner_puzzle_output_conditions", conditions, False)
    assert rendered.startswith("[ff3cffa1cbcbcbcbcbcb… (38 bytes), ")
    assert rendered.endswith("(37 bytes)]")


def test_a_standalone_field_keeps_the_larger_budget():
    """Shortening list elements must not shorten ordinary hash fields."""
    from circuit_analytics.render import INLINE_BYTES

    h = bytes.fromhex("73" * 32)
    assert _fmt_value("statutes_inner_puzzle_hash", h, False) == h.hex()
    assert len(h) <= INLINE_BYTES


def test_full_ignores_the_element_budget():
    conditions = [bytes.fromhex("ab" * 40), bytes.fromhex("cd" * 40)]
    rendered = _fmt_value("x", conditions, False, full=True)
    assert "…" not in rendered
    assert ("ab" * 40) in rendered and ("cd" * 40) in rendered
