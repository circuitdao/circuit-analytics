"""Detecting a local puzzle set that does not match the deployed protocol.

This is the failure that makes every other tool lie: with mismatched puzzles no protocol coin
is recognised by its puzzle, so blocks look empty and CAT spends look ordinary.
"""

from circuit_analytics.verify_config import ModComparison, compare_puzzle_set


def test_comparison_covers_all_five_approval_mods():
    comparisons = compare_puzzle_set()
    assert [c.name for c in comparisons] == [
        "collateral vault",
        "surplus auction",
        "recharge auction",
        "savings vault",
        "announcer registry",
    ]


def test_matching_hashes_produce_no_warning(monkeypatch):
    import circuit_analytics.verify_config as vc

    same = [
        ModComparison(name="collateral vault", deployed=b"\x01" * 32, local=b"\x01" * 32),
        ModComparison(name="savings vault", deployed=b"\x02" * 32, local=b"\x02" * 32),
    ]
    monkeypatch.setattr(vc, "compare_puzzle_set", lambda: same)
    assert vc.puzzle_set_warning() is None


def test_mismatch_names_the_puzzles_and_both_hashes(monkeypatch):
    import circuit_analytics.verify_config as vc

    mixed = [
        ModComparison(name="collateral vault", deployed=b"\x01" * 32, local=b"\x01" * 32),
        ModComparison(name="savings vault", deployed=b"\xaa" * 32, local=b"\xbb" * 32),
    ]
    monkeypatch.setattr(vc, "compare_puzzle_set", lambda: mixed)
    warning = vc.puzzle_set_warning()
    assert warning is not None
    assert "savings vault" in warning
    assert ("aa" * 32) in warning and ("bb" * 32) in warning
    assert "collateral vault" not in warning  # only the mismatched ones are listed


def test_check_never_raises(monkeypatch):
    """A broken check must not take the command down with it."""
    import circuit_analytics.verify_config as vc

    def _boom():
        raise RuntimeError("no env")

    monkeypatch.setattr(vc, "compare_puzzle_set", _boom)
    assert "could not compare" in vc.puzzle_set_warning()
