"""Unit tests for CollateralVaultState.forbidden_byc_amount_to_bid_range.

The forbidden range must exactly match a brute-force scan of the on-chain bid predicate
(min-bid and treasury `any` clauses of vault_keeper_bid.clsp), restricted to the biddable
range [min_byc_amount_to_bid, max_byc_amount_to_bid]. Auction params are chosen so that
required_byc_bid_amount >> debt (max_byc_amount_to_bid == debt), which keeps the predicate
pure arithmetic -- the leftover_collateral == 0 escape never fires below the full debt.
"""

import pytest
from chia.types.blockchain_format.program import Program

from circuit_analytics.drivers.vault import CollateralVaultState
from circuit_analytics.drivers.protocol_math import calculate_expected_collateral

# collateral/start_price big enough that required_byc_bid_amount (5,000,000 mBYC) far exceeds
# every debt used below, so max_byc_amount_to_bid clips to seized_debt.
COLLATERAL = 1_000_000_000_000
START_PRICE = 500_000
START_TIME = 1000
CURRENT_TIME = 1000


def _make_state(
    ii: int, fees: int, melt: int, mba: int, collateral: int = COLLATERAL, start_price: int = START_PRICE
) -> CollateralVaultState:
    # auction_state layout: start_time, start_price, step_price_decrease_factor,
    # step_time_interval, initiator_puzzle_hash, initiator_incentive_balance, auction_ttl,
    # byc_to_treasury_balance, byc_to_melt_balance, minimum_bid_amount, min_price
    auction_state = Program.to(
        [START_TIME, start_price, 100, 60, bytes(32), ii, 10_000, fees, melt, mba, 1]
    )
    return CollateralVaultState(
        vault_mod_hash=bytes(32),
        statutes_struct=Program.to(0),
        collateral=collateral,
        principal=melt,
        auction_state=auction_state,
        inner_puzzle_hash=bytes(32),
        discounted_principal=0,
    )


def _bid_is_valid(bid, ii, fees, mba, delta):
    """The on-chain bid predicate, sans the leftover_collateral escape (required > debt here)."""
    byc_to_treasury = min(fees, max(0, bid - ii))
    min_ok = bid > mba
    treasury_ok = byc_to_treasury > delta or byc_to_treasury == fees or byc_to_treasury == 0
    return min_ok and treasury_ok


def _brute_force_forbidden(ii, fees, melt, mba, delta, min_bid, max_bid):
    """Scan every integer bid and return the contiguous forbidden run inside [min_bid, max_bid]."""
    debt = ii + fees + melt
    valid = {bid for bid in range(1, debt + 1) if _bid_is_valid(bid, ii, fees, mba, delta)}
    forbidden = [bid for bid in range(min_bid, max_bid + 1) if bid not in valid]
    return (forbidden[0], forbidden[-1]) if forbidden else None


# (name, ii, fees(T), melt(M), mba, delta)
SCENARIOS = [
    ("delta<T two-interval", 60, 1140, 10_000, 5, 40),
    ("delta>=T ends at full-clear", 60, 30, 10_000, 5, 40),
    ("mba==II gap at bottom", 60, 1140, 10_000, 60, 40),
    ("mba inside (II, II+delta)", 60, 1140, 10_000, 80, 40),
    ("mba above band -> no gap", 60, 1140, 10_000, 200, 40),
    ("no treasury fees -> no gap", 60, 0, 10_000, 5, 40),
    ("zero delta -> no gap", 60, 1140, 10_000, 5, 0),
    ("T==1 must clear fully -> no gap", 60, 1, 10_000, 5, 40),
]


@pytest.mark.parametrize("name,ii,fees,melt,mba,delta", SCENARIOS)
def test_forbidden_range_matches_brute_force(name, ii, fees, melt, mba, delta):
    state = _make_state(ii, fees, melt, mba)
    min_bid = state.min_byc_amount_to_bid(CURRENT_TIME, delta)
    max_bid = state.max_byc_amount_to_bid(CURRENT_TIME)
    # sanity: our params keep the envelope top at the full debt (escape never fires below it)
    assert max_bid == state.seized_debt

    # min_byc_amount_to_bid must itself be a valid bid -- never inside the dust band.
    assert _bid_is_valid(min_bid, ii, fees, mba, delta), f"{name}: min_bid {min_bid} is not valid"
    # max_byc_amount_to_bid must be a valid bid too. Here required > debt so max == debt, which
    # clears the whole treasury (byc_to_treasury == fees); _bid_is_valid captures that escape.
    assert _bid_is_valid(max_bid, ii, fees, mba, delta), f"{name}: max_bid {max_bid} is not valid"

    got = state.forbidden_byc_amount_to_bid_range(CURRENT_TIME, delta)
    expected = _brute_force_forbidden(ii, fees, melt, mba, delta, min_bid, max_bid)
    assert got == expected, f"{name}: got {got}, brute-force {expected}"

    if got is not None:
        # a genuine interior gap: both endpoints strictly inside (min, max), and the range itself
        # is entirely invalid while the amounts bracketing it are valid.
        lo, hi = got
        assert min_bid < lo <= hi < max_bid
        assert all(not _bid_is_valid(b, ii, fees, mba, delta) for b in range(lo, hi + 1))
        assert _bid_is_valid(lo - 1, ii, fees, mba, delta)
        assert _bid_is_valid(hi + 1, ii, fees, mba, delta)


def test_min_bid_bumped_past_dust_band_when_mba_ge_ii():
    # mba == II: raw min (mba+1) lands in the dust band; with delta known it must bump to the
    # first valid amount above the band (II + delta + 1), and no interior gap is reported.
    ii, fees, melt, mba, delta = 60, 1140, 10_000, 60, 40
    state = _make_state(ii, fees, melt, mba)
    assert state.min_byc_amount_to_bid(CURRENT_TIME) == mba + 1  # raw, no delta -> backward compatible
    assert state.min_byc_amount_to_bid(CURRENT_TIME, delta) == ii + delta + 1  # bumped past band
    assert state.forbidden_byc_amount_to_bid_range(CURRENT_TIME, delta) is None


def test_min_bid_unchanged_below_incentive_balance():
    # mba < II: raw min (mba+1) pays the incentive only (valid), so it is not bumped even with delta.
    ii, fees, melt, mba, delta = 60, 1140, 10_000, 5, 40
    state = _make_state(ii, fees, melt, mba)
    assert state.min_byc_amount_to_bid(CURRENT_TIME, delta) == mba + 1
    assert state.forbidden_byc_amount_to_bid_range(CURRENT_TIME, delta) == (ii + 1, ii + delta)


def test_max_bid_valid_when_required_below_debt():
    # A cheap auction where the bid for all collateral (required) is below the full debt but still a
    # genuine multi-point range: max_byc_amount_to_bid == required, and bidding it seizes all
    # collateral (leftover_collateral == 0), the escape that makes it valid in both `any` clauses.
    ii, fees, melt, mba = 60, 1140, 10_000, 5
    collateral, start_price = 10_000_000, 500_000  # -> required == 50 (below the dust band)
    state = _make_state(ii, fees, melt, mba, collateral=collateral, start_price=start_price)
    debt = state.seized_debt
    min_bid = state.min_byc_amount_to_bid(CURRENT_TIME, 40)
    max_bid = state.max_byc_amount_to_bid(CURRENT_TIME)
    assert min_bid < max_bid < debt  # required < debt, non-degenerate interval
    assert state.forbidden_byc_amount_to_bid_range(CURRENT_TIME, 40) is None  # one contiguous interval
    expected_collateral = calculate_expected_collateral(
        max_bid, start_price, 100, 60, START_TIME, CURRENT_TIME
    )
    assert expected_collateral >= collateral  # leftover_collateral == 0 -> valid via the escape


def test_single_point_biddable_range_when_required_at_or_below_min_bid():
    # Auction decayed so far that the all-collateral bid (required) is at/below the minimum bid:
    # required <= minimum_bid_amount + 1, so min_byc_amount_to_bid == max_byc_amount_to_bid ==
    # required and forbidden is None. The entire biddable set is the single point {required},
    # valid only via the leftover_collateral == 0 escape.
    ii, fees, melt, mba = 60, 1140, 10_000, 5
    collateral, start_price = 1_000_000, 500_000  # -> required == 5 (<= mba + 1)
    state = _make_state(ii, fees, melt, mba, collateral=collateral, start_price=start_price)
    min_bid = state.min_byc_amount_to_bid(CURRENT_TIME, 40)
    max_bid = state.max_byc_amount_to_bid(CURRENT_TIME)
    assert min_bid == max_bid  # single-point range [x, x]
    assert min_bid <= mba + 1  # required collapsed to at/below the min bid
    assert state.forbidden_byc_amount_to_bid_range(CURRENT_TIME, 40) is None
    expected_collateral = calculate_expected_collateral(
        min_bid, start_price, 100, 60, START_TIME, CURRENT_TIME
    )
    assert expected_collateral >= collateral  # the lone point is valid via the escape


def test_max_bid_inside_dust_band_is_kept_valid_via_all_collateral_escape():
    # Little collateral left: required (the all-collateral bid) falls INSIDE the dust band.
    # max_byc_amount_to_bid must NOT be reduced to the start of the band -- bidding it seizes all
    # collateral (leftover_collateral == 0), a valid escape in both puzzle `any` clauses. Instead
    # the forbidden range is clipped to max-1 so it never swallows this valid (and most efficient)
    # bid. Reducing max here would delete the best bid.
    ii, fees, melt, mba, delta = 60, 1140, 10_000, 5, 40
    collateral, start_price = 16_000_000, 500_000
    state = _make_state(ii, fees, melt, mba, collateral=collateral, start_price=start_price)
    band_lo, band_hi = ii + 1, ii + min(delta, fees - 1)  # [61, 100]

    max_bid = state.max_byc_amount_to_bid(CURRENT_TIME)
    assert max_bid < state.seized_debt  # required < debt regime
    assert band_lo <= max_bid <= band_hi  # required lands strictly inside the dust band

    # bidding max seizes all collateral -> leftover_collateral == 0 -> valid via the escape
    expected_collateral = calculate_expected_collateral(
        max_bid, start_price, 100, 60, START_TIME, CURRENT_TIME
    )
    assert expected_collateral >= collateral

    # forbidden range is clipped to max-1: it stops just below max, keeping max valid
    forbidden = state.forbidden_byc_amount_to_bid_range(CURRENT_TIME, delta)
    assert forbidden == (band_lo, max_bid - 1)
    min_bid = state.min_byc_amount_to_bid(CURRENT_TIME, delta)
    lo, hi = forbidden
    assert min_bid <= lo <= hi < max_bid  # gap sits strictly below the (valid) max bid
    # the valid set is a lower range [min, II] followed by the forbidden band and then a
    # single-point interval {max}: since forbidden.hi == max - 1, the top interval is exactly [max, max]
    assert hi == max_bid - 1  # nothing valid between the band and max -> {max} is a lone point


def test_no_auction_returns_none():
    state = CollateralVaultState(
        vault_mod_hash=bytes(32),
        statutes_struct=Program.to(0),
        collateral=0,
        principal=0,
        auction_state=Program.to(0),  # nil -> not in liquidation
        inner_puzzle_hash=bytes(32),
        discounted_principal=0,
    )
    assert state.forbidden_byc_amount_to_bid_range(CURRENT_TIME, 40) is None
