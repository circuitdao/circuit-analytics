import logging
from math import ceil

from chia_rs.sized_ints import uint16, uint64

LR_BUFFER = (
    1  # liquidation ratio buffer [percentage points]. applies to collateral vault withdraw and borrow operations
)
MINUTES_IN_YEAR = 60 * 24 * 365  # based on 365 days
MCAT = 10**3
MOJOS = 10**12
PRECISION = 10**10
PRECISION_PCT = 10**2
PRECISION_BPS = 10**4
PRICE_PRECISION = 10**2

log = logging.getLogger(__name__)


def undiscount_savings_balance(discounted_savings_balance: int, current_cumulative_ir_df: int) -> int:
    """Undiscount savings vault discounted balance by current cumulative IR discount factor specified.
    current_cumulative_ir_df should be calculated with current_timestamp offset of:
    - discounting balance delta: 0
    - calculate interest: - 3 * MAX_TX_BLOCK_TIME.
    Uses same calculation methodology as calculate-interest function in savings.clsp.
    """
    return (discounted_savings_balance * current_cumulative_ir_df) // PRECISION


def calculate_interest(discounted_savings_balance: int, savings_balance: int, current_cumulative_ir_df: int) -> int:
    """Calculate accrued interest on savings vault.
    Note that it is possible for this function to return a negative value (eg -1).
    Arguments:
    - discounted_savings_balance: curried arg DISCOUNTED_BALANCE of savings vault puzzle
    - savings_balance: amount of savings vault coin
    - current_cumulative_ir_df: calc with current_timestamp offset of - 3 * MAX_TX_BLOCK_TIME
    Equivalent of calculate-interest function in savings.clsp.
    """
    undiscounted_savings_balance = undiscount_savings_balance(discounted_savings_balance, current_cumulative_ir_df)
    return undiscounted_savings_balance - savings_balance


def undiscount_principal(discounted_principal: int, current_cumulative_sf_df: int) -> int:
    """Undiscount collateral vault discounted principal by current cumulative SF discount factor specified.
    current_cumulative_sf_df should be calculated with current_timestamp offset of:
    - deposit (undiscounting principal): + 3 * MAX_TX_BLOCK_TIME
    - withdraw (undiscounting principal): + 3 * MAX_TX_BLOCK_TIME
    - repay: + 3 * MAX_TX_BLOCK_TIME
    - borrow:
      - discounting borrow amount: 0
      - undiscounting new principal: + 3 * MAX_TX_BLOCK_TIME
    - transfer (undiscounting principal): + 3 * MAX_TX_BLOCK_TIME
    - SF transfer (undiscounting principal): 0
    - start auction (undiscounting principal): + 3 * MAX_TX_BLOCK_TIME
    Equivalent of undiscount-principal function in vault.clib.
    """
    return -1 * ((-1 * discounted_principal * current_cumulative_sf_df) // PRECISION)


def annual_rate_to_per_minute_discount_factor(r_annual: float, precision: int = PRECISION) -> int:
    """Convert the annual percentage rate to a per-minute discount factor with given precision."""
    r_annual = r_annual / 100
    df_per_minute = (1 + r_annual) ** (1 / MINUTES_IN_YEAR)
    return int(
        df_per_minute * PRECISION
    )  # note: int() truncates decimal part, ie behaves like floor() for positive numbers


def per_minute_discount_factor_to_annual_rate(df_per_minute: int, precision: int = PRECISION) -> float:
    """Convert a per-minute discount factor with given precision to an annual percentage rate.

    This is not an exact implementation since there's no rounding every minute, but that's good enough
    for our purposes.

    An exact implementation that mirrors what Chialisp does is:
      df = PRECISION
      for i in range(MINUTES_IN_YEAR):
         df = (df * df_per_minute) // PRECISION
      return 100 * (df - PRECISION) / PRECISION
    """
    return 100 * ((df_per_minute / precision) ** MINUTES_IN_YEAR - 1)


def calculate_fees_to_pay(
    repayment_amount: int | None,  # None to calculate full accrued stability fees (incl LP if specified)
    cumulative_sf_df: int,
    principal: uint64,
    discounted_principal: uint64,
    liquidation_penalty_bps: uint64 = None,
):
    if liquidation_penalty_bps is None:
        liquidation_penalty_bps = 0
    # log.debug(
    #    f"Calculating fees to pay: {repayment_amount=} {cumulative_sf_df=} {principal=} "
    #    f"{discounted_principal=} {liquidation_penalty_bps=}"
    # )
    if repayment_amount == 0:
        return uint64(0)
    undiscounted_principal = undiscount_principal(discounted_principal, cumulative_sf_df)
    sf_outstanding = (undiscounted_principal - principal) if undiscounted_principal > principal else 0
    # log.debug("SFs outstanding: %s = %s - %s)", sf_outstanding, undiscounted_principal, principal)
    outstanding_liquidation_penalty = -1 * (
        (-1 * (principal + sf_outstanding) * liquidation_penalty_bps) // PRECISION_BPS
    )
    # log.debug("Outstanding liquidation penalty: %s", outstanding_liquidation_penalty)
    total_debt = principal + sf_outstanding + outstanding_liquidation_penalty
    # log.debug("Total debt, fees: %s, %s", total_debt, sf_outstanding + outstanding_liquidation_penalty)
    if total_debt == 0:
        return uint64(0)
    elif total_debt < 0:
        raise ValueError("Total debt is negative")
    if repayment_amount is None:
        # None means full repayment
        # log.debug("Forcing full stability fee (incl liquidation penalty of %s)", liquidation_penalty_bps)
        pass
    elif repayment_amount > total_debt:
        raise ValueError("Repayment amount is greater than total debt (%s > %s)" % (repayment_amount, total_debt))
    else:
        total_repayment_ratio = repayment_amount * PRECISION // total_debt
    if repayment_amount is None or total_repayment_ratio == PRECISION:
        fees_to_pay_now = sf_outstanding + outstanding_liquidation_penalty
        liquidation_penalty = outstanding_liquidation_penalty
        # log.debug("Stability fees (incl LP): %s", fees_to_pay_now)
    else:
        # calculate the share to pay now
        fees_to_pay_now = -1 * (
            (-1 * total_repayment_ratio * (sf_outstanding + outstanding_liquidation_penalty)) // PRECISION
        )
        liquidation_penalty = -1 * ((-1 * total_repayment_ratio * outstanding_liquidation_penalty) // PRECISION)
        # log.debug(
        #    "Fees proportional: %s, %s lp=%s",
        #    (sf_outstanding + outstanding_liquidation_penalty), fees_to_pay_now, liquidation_penalty
        # )
        if fees_to_pay_now < 0:
            # negative fees, return 0
            fees_to_pay_now = uint64(0)
        # log.debug("Stability fees (incl LP): %s ", fees_to_pay_now)
    if liquidation_penalty == 0:
        return uint64(fees_to_pay_now)
    else:
        return uint64(fees_to_pay_now), uint64(liquidation_penalty)


def calculate_discounted_principal_for_mint(
    byc_amount_to_mint, discounted_principal, cumulative_stability_rate
) -> uint64:
    discounted_byc_amount_to_mint = -1 * ((-1 * byc_amount_to_mint * PRECISION) // cumulative_stability_rate)
    return discounted_principal + discounted_byc_amount_to_mint


def calculate_discounted_principal_for_repay(
    discounted_principal: int, repay_amount: int, current_cumulative_sf_df: int
) -> int:
    """Calculate new discounted principal after repay given existing discounted principal
    and current cumulative stability fee discount factor.
    """
    # TODO: check that where this function is used we are using the correct current_cumulative_sf_df, which should contain a 3-block time offset

    debt = undiscount_principal(discounted_principal, current_cumulative_sf_df)
    log.debug(
        "Discounted principal: %s, Debt: %s, Amount to melt: %s, Current cumulative SF DF: %s",
        discounted_principal,
        debt,
        repay_amount,
        current_cumulative_sf_df,
    )
    if repay_amount > debt:
        raise ValueError(f"Repay amount is greater than outstanding debt: {repay_amount} > {debt}")
    elif repay_amount == debt:
        new_discounted_principal = 0
    else:
        new_discounted_principal = int(
            discounted_principal - (1 * ((1 * repay_amount * PRECISION) // current_cumulative_sf_df))
        )
    log.debug(f"Post-repay discounted principal: {new_discounted_principal}")
    return new_discounted_principal


def calculate_current_auction_price_bps(
    start_price: int,
    current_time: int,
    auction_start_time: int,
    step_price_decrease_factor_bps: int,
    step_time_interval: int,
):
    """Calculate current liquidation auction price.

    Unit of returned auction price is 1/(PRICE_PRECISION * PRECISION_BPS) BYC per XCH.

    Uses same methodology as in vault_keeper_bid.clsp.
    """
    price_decrease_per_step_bps = start_price * step_price_decrease_factor_bps
    num_steps = (current_time - auction_start_time) // step_time_interval  # no. (completed) steps
    total_price_decrease_bps = price_decrease_per_step_bps * num_steps
    start_price_bps = start_price * PRECISION_BPS
    auction_price_bps = start_price_bps - total_price_decrease_bps
    log.debug(
        "Start price bps, price decrease per step bps, no. steps, auction price bps: %s %s %s %s",
        start_price_bps,
        price_decrease_per_step_bps,
        num_steps,
        auction_price_bps,
    )
    return auction_price_bps


def calculate_expected_collateral(
    byc_bid_amount: uint64,
    start_price: uint64,
    step_price_decrease_factor_bps: uint16,
    step_time_interval: uint16,
    auction_start_time: uint64,
    current_time: uint64,
):
    """
    Calculate amount of collateral we expect to receive for given BYC bid amount at current liquidation auction price

    Unit of returned collateral amount is mojos.

    This function matches the calculation of bid_xch_collateral_amount_pre in vault_keeper_bid.clsp.
    The amount of collteral released by the vault is then calculated as min(bid_xch_collateral_amount_pre, COLLATERAL).
    """
    log.debug(
        "Calculating expected collateral: %s %s %s %s %s %s",
        byc_bid_amount,
        start_price,
        step_price_decrease_factor_bps,
        step_time_interval,
        auction_start_time,
        current_time,
    )
    auction_price_bps = calculate_current_auction_price_bps(
        start_price, current_time, auction_start_time, step_price_decrease_factor_bps, step_time_interval
    )
    if auction_price_bps <= 0:
        raise ValueError(
            f"Collateral vault auction price is non-positive: {auction_price_bps}. Cannot calculate expected collateral"
        )
    intermediate = (byc_bid_amount * PRICE_PRECISION * PRECISION_BPS * MOJOS) // auction_price_bps
    collateral = intermediate // 1000
    log.debug("Collateral expected from bid: %s @ %s", collateral, auction_price_bps / PRECISION_BPS)
    return collateral


def calculate_required_byc_bid_amount(
    collateral_amount: uint64,
    start_price: uint64,
    step_price_decrease_factor_bps: uint16,
    step_time_interval: uint16,
    auction_start_time: uint64,
    current_time: uint64,
) -> int:
    """
    Calculate the minimum amount of BYC we need to bid to receive at least the specified collateral amount at current liquidation auction price
    """
    log.debug(
        "Calculating required BYC bid amount: %s %s %s %s %s %s",
        collateral_amount,
        start_price,
        step_price_decrease_factor_bps,
        step_time_interval,
        auction_start_time,
        current_time,
    )
    auction_price_bps = calculate_current_auction_price_bps(
        start_price, current_time, auction_start_time, step_price_decrease_factor_bps, step_time_interval
    )
    if auction_price_bps <= 0:
        raise ValueError(
            f"Collateral vault auction price is non-positive: {auction_price_bps}. Cannot calculate required BYC bid amount"
        )
    byc_bid_amount = int(collateral_amount * 1000 * auction_price_bps / (PRICE_PRECISION * PRECISION_BPS * MOJOS))
    # Iterate to determine smallest byc bid amount that gives us at least the collateral amount
    expected_collateral = calculate_expected_collateral(
        byc_bid_amount,
        start_price,
        step_price_decrease_factor_bps,
        step_time_interval,
        auction_start_time,
        current_time,
    )
    num_iterations = 0
    if expected_collateral >= collateral_amount:
        while expected_collateral >= collateral_amount:
            num_iterations += 1
            byc_bid_amount -= 1
            expected_collateral = calculate_expected_collateral(
                byc_bid_amount,
                start_price,
                step_price_decrease_factor_bps,
                step_time_interval,
                auction_start_time,
                current_time,
            )
        if expected_collateral < collateral_amount:
            byc_bid_amount += 1
    else:
        while expected_collateral < collateral_amount:
            num_iterations += 1
            byc_bid_amount += 1
            expected_collateral = calculate_expected_collateral(
                byc_bid_amount,
                start_price,
                step_price_decrease_factor_bps,
                step_time_interval,
                auction_start_time,
                current_time,
            )
    log.debug(
        "BYC bid amount for collateral: %s @ %s -> %s (no. iters: %s)",
        byc_bid_amount,
        auction_price_bps / PRECISION_BPS,
        collateral_amount,
        num_iterations,
    )
    return byc_bid_amount


def calculate_total_fees(undiscounted_principal: int, principal: int, liquidation_penalty_bps: int) -> int:
    """Returns accrued Stability Fees plus liquidation penalty if provided.

    Equivalent of calculate-total-fees function in vault.clib.
    """
    stability_fees = max(0, undiscounted_principal - principal)
    debt = principal + stability_fees
    liquidation_penalty = ceil(debt * liquidation_penalty_bps / PRECISION_BPS)
    return stability_fees + liquidation_penalty


def _pow_discount(base: int, exp: int) -> int:
    """Exponentiation-by-squaring in fixed point with per-multiplication truncation.

    Mirrors pow-discount in utils.clib: returns PRECISION when exp == 0, and for each
    multiplication applies integer division by PRECISION immediately to preserve the
    same rounding behaviour as applying the factor minute-by-minute.
    """
    if exp == 0:
        return PRECISION
    squared = (base * base) // PRECISION
    recur = _pow_discount(squared, exp // 2)
    if exp % 2 == 1:
        return (recur * base) // PRECISION
    return recur


def calculate_cumulative_discount_factor(
    cumulative_df: int, current_df: int, timestamp_previous: int, timestamp_current: int
):
    """Calculate current cumulative discount factor.

    Equivalent of calculate-cumulative-discount-factor in utils.clib. Computes:
    cumulative_df * (current_df / PRECISION)^(minutes elapsed).
    """
    log.debug(
        "Calculating current cumulative discount factor: %s %s %s %s",
        cumulative_df,
        current_df,
        timestamp_previous,
        timestamp_current,
    )
    assert current_df >= PRECISION, "Negative rates not allowed. Something is wrong"

    # Align to the next minute boundary after timestamp_previous
    remainder = timestamp_previous % 60
    timestamp_start = timestamp_previous + (60 - remainder)

    raw_elapsed = timestamp_current - timestamp_start
    if raw_elapsed >= 0:
        elapsed_minutes = (raw_elapsed // 60) + 1
    else:
        elapsed_minutes = 0

    multiplier = _pow_discount(current_df, elapsed_minutes)
    result = int((cumulative_df * multiplier) // PRECISION)
    log.debug("Current cumulative discount factor: %s (elapsed minutes: %s)", result, elapsed_minutes)
    return result


def calculate_accrued_interest(discounted_balance: int, current_amount: uint64, cumulative_interest_df: int) -> int:
    """Calculate accrued interest of savings vault.

    Here, cumulative_interest_df should have been computed using calculate_cumulative_discount_factor with 3x MAX_TX_BLOCK_TIME
    deducted from timestamp_current.

    Equivalent of calculate-interest function in savings_vault.clsp
    """
    accrued_interest = (discounted_balance * cumulative_interest_df // PRECISION) - current_amount
    if accrued_interest > 0:
        return accrued_interest
    return 0


def find_oracle_median_price(prices: list[int]) -> tuple[int, int]:
    """Returns the median price and median index for given list of prices.

    If the given list contains an even number of elements, the upper price and index are returned.
    E.g. [10, 20, 40, 50] -> (40, 2)
    """
    sorted_prices = sorted(prices)
    n = len(sorted_prices)
    median_index = n // 2
    return int(sorted_prices[median_index]), median_index


def calculate_savings_vault_discounted_balance(delta_amount, cumulative_interest_rate, discounted_balance) -> uint64:
    log.debug("Calculating discounted balance: %s %s %s", delta_amount, cumulative_interest_rate, discounted_balance)
    return ((delta_amount * PRECISION) // cumulative_interest_rate) + discounted_balance


def calculate_collateral_ratio(debt: int, collateral: int, xch_price: int) -> float | None:
    """Return collateral ratio as a floating point number.

    For example 1.3645 = 136.45%.

    Returns float('inf') if debt is 0, None if debt is negative.

    Arguments:
    - debt: vault debt [mBYC]. should be calculated with + 3 * MAX_TX_BLOCK_TIME current timestamp offset in current cumulative stability fee df calculation
    - collateral: vault collateral [mojos]
    - xch_price: XCH price [cBYC per XCH]
    This function is for information only. It has no equivalent in the protocol.
    """
    # LATER: verify that this results in the actual collateral ratio being greater than the one calculated (to avoid unexpected liquidations).
    # debt = self.get_debt(cumulative_stability_fee_df)
    if debt < 0:
        raise ValueError("Debt cannot be negative")
    elif debt == 0:
        return float("inf")
    collateral_value = (collateral / MOJOS) * (xch_price / PRICE_PRECISION)
    effective_debt = debt / 1000
    collateral_ratio = collateral_value / effective_debt
    log.debug(f"{collateral_value=} {effective_debt=} -> {collateral_ratio=}")
    return collateral_ratio


def calculate_min_collateral_amount(debt: int, liquidation_ratio_pct: int, xch_price: int) -> uint64:
    """Minimum amount of collateral (in mojos) for which the vault is not liquidatable.

    Arguments:
    - debt [mBYC]
    - liquidation_ratio_pct [percentage points]
    - current_price [BYC / PRICE_PRECISION per XCH]

    Equivalent of get-min-collateral-amount in vault.clib
    """
    # log.debug("Calculating min collateral amount: %s %s %s", debt, liquidation_ratio_pct, xch_price)
    numerator = -1 * debt * PRICE_PRECISION * liquidation_ratio_pct * MOJOS
    denominator = xch_price * PRECISION_PCT * 1000
    result = -1 * (numerator // denominator)
    return uint64(result)


def _approximate_liquidation_price(
    debt,
    liquidation_ratio_pct,
    collateral,
    xch_price,
    delta,
) -> int:
    """Returns liquidation price approximation"""
    min_collateral_amount = calculate_min_collateral_amount(debt, liquidation_ratio_pct, xch_price)
    if not min_collateral_amount > collateral:
        # not liquidatable
        while not min_collateral_amount > collateral:
            xch_price -= delta
            if xch_price <= 0:
                break  # min collateral amount is infinity
            min_collateral_amount = calculate_min_collateral_amount(debt, liquidation_ratio_pct, xch_price)
        # Overshot (now liquidatable), step back and return for next iteration
        xch_price += delta
        return xch_price
    else:
        # liquidatable
        while min_collateral_amount > collateral:
            xch_price += delta
            min_collateral_amount = calculate_min_collateral_amount(debt, liquidation_ratio_pct, xch_price)
        # no longer liquidatable. return for next iteration
        return xch_price


def calculate_liquidation_price(debt: int, liquidation_ratio_pct: int, collateral: int) -> int | None:
    """Highest XCH price at which vault can be liquidated assuming current debt and collateral.

    Returns None if vault cannot be liquidated at any price (debt = 0 or collateral = 0).

    Price returned is an integer given in PRICE_PRECISION.

    Arguments:
    - debt [mBYC]
    - liquidation_ratio_pct [percentage points]
    - collateral [mojos]
    """
    if debt <= 0:
        return None
    if collateral <= 0:
        return None
    # Estimate price
    xch_price = max(1, int(100 * (debt / MCAT) / (collateral / MOJOS)))
    min_collateral_amount = calculate_min_collateral_amount(debt, liquidation_ratio_pct, xch_price)
    delta = max(1, xch_price // 10)  # start with something reasonable
    while delta >= 1:
        xch_price = _approximate_liquidation_price(
            debt,
            liquidation_ratio_pct,
            collateral,
            xch_price,
            delta,
        )
        if delta == 1:
            # step back to get liquidation price
            xch_price -= 1
            break
        delta = delta // 2
    log.debug(f"calculated liquidation price: {xch_price}")
    if xch_price > 0:
        min_collateral_amount = calculate_min_collateral_amount(debt, liquidation_ratio_pct, xch_price)
        assert min_collateral_amount > collateral  # vault can be liquidated
    min_collateral_amount = calculate_min_collateral_amount(debt, liquidation_ratio_pct, xch_price + 1)
    assert min_collateral_amount <= collateral  # vault cannot be liquidated
    return xch_price


def calculate_max_debt(collateral: int, liquidation_ratio_pct: int, current_price: int) -> uint64:
    """Maximum amount of debt (in mBYC) vault may have to be at or above liquidation threshold
    for a given liquidation ratio and amount of collateral.

    Arguments:
    - collateral [mojos]
    - liquidation_ratio_pct [%]
    - current_price [BYC / PRICE_PRECISION per XCH]

    Uses same methodology to calculate max debt as function available-to-mint in vault.clib.
    """
    # log.debug("Calculating max debt: %s %s %s", collateral, liquidation_ratio_pct, current_price)
    numerator = collateral * current_price * 1000 * PRECISION_PCT
    denominator = liquidation_ratio_pct * PRICE_PRECISION
    quotient = numerator // denominator
    result = quotient // MOJOS
    return uint64(result)


def _borrow_amount_valid(
    collateral: int,
    discounted_principal: int,
    borrow_amount: int,
    cumulative_stability_fee_df: int,  # excl 3 * MAX_TX_BLOCK_TIME add-on
    cumulative_stability_fee_df_adj: int,  # incl 3 * MAX_TX_BLOCK_TIME add-on
    liquidation_ratio: int,  # not adjusted (ie without +1)
    current_price: int,
) -> int:  # -1: too small, 0: too large, 1: valid
    if borrow_amount < 1:
        return False
    discounted_borrow_amount = -((-borrow_amount * PRECISION) // cumulative_stability_fee_df)
    new_discounted_principal = discounted_principal + discounted_borrow_amount
    new_undiscounted_principal = undiscount_principal(new_discounted_principal, cumulative_stability_fee_df_adj)
    min_collateral_required = calculate_min_collateral_amount(
        new_undiscounted_principal, liquidation_ratio + LR_BUFFER, current_price
    )
    if collateral >= min_collateral_required:
        return True
    return False


def _approximate_borrow_amount(
    collateral: int,
    discounted_principal: int,
    borrow_amount: int,
    cumulative_stability_fee_df: int,
    cumulative_stability_fee_df_adj: int,
    liquidation_ratio: int,
    current_price: int,
    delta: int,
) -> int:
    """Returns borrow amount approximation"""
    # First loop: increment by delta while valid
    is_valid = _borrow_amount_valid(
        collateral,
        discounted_principal,
        borrow_amount,
        cumulative_stability_fee_df,
        cumulative_stability_fee_df_adj,
        liquidation_ratio,
        current_price,
    )
    if is_valid:
        # Keep incrementing while valid
        while True:
            borrow_amount += delta
            is_valid = _borrow_amount_valid(
                collateral,
                discounted_principal,
                borrow_amount,
                cumulative_stability_fee_df,
                cumulative_stability_fee_df_adj,
                liquidation_ratio,
                current_price,
            )
            if not is_valid:
                # Overshot, step back and return
                borrow_amount -= delta
                return borrow_amount
    else:
        # Second loop: decrement by delta while invalid
        while not is_valid:
            if borrow_amount <= delta:
                if delta == 1:
                    return 0
                break
            borrow_amount -= delta
            is_valid = _borrow_amount_valid(
                collateral,
                discounted_principal,
                borrow_amount,
                cumulative_stability_fee_df,
                cumulative_stability_fee_df_adj,
                liquidation_ratio,
                current_price,
            )
    return borrow_amount


def calculate_max_borrow_amount(
    collateral: uint64,
    discounted_principal: uint64,
    liquidation_ratio: int,
    current_price: int,
    cumulative_stability_fee_df: uint64,  # current cumulative SF DF (excl 3 * MAX_TX_BLOCK_TIME add-on)
    current_sf_df: uint64,
) -> int:
    """
    Maximum amount that can be borrowed from collateral vault.

    Arguments:
    - collateral [mojos]
    - discounted_principal [mBYC]: as curried in state of vault
    - liquidation_ratio_pct [percentage points]
    - current_price [BYC / PRICE_PRECISION per XCH]
    - cumulative_stability_fee_df: current cumulative SF DF. must have been calculated excluding the 3 * MAX_TX_BLOCK_TIME add-on
    - current_sf_df: as used to calculate cumulative_stability_fee_df
    """
    # Calculate approximate max borrow amount ignoring integer arithmetic
    collateral_in_mbyc = collateral * (10 * current_price / MOJOS)
    max_debt_amount = calculate_max_debt(
        collateral, liquidation_ratio + 1, current_price
    )  # TODO: is using LR_BUFFER correct?
    # TODO: check that calculating adj cumulative SF DF as below is still correct with new, O(log min) methodology
    cumulative_stability_fee_df_adj = cumulative_stability_fee_df
    for i in range(6):
        cumulative_stability_fee_df_adj = (cumulative_stability_fee_df_adj * current_sf_df) // PRECISION
    borrow_amount = max(1, int(max_debt_amount - (discounted_principal * cumulative_stability_fee_df_adj / PRECISION)))
    delta = max(1, int(collateral_in_mbyc / 100))  # start with something reasonable
    while delta >= 1:
        borrow_amount = _approximate_borrow_amount(
            collateral,
            discounted_principal,
            borrow_amount,
            cumulative_stability_fee_df,
            cumulative_stability_fee_df_adj,
            liquidation_ratio,
            current_price,
            delta,
        )
        delta = delta // 2
    log.debug(f"calculated max borrow amount: {borrow_amount}")
    return borrow_amount


def treasury_withdrawal_amounts(nums: list[int], k: int) -> list[int]:
    """Given list of treasury coin amounts and a withdrawal amount k, returns list of withdrawal amounts for each treasury coin"""
    # Validate inputs
    if not nums or k <= 0 or any(n < 0 for n in nums):
        return []
    original_sum = sum(nums)
    if original_sum < k:
        return []

    # Target sum after subtraction
    target_sum = original_sum - k
    if target_sum < 0:
        return []

    # Create subtrahends list, initially all zeros
    subtrahends = [0] * len(nums)

    # Process numbers from largest to smallest
    remaining_k = k

    # Process while deductions are needed
    while remaining_k > 0:
        # Current state
        current = [nums[i] - subtrahends[i] for i in range(len(nums))]

        # Find distinct values
        unique_vals = sorted(set(current), reverse=True)
        if not unique_vals:
            return []

        # Largest value and next lower value
        current_val = unique_vals[0]
        next_val = unique_vals[1] if len(unique_vals) > 1 else 0

        # Indices of elements at current_val, sorted by original value descending
        indices = [i for i in range(len(current)) if current[i] == current_val]
        indices.sort(key=lambda i: (-nums[i], i))
        count = len(indices)

        # Reduction to next level
        reduction = current_val - next_val
        total_reduction = reduction * count

        if total_reduction <= remaining_k:
            # Reduce all to next_val
            for idx in indices:
                subtrahends[idx] += reduction
            remaining_k -= total_reduction
        else:
            # Distribute remaining_k evenly
            common_reduction, remainder = divmod(remaining_k, count)
            assert common_reduction * count + remainder == remaining_k
            for i, idx in enumerate(indices):
                subtrahends[idx] += common_reduction
                if i < remainder:
                    subtrahends[idx] += 1
            remaining_k -= total_reduction

    # Verify result
    result = [nums[i] - subtrahends[i] for i in range(len(nums))]
    if any(r < 0 for r in result) or sum(result) != target_sum:
        return []

    return subtrahends
