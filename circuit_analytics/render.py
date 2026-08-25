"""Render parsed spends for a terminal.

Driver info objects are dataclasses holding Programs, bytes and raw integers, so repr() is
unreadable at the sizes involved. This prints the fields with hashes abbreviated and amounts
converted, which is what makes the output usable when comparing a spend against a puzzle.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Optional

from chia.types.blockchain_format.program import Program

MOJOS = 10**12
MCAT = 10**3

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"

# Fields whose integers are protocol amounts rather than plain numbers. Everything else is
# printed as-is: guessing wrong is worse than showing the raw value.
BYC_FIELDS = {
    "amount", "funded_amount", "treasury_amount", "new_treasury_amount", "current_amount",
    "new_amount", "approver_amount", "recover_amount", "min_treasury_delta", "lot_amount",
    "byc_bid_amount", "byc_lot_amount", "borrow_amount", "repay_amount", "deposit_amount",
    "withdraw_amount", "interest_payment", "principal", "byc_to_melt_balance",
    "byc_to_treasury_balance", "initiator_incentive_balance", "minimum_bid_amount",
}
CRT_FIELDS = {"crt_bid_amount", "rewards_per_interval", "delta_amount", "extra_delta"}
XCH_FIELDS = {"collateral", "deposit", "prev_deposit", "min_deposit", "leftover_collateral"}


def _scaled(value: int, divisor: int, decimals: int, *, strip: bool = False) -> str:
    """Exact fixed-point text for value/divisor.

    Integer arithmetic throughout: float division loses precision well before the amounts
    this protocol reaches. A flashloan mints up to Chia's MAX_COIN_AMOUNT of 2**64-1, and
    18446744073709551615 / 1000 as a float rounds to 18446744073709552.0, misreporting the
    amount by hundreds of milli-units.
    """
    sign = "-" if value < 0 else ""
    whole, frac = divmod(abs(value), divisor)
    text = f"{sign}{whole:,}.{frac:0{decimals}d}"
    if strip:
        text = text.rstrip("0").rstrip(".")
    return text


def _colour(enabled: bool, code: str, text: str) -> str:
    return f"{code}{text}{RESET}" if enabled else text


INLINE_BYTES = 34
"""Values up to this size are shown whole.

A 32-byte hash is the thing you want to copy, and a little headroom above it keeps the
slightly-larger values that carry one -- a hash inside a short structure, say -- readable too.
"""


def _abbrev(value: bytes) -> str:
    h = bytes(value).hex()
    return h if len(h) <= 20 else f"{h[:10]}\u2026{h[-6:]}"


ELEMENT_BYTES = 10
"""Budget for an element inside an inline list.

A list rendered on one line has to fit several values, so each gets far less room than a
field of its own. Enough to recognise a value and tell it from its neighbours; use --details
to read one properly.
"""


def _fmt_bytes(value: bytes, full: bool, limit: int = INLINE_BYTES) -> str:
    """Hex, whole up to the limit, truncated beyond it unless asked for in full."""
    raw = bytes(value)
    if not raw:
        return "()"
    text = raw.hex()
    if full or len(raw) <= limit:
        return text
    return f"{text[: limit * 2]}\u2026 ({len(raw)} bytes)"


def _list_items(value: Any) -> Optional[list]:
    """The elements of value if it is a sequence worth breaking apart, else None.

    A Program that is a proper list counts: its elements are usually the solution args, and
    reading them side by side on one line is what made the old output unusable.
    """
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, Program):
        if value.atom is not None:
            return None
        try:
            return list(value.as_iter())
        except Exception:  # noqa: BLE001 - an improper list is rendered as a scalar
            return None
    return None


def _fmt_value(
    name: str, value: Any, colour: bool, full: bool = False, limit: int = INLINE_BYTES
) -> str:
    """A single value on one line. Sequences are expanded by _render_field instead."""
    if value is None:
        return _colour(colour, DIM, "None")
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return _fmt_bytes(value, full, limit)
    if isinstance(value, int):
        # Large values are printed as they are. It is tempting to read anything near 2**64 as
        # a negative serialised unsigned, but BYC amounts legitimately reach Chia's
        # MAX_COIN_AMOUNT of 2**64-1 during a flashloan, and CAT ring subtotals follow the
        # amounts, so that reading would misreport real values.
        if name in BYC_FIELDS:
            return f"{value:,} {_colour(colour, DIM, f'({_scaled(value, MCAT, 3)} BYC)')}"
        if name in CRT_FIELDS:
            return f"{value:,} {_colour(colour, DIM, f'({_scaled(value, MCAT, 3)} CRT)')}"
        if name in XCH_FIELDS:
            return f"{value:,} {_colour(colour, DIM, f'({_scaled(value, MOJOS, 12, strip=True)} XCH)')}"
        return f"{value:,}"
    if isinstance(value, Program):
        atom = value.atom
        if atom is not None:
            return _fmt_bytes(atom, full, limit) if atom else "()"
        # A list Program serialises to the form CLVM tools accept, which is what makes it
        # worth copying whole.
        return _fmt_bytes(value.as_bin(), full, limit)
    if isinstance(value, (list, tuple)):
        if not value:
            return "()"
        # Each element gets the smaller budget, since they share one line.
        return "[" + ", ".join(
            _fmt_value(name, v, colour, full, ELEMENT_BYTES) for v in value
        ) + "]"
    return str(value)


MAX_NESTING = 3
"""How deep to keep breaking sequences apart before rendering one inline."""


def _render_field(
    label: str,
    name: str,
    value: Any,
    *,
    colour: bool,
    full: bool,
    indent: str,
    details: bool = False,
    depth: int = 0,
) -> list:
    """Lines for one field.

    Expanding sequences and truncating long values are independent choices, so they are
    separate flags: the compact default does neither in the way that adds lines, --details
    expands, --full stops truncating.
    """
    items = _list_items(value) if details else None
    if items is None or depth >= MAX_NESTING:
        return [f"{indent}{label} {_fmt_value(name, value, colour, full)}"]
    if not items:
        return [f"{indent}{label} {_colour(colour, DIM, '()')}"]
    lines = [f"{indent}{label}"]
    for i, item in enumerate(items):
        lines.extend(
            _render_field(
                _colour(colour, DIM, f"[{i}]"),
                name,
                item,
                colour=colour,
                full=full,
                indent=indent + "  ",
                details=details,
                depth=depth + 1,
            )
        )
    return lines


# Puzzles and raw solution blobs: reveals rather than data, and long enough to bury
# everything else. Available with -v.
NOISY_FIELDS = frozenset({
    "inner_puzzle", "inner_solution", "solution", "operation", "solution_or_conditions",
    "final_output_conditions", "inner_conditions",
})


def _fmt_coin_amount(parsed, colour: bool) -> str:
    """The coin's own amount, with the units it is denominated in.

    A CAT coin's amount is in the asset's smallest unit; everything else is mojos. Circuit's
    CATs use three decimals, as most Chia CATs do, so an unrecognised asset is shown that way
    too -- labelled CAT rather than guessed at by name.
    """
    if parsed.layer == "CAT":
        unit = parsed.asset if parsed.asset in ("BYC", "CRT") else "CAT"
        converted = f"{_scaled(parsed.amount, MCAT, 3)} {unit}"
    else:
        converted = f"{_scaled(parsed.amount, MOJOS, 12, strip=True)} XCH"
    return f"{parsed.amount:,} {_colour(colour, DIM, f'({converted})')}"


def render_spend(
    parsed, *, verbose: bool = False, colour: bool = True, full: bool = False, details: bool = False
) -> str:
    """One spend, as a header line plus the driver's fields."""
    lines = []
    head = f"[{parsed.index}] {_colour(colour, BOLD, parsed.coin_type)}"
    lines.append(f"{head}  {_colour(colour, DIM, parsed.coin_id)}")
    lines.append(f"     {_colour(colour, DIM, 'amount  ')} {_fmt_coin_amount(parsed, colour)}")
    # Asset and launcher are separate from the role because they are independent facts: a
    # treasury coin and a coin someone is holding are both BYC. Both are shown whole: an
    # abbreviated launcher or asset ID cannot be looked up or pasted anywhere.
    if parsed.layer:
        lines.append(f"     {_colour(colour, DIM, 'layer   ')} {parsed.layer}")
    if parsed.asset:
        lines.append(f"     {_colour(colour, DIM, 'asset   ')} {parsed.asset}")
    if parsed.launcher_id:
        lines.append(f"     {_colour(colour, DIM, 'launcher')} {parsed.launcher_id}")

    if parsed.failed:
        lines.append(f"     {_colour(colour, RED, 'PARSE FAILED')} {parsed.error}")
        if parsed.note:
            lines.append(f"     {_colour(colour, YELLOW, 'note:')} {parsed.note}")
        return "\n".join(lines)

    if parsed.info is None:
        lines.append(f"     {_colour(colour, YELLOW, 'no driver claimed this spend')}")
        return "\n".join(lines)

    lines.append(f"     {_colour(colour, GREEN, type(parsed.info).__name__)}")
    if parsed.note:
        for i, note_line in enumerate(parsed.note.split(". ")):
            prefix = "note:" if i == 0 else "     "
            lines.append(f"     {_colour(colour, YELLOW, prefix)} {note_line.strip().rstrip('.')}.")
    if dataclasses.is_dataclass(parsed.info):
        for field in dataclasses.fields(parsed.info):
            if not verbose and field.name in NOISY_FIELDS:
                continue
            value = getattr(parsed.info, field.name, None)
            lines.extend(
                _render_field(
                    f"{field.name:<34}",
                    field.name,
                    value,
                    colour=colour,
                    full=full,
                    indent="       ",
                    details=details,
                )
            )
    else:
        lines.append(f"       {parsed.info}")
    return "\n".join(lines)


def render_bundle(
    parsed_spends, *, verbose: bool = False, colour: bool = True, full: bool = False,
    details: bool = False,
) -> str:
    body = "\n".join(
        render_spend(p, verbose=verbose, colour=colour, full=full, details=details)
        for p in parsed_spends
    )
    failed = sum(1 for p in parsed_spends if p.failed)
    unclaimed = sum(1 for p in parsed_spends if not p.failed and p.info is None)
    summary = f"{len(parsed_spends)} spend(s)"
    if failed:
        summary += f", {_colour(colour, RED, f'{failed} failed to parse')}"
    if unclaimed:
        summary += f", {unclaimed} unrecognised"
    return f"{body}\n\n{summary}"


def render_block(
    pairs,
    *,
    height: int,
    header_hash: str,
    total_spends: int,
    verbose: bool = False,
    colour: bool = True,
    full: bool = False,
    details: bool = False,
) -> str:
    """A block's selected spends, each showing why it was pulled in.

    `pairs` is a list of (BlockSpend, ParsedSpend). The reason line is what distinguishes
    this from bundle output: a spend of some ordinary XCH coin means nothing on its own, and
    everything once you can see it paid the fee for the vault spend two lines up.
    """
    from circuit_analytics.linkage import REASON_PROTOCOL

    lines = [
        f"{_colour(colour, BOLD, f'block {height}')} {_colour(colour, DIM, header_hash)}",
        _colour(colour, DIM, f"{len(pairs)} of {total_spends} spend(s) selected"),
        "",
    ]
    for block_spend, parsed in pairs:
        if block_spend.reason == REASON_PROTOCOL:
            why = _colour(colour, GREEN, REASON_PROTOCOL)
        else:
            link = block_spend.links[0] if block_spend.links else None
            why = (
                _colour(colour, YELLOW, f"{link.via} [{link.source_index}]"
                        + (f" ({link.detail})" if link.detail else ""))
                if link
                else _colour(colour, YELLOW, str(block_spend.reason))
            )
        lines.append(render_spend(parsed, verbose=verbose, colour=colour, full=full, details=details))
        lines.append(f"     {_colour(colour, DIM, 'included:')} {why}")
        if block_spend.condition_error:
            lines.append(f"     {_colour(colour, DIM, 'conditions unavailable:')} {block_spend.condition_error}")
        lines.append("")

    failed = sum(1 for _b, r in pairs if r.failed)
    summary = f"{len(pairs)} spend(s)"
    if failed:
        summary += f", {_colour(colour, RED, f'{failed} failed to parse')}"
    return "\n".join(lines) + summary
