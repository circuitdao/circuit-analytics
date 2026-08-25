"""Render parsed spends for a terminal.

Driver info objects are dataclasses holding Programs, bytes and raw integers, so repr() is
unreadable at the sizes involved. This prints the fields with hashes abbreviated and amounts
converted, which is what makes the output usable when comparing a spend against a puzzle.
"""

from __future__ import annotations

import dataclasses
from typing import Any

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


def _colour(enabled: bool, code: str, text: str) -> str:
    return f"{code}{text}{RESET}" if enabled else text


def _abbrev(value: bytes) -> str:
    h = value.hex()
    return h if len(h) <= 20 else f"{h[:10]}…{h[-6:]}"


def _fmt_value(name: str, value: Any, colour: bool) -> str:
    if value is None:
        return _colour(colour, DIM, "None")
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return _abbrev(bytes(value))
    if isinstance(value, int):
        if name in BYC_FIELDS:
            return f"{value:,} {_colour(colour, DIM, f'({value / MCAT:.3f} BYC)')}"
        if name in CRT_FIELDS:
            return f"{value:,} {_colour(colour, DIM, f'({value / MCAT:.3f} CRT)')}"
        if name in XCH_FIELDS:
            return f"{value:,} {_colour(colour, DIM, f'({value / MOJOS:.12g} XCH)')}"
        return f"{value:,}"
    if isinstance(value, Program):
        h = value.as_bin().hex()
        return h if len(h) <= 40 else f"{h[:20]}… ({len(h) // 2} bytes)"
    if isinstance(value, (list, tuple)):
        if not value:
            return "[]"
        return "[" + ", ".join(_fmt_value(name, v, colour) for v in value[:4]) + ("…]" if len(value) > 4 else "]")
    return str(value)


def render_spend(parsed, *, verbose: bool = False, colour: bool = True) -> str:
    """One spend, as a header line plus the driver's fields."""
    lines = []
    head = f"[{parsed.index}] {_colour(colour, BOLD, parsed.coin_type)}"
    lines.append(f"{head}  {_colour(colour, DIM, parsed.coin_id)}")
    lines.append(f"     {_colour(colour, DIM, 'amount')} {parsed.amount:,}")

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
            # Programs and conditions are noise unless asked for.
            if not verbose and field.name in (
                "inner_puzzle", "inner_solution", "solution", "operation", "lineage_proof",
                "final_output_conditions", "inner_conditions", "args", "args_and_memos",
                "solution_or_conditions", "rest_of_condition", "mutation", "rebalance_args",
            ):
                continue
            value = getattr(parsed.info, field.name, None)
            lines.append(f"       {field.name:<34} {_fmt_value(field.name, value, colour)}")
    else:
        lines.append(f"       {parsed.info}")
    return "\n".join(lines)


def render_bundle(parsed_spends, *, verbose: bool = False, colour: bool = True) -> str:
    body = "\n".join(render_spend(p, verbose=verbose, colour=colour) for p in parsed_spends)
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
        lines.append(render_spend(parsed, verbose=verbose, colour=colour))
        lines.append(f"     {_colour(colour, DIM, 'included:')} {why}")
        if block_spend.condition_error:
            lines.append(f"     {_colour(colour, DIM, 'conditions unavailable:')} {block_spend.condition_error}")
        lines.append("")

    failed = sum(1 for _b, r in pairs if r.failed)
    summary = f"{len(pairs)} spend(s)"
    if failed:
        summary += f", {_colour(colour, RED, f'{failed} failed to parse')}"
    return "\n".join(lines) + summary
