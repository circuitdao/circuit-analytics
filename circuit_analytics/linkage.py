"""Find the spends in a block that belong to a protocol transaction.

A Circuit transaction is rarely one coin. A liquidation bid spends the vault, a treasury
coin, a BYC coin and a fee coin, and those are held together by conditions rather than by
their puzzles: announcements, messages, ephemeral coin creation. Looking only at coins whose
puzzle the drivers recognise would show the vault and miss the fee coin that authorised it,
or the treasury coin that received the proceeds.

So this starts from the spends the drivers claim and walks the condition graph outwards until
it stops growing, recording why each spend was pulled in. The relations are the ones the
protocol actually relies on:

* **coin creation** -- one spend creates a coin another spend spends in the same block
* **coin / puzzle announcements** -- CREATE_*_ANNOUNCEMENT against the matching ASSERT_*
* **messages** -- SEND_MESSAGE against RECEIVE_MESSAGE
* **concurrency assertions** -- ASSERT_CONCURRENT_SPEND / _PUZZLE naming a coin or puzzle

Announcement IDs are recomputed here rather than trusted from the asserting side, so a
mismatched assertion simply fails to link instead of inventing an edge.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable, Optional

from chia.types.blockchain_format.program import Program
from chia.types.condition_opcodes import ConditionOpcode
from chia.util.hash import std_hash
from chia_rs import Coin, CoinSpend

log = logging.getLogger(__name__)

# How a spend came to be included.
REASON_PROTOCOL = "protocol coin"


@dataclass
class SpendLink:
    """One edge: `index` was pulled in because of `via`, from spend `source_index`."""

    source_index: int
    via: str
    detail: str = ""


@dataclass
class BlockSpend:
    index: int
    coin_spend: CoinSpend
    coin_id: bytes
    puzzle_hash: bytes
    conditions: list = field(default_factory=list)
    reason: Optional[str] = None
    """Why this spend is in the result: REASON_PROTOCOL, or set from the first link found."""
    links: list = field(default_factory=list)
    coin_type: str = ""
    condition_error: Optional[str] = None
    """Set when the puzzle could not be run, so its conditions are unknown."""


def _run_conditions(coin_spend: CoinSpend) -> tuple[list, Optional[str]]:
    """Conditions the spend outputs, or the reason they could not be obtained.

    A puzzle that fails to run is not fatal here: the spend is still reported, it just
    cannot contribute edges.
    """
    try:
        puzzle = Program.from_serialized(coin_spend.puzzle_reveal)
        solution = Program.from_serialized(coin_spend.solution)
        return list(puzzle.run(solution).as_iter()), None
    except Exception as err:  # noqa: BLE001 - a broken puzzle is data, not a crash
        return [], f"{type(err).__name__}: {err}"


def _atom(condition, position: int) -> Optional[bytes]:
    try:
        value = condition.at("r" * position + "f")
    except Exception:  # noqa: BLE001
        return None
    atom = value.atom
    return bytes(atom) if atom is not None else None


def _opcode(condition) -> Optional[bytes]:
    try:
        atom = condition.first().atom
    except Exception:  # noqa: BLE001
        return None
    return bytes(atom) if atom is not None else None


@dataclass
class _Index:
    """Everything a spend offers that another spend might refer to."""

    by_coin_id: dict = field(default_factory=dict)
    by_puzzle_hash: dict = field(default_factory=dict)
    created_coin_ids: dict = field(default_factory=dict)
    coin_announcements: dict = field(default_factory=dict)
    puzzle_announcements: dict = field(default_factory=dict)
    sent_messages: dict = field(default_factory=dict)


def _build_index(spends: list) -> _Index:
    index = _Index()
    for spend in spends:
        index.by_coin_id[spend.coin_id] = spend.index
        index.by_puzzle_hash.setdefault(spend.puzzle_hash, []).append(spend.index)

        for condition in spend.conditions:
            opcode = _opcode(condition)
            if opcode == ConditionOpcode.CREATE_COIN.value:
                puzzle_hash = _atom(condition, 1)
                amount_prog = condition.at("rrf") if condition.at("rr") is not None else None
                if puzzle_hash is None or amount_prog is None:
                    continue
                try:
                    amount = amount_prog.as_int()
                except Exception:  # noqa: BLE001
                    continue
                # The coin this spend creates, named the way the chain will name it.
                created = bytes(Coin(spend.coin_id, puzzle_hash, amount).name())
                index.created_coin_ids.setdefault(created, []).append(spend.index)
            elif opcode == ConditionOpcode.CREATE_COIN_ANNOUNCEMENT.value:
                message = _atom(condition, 1)
                if message is not None:
                    index.coin_announcements.setdefault(std_hash(spend.coin_id + message), []).append(spend.index)
            elif opcode == ConditionOpcode.CREATE_PUZZLE_ANNOUNCEMENT.value:
                message = _atom(condition, 1)
                if message is not None:
                    index.puzzle_announcements.setdefault(
                        std_hash(spend.puzzle_hash + message), []
                    ).append(spend.index)
            elif opcode == ConditionOpcode.SEND_MESSAGE.value:
                mode, message = _atom(condition, 1), _atom(condition, 2)
                if message is not None:
                    index.sent_messages.setdefault((mode, message), []).append(spend.index)
    return index


def _edges_from(spend: BlockSpend, index: _Index) -> Iterable[tuple[int, str, str]]:
    """(other spend index, relation, detail) for everything this spend refers to."""
    # The coin being spent was created by another spend in this block.
    parent = bytes(spend.coin_spend.coin.parent_coin_info)
    if parent in index.by_coin_id:
        yield index.by_coin_id[parent], "spends coin created by", ""
    for other in index.created_coin_ids.get(spend.coin_id, []):
        if other != spend.index:
            yield other, "coin created by", ""

    for condition in spend.conditions:
        opcode = _opcode(condition)
        if opcode == ConditionOpcode.CREATE_COIN.value:
            puzzle_hash = _atom(condition, 1)
            amount_prog = condition.at("rrf") if condition.at("rr") is not None else None
            if puzzle_hash is None or amount_prog is None:
                continue
            try:
                created = bytes(Coin(spend.coin_id, puzzle_hash, amount_prog.as_int()).name())
            except Exception:  # noqa: BLE001
                continue
            if created in index.by_coin_id:
                yield index.by_coin_id[created], "creates coin spent by", ""
        elif opcode == ConditionOpcode.ASSERT_COIN_ANNOUNCEMENT.value:
            announcement_id = _atom(condition, 1)
            for other in index.coin_announcements.get(announcement_id, []):
                yield other, "asserts coin announcement of", ""
        elif opcode == ConditionOpcode.ASSERT_PUZZLE_ANNOUNCEMENT.value:
            announcement_id = _atom(condition, 1)
            for other in index.puzzle_announcements.get(announcement_id, []):
                yield other, "asserts puzzle announcement of", ""
        elif opcode == ConditionOpcode.ASSERT_CONCURRENT_SPEND.value:
            coin_id = _atom(condition, 1)
            if coin_id in index.by_coin_id:
                yield index.by_coin_id[coin_id], "asserts concurrent spend of", ""
        elif opcode == ConditionOpcode.ASSERT_CONCURRENT_PUZZLE.value:
            puzzle_hash = _atom(condition, 1)
            for other in index.by_puzzle_hash.get(puzzle_hash, []):
                if other != spend.index:
                    yield other, "asserts concurrent puzzle of", ""
        elif opcode == ConditionOpcode.RECEIVE_MESSAGE.value:
            mode, message = _atom(condition, 1), _atom(condition, 2)
            for other in index.sent_messages.get((mode, message), []):
                if other != spend.index:
                    mode_hex = mode.hex() if mode else "00"
                    yield other, "receives message from", f"mode 0x{mode_hex}"


def collect_block_spends(coin_spends: list, is_protocol) -> list:
    """Spends in the block that are protocol coins, plus everything tied to them.

    `is_protocol(coin_spend) -> str | None` names the coin type for a protocol coin, or
    returns None. Growth stops when a full pass adds nothing, so a chain of links is followed
    however long it is -- a fee coin asserting an announcement from a BYC coin that received a
    message from a vault all comes back together.
    """
    spends = []
    for i, coin_spend in enumerate(coin_spends):
        conditions, error = _run_conditions(coin_spend)
        spends.append(
            BlockSpend(
                index=i,
                coin_spend=coin_spend,
                coin_id=bytes(coin_spend.coin.name()),
                puzzle_hash=bytes(coin_spend.coin.puzzle_hash),
                conditions=conditions,
                condition_error=error,
            )
        )

    index = _build_index(spends)

    included = {}
    for spend in spends:
        coin_type = is_protocol(spend.coin_spend)
        if coin_type:
            spend.coin_type = coin_type
            spend.reason = REASON_PROTOCOL
            included[spend.index] = spend

    # Walk outwards. Edges are followed in both directions: a fee coin asserting a protocol
    # coin's announcement is as much a part of the transaction as the protocol coin naming it.
    # Each edge is stored under both endpoints, labelled from the perspective of the spend
    # being reached: walking from the protocol coin to the fee coin should say the fee coin
    # asserts its announcement, not the other way round.
    edges = {}
    for spend in spends:
        for other_index, via, detail in _edges_from(spend, index):
            # `via` describes what `spend` does to `other`.
            edges.setdefault(other_index, []).append((spend.index, via, detail))
            edges.setdefault(spend.index, []).append((other_index, _invert(via), detail))

    frontier = list(included)
    while frontier:
        current = frontier.pop()
        for other_index, via, detail in edges.get(current, []):
            if other_index in included:
                continue
            other = spends[other_index]
            other.reason = f"linked to [{current}]"
            other.links.append(SpendLink(source_index=current, via=via, detail=detail))
            included[other_index] = other
            frontier.append(other_index)

    return [included[i] for i in sorted(included)]


_INVERSE = {
    "spends coin created by": "creates coin spent by",
    "creates coin spent by": "spends coin created by",
    "coin created by": "creates coin spent by",
    "asserts coin announcement of": "coin announcement asserted by",
    "asserts puzzle announcement of": "puzzle announcement asserted by",
    "asserts concurrent spend of": "concurrent spend asserted by",
    "asserts concurrent puzzle of": "concurrent puzzle asserted by",
    "receives message from": "sends message to",
}


def _invert(via: str) -> str:
    return _INVERSE.get(via, via)
