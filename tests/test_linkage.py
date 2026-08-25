"""Selecting the spends in a block that belong to a protocol transaction.

The point of the linkage walk is that a Circuit transaction is several coins held together by
conditions, not by their puzzles. These build small blocks by hand and check that each
relation pulls the right spend in, and -- just as important -- that unrelated spends stay out.
"""

from chia.types.blockchain_format.program import Program
from chia.types.coin_spend import make_spend
from chia.types.condition_opcodes import ConditionOpcode
from chia.util.hash import std_hash
from chia_rs import Coin

from circuit_analytics.linkage import REASON_PROTOCOL, collect_block_spends

# A puzzle that returns its solution, so a spend's conditions are whatever we hand it.
ECHO = Program.to(2)
ECHO_HASH = ECHO.get_tree_hash()


def _spend(conditions, *, parent=b"p" * 32, amount=1, puzzle=ECHO):
    # ECHO is CLVM path 2, i.e. the solution's first element, so the condition list has to
    # be wrapped for the spend to output it.
    coin = Coin(parent, puzzle.get_tree_hash(), amount)
    return make_spend(coin, puzzle, Program.to([conditions]))


def _protocol_by_index(*indexes):
    """Treat the spends at these positions as protocol coins."""
    wanted = set(indexes)
    seen = {"n": 0}

    def selector(_coin_spend):
        i = seen["n"]
        seen["n"] += 1
        return "test protocol coin" if i in wanted else None

    return selector


def _by_coin_id(*coin_ids):
    wanted = {bytes(c) for c in coin_ids}
    return lambda cs: "test protocol coin" if bytes(cs.coin.name()) in wanted else None


def test_protocol_spend_alone_is_selected():
    protocol = _spend([])
    unrelated = _spend([], parent=b"u" * 32)
    selected = collect_block_spends([protocol, unrelated], _by_coin_id(protocol.coin.name()))
    assert len(selected) == 1
    assert selected[0].reason == REASON_PROTOCOL
    assert selected[0].coin_type == "test protocol coin"


def test_unrelated_spends_are_left_out():
    """A block is mostly other people's transactions; they must not be dragged in."""
    protocol = _spend([])
    others = [_spend([], parent=bytes([i]) * 32) for i in range(1, 5)]
    selected = collect_block_spends([protocol, *others], _by_coin_id(protocol.coin.name()))
    assert [s.coin_id for s in selected] == [bytes(protocol.coin.name())]


def test_fee_coin_asserting_a_coin_announcement_is_pulled_in():
    """The usual shape: a protocol coin announces, and the fee coin asserts it."""
    message = b"*"
    protocol = _spend([[ConditionOpcode.CREATE_COIN_ANNOUNCEMENT, message]])
    announcement_id = std_hash(bytes(protocol.coin.name()) + message)
    fee = _spend([[ConditionOpcode.ASSERT_COIN_ANNOUNCEMENT, announcement_id]], parent=b"f" * 32)

    selected = collect_block_spends([protocol, fee], _by_coin_id(protocol.coin.name()))
    assert len(selected) == 2
    linked = selected[1]
    assert linked.reason == "linked to [0]"
    assert linked.links[0].via == "asserts coin announcement of"


def test_puzzle_announcement_links_both_ways():
    message = b"$"
    protocol = _spend([[ConditionOpcode.CREATE_PUZZLE_ANNOUNCEMENT, message]])
    announcement_id = std_hash(bytes(protocol.coin.puzzle_hash) + message)
    other = _spend([[ConditionOpcode.ASSERT_PUZZLE_ANNOUNCEMENT, announcement_id]], parent=b"f" * 32)

    # Selected from either end: the announcement is the same edge whichever side is protocol.
    assert len(collect_block_spends([protocol, other], _by_coin_id(protocol.coin.name()))) == 2
    assert len(collect_block_spends([protocol, other], _by_coin_id(other.coin.name()))) == 2


def test_mismatched_announcement_does_not_link():
    """An assertion naming an announcement nobody made must not invent an edge."""
    protocol = _spend([[ConditionOpcode.CREATE_COIN_ANNOUNCEMENT, b"*"]])
    stranger = _spend([[ConditionOpcode.ASSERT_COIN_ANNOUNCEMENT, b"x" * 32]], parent=b"f" * 32)
    selected = collect_block_spends([protocol, stranger], _by_coin_id(protocol.coin.name()))
    assert len(selected) == 1


def test_ephemeral_child_spend_is_pulled_in():
    """A coin created and spent in the same block, e.g. a vault's child coin."""
    child_puzzle_hash = ECHO_HASH
    protocol = _spend([[ConditionOpcode.CREATE_COIN, child_puzzle_hash, 500]])
    child_coin = Coin(protocol.coin.name(), child_puzzle_hash, 500)
    child = make_spend(child_coin, ECHO, Program.to([[]]))

    selected = collect_block_spends([protocol, child], _by_coin_id(protocol.coin.name()))
    assert len(selected) == 2
    assert selected[1].links[0].via in ("creates coin spent by", "spends coin created by")


def test_parent_side_is_pulled_in_from_the_child():
    """Starting from the created coin still finds the spend that made it."""
    protocol_child_ph = ECHO_HASH
    parent = _spend([[ConditionOpcode.CREATE_COIN, protocol_child_ph, 500]])
    child_coin = Coin(parent.coin.name(), protocol_child_ph, 500)
    child = make_spend(child_coin, ECHO, Program.to([[]]))

    selected = collect_block_spends([parent, child], _by_coin_id(child_coin.name()))
    assert len(selected) == 2
    assert {s.index for s in selected} == {0, 1}


def test_message_pairs_link():
    """SEND_MESSAGE / RECEIVE_MESSAGE, as a vault uses to approve a treasury deposit."""
    mode, message = 0x3F, b"protocol message body"
    sender = _spend([[ConditionOpcode.SEND_MESSAGE, mode, message, b"d" * 32]])
    receiver = _spend(
        [[ConditionOpcode.RECEIVE_MESSAGE, mode, message, b"s" * 32]], parent=b"r" * 32
    )
    selected = collect_block_spends([sender, receiver], _by_coin_id(sender.coin.name()))
    assert len(selected) == 2
    assert selected[1].links[0].via == "receives message from"
    assert "0x3f" in selected[1].links[0].detail


def test_concurrent_spend_assertion_links():
    protocol = _spend([])
    other = _spend(
        [[ConditionOpcode.ASSERT_CONCURRENT_SPEND, protocol.coin.name()]], parent=b"f" * 32
    )
    selected = collect_block_spends([protocol, other], _by_coin_id(protocol.coin.name()))
    assert len(selected) == 2
    assert selected[1].links[0].via == "asserts concurrent spend of"


def test_links_are_followed_transitively():
    """A -> B -> C: the fee coin two hops from the protocol coin still comes back."""
    msg_a, msg_b = b"a", b"b"
    a = _spend([[ConditionOpcode.CREATE_COIN_ANNOUNCEMENT, msg_a]])
    ann_a = std_hash(bytes(a.coin.name()) + msg_a)
    b = _spend(
        [
            [ConditionOpcode.ASSERT_COIN_ANNOUNCEMENT, ann_a],
            [ConditionOpcode.CREATE_COIN_ANNOUNCEMENT, msg_b],
        ],
        parent=b"b" * 32,
    )
    ann_b = std_hash(bytes(b.coin.name()) + msg_b)
    c = _spend([[ConditionOpcode.ASSERT_COIN_ANNOUNCEMENT, ann_b]], parent=b"c" * 32)
    unrelated = _spend([], parent=b"z" * 32)

    selected = collect_block_spends([a, b, c, unrelated], _by_coin_id(a.coin.name()))
    assert {s.index for s in selected} == {0, 1, 2}


def test_unrunnable_puzzle_is_reported_not_fatal():
    """A spend whose puzzle will not run still appears; it just contributes no edges."""
    broken = make_spend(Coin(b"p" * 32, ECHO_HASH, 1), ECHO, Program.to(b"not a cons cell"))
    selected = collect_block_spends([broken], _by_coin_id(broken.coin.name()))
    assert len(selected) == 1
    assert selected[0].condition_error is not None
