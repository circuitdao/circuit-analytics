"""Choosing which Chia full node to talk to.

Which node to use belongs to whoever runs the tool, never to this repository, so the whole
point of this module is that it is configurable from outside. These pin the precedence and
the parsing, both of which are easy to break silently.
"""

import pytest

from circuit_analytics.node import _parse_nodes, resolve_nodes

CONFIG = {"self_hostname": "config-host", "full_node": {"rpc_port": 9999}}


@pytest.fixture(autouse=True)
def _clear_node_env(monkeypatch):
    for name in ("CHIA_NODES", "CHIA_RPC_HOST", "CHIA_RPC_PORT"):
        monkeypatch.delenv(name, raising=False)


def test_explicit_nodes_win_over_everything():
    monkey_env = "should-not-be-used:1"
    import os

    os.environ["CHIA_NODES"] = monkey_env
    try:
        assert resolve_nodes(["192.0.2.10:1111"], CONFIG) == [("192.0.2.10", 1111)]
    finally:
        os.environ.pop("CHIA_NODES")


def test_multiple_explicit_nodes_keep_their_order():
    """Order is the failover order, so it has to survive."""
    assert resolve_nodes(["a:1", "b:2", "c:3"], CONFIG) == [("a", 1), ("b", 2), ("c", 3)]


def test_chia_nodes_env_is_used_when_no_explicit_nodes(monkeypatch):
    monkeypatch.setenv("CHIA_NODES", "remote:8555,127.0.0.1:8555")
    assert resolve_nodes(None, CONFIG) == [("remote", 8555), ("127.0.0.1", 8555)]


def test_rpc_host_and_port_env(monkeypatch):
    monkeypatch.setenv("CHIA_RPC_HOST", "single")
    monkeypatch.setenv("CHIA_RPC_PORT", "1234")
    assert resolve_nodes(None, CONFIG) == [("single", 1234)]


def test_rpc_host_alone_takes_the_port_from_config(monkeypatch):
    monkeypatch.setenv("CHIA_RPC_HOST", "single")
    assert resolve_nodes(None, CONFIG) == [("single", 9999)]


def test_falls_back_to_the_chia_config():
    assert resolve_nodes(None, CONFIG) == [("config-host", 9999)]


def test_falls_back_to_loopback_without_a_config():
    assert resolve_nodes(None, None) == [("127.0.0.1", 8555)]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("host:8555", [("host", 8555)]),
        (" host:8555 , other:8556 ", [("host", 8555), ("other", 8556)]),
        ("host:8555,,", [("host", 8555)]),
        ("nocolon", []),
        ("host:notaport", []),
        (":8555", []),  # no host
        ("2001:db8::1:8555", [("2001:db8::1", 8555)]),  # IPv6, port after the last colon
    ],
)
def test_node_string_parsing(raw, expected):
    assert _parse_nodes(raw) == expected


def test_malformed_entries_do_not_discard_the_good_ones():
    assert _parse_nodes("bad, good:8555, alsobad:x") == [("good", 8555)]
