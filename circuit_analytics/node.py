"""Connecting to a Chia full node, local or remote.

Nothing here hardcodes a host: which node to talk to is a property of whoever is running the
tool, not of this repository. It is resolved in this order, first match winning:

1. an explicit list passed in (the CLI's ``--node``)
2. ``CHIA_NODES``, a comma-separated ``host:port`` list
3. ``CHIA_RPC_HOST`` / ``CHIA_RPC_PORT``
4. the ``full_node.rpc_port`` in the Chia config at ``CHIA_ROOT``, on ``self_hostname``

Several nodes may be given; each is health-checked and the first that answers is used, so a
laptop can list a remote node and fall back to a local one without changing anything.

Connecting to a *remote* node needs that node's TLS client certificate, because the RPC
interface is mutually authenticated. Point ``CHIA_ROOT`` at a directory holding the remote
node's ``config/ssl`` material; a copy of the remote ``~/.chia/mainnet`` is the usual way.
Keep that directory outside this repository -- it contains private keys.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterable, Optional

log = logging.getLogger(__name__)

DEFAULT_RPC_TIMEOUT = 15


class NodeConnectionError(RuntimeError):
    """No configured node could be reached."""


def _parse_nodes(raw: str) -> list:
    """`host:port,host:port` -> [(host, port)], skipping anything malformed."""
    nodes = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" not in entry:
            log.warning("ignoring node %r: expected host:port", entry)
            continue
        host, _, port = entry.rpartition(":")
        host = host.strip()
        if not host:
            log.warning("ignoring node %r: no host", entry)
            continue
        try:
            nodes.append((host, int(port)))
        except ValueError:
            log.warning("ignoring node %r: port is not a number", entry)
    return nodes


def resolve_nodes(explicit: Optional[Iterable[str]] = None, config: Optional[dict] = None) -> list:
    """The (host, port) candidates to try, in order."""
    if explicit:
        nodes = _parse_nodes(",".join(explicit))
        if nodes:
            return nodes

    from_env = os.environ.get("CHIA_NODES")
    if from_env:
        nodes = _parse_nodes(from_env)
        if nodes:
            return nodes

    host = os.environ.get("CHIA_RPC_HOST")
    port = os.environ.get("CHIA_RPC_PORT")
    if host or port:
        config = config or {}
        return [(
            host or config.get("self_hostname", "127.0.0.1"),
            int(port) if port else int(config.get("full_node", {}).get("rpc_port", 8555)),
        )]

    config = config or {}
    return [(
        config.get("self_hostname", "127.0.0.1"),
        int(config.get("full_node", {}).get("rpc_port", 8555)),
    )]


def chia_root() -> Path:
    return Path(os.path.expanduser(os.environ.get("CHIA_ROOT", "~/.chia/mainnet")))


async def get_full_node_client(
    explicit_nodes: Optional[Iterable[str]] = None,
    root_path: Optional[Path] = None,
    rpc_timeout: int = DEFAULT_RPC_TIMEOUT,
):
    """A connected FullNodeRpcClient, or NodeConnectionError naming everything that failed.

    Each candidate is health-checked before being returned, so a node that is listening but
    not answering RPC does not surface later as a confusing failure mid-scan.
    """
    from chia.full_node.full_node_rpc_client import FullNodeRpcClient
    from chia.util.config import load_config
    from chia_rs.sized_ints import uint16

    root_path = root_path or chia_root()
    if not (root_path / "config" / "config.yaml").exists():
        raise NodeConnectionError(
            f"no Chia config at {root_path}. Set CHIA_ROOT (or pass --chia-root) to a "
            "directory holding the node's config and ssl material."
        )
    config = load_config(root_path, "config.yaml")
    config["rpc_timeout"] = rpc_timeout

    candidates = resolve_nodes(explicit_nodes, config)
    failures = []
    for host, port in candidates:
        client = None
        try:
            log.debug("trying Chia node %s:%s", host, port)
            client = await FullNodeRpcClient.create(host, uint16(port), root_path, config)
            await client.healthz()
            log.info("connected to Chia node %s:%s", host, port)
            return client
        except Exception as err:  # noqa: BLE001 - try the next candidate
            failures.append(f"{host}:{port} ({type(err).__name__}: {err})")
            if client is not None:
                try:
                    client.close()
                    await client.await_closed()
                except Exception:  # noqa: BLE001
                    pass

    raise NodeConnectionError(
        "could not reach a Chia full node. Tried: "
        + "; ".join(failures)
        + f"\nUsing CHIA_ROOT={root_path}. Pass --node HOST:PORT, or set CHIA_NODES, to "
        "point at a different node. A remote node also needs its ssl material at CHIA_ROOT."
    )
