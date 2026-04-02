"""
circuit-scan CLI entry point.

Commands:
  circuit-scan serve              # start the analytics HTTP server
  circuit-scan run                # scan blocks once (from last checkpoint)
  circuit-scan run --max-blocks N # scan at most N blocks
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path


def _make_client():
    from chia.full_node.full_node_rpc_client import FullNodeRpcClient
    from chia.util.config import load_config
    from chia_rs.sized_ints import uint16

    root_path = Path(os.environ.get("CHIA_ROOT", Path.home() / ".chia" / "mainnet"))
    config = load_config(root_path, "config.yaml")

    node_str = os.environ.get("CHIA_NODES", "").split(",")[0].strip()
    if node_str:
        host, port = node_str.rsplit(":", 1)
    else:
        host = config.get("self_hostname", "127.0.0.1")
        port = config.get("full_node", {}).get("rpc_port", 8555)

    return FullNodeRpcClient.create(host, uint16(int(port)), root_path, config)


async def _run(db_path: str, max_blocks: int) -> None:
    from circuit_analytics.scanner.block_scanner import scan_blocks

    client = await _make_client()
    try:
        result = await scan_blocks(client, db_path, max_blocks=max_blocks)
        print(
            f"blocks_synced={result['blocks_synced']}  "
            f"blocks_with_ops={result['blocks_with_ops']}  "
            f"last_height={result['last_height']}"
        )
    finally:
        client.close()
        await client.await_closed()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(prog="circuit-scan")
    sub = parser.add_subparsers(dest="command", required=True)

    serve_p = sub.add_parser("serve", help="Start the analytics HTTP server")
    serve_p.add_argument("--host", default="0.0.0.0")
    serve_p.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)))
    serve_p.add_argument("--db", default=os.environ.get("DB_PATH", str(Path.home() / ".circuit" / "analytics.db")))
    serve_p.add_argument("--reload", action="store_true")

    run_p = sub.add_parser("run", help="Scan blocks once from last checkpoint")
    run_p.add_argument("--max-blocks", type=int, default=None)
    run_p.add_argument("--db", default=os.environ.get("DB_PATH", str(Path.home() / ".circuit" / "analytics.db")))

    args = parser.parse_args()

    if args.command == "serve":
        import uvicorn
        os.environ.setdefault("DB_PATH", args.db)
        uvicorn.run(
            "circuit_analytics.server:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
        )

    elif args.command == "run":
        asyncio.run(_run(args.db, args.max_blocks))
