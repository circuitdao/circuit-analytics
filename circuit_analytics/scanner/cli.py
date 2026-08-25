"""
circuit-scan CLI entry point.

Commands:
  circuit-scan serve              # start the analytics HTTP server
  circuit-scan run                # scan blocks once (from last checkpoint)
  circuit-scan run --max-blocks N # scan at most N blocks
  circuit-scan parse <source>     # parse a spend bundle through the drivers
  circuit-scan parse --height N   # parse a block's protocol spends and everything tied to them
  circuit-scan verify-config      # check the local puzzle set matches the deployed protocol
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path


# Set from --node / --chia-root before any command runs, so every code path that opens a
# client honours them without threading arguments through.
_NODE_OVERRIDES: dict = {"nodes": None, "root_path": None}


async def _make_client():
    from circuit_analytics.node import NodeConnectionError, get_full_node_client

    try:
        return await get_full_node_client(
            explicit_nodes=_NODE_OVERRIDES["nodes"],
            root_path=_NODE_OVERRIDES["root_path"],
        )
    except NodeConnectionError as err:
        raise SystemExit(str(err))


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


# Config is read from the environment at import time, so a missing variable surfaces as a
# traceback from deep inside a driver. Name what is missing and where to get it instead.
REQUIRED_ENV = (
    "BYC_TAIL_HASH",
    "CRT_TAIL_HASH",
    "STATUTES_LAUNCHER_ID",
    "ANNOUNCER_REGISTRY_LAUNCHER_ID",
    "GENESIS_CHALLENGE",
    "CIRCUIT_ANNOUNCER_REGISTRY_CONSTRAINTS",
    "CIRCUIT_APPROVED_MOD_HASHES",
)



def _check_env() -> None:
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        raise SystemExit(
            "missing required environment variable(s): "
            + ", ".join(missing)
            + "\n\nThese describe the deployed protocol and are the same for everyone on a "
            "network.\nSource them with:  . ./env.sh set"
        )


async def _fetch_coin_spend(coin_id: str):
    """Pull a spent coin's spend from a full node, so a coin ID alone is enough."""
    from chia_rs.sized_bytes import bytes32

    client = await _make_client()
    try:
        record = await client.get_coin_record_by_name(bytes32.from_hexstr(coin_id))
        if record is None:
            raise SystemExit(f"coin {coin_id} not found")
        if not record.spent:
            raise SystemExit(f"coin {coin_id} is unspent, so it has no spend to parse")
        spend = await client.get_puzzle_and_solution(record.coin.name(), record.spent_block_index)
        if spend is None:
            raise SystemExit(f"no puzzle and solution for coin {coin_id}")
        return [spend]
    finally:
        client.close()
        await client.await_closed()


def _load_coin_spends(source: str):
    """A bundle from a file, a hex blob, or a coin ID fetched from a node."""
    import json
    from pathlib import Path as _Path

    from chia_rs import SpendBundle

    text = source
    path = _Path(source)
    if path.exists():
        text = path.read_text().strip()

    stripped = text.strip()
    # A bare 32-byte hex string is a coin ID, not a bundle.
    if len(stripped) == 64 and not stripped.startswith("{"):
        try:
            bytes.fromhex(stripped)
        except ValueError:
            pass
        else:
            return asyncio.run(_fetch_coin_spend(stripped))

    if stripped.startswith("{"):
        data = json.loads(stripped)
        # Accept a bare bundle or an RPC response wrapping one.
        for key in ("spend_bundle", "bundle"):
            if key in data:
                data = data[key]
                break
        return list(SpendBundle.from_json_dict(data).coin_spends)

    try:
        return list(SpendBundle.from_bytes(bytes.fromhex(stripped)).coin_spends)
    except Exception as err:
        raise SystemExit(
            f"could not read a spend bundle from {source!r}: {err}\n"
            "Expected a JSON bundle (file or inline), a hex-encoded bundle, or a spent coin ID."
        )


def _parse(source: str, verbose: bool, as_json: bool, colour: bool) -> None:
    from circuit_analytics.parse import parse_spend_bundle
    from circuit_analytics.render import render_bundle

    coin_spends = _load_coin_spends(source)
    parsed = parse_spend_bundle(coin_spends)

    if as_json:
        import json

        print(json.dumps([
            {
                "index": p.index,
                "coin_id": p.coin_id,
                "puzzle_hash": p.puzzle_hash,
                "amount": p.amount,
                "coin_type": p.coin_type,
                "info": type(p.info).__name__ if p.info is not None else None,
                "error": p.error,
            }
            for p in parsed
        ], indent=2))
    else:
        print(render_bundle(parsed, verbose=verbose, colour=colour))

    if any(p.failed for p in parsed):
        raise SystemExit(1)


async def _fetch_block_spends_for(height: int | None, header_hash: str | None):
    """Every spend in a block, by height or header hash."""
    from chia_rs.sized_bytes import bytes32

    client = await _make_client()
    try:
        if header_hash is None:
            record = await client.get_block_record_by_height(height)
            if record is None:
                raise SystemExit(f"no block at height {height}")
            hh = record.header_hash
        else:
            hh = bytes32.from_hexstr(header_hash)
            record = await client.get_block_record(hh)
            if record is None:
                raise SystemExit(f"no block with header hash {header_hash}")
        spends = await client.get_block_spends(hh)
        return list(spends or []), record
    finally:
        client.close()
        await client.await_closed()


def _parse_block(height, header_hash, verbose: bool, as_json: bool, colour: bool, everything: bool) -> None:
    from circuit_analytics.linkage import collect_block_spends
    from circuit_analytics.parse import parse_coin_spend, protocol_coin_type
    from circuit_analytics.render import render_block

    all_spends, record = asyncio.run(_fetch_block_spends_for(height, header_hash))
    if not all_spends:
        print(f"block {record.height} ({record.header_hash.hex()}) has no spends")
        return

    selector = (lambda _cs: "spend") if everything else protocol_coin_type
    selected = collect_block_spends(all_spends, selector)
    parsed = []
    for block_spend in selected:
        result = parse_coin_spend(block_spend.coin_spend)
        result.index = block_spend.index
        parsed.append((block_spend, result))

    if as_json:
        import json

        print(json.dumps({
            "height": record.height,
            "header_hash": record.header_hash.hex(),
            "spends_in_block": len(all_spends),
            "spends_selected": len(parsed),
            "spends": [
                {
                    "index": b.index,
                    "coin_id": r.coin_id,
                    "coin_type": r.coin_type,
                    "reason": b.reason,
                    "links": [
                        {"source_index": link.source_index, "via": link.via, "detail": link.detail}
                        for link in b.links
                    ],
                    "info": type(r.info).__name__ if r.info is not None else None,
                    "error": r.error,
                }
                for b, r in parsed
            ],
        }, indent=2))
    else:
        print(render_block(
            parsed, height=record.height, header_hash=record.header_hash.hex(),
            total_spends=len(all_spends), verbose=verbose, colour=colour,
        ))

    if any(r.failed for _b, r in parsed):
        raise SystemExit(1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(prog="circuit-scan")
    sub = parser.add_subparsers(dest="command", required=True)

    serve_p = sub.add_parser("serve", help="Start the analytics HTTP server")
    serve_p.add_argument("--host", default="0.0.0.0")
    serve_p.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)))
    serve_p.add_argument("--db", default=os.environ.get("DB_PATH", str(Path.home() / ".circuit" / "analytics.db")))
    serve_p.add_argument("--reload", action="store_true")

    def _add_node_args(sub_parser):
        """Where to find a full node. Never stored in the repo -- see README."""
        sub_parser.add_argument(
            "--node",
            action="append",
            metavar="HOST:PORT",
            help="full node RPC endpoint; repeat to give fallbacks. Overrides CHIA_NODES.",
        )
        sub_parser.add_argument(
            "--chia-root",
            metavar="PATH",
            help="directory holding the node's config and ssl material. Overrides CHIA_ROOT.",
        )

    run_p = sub.add_parser("run", help="Scan blocks once from last checkpoint")
    run_p.add_argument("--max-blocks", type=int, default=None)
    run_p.add_argument("--db", default=os.environ.get("DB_PATH", str(Path.home() / ".circuit" / "analytics.db")))
    _add_node_args(run_p)

    parse_p = sub.add_parser(
        "parse",
        help="Parse a spend bundle through the drivers",
        description=(
            "Route each spend in a bundle to its driver and print the resulting solution info. "
            "Needs no database. Exits non-zero if any spend fails to parse, which is what a "
            "drifted parser looks like -- the same failure would stall the block scanner."
        ),
    )
    parse_p.add_argument(
        "source",
        nargs="?",
        help="path to a JSON bundle, an inline JSON bundle, a hex-encoded bundle, or a spent coin ID",
    )
    block_group = parse_p.add_argument_group("block mode")
    block_group.add_argument("--height", type=int, help="parse the protocol spends in this block")
    block_group.add_argument("--header-hash", help="parse the protocol spends in this block")
    block_group.add_argument(
        "--all",
        action="store_true",
        dest="everything",
        help="include every spend in the block, not just protocol coins and what is tied to them",
    )
    parse_p.add_argument("-v", "--verbose", action="store_true", help="include puzzles, solutions and conditions")
    parse_p.add_argument("--json", action="store_true", dest="as_json", help="machine-readable summary")
    parse_p.add_argument("--no-color", action="store_true", help="disable colour")
    _add_node_args(parse_p)

    sub.add_parser(
        "verify-config",
        help="Check config and that the local puzzle set matches the deployed protocol",
    )

    args = parser.parse_args()

    if getattr(args, "node", None):
        _NODE_OVERRIDES["nodes"] = args.node
    if getattr(args, "chia_root", None):
        _NODE_OVERRIDES["root_path"] = Path(os.path.expanduser(args.chia_root))

    if args.command == "serve":
        import uvicorn
        os.environ.setdefault("DB_PATH", args.db)
        uvicorn.run(
            "circuit_analytics.server:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
        )

    elif args.command == "verify-config":
        _check_env()
        from circuit_analytics.verify_config import verify

        verify()

    elif args.command == "run":
        asyncio.run(_run(args.db, args.max_blocks))

    elif args.command == "parse":
        # The drivers log warnings for values they merely find odd; those are noise here.
        logging.getLogger("circuit_analytics").setLevel(logging.ERROR)
        _check_env()
        from circuit_analytics.verify_config import puzzle_set_warning

        warning = puzzle_set_warning()
        if warning:
            print(f"WARNING: {warning}\n", file=sys.stderr)
        by_block = args.height is not None or args.header_hash is not None
        if by_block and args.source:
            parser.error("give either a bundle source or --height/--header-hash, not both")
        if not by_block and not args.source:
            parser.error("give a bundle source, or --height/--header-hash")
        if args.everything and not by_block:
            parser.error("--all only applies to --height/--header-hash")
        if by_block:
            _parse_block(
                args.height, args.header_hash, args.verbose, args.as_json,
                colour=not args.no_color, everything=args.everything,
            )
        else:
            _parse(args.source, args.verbose, args.as_json, colour=not args.no_color)
