# circuit-analytics

Block scanner and analytics server for the Circuit protocol on Chia. Scans on-chain coin spends, stores protocol statistics in a local SQLite database, and exposes an HTTP API consumed by the Circuit dapp's analytics page.

## Installation

```bash
poetry install
```

## Configuration

Source `env.sh` before running any command to set the required environment variables (tail hashes, launcher IDs, etc.):

```bash
. ./env.sh set                        # default DB path: ~/.circuit/analytics.db
. ./env.sh set /path/to/custom.db     # custom DB path
. ./env.sh clear                      # unset all vars
. ./env.sh show                       # show current values
```

## Running tests

```bash
pytest tests/                          # unit tests (integration tests skipped by default)
pytest tests/ -m integration -s        # full rescan integration test (requires a live Chia full node)
```

The integration test rescans the chain from protocol genesis and validates the resulting statistics. It writes to a temporary database and cleans up after itself — your `~/.circuit/analytics.db` is not affected. It requires a reachable Chia full node and all env vars from `env.sh` except `DB_PATH` (the test passes the DB path directly).

## Scanning blocks

Scan from the last checkpoint (or from protocol genesis on first run):

```bash
circuit-scan run
```

Options:
- `--max-blocks N` — stop after N blocks (default: unlimited)
- `--db PATH` — override DB path

To rescan from genesis, delete the existing DB first:
```bash
rm /path/to/analytics.db
```

## Running the analytics server

```bash
circuit-scan serve
```

Options:
- `--host` / `--port` — default `0.0.0.0:8080`
- `--db PATH` — override DB path
- `--reload` — enable auto-reload for development

The server exposes:
- `GET /protocol/stats` — protocol statistics (same format as Circuit API)
- `POST /sync_block_stats` — trigger a block scan

## Viewing the analytics page locally

To connect the Circuit dapp's analytics page to a local `circuit-analytics` database:

**1. Start the analytics server** (in the `circuit-analytics/` directory):
```bash
. ./env.sh set
circuit-scan serve
```

**2. Start the dapp dev server** (in the `dapp/` directory):
```bash
. ./env.sh set analytics local
pnpm dev
```

**4.** Open `http://localhost:5173/analytics` in your browser.

`set analytics local` sets `PUBLIC_ANALYTICS_URL=http://localhost:8080`. Use `set main` or `set main local` to revert to production or local Circuit API.
