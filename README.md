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

## Inspecting spends

Route spends through the same drivers the scanner uses and print what they parsed. Source
`env.sh` first — `circuit-scan` tells you which variables are missing if you forget.

```bash
circuit-scan parse bundle.json          # a spend bundle file
circuit-scan parse "$(cat bundle.json)" # inline JSON
circuit-scan parse <hex-encoded-bundle>
circuit-scan parse <spent-coin-id>      # fetched from a node
```

### From a block

```bash
circuit-scan parse --height 6543210
circuit-scan parse --header-hash <hash>
circuit-scan parse --height 6543210 --all   # every spend in the block, not just ours
```

A Circuit transaction is several coins held together by conditions rather than by their
puzzles — a liquidation bid spends the vault, a treasury coin, a BYC coin and a fee coin. So
this starts from the coins the drivers recognise and follows announcements, messages,
ephemeral coin creation and concurrency assertions outwards until the set stops growing. Each
spend reports why it was included:

```
[4] unrecognised  dd44e4a29433446b12b4236418bff045c31922d3064bad2ed60da568aeb045b8
     amount 1
     no driver claimed this spend
     included: asserts concurrent spend of [3]
```

That last line is the point: a spend of an ordinary XCH coin means nothing on its own, and
everything once you can see it paid the fee for the vault spend above it.

Coins that merely *hold* BYC or CRT — a wallet coin, or one locked in an offer — are not
treated as protocol coins, so two people trading BYC does not show up as protocol activity.
They appear when something ties them to a protocol spend, and then the link says what.

By default every field is one line, truncated past 34 bytes — compact enough to scan a whole
block. Two options change that, and they are independent:

- `-d`/`--details` breaks sequences out, one element per line, so an individual hash can be
  read and copied. Values are still truncated.
- `-f`/`--full` stops truncating, keeping every argument on one line for pasting elsewhere. A
  list argument appears in its serialised form, which is what CLVM tools accept.

Giving both expands sequences *and* shows them whole.

The 34-byte cutoff covers hashes, launcher IDs, asset IDs and the slightly larger structures
that carry one, since an abbreviated hash cannot be looked up or pasted anywhere. Elements
inside a list share one line, so they are cut to 10 bytes — enough to tell them apart, with
`--details` to read one properly. Coin amounts
are shown in their own units: BYC, CRT or XCH.

Also: `-v` includes puzzle reveals and raw solutions; `--json` gives a machine-readable
summary; `--no-color` disables colour. The exit code is non-zero if any spend fails to parse
— that is what a driver whose expected solution shape has drifted from its puzzle looks like,
and the same failure stalls the block scanner at that block.

## Checking the puzzle set

```bash
circuit-scan verify-config
```

Every protocol puzzle hash is derived from the installed `circuit_puzzles`. If that build
differs from what is deployed, no protocol coin is recognised by its puzzle and the tools
degrade quietly rather than failing: protocol spends parse as plain CATs and a block full of
activity looks almost empty. `verify-config` compares the local hashes against the deployed
ones in `CIRCUIT_APPROVED_MOD_HASHES` and exits non-zero if they differ; `parse` prints the
same warning before it runs.

## Choosing a full node

Bundle mode needs no node, unless you pass a coin ID. Block mode and `run` do. By default the
node comes from the Chia config at `CHIA_ROOT`, i.e. a node on this machine. To use one
elsewhere on the network:

```bash
circuit-scan parse --height 6543210 --node chia-node.example:8555
circuit-scan parse --height 6543210 --node chia-node.example:8555 --node 127.0.0.1:8555  # with fallback
```

Repeat `--node` to give fallbacks: each is health-checked and the first that answers is used.
`--chia-root PATH` overrides `CHIA_ROOT`.

A **remote** node's RPC is mutually authenticated, so it also needs that node's TLS client
certificate. Point `CHIA_ROOT` at a directory holding the remote node's `config/ssl`
material — usually a copy of its `~/.chia/mainnet`. **Keep that directory outside this
repository:** it contains private keys.

Addresses and paths are specific to your machine, so they are not committed. Put them in
`env.local.sh`, which is gitignored and sourced by `env.sh set`:

```bash
# env.local.sh
export CHIA_ROOT=~/chia-roots/remote-node
export CHIA_NODES=chia-node.example:8555,127.0.0.1:8555
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
