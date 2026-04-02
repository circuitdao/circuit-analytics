# Circuit DefiLlama Adapters

Adapters to enrich Circuit's DefiLlama listing with fees, revenue, and yield data.

## Adapters

### 1. Fees/Revenue — `dimension-adapters`

**File:** `fees/circuitdao.ts`
**Repo:** https://github.com/DefiLlama/dimension-adapters

Adds to the Circuit DefiLlama page:
- **Fees**: Daily stability fees paid by BYC borrowers + liquidation penalties
- **Revenue**: Fees retained by protocol after paying savings vault depositors
- **Supply-side revenue**: Daily interest paid to BYC savings vault depositors

**To test locally:**
```bash
git clone https://github.com/DefiLlama/dimension-adapters
cd dimension-adapters
pnpm install
cp /path/to/this/fees/circuitdao.ts fees/circuitdao.ts
pnpm test fees circuitdao
```

### 2. Borrow/Savings Rates — `yield-server`

**Directory:** `src/adaptors/circuit/`
**Repo:** https://github.com/DefiLlama/yield-server

Adds to DefiLlama's yield/rates pages:
- **XCH collateral vault**: stability fee APY as borrow rate, collateral and debt totals
- **BYC savings vault**: savings interest APY

**To test locally:**
```bash
git clone https://github.com/DefiLlama/yield-server
cd yield-server/src/adaptors
npm install
cp -r /path/to/this/yield/circuit circuit/
npm run test --adapter=circuit
```

## Data Source

Both adapters fetch from `https://api.circuitdao.com/protocol/stats`.

Key fields used:
| Field | Unit | Used for |
|---|---|---|
| `fees_received` | mBYC (cumulative) | Daily fees delta |
| `interest_paid` | mBYC (cumulative) | Daily supply-side revenue delta |
| `projected_revenue` | mBYC/year | Stability fee APY numerator |
| `undiscounted_principal` | mBYC | Stability fee APY denominator |
| `projected_cost` | mBYC/year | Savings APY numerator |
| `savings_balance` + `accrued_interest` | mBYC | Savings APY denominator |
| `byc_in_circulation` | mBYC | Total BYC borrowed (USD) |
| `collateral_usd` | USD | Collateral TVL |

1 BYC = 1000 mBYC = $1 USD (BYC is pegged 1:1 to USD).
