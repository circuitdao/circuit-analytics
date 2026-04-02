#!/usr/bin/env bash
# Mainnet environment variables for circuit-analytics.
# Source this file before running circuit-scan:
#
#   . ./env.sh set                          # use default DB path (~/.circuit/analytics.db)
#   . ./env.sh set /path/to/custom.db       # use a custom DB path
#   . ./env.sh clear                        # unset all circuit-analytics env vars
#   . ./env.sh show                         # show current env vars
#
# Set the following separately as required by your setup (or add them here for your machine):
#   export CHIA_ROOT=~/.chia/mainnet
#   export CHIA_NODES=127.0.0.1:8555 (optional if ${CHIA_ROOT}/config/config.yaml already points to desired node)

if [ "$1" = "set" ]; then
  export DB_PATH="${2:-$HOME/.circuit/analytics.db}"
  export BYC_TAIL_HASH=ae1536f56760e471ad85ead45f00d680ff9cca73b8cc3407be778f1c0c606eac
  export CRT_TAIL_HASH=ea3ace5525d6aaf6d921b66052afc67da11c820b676de91d61ae1a766c8ce615
  export STATUTES_LAUNCHER_ID=101d3e673757782c8f8ac1eb3d531c543df899022bf81a427db4199108d4cdb1
  export ANNOUNCER_REGISTRY_LAUNCHER_ID=01734254bdfb1ec3abfde934bd9ea3b8b19645ee43eff9fea0b8ff51c39a8ae7
  export GENESIS_CHALLENGE=ccd5bb71183532bff220ba46c268991a3ff07eb358e8255a65c30a2dce0e5fbb
  export CIRCUIT_ANNOUNCER_REGISTRY_CONSTRAINTS=300000001,99
  # CLVM-encoded list of 5 approved mod hashes (vault, surplus auction, recharge auction,
  # savings vault, announcer registry)
  export CIRCUIT_APPROVED_MOD_HASHES=ffa0c092cc686dad5f31cd3c008d2daa3b1bae044bd50c1fd01ca0af96660dc8e391ffa06253104cf7de1bcbbd34cd10897794737db32fcf9d57bfa9bec13c741fb4c8d2ffa081e0cc376e53e97da0ee154992a3554aa679b8818c88dc54b1fb4c2463c0c786ffa02a3922ea385178c37687a958ab9b51d698888ba6bd8782e7b6c97a771b130aa3ffa0faa2ed871f9b4f5f679cf7d4d306d8b13c9ecceb46c7b1d4f9dd8d3e86c0fc7280
elif [ "$1" = "clear" ]; then
  unset DB_PATH
  unset BYC_TAIL_HASH
  unset CRT_TAIL_HASH
  unset STATUTES_LAUNCHER_ID
  unset ANNOUNCER_REGISTRY_LAUNCHER_ID
  unset GENESIS_CHALLENGE
  unset CIRCUIT_ANNOUNCER_REGISTRY_CONSTRAINTS
  unset CIRCUIT_APPROVED_MOD_HASHES
elif [ "$1" = "show" ] || [ -z "$1" ]; then
  :
else
  echo "Usage: . ./env.sh [set [DB_PATH] | clear | show]"
  return 1 2>/dev/null || exit 1
fi

echo "DB_PATH:                              $DB_PATH"
echo "BYC_TAIL_HASH:                        $BYC_TAIL_HASH"
echo "CRT_TAIL_HASH:                        $CRT_TAIL_HASH"
echo "STATUTES_LAUNCHER_ID:                 $STATUTES_LAUNCHER_ID"
echo "ANNOUNCER_REGISTRY_LAUNCHER_ID:       $ANNOUNCER_REGISTRY_LAUNCHER_ID"
echo "GENESIS_CHALLENGE:                    $GENESIS_CHALLENGE"
echo "CIRCUIT_ANNOUNCER_REGISTRY_CONSTRAINTS: $CIRCUIT_ANNOUNCER_REGISTRY_CONSTRAINTS"
echo "CIRCUIT_APPROVED_MOD_HASHES:          ${CIRCUIT_APPROVED_MOD_HASHES:0:20}..."
