class SpendError(ValueError):
    """Use to raise error with information on why a coin spend failed"""
    pass


# Taken from
#   https://github.com/Chia-Network/clvm_rs/blob/main/src/error.rs
# This might not be the correct list. e.g. 'sha256 on list' seems to be missing
CLVM_ERRORS = [
    "bad encoding",
    "invalid backreference during deserialisation",
    "Out of Memory",
    "path into atom",
    "too many pairs",
    "Too Many Atoms",
    "cost exceeded or below zero",
    "unknown softfork extension",
    "softfork specified cost mismatch",
    "Internal Error: {1}",
    "clvm raise",
    "Invalid Nil Terminator in operand list",
    "Division by zero",
    "Value Stack Limit Reached",
    "Environment Stack Limit Reached",
    "Shift too large",
    "Reserved operator",
    "invalid operator",
    "unimplemented operator",
    "InvalidOperatorArg: {1}",
    "InvalidAllocatorArg: {1}",
    "bls_pairing_identity failed",
    "bls_verify failed",
    "Secp256 Verify Error: failed",
]
