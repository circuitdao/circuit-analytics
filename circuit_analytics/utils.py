from typing import Optional

from chia.types.blockchain_format.program import Program
from chia_rs.sized_bytes import bytes32
from chia.util.hash import std_hash
from clvm_rs.casts import int_to_bytes

from circuit_analytics.errors import SpendError

MAX_TX_BLOCK_TIME = 120


def tree_hash_of_apply(mod_hash, environment_hash):
    return std_hash(
        bytes.fromhex("02a12871fee210fb8619291eaea194581cbd2531e4b23759d225f6806923f63222")
        + std_hash(
            int_to_bytes(2)
            + std_hash(
                int_to_bytes(2)
                + bytes.fromhex("9dcf97a184f32623d11a73124ceb99a5709b083721e878a16d78f596718ba7b2")
                + mod_hash
            )
            + std_hash(
                int_to_bytes(2)
                + environment_hash
                + bytes.fromhex("4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a")
            )
        )
    )


def _tuple_to_struct(tpl: tuple) -> Program:
    if len(tpl) > 2:
        return _tuple_to_struct(tpl[:-2] + ((tpl[-2], tpl[-1]),))
    else:
        return tpl


def tuple_to_struct(tpl: tuple) -> Program:
    """Converts a Python tuple to a Program which is correctly interpreted as a struct in Chialisp:
    (A, B, ..., M, N) -> Program.to((A, (B, ... (M, N)))), which is (A B ... M . N) in Chialisp
    """

    return Program.to(_tuple_to_struct(tpl))


types = [
    None,  # Program
    "bool",
    "bytes",  # rename to atom?! (TODO)
    "bytes32",
    "bytes32_or_nil",
    "bytes32_or_none",
    "int",
    "int64",
    "uint",
    "uint64",
]


def to_type(
    prog: Program,
    typ: str | None,
    prog_name: str = "",
) -> Program | bool | bytes | bytes32 | int | None:
    if not isinstance(prog, Program):
        raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to {typ}. Not a Program ({prog})")

    match typ:
        case None:
            return prog
        case "bool":
            return False if prog.nullp() else True
        case "bytes":
            try:
                converted = bytes(prog.atom)
            except Exception:
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to bytes ({prog})")
        case "bytes32":
            try:
                converted = bytes32(prog.atom)
            except Exception:
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to bytes32 ({prog})")
        case "bytes32_or_nil":
            # if prog is nil or can be converted to bytes32, return prog (ie a Program), else raise
            if prog.nullp():
                return prog
            try:
                bytes32(prog.atom)
            except Exception:
                raise SpendError(
                    f"{prog_name + ' ' if prog_name else ''} is not nil or convertible to bytes32 ({prog})"
                )
            return prog
        case "bytes32_or_none":
            # if prog is nil return None
            # elif prog can be converted to bytes32, return bytes32
            # else raise
            if prog.nullp():
                return None
            try:
                converted = bytes32(prog.atom)
            except Exception:
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to bytes32 or None ({prog})")
            return converted
        case "int":
            try:
                converted = prog.as_int()
            except Exception:
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to int ({prog})")
        case "int64":
            try:
                converted = prog.as_int()
            except Exception:
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to int64 ({prog})")
            if not (-(2**63) <= converted < 2**63):
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to int64 ({converted})")
            # TODO: check canonical representation (no excess leading bytes), mirroring puzzle-level check (= (+ x) x)
        case "uint":
            try:
                converted = prog.as_int()
            except Exception:
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to uint ({prog})")
            if not converted >= 0:
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to uint ({converted})")
        case "uint64":
            try:
                converted = prog.as_int()
            except Exception:
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to uint64 ({prog})")
            if not (0 <= converted < 2**64):
                raise SpendError(f"Failed to convert {prog_name + ' ' if prog_name else ''}to uint64 ({converted})")
            # TODO: check canonical representation (no excess leading bytes), mirroring puzzle-level check (= (+ x) x)
        case _:
            raise SpendError(f"Encountered unkown type {typ} in Program to type conversion. Must be one of {types}")

    return converted


def to_list(
    prog: Program,
    num: int,
    types: Optional[list[str | None]] = None,
    prog_name: str = "",
) -> tuple[Program | bytes | bytes32 | int, ...]:
    """Unpacks a Program into a tuple with exactly num elements, dropping any excess elements.

    This function is the Python equivalent of destructuring a variable into a proper list in Chialisp.

    If types argument is given, list elements will be converted according to the types specified.
    Allowed types are:
    - None: don't convert, remains a Program
    - "bool"
    - "bytes"
    - "bytes32"
    - "bytes32_or_nil": don't convert remains a Program
    - "bytes32_or_none": convert to bytes32 or None
    - "int"
    - "int64"
    - "uint"
    - "uint64"
    """
    if num < 1:
        raise ValueError(
            f"Cannot unpack {prog_name + ' ' if prog_name else ''}Program into list with non-positive number of elements"
        )
    lst = []
    if not types:
        # unpacking without type conversion, i.e. returning list of Programs
        for i in range(num):
            lst.append(prog.at("r" * i + "f"))
        return tuple(lst)
    # unpacking and converting to desired types
    if len(types) != num:
        raise ValueError(
            f"Must specify desired type for each element in {prog_name + ' ' if prog_name else ''}tuple to be returned"
        )
    for i in range(len(types)):
        prog_name_elt = prog_name + f"[{i}]" if prog_name else prog_name
        lst.append(to_type(prog.at("r" * i + "f"), types[i], prog_name_elt))
    return tuple(lst)


def to_tuple(
    prog: Program,
    num: int,
    types: Optional[list[str | None]] = None,
    prog_name: str = "",
) -> tuple[Program | bytes | bytes32 | int, ...] | Program:
    """Unpacks a Program into a tuple with exactly num elements, absorbing any excess elements into final element.

    This function is the Python equivalent of destructuring a variable into a struct in Chialisp.

    If types argument is given, tuple elements will be converted according to the types specified.
    Allowed types are:
    - None: don't convert, remains a Program
    - "bool"
    - "bytes"
    - "bytes32"
    - "bytes32_or_nil": don't convert remains a Program
    - "bytes32_or_none": convert to bytes32 or None
    - "int"
    - "int64"
    - "uint"
    - "uint64"
    """
    if num < 1:
        raise ValueError(
            f"Cannot unpack {prog_name + ' ' if prog_name else ''}Program into tuple with non-positive number of elements"
        )
    tup = []
    if not types:
        # unpacking without type conversion, i.e. returning tuple of Programs
        if num == 1:
            return prog
        tup.append(prog.first())
        for i in range(1, num - 1):
            tup.append(prog.at("r" * i + "f"))
        tup.append(prog.at("r" * (num - 1)))
        return tuple(tup)
    # unpacking and converting to desired types
    if len(types) != num:
        raise ValueError(
            f"Must specify desired type for each element in {prog_name + ' ' if prog_name else ''}tuple to be returned"
        )
    prog_name_elt = prog_name + f"[{0}]" if prog_name else prog_name
    if len(types) == 1:
        return to_type(prog, types[0], prog_name_elt)
    tup.append(to_type(prog.first(), types[0], prog_name_elt))
    for i in range(1, len(types) - 1):
        prog_name_elt = prog_name + f"[{i}]" if prog_name else prog_name
        tup.append(to_type(prog.at("r" * i + "f"), types[i], prog_name_elt))
    prog_name_elt = prog_name + f"[{num}]" if prog_name else prog_name
    tup.append(to_type(prog.at("r" * (num - 1)), types[num - 1], prog_name_elt))
    return tuple(tup)


def unique_launcher_ids(treasury_coins: "Program") -> "bytes32 | None":
    """Returns None if treasury coins have mutually distinct launcher IDs, else the first duplicate launcher ID encountered.

    treasury_coins is a list of lists with treasury coin launcher ID in second position in inner lists.
    This function can be used with treasury coins passed into Surplus and Recharge auction solution.
    """
    from chia_rs.sized_bytes import bytes32

    launcher_ids = [bytes32(coin.at("rf").atom) for coin in treasury_coins.as_iter()]
    seen = set()
    for launcher_id in launcher_ids:
        if launcher_id in seen:
            return launcher_id
        seen.add(launcher_id)
    return None
