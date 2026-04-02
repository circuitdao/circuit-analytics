from enum import Enum
from typing import Optional

from chia.types.blockchain_format.program import Program
from chia.types.condition_opcodes import ConditionOpcode


CHIALISP_PRECISION = 10**10

PROTOCOL_PREFIX = b"C"
SOLUTION_PREFIX = b"S"
STATUTE_PREFIX = b"s"
STATUTE_FULL_PREFIX = b"S"
PRICE_PREFIX = b"p"
CUSTOM_CONDITION_PREFIX = b"U"
# Governance prefixes
PROPOSED_PREFIX = b"^"
IMPLEMENTED_PREFIX = b"$"
VETOED_PREFIX = b"x"
MELT_PREFIX = b"x"
ISSUE_PREFIX = b"i"


### Auctions ###
class AuctionType(Enum):
    SURPLUS = 0
    RECHARGE = 1


class AuctionStatus(Enum):
    STANDBY = 0
    RUNNING = 1


### Conditions ###
def is_protocol_condition(condition: Program) -> bool:
    if (
        condition.first().atom in [ConditionOpcode.CREATE_COIN_ANNOUNCEMENT, ConditionOpcode.CREATE_PUZZLE_ANNOUNCEMENT]
        and (condition.at("rf").atom)[0] == PROTOCOL_PREFIX
    ):
        return True
    elif (
        condition.first().atom
        in [
            ConditionOpcode.SEND_MESSAGE,
            ConditionOpcode.RECEIVE_MESSAGE,
        ]
        and (condition.at("rrf").atom)[0] == PROTOCOL_PREFIX
    ):
        return True
    elif condition.first().atom == ConditionOpcode.REMARK and condition.at("rf").atom == PROTOCOL_PREFIX:
        return True
    else:
        return False


def contains_condition(conditions: Program, protocol: bool = None, opcode: Optional[ConditionOpcode] = None) -> bool:
    if protocol is None and opcode is None:
        # no requirement on type of condition
        return conditions.list_len() > 0

    found_condition = False
    for cond in conditions.as_iter():
        if opcode is None:
            # we are not looking for a specific opcode
            if protocol and is_protocol_condition(cond):
                found_condition = True
            elif not protocol and not is_protocol_condition(cond):
                found_condition = True
        elif cond.first().atom == opcode:
            # requested opcode detected
            if protocol is None:
                found_condition = True
            elif protocol and is_protocol_condition(cond):
                found_condition = True
            elif not protocol and not is_protocol_condition(cond):
                found_condition = True

    return found_condition


def get_driver_info(conditions: Program, must_find_driver_info: bool = True) -> Optional[list[Program]]:
    """Extract driver info from a list of conditions

    Driver info is contained in protocol REMARK conditions, which have the form
      (list REMARK PROTOCOL_PREFIX ...)
    where ... is the driver info.

    If exactly one protocol REMARK condition is found, the function returns the correpsonding
    driver info.
    """

    remark_conditions = None
    for cond in conditions.as_iter():
        if cond.first().as_atom() == ConditionOpcode.REMARK and cond.rest().listp():
            if cond.rest().first().atom == PROTOCOL_PREFIX:
                remark_conditions = cond
                break
    else:
        if must_find_driver_info:
            raise ValueError("No protocol REMARK condition found")
        else:
            return remark_conditions
    return list(remark_conditions.rest().rest().as_iter())
