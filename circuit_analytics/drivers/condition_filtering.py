from chia.types.blockchain_format.program import Program
from chia.types.condition_opcodes import ConditionOpcode

from circuit_analytics.drivers import PROTOCOL_PREFIX, SOLUTION_PREFIX
from circuit_analytics.errors import SpendError


def is_valid_rmk_cond(condition_body: Program) -> bool:
    """Check whether a Program would make a valid REMARK condition if prepended by REMARK opcode."""
    if condition_body.list_len() > 0 and condition_body.first().atom == PROTOCOL_PREFIX:
        return False
    return True


def is_valid_msg_cond(condition_body: Program) -> bool:
    """Check whether a Program would make a valid MESSAGE condition if prepended
    by a SEND_MESSAGE or RECEIVE_MESSAGE opcode.
    """
    if (
        condition_body.list_len() > 1
        and condition_body.rest().first().list_len() == 0
        and len(condition_body.rest().first().atom) == 33
        and condition_body.rest().first().atom[:1]
        == PROTOCOL_PREFIX  # [:1] instead of [0] as slicing returns bytes, indexing int
    ):
        return False
    return True


def is_valid_ann_cond(condition_body: Program) -> bool:
    """Check whether a Program would make a valid ANNOUNCEMENT condition if prepended
    by an CREATE_COIN_ANNOUNCEMENT or CREATE_PUZZLE ANNOUNCEMENT opcode.
    """
    if (
        condition_body.list_len() > 0
        and condition_body.first().list_len() == 0
        and len(condition_body.first().atom) == 33
        and condition_body.first().atom[:1]
        == PROTOCOL_PREFIX  # [:1] instead of [0] as slicing returns bytes, indexing int
    ):
        return False
    return True


def fail_on_protocol_condition(conditions: list[Program]) -> bool:
    """Filters conditions. Fails if a protocol condition is encountered. Otherwise, returns 1."""

    for cond in conditions:
        if cond.first().atom == ConditionOpcode.REMARK:
            if not is_valid_rmk_cond(cond.rest()):
                raise ValueError("Encountered a protocol REMARK condition")
        elif cond.first().atom in [ConditionOpcode.SEND_MESSAGE, ConditionOpcode.RECEIVE_MESSAGE]:
            if not is_valid_msg_cond(cond.rest()):
                raise ValueError("Encountered a protocol MESSAGE condition")
        elif cond.first().atom in [
            ConditionOpcode.CREATE_COIN_ANNOUNCEMENT,
            ConditionOpcode.CREATE_PUZZLE_ANNOUNCEMENT,
        ]:
            if not is_valid_ann_cond(cond.rest()):
                raise ValueError("Encountered protocol ANNOUNCEMENT condition")

    return True


def fail_on_protocol_condition_or_create_coin(conditions: list[Program]) -> bool:
    """Filters conditions. Fails if a protocol or create coin condition is encountered.
    Otherwise, returns 1.
    """

    for cond in conditions:
        if cond.first().atom == ConditionOpcode.CREATE_COIN:
            raise ValueError("Encountered a CREATE_COIN condition")
        elif cond.first().atom == ConditionOpcode.REMARK:
            if not is_valid_rmk_cond(cond.rest()):
                raise ValueError("Encountered a protocol REMARK condition")
        elif cond.first().atom in [ConditionOpcode.SEND_MESSAGE, ConditionOpcode.RECEIVE_MESSAGE]:
            if not is_valid_msg_cond(cond.rest()):
                raise ValueError("Encountered a protocol MESSAGE condition")
        elif cond.first().atom in [
            ConditionOpcode.CREATE_COIN_ANNOUNCEMENT,
            ConditionOpcode.CREATE_PUZZLE_ANNOUNCEMENT,
        ]:
            if not is_valid_ann_cond(cond.rest()):
                raise ValueError("Encountered protocol ANNOUNCEMENT condition")

    return True


def filter_and_extract_unique_create_coin(conditions: list[Program]) -> tuple[Program, list[Program]]:
    """Filters conditions and extracts unique create coin condition.

    Fails if protocol condition encountered
    Fails if not exactly one create coin encountered
    Returns body of create coin condition and all other conditions
    """

    found_create_coin: Program = Program.to(None)
    filtered_conditions: list[Program] = []

    for cond in conditions:
        if cond.first().atom == ConditionOpcode.CREATE_COIN:
            if not found_create_coin.nullp():
                raise ValueError("Encountered more than one CREATE_COIN condition")
            found_create_coin = cond.rest()  # return condition body
            continue
        elif cond.first().atom == ConditionOpcode.REMARK:
            if not is_valid_rmk_cond(cond.rest()):
                raise ValueError("Encountered invalid REMARK condition")
        elif cond.first().atom in [ConditionOpcode.SEND_MESSAGE, ConditionOpcode.RECEIVE_MESSAGE]:
            if not is_valid_msg_cond(cond.rest()):
                raise ValueError("Encountered invalid MESSAGE condition")
        elif cond.first().atom in [
            ConditionOpcode.CREATE_COIN_ANNOUNCEMENT,
            ConditionOpcode.CREATE_PUZZLE_ANNOUNCEMENT,
        ]:
            if not is_valid_ann_cond(cond.rest()):
                raise ValueError("Encountered invalid ANNOUNCEMENT condition")
        filtered_conditions.append(cond)

    if found_create_coin.nullp():
        raise ValueError("No CREATE_COIN condition encountered")

    return found_create_coin, filtered_conditions


def filter_and_extract_remark_solution(conditions: list[Program]) -> tuple[Program, list[Program]]:
    """Filters conditions and extracts first solution remark.

    Solution remark condition format: (REMARK SOLUTION_PREFIX inner_puzzle_hash)

    Fails if create coin or protocol condition encountered
    Fails if no solution remark condition encountered
    Returns:
      - third field from first solution remark condition encountered
      - all other conditions
    """

    found_solution: Program = Program.to(None)
    filtered_conditions: list[Program] = []

    for cond in conditions:
        if cond.first().atom == ConditionOpcode.CREATE_COIN:
            raise ValueError("Encountered CREATE_COIN condition")
        elif cond.first().atom == ConditionOpcode.REMARK:
            if not is_valid_rmk_cond(cond.rest()):
                raise ValueError(f"Encountered invalid REMARK condition: {cond}")
            if cond.rest().first().atom == SOLUTION_PREFIX:
                if not found_solution.nullp():
                    raise ValueError("Encountered more than one solution REMARK condition")
                found_solution = cond.rest().rest().first()
                continue
        elif cond.first().atom in [ConditionOpcode.SEND_MESSAGE, ConditionOpcode.RECEIVE_MESSAGE]:
            if not is_valid_msg_cond(cond.rest()):
                raise ValueError(f"Encountered invalid MESSAGE condition: {cond}")
        elif cond.first().atom in [
            ConditionOpcode.CREATE_COIN_ANNOUNCEMENT,
            ConditionOpcode.CREATE_PUZZLE_ANNOUNCEMENT,
        ]:
            if not is_valid_ann_cond(cond.rest()):
                raise ValueError(f"Encountered invalid ANNOUNCEMENT condition: {cond}")
        filtered_conditions.append(cond)

    if found_solution.nullp():
        raise ValueError("No solution REMARK condition encountered")

    return found_solution, filtered_conditions


def extract_solution_from_remark(conditions: Program) -> Program:
    """Extracts solution from solution REMARK condition."""
    found: Program | None = None
    for cond in conditions.as_iter():
        if cond.first().atom == ConditionOpcode.REMARK and cond.rest().first().atom == SOLUTION_PREFIX:
            if found is not None:
                raise SpendError(f"Multiple solution REMARK conditions found. Conditions: {conditions}")
            found = cond.at("rrf")
    if found is None:
        raise SpendError(f"No solution REMARK condition found. Conditions: {conditions}")
    return found
