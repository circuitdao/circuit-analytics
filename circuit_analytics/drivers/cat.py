from dataclasses import dataclass
import logging

from chia.types.blockchain_format.program import Program, uncurry
from chia.types.condition_opcodes import ConditionOpcode
from chia.util.hash import std_hash
from chia.wallet.util.curry_and_treehash import calculate_hash_of_quoted_mod_hash, curry_and_treehash

from chia_rs import Coin, CoinSpend
from chia_rs.sized_bytes import bytes32

from circuit_analytics.drivers.condition_filtering import fail_on_protocol_condition
from circuit_analytics.drivers.statutes import calculate_statutes_puzzle_hash
from circuit_analytics.errors import SpendError
from circuit_analytics.mods import (
    CAT_MOD,
    CAT_MOD_HASH,
)
from circuit_analytics.utils import (
    to_list,
    to_tuple,
    to_type,
    tree_hash_of_apply,
    tuple_to_struct,
)


log = logging.getLogger(__name__)


RING_MORPH_BYTE = b"\xcb"


@dataclass
class CrtTailSolutionInfo:
    # CAT Truths
    my_inner_puzzle_hash_cat_truth: bytes32  # RUN_TAIL_MOD_HASH
    cat_mod_hash_truth: bytes32
    cat_mod_hash_hash_truth: bytes32
    cat_tail_program_hash_truth: bytes32
    my_id_cat_truth: bytes32
    my_parent_cat_truth: bytes32
    my_full_puzzle_hash_cat_truth: bytes32
    my_amount_cat_truth: int
    # other limitation solution args
    parent_is_cat: bool
    lineage_proof: Program
    extra_delta: int
    final_output_conditions: list[Program]  # output conditions of inner puzzle prepended with two CAT layer conditions


@dataclass
class CrtTailStandardInfo(CrtTailSolutionInfo):
    # tail solution
    approval_mod_hash: bytes32
    approver_parent_id: bytes32
    approver_curried_mod_args: bytes32
    byc_tail_hash: bytes32
    approver_amount: int
    delta_amount: int  # amount being issued (if positive) or melted (if negative)
    approval_mod_hashes: list[bytes32, bytes32, bytes32, bytes32, bytes32]
    statutes_inner_puzzle_hash: bytes32
    target_puzzle_hash: bytes32 | None  # where this coin is going to if issuance, else None
    # calculated
    inner_puzzle_hash: bytes32
    approver_puzzle_hash: bytes32
    approver_coin_id: bytes32
    full_target_puzzle_hash: bytes32 | None


@dataclass
class CrtTailLaunchInfo(CrtTailSolutionInfo):
    # no tail solution
    delta_amount: int  # amount being issued on protocol launch


def get_statutes_puzzle_hash(statutes_struct: Program, statutes_inner_puzzle_hash: bytes32) -> tuple[bytes32, bytes32]:
    """Equivalent of get-statutes-puzzle-hash in crt_tail.clsp."""
    statutes_struct_hash = statutes_struct.get_tree_hash()
    statutes_puzzle_hash = calculate_statutes_puzzle_hash(statutes_struct, statutes_inner_puzzle_hash)
    return statutes_struct_hash, statutes_puzzle_hash


def destructure_truths(truths: Program) -> list:
    (
        truths_first,
        my_id_cat_truth,
        my_parent_cat_truth,
        my_full_puzzle_hash_cat_truth,
        my_amount_cat_truth,
    ) = to_list(truths, 5, [None, "bytes32", "bytes32", "bytes32", "uint"])
    (
        my_inner_puzzle_hash_cat_truth,
        cat_struct_truth,
    ) = to_tuple(truths_first, 2, ["bytes32", None])
    # cat_struct is a proper 3-element list (as constructed by on-chain CAT2 with `list`),
    # so use to_list (r^i+f access) not to_tuple (tail access for last element).
    (
        cat_mod_hash_truth,
        cat_mod_hash_hash_truth,
        cat_tail_program_hash_truth,
    ) = to_list(cat_struct_truth, 3, ["bytes32", "bytes32", "bytes32"])
    return [
        my_inner_puzzle_hash_cat_truth,
        cat_mod_hash_truth,
        cat_mod_hash_hash_truth,
        cat_tail_program_hash_truth,
        my_id_cat_truth,
        my_parent_cat_truth,
        my_full_puzzle_hash_cat_truth,
        my_amount_cat_truth,
    ]


def get_crt_tail_solution_info(limitations_solution: Program) -> CrtTailSolutionInfo:
    (
        truths,
        parent_is_cat,
        lineage_proof,
        extra_delta,
        final_output_conditions,  # output conditions of inner puzzle prepended with two CAT layer conditions
        tail_solution,  # extracted in CAT layer from magic condition
    ) = to_list(limitations_solution, 6, [None, "bool", None, "int", None, None])
    (
        my_inner_puzzle_hash_cat_truth,
        cat_mod_hash_truth,
        cat_mod_hash_hash_truth,
        cat_tail_program_hash_truth,
        my_id_cat_truth,
        my_parent_cat_truth,
        my_full_puzzle_hash_cat_truth,
        my_amount_cat_truth,
    ) = destructure_truths(truths)
    solution_info = CrtTailSolutionInfo(
        # CAT Truths
        my_inner_puzzle_hash_cat_truth=my_inner_puzzle_hash_cat_truth,
        cat_mod_hash_truth=cat_mod_hash_truth,
        cat_mod_hash_hash_truth=cat_mod_hash_hash_truth,
        cat_tail_program_hash_truth=cat_tail_program_hash_truth,
        my_id_cat_truth=my_id_cat_truth,
        my_parent_cat_truth=my_parent_cat_truth,
        my_full_puzzle_hash_cat_truth=my_full_puzzle_hash_cat_truth,
        my_amount_cat_truth=my_amount_cat_truth,
        # other limitation solution args
        parent_is_cat=parent_is_cat,
        lineage_proof=lineage_proof,
        extra_delta=extra_delta,
        final_output_conditions=final_output_conditions,
    )
    not_failed = fail_on_protocol_condition(list(final_output_conditions.as_iter()))
    if (not tail_solution.nullp()) and 0 > extra_delta and not_failed:
        inner_puzzle_hash = final_output_conditions.at("rrfrf")
        crt_tail_hash = cat_tail_program_hash_truth
        (
            approval_mod_hash,
            approver_parent_id,
            approver_curried_mod_args,
            byc_tail_hash,
            approver_amount,
            delta_amount,
            approval_mod_hashes,
            statutes_inner_puzzle_hash,
            target_puzzle_hash,  # where this coin is going to, if issuance
        ) = to_list(
            tail_solution,
            9,
            ["bytes32", "bytes32", "bytes32", "bytes32_or_nil", "int", "int", None, "bytes32", "bytes32_or_none"],
        )
        (
            collateral_vault_mod_hash,
            surplus_auction_mod_hash,
            recharge_auction_mod_hash,
            savings_vault_mod_hash,
            announcer_registry_mod_hash,
        ) = to_list(approval_mod_hashes, 5, ["bytes32", "bytes32", "bytes32", "bytes32", "bytes32"])
        if not byc_tail_hash.nullp():
            if approval_mod_hash != recharge_auction_mod_hash:
                raise SpendError(
                    f"The only BYC approval mod allowed to run CRT tail is recharge auction ({approval_mod_hash} == {recharge_auction_mod_hash})"
                )
            if extra_delta != -delta_amount:
                raise SpendError(
                    f"When recharge auction is approval mod for running CRT tail, extra delta must equal -delta from tail solution ({extra_delta} = {-delta_amount})"
                )
            approver_inner_puzzle_hash = tree_hash_of_apply(approval_mod_hash, approver_curried_mod_args)
            approver_puzzle_hash = curry_and_treehash(
                calculate_hash_of_quoted_mod_hash(cat_mod_hash_truth),
                cat_mod_hash_hash_truth,
                Program.to(byc_tail_hash).get_tree_hash(),
                approver_inner_puzzle_hash,
            )
        elif approval_mod_hash == surplus_auction_mod_hash:
            if extra_delta != delta_amount:
                raise SpendError(
                    f"When surplus auction is approval mod for running CRT tail, extra delta must equal delta from tail solution ({extra_delta} = {delta_amount})"
                )
            if not parent_is_cat:
                raise SpendError(
                    f"When surplus auction is approval mod for running CRT tail, parent of CRT coin must be a CAT"
                )
            approver_inner_puzzle_hash = tree_hash_of_apply(approval_mod_hash, approver_curried_mod_args)
            approver_puzzle_hash = curry_and_treehash(
                calculate_hash_of_quoted_mod_hash(cat_mod_hash_truth),
                cat_mod_hash_hash_truth,
                Program.to(crt_tail_hash).get_tree_hash(),
                approver_inner_puzzle_hash,
            )
        else:
            if approval_mod_hash != announcer_registry_mod_hash:
                raise SpendError(
                    f"Approval mod for running CRT tail must be announcer registry if it's not recharge or surplus auction ({approval_mod_hash} != {announcer_registry_mod_hash})"
                )
            if extra_delta != -delta_amount:
                raise SpendError(
                    f"When registry is approval mod for running CRT tail, extra delta must equal -delta from tail solution ({extra_delta} = {-delta_amount})"
                )
            approver_puzzle_hash = tree_hash_of_apply(approval_mod_hash, approver_curried_mod_args)
        approver_coin_id = bytes32(Coin(approver_parent_id, approver_puzzle_hash, approver_amount).name())
        if target_puzzle_hash:
            curried_target_puzzle_hash = curry_and_treehash(
                calculate_hash_of_quoted_mod_hash(cat_mod_hash_truth),
                cat_mod_hash_hash_truth,
                Program.to(crt_tail_hash).get_tree_hash(),
                target_puzzle_hash,
            )
        else:
            curried_target_puzzle_hash = None
        return CrtTailStandardInfo(
            **solution_info.__dict__,
            # tail solution
            approval_mod_hash=approval_mod_hash,
            approver_parent_id=approver_parent_id,
            approver_curried_mod_args=approver_curried_mod_args,
            byc_tail_hash=byc_tail_hash,
            approver_amount=approver_amount,
            delta_amount=delta_amount,
            approval_mod_hashes=approval_mod_hashes,
            statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
            target_puzzle_hash=target_puzzle_hash,
            # calculated
            inner_puzzle_hash=inner_puzzle_hash,
            approver_puzzle_hash=approver_puzzle_hash,
            approver_coin_id=approver_coin_id,
            full_target_puzzle_hash=curried_target_puzzle_hash,
        )
    else:
        # launch
        if extra_delta != 0:
            raise SpendError(
                f"Issuing CRT at protocol deployment is only possible if extra delta is zero ({extra_delta} = 0)"
            )
        return CrtTailLaunchInfo(
            **solution_info.__dict__,
            delta_amount=my_amount_cat_truth,
        )


@dataclass
class BycTailSolutionInfo:
    # CAT Truths
    my_inner_puzzle_hash_cat_truth: bytes32  # RUN_TAIL_MOD_HASH
    cat_mod_hash_truth: bytes32
    cat_mod_hash_hash_truth: bytes32
    cat_tail_program_hash_truth: bytes32
    my_id_cat_truth: bytes32
    my_parent_cat_truth: bytes32
    my_full_puzzle_hash_cat_truth: bytes32
    my_amount_cat_truth: int
    # other limitation solution args
    parent_is_cat: bool
    lineage_proof: Program
    extra_delta: int
    final_output_conditions: list[Program]  # output conditions of inner puzzle prepended with two CAT layer conditions
    inner_puzzle_hash: (
        bytes32  # hash of inner puzzle passed into RUN TAIL puzzle. extracted by tail from final_output_conditions
    )
    # tail solution
    vault_parent_id: bytes32
    vault_mod_hash: bytes32
    vault_curried_args_hash: bytes32
    vault_amount: int
    statutes_inner_puzzle_hash: bytes32
    approval_mod_hashes: Program
    current_coin_amount: int

    @property
    def truths(self) -> Program:
        return tuple_to_struct(
            (
                tuple_to_struct(
                    (
                        self.my_inner_puzzle_hash_cat_truth,
                        self.cat_mod_hash_truth,
                        self.cat_mod_hash_hash_truth,
                        self.cat_tail_program_hash_truth,
                    )
                ),
                Program.to(
                    [
                        self.my_id_cat_truth,
                        self.my_parent_cat_truth,
                        self.my_full_puzzle_hash_cat_truth,
                        self.my_amount_cat_truth,
                    ]
                ),
            )
        )

    @property
    def vault_coin_id(self) -> bytes32:
        vault_puzzle_hash = tree_hash_of_apply(self.vault_mod_hash, self.vault_curried_args_hash)
        vault_coin_id = Coin(
            self.vault_parent_id,
            vault_puzzle_hash,
            self.vault_amount,
        ).name()
        return vault_coin_id


def get_byc_tail_solution_info(limitations_solution: Program) -> BycTailSolutionInfo:
    (
        truths,
        parent_is_cat,
        lineage_proof,
        extra_delta,
        final_output_conditions,  # output conditions of inner puzzle prepended with two CAT layer conditions
        tail_solution,  # extracted in CAT layer from magic condition
    ) = to_list(limitations_solution, 6, [None, "bool", None, "int", None, None])
    (
        truths_first,
        my_id_cat_truth,
        my_parent_cat_truth,
        my_full_puzzle_hash_cat_truth,
        my_amount_cat_truth,
    ) = to_list(truths, 5, [None, "bytes32", "bytes32", "bytes32", "uint"])
    (
        my_inner_puzzle_hash_cat_truth,
        cat_struct_truth,
    ) = to_tuple(truths_first, 2, ["bytes32", None])
    (
        cat_mod_hash_truth,
        cat_mod_hash_hash_truth,
        cat_tail_program_hash_truth,
    ) = to_tuple(cat_struct_truth, 3, ["bytes32", "bytes32", "bytes32"])
    (
        vault_parent_id,
        vault_mod_hash,
        vault_curried_args_hash,
        vault_amount,
        statutes_inner_puzzle_hash,
        approval_mod_hashes,
        current_coin_amount,
    ) = to_list(
        tail_solution,
        7,
        ["bytes32", "bytes32", "bytes32", "uint", "bytes32", None, "uint"],
    )
    (
        collateral_vault_mod_hash,
        surplus_auction_mod_hash,
        recharge_auction_mod_hash,
        savings_vault_mod_hash,
        announcer_registry_mod_hash,
    ) = to_list(approval_mod_hashes, 5, ["bytes32", "bytes32", "bytes32", "bytes32", "bytes32"])
    if extra_delta > 0:
        raise SpendError(f"BYC tail limitations solution requires non-positive extra delta, got {extra_delta}")
    inner_puzzle_hash = to_type(
        final_output_conditions.at("rrfrf"),
        "bytes32",
        "inner puzzle hash from inner puzzle (most likely RUN_TAIL)",
    )
    fail_on_protocol_condition(list(final_output_conditions.as_iter()))
    if not collateral_vault_mod_hash == vault_mod_hash:
        raise SpendError(
            f"BYC tail limitations solution has non-matching collateral vault mod hashes "
            f"({collateral_vault_mod_hash.hex()} != {vault_mod_hash.hex()})"
        )
    if not (
        all([parent_is_cat, extra_delta < 0, extra_delta + current_coin_amount == 0])
        or all([not parent_is_cat, extra_delta == 0])
    ):
        if not all([parent_is_cat, extra_delta < 0, extra_delta + current_coin_amount == 0]):
            raise SpendError(
                f"Can only melt BYC if parent is a CAT ({parent_is_cat}) "
                f"and coin amount = -extra delta ({current_coin_amount} = -{extra_delta})"
            )
        raise SpendError(
            f"Can only issue BYC if parent is not a CAT ({not parent_is_cat}) and extra delta is 0 ({extra_delta} = 0)"
        )

    return BycTailSolutionInfo(
        # CAT Truths
        my_inner_puzzle_hash_cat_truth=my_inner_puzzle_hash_cat_truth,
        cat_mod_hash_truth=cat_mod_hash_truth,
        cat_mod_hash_hash_truth=cat_mod_hash_hash_truth,
        cat_tail_program_hash_truth=cat_tail_program_hash_truth,
        my_id_cat_truth=my_id_cat_truth,
        my_parent_cat_truth=my_parent_cat_truth,
        my_full_puzzle_hash_cat_truth=my_full_puzzle_hash_cat_truth,
        my_amount_cat_truth=my_amount_cat_truth,
        # other limitation solution args
        parent_is_cat=parent_is_cat,
        lineage_proof=lineage_proof,
        extra_delta=extra_delta,
        final_output_conditions=list(final_output_conditions.as_iter()),
        inner_puzzle_hash=inner_puzzle_hash,
        # tail solution
        vault_parent_id=vault_parent_id,
        vault_mod_hash=vault_mod_hash,
        vault_curried_args_hash=vault_curried_args_hash,
        vault_amount=vault_amount,
        statutes_inner_puzzle_hash=statutes_inner_puzzle_hash,
        approval_mod_hashes=approval_mod_hashes,
        current_coin_amount=current_coin_amount,
    )


@dataclass
class CatSolutionInfo:
    inner_puzzle_solution: Program
    lineage_proof: Program  # (parent_parent_id parent_inner_puzzle_hash parent_amount) or nil
    prev_coin_id: bytes32
    this_coin_info: Program  # (parent_id puzzle_hash amount)
    next_coin_proof: Program  # (parent_id inner_puzzle_hash amount)
    prev_subtotal: int
    extra_delta: int

    @property
    def this_coin_parent_id(self) -> bytes32:
        return self.this_coin_info.first()

    @property
    def this_coin_puzzle_hash(self) -> bytes32:
        return self.this_coin_info.rest().first()

    @property
    def this_coin_amount(self) -> int:
        return self.this_coin_info.rest().rest().first()

    @property
    def next_coin_parent_id(self) -> bytes32:
        return self.next_coin_proof.first()

    @property
    def next_coin_inner_puzzle_hash(self) -> bytes32:
        return self.this_coin_proof.rest().first()

    @property
    def next_coin_amount(self) -> int:
        return self.next_coin_proof.rest().rest().first()


@dataclass
class CatInnerPuzzleInfo(CatSolutionInfo):
    inner_puzzle_output_conditions: list[Program]  # final_output_conditions in puzzle

    @property
    def output_conditions(self) -> list[Program]:
        """Conditions ultimately output by the CAT spend."""
        return self.inner_puzzle_output_conditions


@dataclass
class CatTailRevealInfo(CatSolutionInfo):
    inner_puzzle_output_conditions: list[Program]  # final_output_conditions in puzzle
    tail_reveal: Program
    limitations_solution_info: BycTailSolutionInfo | CrtTailSolutionInfo | Program
    tail_output_conditions: list[Program]

    @property
    def tail_solution(self) -> Program:
        return self.limitations_solution_info.tail_solution

    @property
    def output_conditions(self) -> list[Program]:
        """Conditions ultimately output by the CAT spend."""
        return self.tail_output_conditions + self.inner_puzzle_output_conditions


def morph_condition(condition: Program, mod_hash: bytes32, tail_program_hash: bytes32) -> Program:
    """Wrap CREATE_COIN and CREATE_COIN_ANNOUNCEMENT conditions in CAT layer.

    Note that the wrapping of CREATE_COINs is done independently of the output amount,
    i.e. even negative values are accepted (incl. magic value -113). It's the
    responsibility of the calling function to check for this.
    """
    if condition.first().atom == ConditionOpcode.CREATE_COIN:
        inner_puzzle_hash = to_type(condition.at("rf"), "bytes32", "CREATE_COIN puzhash to be morphed")
        wrapped_puzzle_hash = CAT_MOD.curry(mod_hash, tail_program_hash, inner_puzzle_hash).get_tree_hash_precalc(
            inner_puzzle_hash
        )
        morphed_condition = tuple_to_struct((condition.first(), wrapped_puzzle_hash, condition.rest().rest()))
        return morphed_condition
    elif condition.first().atom == ConditionOpcode.CREATE_COIN_ANNOUNCEMENT:
        msg = condition.rest().first().atom
        if len(msg) == 33 and msg[:1] == RING_MORPH_BYTE:  # [:1] instead of [0] as slicing returns bytes, indexing int
            raise SpendError(
                f"CAT spend failed due to CREATE_COIN_ANNOUNCEMENT inner condition with message "
                f"of byte length 33 starting with ring morph byte ({msg.hex()})"
            )

    return condition


def get_cat_solution_info(
    coin_spend: CoinSpend, byc_tail_hash: bytes32 = None, crt_tail_hash: bytes32 = None
) -> CatSolutionInfo:
    if byc_tail_hash is None or crt_tail_hash is None:
        from circuit_analytics.config import BYC_TAIL_HASH, CRT_TAIL_HASH

        if byc_tail_hash is None:
            byc_tail_hash = BYC_TAIL_HASH
        if crt_tail_hash is None:
            crt_tail_hash = CRT_TAIL_HASH

    mod, curried_args = uncurry(coin_spend.puzzle_reveal)
    (
        mod_hash,
        tail_program_hash,
        inner_puzzle,
    ) = to_list(curried_args, 3, ["bytes32", "bytes32", None])
    assert mod.get_tree_hash() == mod_hash
    assert mod_hash == CAT_MOD_HASH
    inner_puzzle_hash = inner_puzzle.get_tree_hash()
    solution = Program.from_serialized(coin_spend.solution)
    (
        inner_puzzle_solution,
        lineage_proof,  # (parent_parent_id parent_inner_puzzle_hash parent_amount) or nil
        prev_coin_id,
        this_coin_info,  # (parent_id puzzle_hash amount)
        next_coin_proof,  # (parent_id inner_puzzle_hash amount)
        prev_subtotal,
        extra_delta,
    ) = to_list(
        solution,
        7,
        [
            None,
            None,
            "bytes32",
            None,
            None,
            "int",
            "int",
        ],
    )
    (
        this_coin_parent_id,
        this_coin_puzzle_hash,
        this_coin_amount,
    ) = to_list(this_coin_info, 3, ["bytes32", "bytes32", "uint"])
    (
        next_coin_parent_id,
        next_coin_inner_puzzle_hash,
        next_coin_amount,
    ) = to_list(next_coin_proof, 3, ["bytes32", "bytes32", "uint"])
    solution_info = CatSolutionInfo(
        inner_puzzle_solution=inner_puzzle_solution,
        lineage_proof=lineage_proof,
        prev_coin_id=prev_coin_id,
        this_coin_info=this_coin_info,
        next_coin_proof=next_coin_proof,
        prev_subtotal=prev_subtotal,
        extra_delta=extra_delta,
    )
    inner_conditions = inner_puzzle.run(inner_puzzle_solution)
    my_id = bytes32(Coin(this_coin_parent_id, this_coin_puzzle_hash, this_coin_amount).name())
    next_coin_puzzle_hash = CAT_MOD.curry(
        mod_hash, tail_program_hash, next_coin_inner_puzzle_hash
    ).get_tree_hash_precalc(next_coin_inner_puzzle_hash)
    next_coin_id = bytes32(Coin(next_coin_parent_id, next_coin_puzzle_hash, next_coin_amount).name())
    # check whether or not parent coin is a CAT
    if lineage_proof.nullp():
        parent_is_cat = False
    else:
        (
            parent_parent_id,
            parent_inner_puzzle_hash,
            parent_amount,
        ) = to_list(lineage_proof, 3, ["bytes32", "bytes32", "uint"])
        parent_puzzle_hash = CAT_MOD.curry(
            mod_hash,
            tail_program_hash,
            parent_inner_puzzle_hash,
        ).get_tree_hash_precalc(parent_inner_puzzle_hash)
        parent_coin_id = bytes32(Coin(parent_parent_id, parent_puzzle_hash, parent_amount).name())
        if parent_coin_id == this_coin_parent_id:
            parent_is_cat = True
        else:
            parent_is_cat = False

    # find and strip tail info
    tail_reveal_and_solution = Program.to(None)
    output_values = []
    morphed_conditions = []
    for condition in inner_conditions.as_iter():
        if condition.first().atom == ConditionOpcode.CREATE_COIN:
            output_value = condition.at("rrf").as_int()
            if output_value == -113:
                # magic condition. extract tail reveal and solution
                # if there's more than one magic condition, last one takes precedence
                tail_reveal_and_solution = tuple_to_struct((condition.at("rrrf"), condition.at("rrrrf")))
            else:
                morphed_condition = morph_condition(condition, mod_hash, tail_program_hash)
                morphed_conditions.append(morphed_condition)
                output_values.append(output_value)
        else:
            morphed_condition = morph_condition(condition, mod_hash, tail_program_hash)
            morphed_conditions.append(morphed_condition)

    output_sum = sum(output_values)

    # generate final output conditions
    announcement_condition = Program.to(
        [
            ConditionOpcode.CREATE_COIN_ANNOUNCEMENT,
            RING_MORPH_BYTE + Program.to([prev_coin_id, prev_subtotal]).get_tree_hash(),
        ]
    )
    this_subtotal = prev_subtotal + (this_coin_amount - output_sum) + extra_delta
    assert_next_announcement_condition = Program.to(
        [
            ConditionOpcode.ASSERT_COIN_ANNOUNCEMENT,
            std_hash(next_coin_id + RING_MORPH_BYTE + Program.to([my_id, this_subtotal]).get_tree_hash()),
        ]
    )
    final_output_conditions = [
        announcement_condition,
        assert_next_announcement_condition,
    ] + morphed_conditions

    # check lineage or run tail program
    if not tail_reveal_and_solution.nullp():
        tail_reveal = tail_reveal_and_solution.first()
        tail_solution = tail_reveal_and_solution.rest()
        if tail_reveal.get_tree_hash() != tail_program_hash:
            raise SpendError(
                f"CAT spend failed. Tail reveal does not match curried tail hash "
                f"({tail_reveal.get_tree_hash().hex()} != {tail_program_hash.hex()})."
            )
        # The on-chain CAT2 constructs cat_mod_struct as (list MOD_HASH (sha256 ONE MOD_HASH) TAIL_PROGRAM_HASH),
        # a proper list. Using tuple_to_struct would produce an improper cons chain where the last element
        # is an atom, making cat_tail_program_hash_truth (which calls (f (r (r cat_struct)))) fail.
        truths = tuple_to_struct(
            (
                Program.to([inner_puzzle_hash, mod_hash, std_hash(b"\x01" + mod_hash), tail_program_hash]),
                tuple_to_struct(
                    (
                        my_id,
                        this_coin_info,
                    )
                ),
            )
        )
        limitations_solution = Program.to(
            [
                truths,
                1 if parent_is_cat else None,
                lineage_proof,
                extra_delta,
                final_output_conditions,
                tail_solution,
            ]
        )
        if tail_program_hash == BYC_TAIL_HASH:
            limitations_solution_info = get_byc_tail_solution_info(limitations_solution)
        elif tail_program_hash == CRT_TAIL_HASH:
            limitations_solution_info = get_crt_tail_solution_info(limitations_solution)
        else:  # Unknown CAT
            limitations_solution_info = limitations_solution
        tail_output_conditions = tail_reveal.run(limitations_solution)
        return CatTailRevealInfo(
            **solution_info.__dict__,
            inner_puzzle_output_conditions=final_output_conditions,
            tail_reveal=tail_reveal,
            limitations_solution_info=limitations_solution_info,
            tail_output_conditions=list(tail_output_conditions.as_iter()),
        )
    else:
        if not parent_is_cat:
            raise SpendError("CAT spend failed. Tail not revealed and parent is not a CAT")
        if extra_delta:
            raise SpendError(f"CAT spend failed. Tail not revealed but extra delta is non-zero ({extra_delta})")
        return CatInnerPuzzleInfo(
            **solution_info.__dict__,
            inner_puzzle_output_conditions=final_output_conditions,
        )


@dataclass
class LaunchGovernanceSolutionInfo:
    inner_puzzle_hash: bytes32
    parent_parent_id: bytes32
    amount: int


def get_launch_governance_solution_info(
    coin: Coin, inner_puzzle: Program, inner_solution: Program
) -> LaunchGovernanceSolutionInfo:
    (
        inner_puzzle_hash,
        parent_parent_id,
        amount,
    ) = to_list(inner_solution, 3, ["bytes32", "bytes32", "uint64"])
    return LaunchGovernanceSolutionInfo(
        inner_puzzle_hash=inner_puzzle_hash,
        parent_parent_id=parent_parent_id,
        amount=amount,
    )


@dataclass
class ExitGovernanceSolutionInfo:
    parent_id: bytes32


def get_exit_governance_solution_info(
    coin: Coin, inner_puzzle: Program, inner_solution: Program
) -> ExitGovernanceSolutionInfo:
    (parent_id,) = to_list(inner_solution, 1, ["bytes32"])
    return ExitGovernanceSolutionInfo(parent_id=parent_id)


@dataclass
class RunTailSolutionInfo:
    tail_reveal: Program
    limitations_solution: Program
    inner_puzzle: Program
    inner_solution: Program
    output_conditions: list[Program]


def get_run_tail_solution_info(solution: Program) -> RunTailSolutionInfo:
    (
        tail_reveal,
        limitations_solution,
        inner_puzzle,
        inner_solution,
    ) = to_list(solution, 4)
    if inner_puzzle.nullp():
        raise SpendError(f"Run Tail solution must provide non-nil inner puzzle")
    try:
        output_conditions = list(inner_puzzle.run(inner_solution).as_iter())
    except Exception:
        raise SpendError(
            f"Run Tail inner puzzle fails to run. Inner puzzle: {inner_puzzle}. Inner solution: {inner_solution}"
        )
    return RunTailSolutionInfo(
        tail_reveal=tail_reveal,
        limitations_solution=limitations_solution,
        inner_puzzle=inner_puzzle,
        inner_solution=inner_solution,
        output_conditions=output_conditions,
    )


@dataclass
class SettlementPaymentInfo:
    puzzle_hash: bytes32
    amount: int
    memos: Program


@dataclass
class SettlementNotarizedPaymentInfo:
    nonce: bytes
    payments: list[SettlementPaymentInfo]


@dataclass
class SettlementSolutionInfo:
    notarized_payments: list[SettlementNotarizedPaymentInfo]


def get_settlement_solution_info(solution: Program) -> SettlementSolutionInfo:
    """Takes settlement solution and returns info.

    If settlement coin is a CAT, solution must be CAT layer inner solution.
    """
    notarized_payment_infos = []
    for notarized_payment in solution.as_iter():
        (
            nonce,
            payments,
        ) = to_tuple(notarized_payment, 2, ["bytes32", None])
        payment_infos = []
        for payment in payments.as_iter():
            (
                puzzle_hash,
                amount,
                memos,
            ) = to_list(payment, 3, ["bytes32", "int", None])
            if not amount > 0:
                raise SpendError(
                    f"Settlement payment must have positive amount. Failed payment: nonce={nonce.hex()} puzzle_hash={puzzle_hash.hex()} {amount=} {memos=}"
                )
            payment_infos.append(
                SettlementPaymentInfo(
                    puzzle_hash=puzzle_hash,
                    amount=amount,
                    memos=memos,
                )
            )
        notarized_payment_infos.append(SettlementNotarizedPaymentInfo(nonce=nonce, payments=payment_infos))
    return SettlementSolutionInfo(notarized_payments=notarized_payment_infos)
