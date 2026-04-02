from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Union

from chia.types.blockchain_format.program import Program
from chia_rs import CoinSpend

from circuit_analytics.scanner.models import (
    AnnouncerCoin,
    AuctionCoin,
    GoverningCRT,
    SavingsVaultCoin,
    TreasuryCoin,
    VaultCoin,
)


@dataclass
class StatsDelta:
    vault_operations_count: int = 0
    vault_count_incr: int = 0
    vault_count_decr: int = 0
    collateral_deposited: int = 0
    collateral_withdrawn: int = 0
    collateral_sold: int = 0
    byc_borrowed: int = 0
    byc_repaid: int = 0
    sf_repaid: int = 0
    sf_transferred: int = 0
    discounted_principal_delta: int = 0
    liquidation_start_count: int = 0
    liquidation_restart_count: int = 0
    liquidation_ended_count: int = 0
    lp_incurred: int = 0
    ii_incurred: int = 0
    ii_paid: int = 0
    fees_incurred: int = 0
    fees_paid: int = 0
    principal_incurred: int = 0
    principal_paid: int = 0
    bad_debt_count_incr: int = 0
    bad_debt_count_decr: int = 0
    bad_debt_ii_incurred: int = 0
    bad_debt_ii_recovered: int = 0
    bad_debt_fees_incurred: int = 0
    bad_debt_fees_recovered: int = 0
    bad_debt_principal_incurred: int = 0
    bad_debt_principal_recovered: int = 0
    savings_vault_operations_count: int = 0
    savings_vault_count_incr: int = 0
    savings_vault_count_decr: int = 0
    discounted_savings_balance_delta: int = 0
    byc_deposited: int = 0
    byc_withdrawn: int = 0
    interest_paid: int = 0
    announcer_operations_count: int = 0
    approved_announcer_count_delta: int = 0
    treasury_coin_count_delta: int = 0
    treasury_balance_delta: int = 0
    recharge_operations_count: int = 0
    recharge_auction_coin_count_delta: int = 0
    recharge_auction_count_delta: int = 0
    surplus_operations_count: int = 0
    surplus_auction_count_delta: int = 0
    governance_operations_count: int = 0
    governance_coin_count_delta: int = 0
    governance_circulation_delta: int = 0
    crt_circulation_delta: int = 0
    registry_operations_count: int = 0
    registered_announcer_count_delta: int = 0
    statutes_spend_found: bool = False
    statutes_price: Optional[int] = None
    last_updated: Optional[int] = None
    cumulative_stability_fee_df: Optional[int] = None
    cumulative_interest_rate_df: Optional[int] = None
    current_stability_fee_df: Optional[int] = None
    current_interest_rate_df: Optional[int] = None

    def __add__(self, other: "StatsDelta") -> "StatsDelta":
        if not isinstance(other, StatsDelta):
            return NotImplemented
        result = StatsDelta()
        for field_name, field_val in self.__dict__.items():
            if (
                isinstance(field_val, int)
                and not isinstance(field_val, bool)
                and field_name not in [
                    "statutes_price", "last_updated",
                    "cumulative_stability_fee_df", "cumulative_interest_rate_df",
                    "current_stability_fee_df", "current_interest_rate_df",
                ]
            ):
                setattr(result, field_name, field_val + getattr(other, field_name))
        result.statutes_spend_found = self.statutes_spend_found or other.statutes_spend_found
        result.statutes_price = other.statutes_price if other.statutes_price is not None else self.statutes_price
        result.last_updated = other.last_updated if other.last_updated is not None else self.last_updated
        result.cumulative_stability_fee_df = (
            other.cumulative_stability_fee_df if other.cumulative_stability_fee_df is not None
            else self.cumulative_stability_fee_df
        )
        result.cumulative_interest_rate_df = (
            other.cumulative_interest_rate_df if other.cumulative_interest_rate_df is not None
            else self.cumulative_interest_rate_df
        )
        result.current_stability_fee_df = (
            other.current_stability_fee_df if other.current_stability_fee_df is not None
            else self.current_stability_fee_df
        )
        result.current_interest_rate_df = (
            other.current_interest_rate_df if other.current_interest_rate_df is not None
            else self.current_interest_rate_df
        )
        return result


@dataclass
class HandlerResult:
    coins_to_add: List[Union[VaultCoin, AnnouncerCoin, SavingsVaultCoin, TreasuryCoin, AuctionCoin, GoverningCRT]] = field(default_factory=list)
    coins_to_remove: Set[str] = field(default_factory=set)
    stats_delta: StatsDelta = field(default_factory=StatsDelta)
    last_statutes_info: Any = None


class SpendHandler(ABC):
    @abstractmethod
    def handle(
        self, coin_spend: CoinSpend, block_record: Dict[str, Any], statutes_struct: Program
    ) -> Optional[HandlerResult]:
        pass
