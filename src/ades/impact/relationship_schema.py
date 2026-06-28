"""Relationship metadata schema for market impact graph edges."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

DXY_EVENT_TYPES = (
    "policy_rate_cut",
    "policy_rate_hike",
    "policy_rate_hold",
    "inflation_shock",
    "future_policy_expectation",
    "fiscal_expansion",
    "fiscal_austerity",
    "sanctions",
    "war_escalation",
    "ceasefire_risk_relief",
)
COUNTRY_RISK_EVENT_TYPES = (
    "sanctions",
    "tariff",
    "export_control",
    "sector_policy_change",
    "regulatory_enforcement",
    "fiscal_expansion",
    "fiscal_austerity",
    "default",
    "war_escalation",
    "ceasefire_risk_relief",
)
GLOBAL_EQUITY_EVENT_TYPES = (
    "policy_rate_cut",
    "policy_rate_hike",
    "policy_rate_hold",
    "inflation_shock",
    "future_policy_expectation",
    "fiscal_expansion",
    "fiscal_austerity",
    "sanctions",
    "tariff",
    "export_control",
    "sector_policy_change",
    "regulatory_enforcement",
    "earnings_beat",
    "earnings_miss",
    "guidance_raise",
    "guidance_cut",
    "acquisition",
    "merger",
    "divestiture",
    "default",
    "bankruptcy",
    "strike_labor_disruption",
    "war_escalation",
    "ceasefire_risk_relief",
)
POLICY_RATE_EVENT_TYPES = (
    "policy_rate_cut",
    "policy_rate_hike",
    "policy_rate_hold",
    "inflation_shock",
    "future_policy_expectation",
    "fiscal_expansion",
    "fiscal_austerity",
)
CURRENCY_PROXY_EVENT_TYPES = tuple(
    dict.fromkeys(
        (
            *POLICY_RATE_EVENT_TYPES,
            "sanctions",
            "tariff",
            "export_control",
            "war_escalation",
            "ceasefire_risk_relief",
        )
    )
)
EQUITY_EVENT_TYPES = (
    "tariff",
    "export_control",
    "sector_policy_change",
    "regulatory_enforcement",
    "sanctions",
    "earnings_beat",
    "earnings_miss",
    "guidance_raise",
    "guidance_cut",
    "acquisition",
    "merger",
    "divestiture",
    "key_person_ownership_governance",
    "default",
    "bankruptcy",
    "strike_labor_disruption",
)
SUPPLY_CHAIN_EVENT_TYPES = (
    "supply_disruption",
    "production_decrease",
    "production_increase",
    "shipping_chokepoint_disruption",
    "tariff",
    "export_control",
    "sanctions",
    "strike_labor_disruption",
)
POLICY_SECTOR_EVENT_TYPES = (
    "sector_policy_change",
    "regulatory_enforcement",
    "fiscal_expansion",
    "fiscal_austerity",
    "tariff",
    "export_control",
    "sanctions",
)
COMMODITY_EXPOSURE_EVENT_TYPES = (
    "commodity_price_move",
    "safe_haven_commodity_move",
    "currency_market_move",
    "inflation_shock",
    "supply_disruption",
    "production_decrease",
    "production_increase",
    "shipping_chokepoint_disruption",
    "strike_labor_disruption",
    "sanctions",
    "tariff",
    "export_control",
    "sector_policy_change",
    "war_escalation",
    "ceasefire_risk_relief",
)
COUNTRY_REGION_EXPOSURE_EVENT_TYPES = tuple(
    dict.fromkeys(
        (
            *COUNTRY_RISK_EVENT_TYPES,
            *POLICY_SECTOR_EVENT_TYPES,
            "supply_disruption",
            "production_decrease",
            "production_increase",
            "shipping_chokepoint_disruption",
        )
    )
)

PERSON_TO_ISSUER_RELATIONS = {
    "person_is_ceo_of_issuer",
    "person_is_founder_of_issuer",
    "person_is_chair_of_issuer",
    "person_is_board_member_of_issuer",
    "person_is_major_shareholder_of_issuer",
    "person_controls_issuer",
    "person_affects_employer_issuer",
}
ORGANIZATION_CONTROL_RELATIONS = {
    "private_company_controls_public_issuer",
    "holding_company_controls_public_issuer",
}
SUPPLY_CHAIN_RELATIONS = {
    "issuer_supplier_to_issuer",
    "issuer_customer_of_issuer",
    "issuer_partner_of_issuer",
}
IDENTITY_LISTING_RELATIONS = {
    "issuer_has_listed_ticker",
    "ticker_represents_issuer",
    "issuer_has_exchange_listing",
    "ticker_trades_on_exchange",
    "ticker_listed_on_exchange",
}
SECTOR_EXPOSURE_RELATIONS = {
    "issuer_in_sector",
    "sector_affects_index",
}
POLICY_SECTOR_RELATIONS = {
    "government_body_affects_sector",
    "regulator_affects_sector",
    "policy_body_affects_sector",
    "law_affects_sector",
    "sector_affects_issuer",
    "issuer_exposed_to_policy_sector",
}
ISSUER_EXPOSURE_RELATIONS = {
    "issuer_exposed_to_commodity",
    "commodity_affects_issuer_revenue",
    "commodity_affects_issuer_margin",
    "issuer_exposed_to_country",
    "issuer_exposed_to_region",
    "issuer_exposed_to_supply_chain",
}
GEOGRAPHY_COMMODITY_RELATIONS = {
    "location_affects_commodity_route",
    "asset_production_region_affects_commodity",
    "shipping_chokepoint_affects_commodity",
    "chokepoint_affects_commodity",
    "chokepoint_affects_sector",
    "commodity_region_affects_commodity",
    "country_exports_commodity",
    "country_produces_commodity",
    "producer_group_affects_commodity",
}
COMMODITY_SECTOR_RELATIONS = {
    "agriculture_commodity_affects_sector",
    "battery_metal_affects_sector",
    "energy_input_affects_sector",
    "food_commodity_affects_inflation",
    "industrial_metal_affects_sector",
    "pollution_affects_agriculture_commodity",
    "pollution_affects_livestock_commodity",
    "strategic_metal_affects_sector",
}
COMMODITY_MARKET_RELATIONS = {
    "commodity_traded_in_currency",
    *COMMODITY_SECTOR_RELATIONS,
}
SANCTIONS_RELATIONS = {
    "sanction_affects_commodity",
    "sanction_affects_country",
    "sanctions_body_affects_risk_proxy",
    "tanker_sanctions_affects_commodity",
}
CENTRAL_BANK_RELATIONS = {
    "central_bank_affects_currency",
    "central_bank_affects_rates",
}
COUNTRY_MEMBERSHIP_RELATIONS = {
    "country_member_of_bloc",
}
CRYPTO_POLICY_RELATIONS = {
    "digital_asset_policy_affects_crypto",
}


@dataclass(frozen=True)
class RelationDefinition:
    relation: str
    family: str
    compatible_event_types: tuple[str, ...] = ()
    direction_preconditions: tuple[str, ...] = ()


def relation_family_for_relation(relation: str) -> str:
    """Return the canonical manifest family for a relation."""

    if relation in IDENTITY_LISTING_RELATIONS:
        return "identity_listing"
    if relation in PERSON_TO_ISSUER_RELATIONS or relation == "person_affects_employer_ticker":
        return "person_to_issuer"
    if relation.startswith("person_"):
        return "person_to_issuer"
    if relation in ORGANIZATION_CONTROL_RELATIONS:
        return "organization_control"
    if relation in SUPPLY_CHAIN_RELATIONS:
        return "supply_chain_counterparty"
    if relation in SECTOR_EXPOSURE_RELATIONS:
        return "sector_exposure"
    if relation in POLICY_SECTOR_RELATIONS:
        return "policy_sector_exposure"
    if relation in {"issuer_in_index", "index_affects_country_index_proxy"}:
        return "index_membership"
    if relation.endswith("_country_index_proxy"):
        return "global_equity_proxy"
    if relation.startswith("country_affects_") or relation.endswith("_policy_rate_proxy"):
        return "country_macro_policy"
    if relation in ISSUER_EXPOSURE_RELATIONS:
        return "issuer_exposure"
    if (
        relation in GEOGRAPHY_COMMODITY_RELATIONS
        or relation in COMMODITY_MARKET_RELATIONS
        or relation in SANCTIONS_RELATIONS
        or "commodity" in relation
        or "chokepoint" in relation
        or "production_region" in relation
    ):
        return "geography_commodity"
    if relation.endswith("_currency_proxy") or relation.endswith("_usd_proxy"):
        return "currency_proxy"
    if relation.endswith("_dxy_proxy"):
        return "dxy_proxy"
    if relation.endswith("_country_risk_proxy") or relation.endswith("_global_policy_risk_proxy"):
        return "risk_proxy"
    if relation.endswith("_global_equity_proxy"):
        return "global_equity_proxy"
    if relation in CENTRAL_BANK_RELATIONS:
        return "country_macro_policy"
    if relation in COUNTRY_MEMBERSHIP_RELATIONS:
        return "country_membership"
    if relation in CRYPTO_POLICY_RELATIONS:
        return "policy_sector_exposure"
    return "other"


def relation_event_types(relation: str) -> tuple[str, ...]:
    """Return default event compatibility for a relation."""

    if relation.endswith("_dxy_proxy"):
        return DXY_EVENT_TYPES
    if relation.endswith("_country_risk_proxy") or relation.endswith("_global_policy_risk_proxy"):
        return COUNTRY_RISK_EVENT_TYPES
    if relation.endswith("_global_equity_proxy"):
        return GLOBAL_EQUITY_EVENT_TYPES
    if relation.endswith("_country_index_proxy"):
        return GLOBAL_EQUITY_EVENT_TYPES
    if relation.endswith("_policy_rate_proxy"):
        return POLICY_RATE_EVENT_TYPES
    if relation.endswith("_currency_proxy") or relation.endswith("_usd_proxy"):
        return CURRENCY_PROXY_EVENT_TYPES
    if relation in SUPPLY_CHAIN_RELATIONS:
        return SUPPLY_CHAIN_EVENT_TYPES
    if relation in {
        "issuer_in_sector",
        "issuer_in_index",
        "sector_affects_index",
        "index_affects_country_index_proxy",
    }:
        return GLOBAL_EQUITY_EVENT_TYPES
    if relation in POLICY_SECTOR_RELATIONS:
        return POLICY_SECTOR_EVENT_TYPES
    if relation in {"issuer_exposed_to_country", "issuer_exposed_to_region"}:
        return COUNTRY_REGION_EXPOSURE_EVENT_TYPES
    if relation == "issuer_exposed_to_supply_chain":
        return SUPPLY_CHAIN_EVENT_TYPES
    if (
        relation in ISSUER_EXPOSURE_RELATIONS
        or relation in GEOGRAPHY_COMMODITY_RELATIONS
        or relation in COMMODITY_MARKET_RELATIONS
    ):
        return COMMODITY_EXPOSURE_EVENT_TYPES
    if relation in SANCTIONS_RELATIONS:
        return ("sanctions", "export_control", "war_escalation", "ceasefire_risk_relief")
    if relation in CENTRAL_BANK_RELATIONS:
        return POLICY_RATE_EVENT_TYPES
    if relation in COUNTRY_MEMBERSHIP_RELATIONS:
        return COUNTRY_RISK_EVENT_TYPES
    if relation in CRYPTO_POLICY_RELATIONS:
        return (*POLICY_SECTOR_EVENT_TYPES, "war_escalation")
    if (
        relation in IDENTITY_LISTING_RELATIONS
        or relation == "person_affects_employer_ticker"
        or relation in PERSON_TO_ISSUER_RELATIONS
        or relation in ORGANIZATION_CONTROL_RELATIONS
    ):
        return EQUITY_EVENT_TYPES
    return ()


def relation_direction_preconditions(relation: str) -> tuple[str, ...]:
    """Return default direction preconditions for a relation."""

    if relation.endswith("_dxy_proxy"):
        return ("usd_or_rates_context", "risk_or_safe_haven_context")
    if relation.endswith("_country_risk_proxy") or relation.endswith("_global_policy_risk_proxy"):
        return (
            "explicit_risk_event",
            "fiscal_or_credit_signal",
            "fiscal_risk",
            "political_risk",
            "sanctions_risk",
            "sovereign_risk",
        )
    if relation.endswith("_global_equity_proxy"):
        return (
            "macro_policy_or_risk_signal",
            "equity_event_signal",
            "broad_market_context",
            "policy_signal",
            "risk_on_off_signal",
        )
    if relation in {"sector_affects_index", "index_affects_country_index_proxy"}:
        return ("sector_or_index_event_signal",)
    if relation.endswith("_country_index_proxy"):
        return ("macro_policy_or_risk_signal", "equity_event_signal")
    if relation.endswith("_policy_rate_proxy"):
        return ("rate_inflation_central_bank_or_fiscal_signal",)
    if relation.endswith("_currency_proxy") or relation.endswith("_usd_proxy"):
        return ("currency_or_rates_context", "macro_policy_or_risk_signal")
    if relation == "issuer_has_listed_ticker":
        return (
            "direct_issuer_or_security_mention",
            "corporate_action",
            "credit_event",
            "direct_company_mention",
            "earnings_signal",
            "labor_disruption",
        )
    if relation == "person_affects_employer_ticker":
        return ("strong_person_employer_event",)
    if relation in PERSON_TO_ISSUER_RELATIONS:
        return ("strong_key_person_or_ownership_event",)
    if relation in SUPPLY_CHAIN_RELATIONS:
        return ("supply_chain_or_counterparty_event",)
    if relation in {"issuer_in_sector", "issuer_in_index"}:
        return ("direct_issuer_or_sector_membership_evidence",)
    if relation in POLICY_SECTOR_RELATIONS:
        return ("sector_policy_event_signal", "jurisdiction_or_regulator_context")
    if relation in ISSUER_EXPOSURE_RELATIONS:
        return ("source_backed_issuer_exposure", "compatible_event_signal")
    if relation in COMMODITY_SECTOR_RELATIONS:
        return (
            "input_cost_change",
            "supply_shortage",
            "strategic_supply_shortage",
            "trade_restriction",
            "policy_restriction",
            "fuel_price_change",
            "crop_supply_change",
            "weather_damage",
            "export_disruption",
            "export_restriction",
            "contamination",
            "crop_damage",
            "livestock_damage",
            "battery_supply_shortage",
            "food_price_shock",
            "risk_relief",
            "supply_disruption",
        )
    if relation in GEOGRAPHY_COMMODITY_RELATIONS:
        return (
            "location_or_route_event_signal",
            "transit_disruption",
            "transit_risk",
            "war_risk",
            "risk_relief",
            "drought_restriction",
            "rerouting",
            "supply_chain_disruption",
            "mine_disruption",
            "production_change",
            "export_disruption",
            "export_restriction",
            "production_or_export_signal",
            "supply_disruption",
            "supply_allocation",
        )
    if relation == "commodity_traded_in_currency":
        return ("commodity_currency_pricing", "usd_context", "risk_signal")
    if relation in SANCTIONS_RELATIONS:
        return ("new_sanctions", "secondary_sanctions", "export_restriction", "risk_relief")
    if relation in CENTRAL_BANK_RELATIONS:
        return ("rate_cut", "rate_hike", "rate_hold", "inflation_surprise", "fiscal_signal")
    if relation in COUNTRY_MEMBERSHIP_RELATIONS:
        return ("country_bloc_membership", "production_or_export_signal")
    if relation in CRYPTO_POLICY_RELATIONS:
        return ("approval", "legal_risk", "regulatory_restriction")
    if relation in ORGANIZATION_CONTROL_RELATIONS:
        return ("strong_ownership_or_control_event",)
    return ()


def _normalized_items(values: Iterable[str] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    return tuple(dict.fromkeys(value.strip() for value in values if value and value.strip()))


def normalized_relation_event_types(
    relation: str,
    configured_event_types: Iterable[str] | None,
) -> tuple[str, ...]:
    """Return row-level event types, falling back to the relation schema."""

    normalized = _normalized_items(configured_event_types)
    if normalized:
        return normalized
    return relation_event_types(relation)


def normalized_relation_direction_preconditions(
    relation: str,
    configured_preconditions: Iterable[str] | None,
) -> tuple[str, ...]:
    """Return row-level direction preconditions, falling back to the schema."""

    normalized = _normalized_items(configured_preconditions)
    if normalized:
        return normalized
    return relation_direction_preconditions(relation)


def validate_relation_metadata(
    *,
    relation: str,
    configured_event_types: Iterable[str] | None,
    configured_preconditions: Iterable[str] | None,
) -> list[str]:
    """Return non-fatal schema warnings for one normalized relationship row."""

    warnings: list[str] = []
    configured_events = _normalized_items(configured_event_types)
    configured_direction_preconditions = _normalized_items(configured_preconditions)
    schema_events = relation_event_types(relation)
    schema_preconditions = relation_direction_preconditions(relation)
    family = relation_family_for_relation(relation)

    if family == "other" and not schema_events and not schema_preconditions:
        warnings.append("unknown_relation_schema")
    elif (
        not schema_events
        and not schema_preconditions
        and not configured_events
        and not configured_direction_preconditions
    ):
        warnings.append("relation_schema_missing_defaults")

    if configured_events and schema_events:
        schema_event_set = {event.casefold() for event in schema_events}
        for event in configured_events:
            if event.casefold() not in schema_event_set:
                warnings.append(f"unsupported_compatible_event_type:{event}")
    if configured_direction_preconditions and schema_preconditions:
        schema_precondition_set = {precondition.casefold() for precondition in schema_preconditions}
        for precondition in configured_direction_preconditions:
            if precondition.casefold() not in schema_precondition_set:
                warnings.append(f"unsupported_direction_precondition:{precondition}")
    return warnings


def relation_definition_for(relation: str) -> RelationDefinition:
    return RelationDefinition(
        relation=relation,
        family=relation_family_for_relation(relation),
        compatible_event_types=relation_event_types(relation),
        direction_preconditions=relation_direction_preconditions(relation),
    )
