"""Validate normalized market impact source-lane TSV files."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
import re
from typing import Iterable
from urllib.parse import urlparse

from .relationship_schema import (
    relation_conflict_policy,
    validate_node_type_metadata,
    validate_relation_metadata,
)
from .source_catalog import (
    SOURCE_TIER_UNKNOWN,
    classify_source_tier,
    is_source_tier_production_eligible,
    source_tier_policy_label,
    validate_source_attribution,
)
from .source_row_schema import (
    row_uses_canonical_source_schema,
    validate_canonical_source_row,
    validate_production_required_source_row,
)

_ACTIVE_PARENT_CONFLICT_POLICY = "exclusive_active_parent_requires_resolution"
_TICKER_SECURITY_CONFLICT_RELATIONS = {
    "issuer_has_listed_ticker",
    "issuer_has_security",
    "security_has_ticker",
}
_EXCHANGE_RELATIONS = {
    "exchange_lists_security",
    "issuer_has_exchange_listing",
    "security_listed_on_exchange",
    "security_trades_on_exchange",
    "ticker_listed_on_exchange",
    "ticker_trades_on_exchange",
}
_EXCHANGE_COUNTRY_HINTS = {
    "amex": "US",
    "asx": "AU",
    "b3": "BR",
    "bist": "TR",
    "bmv": "MX",
    "bse": "IN",
    "byma": "AR",
    "cboe": "US",
    "fra": "DE",
    "hkex": "HK",
    "idx": "ID",
    "jpx": "JP",
    "jse": "ZA",
    "krx": "KR",
    "kosdaq": "KR",
    "kospi": "KR",
    "lse": "GB",
    "moex": "RU",
    "nasdaq": "US",
    "nse": "IN",
    "nyse": "US",
    "otc": "US",
    "sase": "SA",
    "tadawul": "SA",
    "tsx": "CA",
    "xetra": "DE",
    "xist": "TR",
    "xlon": "GB",
}
_COUNTRY_ALIASES = {
    "UK": "GB",
    "GBR": "GB",
    "USA": "US",
}


@dataclass(frozen=True)
class ImpactSourceLaneValidationResult:
    edge_tsv_paths: tuple[Path, ...]
    node_tsv_paths: tuple[Path, ...]
    row_count: int
    invalid_row_count: int
    relation_counts: dict[str, int]
    node_type_counts: dict[str, int]
    source_tier_counts: dict[str, int]
    source_warning_counts: dict[str, int]
    relation_warning_counts: dict[str, int]
    node_warning_counts: dict[str, int]
    warning_samples: tuple[dict[str, object], ...]
    source_policy_counts: dict[str, int] = field(default_factory=dict)
    production_eligible_row_count: int = 0
    proposal_only_row_count: int = 0
    canonical_schema_warning_counts: dict[str, int] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, object]:
        return {
            "edge_tsv_paths": [str(path) for path in self.edge_tsv_paths],
            "node_tsv_paths": [str(path) for path in self.node_tsv_paths],
            "row_count": self.row_count,
            "invalid_row_count": self.invalid_row_count,
            "relation_counts": self.relation_counts,
            "node_type_counts": self.node_type_counts,
            "source_tier_counts": self.source_tier_counts,
            "source_policy_counts": self.source_policy_counts,
            "production_eligible_row_count": self.production_eligible_row_count,
            "proposal_only_row_count": self.proposal_only_row_count,
            "source_warning_counts": self.source_warning_counts,
            "relation_warning_counts": self.relation_warning_counts,
            "node_warning_counts": self.node_warning_counts,
            "canonical_schema_warning_counts": self.canonical_schema_warning_counts,
            "warning_samples": list(self.warning_samples),
        }


def _read_string(row: dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def _parse_items(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in re.split(r"[,|]", value) if item.strip())


def _add_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _sorted_counts(counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _warning_sample(
    *,
    path: Path,
    line_number: int,
    row: dict[str, str],
    scope: str,
    warning: str,
) -> dict[str, object]:
    return {
        "path": str(path),
        "line": line_number,
        "scope": scope,
        "warning": warning,
        "source_ref": _read_string(row, "source_ref") or None,
        "target_ref": _read_string(row, "target_ref") or None,
        "entity_ref": _read_string(row, "entity_ref") or None,
        "entity_type": _read_string(row, "entity_type") or None,
        "relation": _read_string(row, "relation") or None,
    }


def _read_first_string(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = _read_string(row, key)
        if value:
            return value
    return ""


def _canonical_country(value: str) -> str:
    normalized = value.strip().upper().replace("-", "_")
    if not normalized or normalized == "GLOBAL":
        return ""
    return _COUNTRY_ALIASES.get(normalized, normalized)


def _host(source_url: str) -> str:
    parsed = urlparse(source_url)
    return (parsed.hostname or parsed.netloc or "").casefold()


def _exchange_country_for_ref(ref: str) -> str:
    normalized = ref.strip().casefold()
    if not normalized:
        return ""
    prefix = re.split(r"[:|/._-]", normalized, maxsplit=1)[0]
    return _EXCHANGE_COUNTRY_HINTS.get(prefix, "")


def _exchange_ref(row: dict[str, str]) -> str:
    return _read_first_string(
        row,
        "exchange_ref",
        "exchange",
        "exchange_code",
        "listing_exchange",
    )


def _security_exchange_key(row: dict[str, str], security_ref: str) -> str:
    exchange_ref = _exchange_ref(row)
    if not exchange_ref:
        return security_ref
    return f"{security_ref}@{exchange_ref.casefold()}"


def _has_dual_class_explanation(row: dict[str, str]) -> bool:
    text = " ".join(
        _read_string(row, key)
        for key in ("notes", "evidence", "confidence_basis")
        if _read_string(row, key)
    ).casefold()
    return any(
        marker in text
        for marker in (
            "dual class",
            "dual-class",
            "share class",
            "multiple share classes",
        )
    )


def _is_active_row(row: dict[str, str], *, today: date) -> bool:
    effective_end = _read_first_string(row, "effective_end_date", "effective_to", "end_date")
    parsed_end = _parse_iso_date(effective_end)
    return parsed_end is None or parsed_end >= today


def _parse_iso_date(value: str) -> date | None:
    normalized = value.strip()
    if not normalized:
        return None
    try:
        return date.fromisoformat(normalized[:10])
    except ValueError:
        return None


def _active_edge_warning(row: dict[str, str], *, today: date) -> str | None:
    effective_end = _read_first_string(row, "effective_end_date", "effective_to", "end_date")
    parsed_end = _parse_iso_date(effective_end)
    if parsed_end is not None and parsed_end < today:
        return "expired_edge"
    return None


def _wrong_exchange_country_warning(row: dict[str, str], relation: str) -> str | None:
    if relation not in _EXCHANGE_RELATIONS and "ticker" not in relation:
        return None
    jurisdiction = _canonical_country(
        _read_first_string(row, "exchange_country", "jurisdiction", "country_code", "country")
    )
    if not jurisdiction:
        return None
    exchange_ref = _read_first_string(row, "exchange_ref")
    source_type = _read_string(row, "source_type").casefold()
    target_type = _read_string(row, "target_type").casefold()
    if not exchange_ref:
        if target_type == "exchange":
            exchange_ref = _read_string(row, "target_ref")
        elif source_type == "exchange":
            exchange_ref = _read_string(row, "source_ref")
        elif "ticker" in relation:
            exchange_ref = _read_first_string(row, "target_ref", "source_ref")
    expected = _exchange_country_for_ref(exchange_ref)
    if expected and jurisdiction != expected:
        return f"wrong_exchange_country:{exchange_ref}:{expected}:{jurisdiction}"
    return None


def _shortcut_warning(source_type: str, target_type: str) -> str | None:
    if source_type in {"program", "product", "brand"} and target_type in {
        "security",
        "ticker",
    }:
        return "direct_program_to_ticker_shortcut"
    if source_type in {"sector", "industry"} and target_type in {
        "currency",
        "rates_proxy",
    }:
        return "sector_to_currency_shortcut"
    return None


def _append_relation_warning(
    *,
    warning: str,
    path: Path,
    line_number: int,
    row: dict[str, str],
    relation_warning_counts: dict[str, int],
    warning_samples: list[dict[str, object]],
    sample_limit: int,
) -> None:
    _add_count(relation_warning_counts, warning)
    if len(warning_samples) < sample_limit:
        warning_samples.append(
            _warning_sample(
                path=path,
                line_number=line_number,
                row=row,
                scope="relation",
                warning=warning,
            )
        )


def _conflicting_active_mapping_warning(
    mapping: dict[str, str],
    *,
    key: str,
    value: str,
    warning: str,
) -> str | None:
    if not key or not value:
        return None
    existing = mapping.get(key)
    if existing is None:
        mapping[key] = value
        return None
    if existing != value:
        return warning
    return None


def _relationship_conflict_warnings(
    *,
    row: dict[str, str],
    relation: str,
    source_ref: str,
    target_ref: str,
    source_type: str,
    target_type: str,
    today: date,
    active_parent_by_child: dict[str, str],
    issuer_by_ticker: dict[str, str],
    issuer_by_security: dict[str, str],
    security_by_ticker: dict[str, str],
    ticker_by_security: dict[str, str],
    tickers_by_security: dict[str, set[str]],
) -> tuple[str, ...]:
    warnings: list[str] = []
    expired_warning = _active_edge_warning(row, today=today)
    if expired_warning:
        warnings.append(expired_warning)
    shortcut_warning = _shortcut_warning(source_type, target_type)
    if shortcut_warning:
        warnings.append(shortcut_warning)
    exchange_country_warning = _wrong_exchange_country_warning(row, relation)
    if exchange_country_warning:
        warnings.append(exchange_country_warning)

    if not _is_active_row(row, today=today):
        return tuple(warnings)

    if relation_conflict_policy(relation) == _ACTIVE_PARENT_CONFLICT_POLICY:
        warning = _conflicting_active_mapping_warning(
            active_parent_by_child,
            key=target_ref,
            value=source_ref,
            warning=f"active_parent_conflict:{target_ref}",
        )
        if warning:
            warnings.append(warning)

    if relation not in _TICKER_SECURITY_CONFLICT_RELATIONS:
        return tuple(warnings)

    if relation == "issuer_has_listed_ticker":
        warning = _conflicting_active_mapping_warning(
            issuer_by_ticker,
            key=target_ref,
            value=source_ref,
            warning=f"ticker_security_conflict:ticker_parent:{target_ref}",
        )
        if warning:
            warnings.append(warning)
        security_ref = security_by_ticker.get(target_ref)
        if security_ref and issuer_by_security.get(security_ref) not in {None, source_ref}:
            warnings.append(f"ticker_security_conflict:issuer_mismatch:{security_ref}:{target_ref}")
    elif relation == "issuer_has_security":
        warning = _conflicting_active_mapping_warning(
            issuer_by_security,
            key=target_ref,
            value=source_ref,
            warning=f"ticker_security_conflict:security_parent:{target_ref}",
        )
        if warning:
            warnings.append(warning)
        for ticker_ref in sorted(tickers_by_security.get(target_ref, set())):
            if issuer_by_ticker.get(ticker_ref) not in {None, source_ref}:
                warnings.append(
                    f"ticker_security_conflict:issuer_mismatch:{target_ref}:{ticker_ref}"
                )
    elif relation == "security_has_ticker":
        warning = _conflicting_active_mapping_warning(
            security_by_ticker,
            key=target_ref,
            value=source_ref,
            warning=f"ticker_security_conflict:ticker_security:{target_ref}",
        )
        if warning:
            warnings.append(warning)
        tickers_by_security.setdefault(source_ref, set()).add(target_ref)
        warning = _conflicting_active_mapping_warning(
            ticker_by_security,
            key=_security_exchange_key(row, source_ref),
            value=target_ref,
            warning=f"ticker_security_conflict:security_ticker:{source_ref}",
        )
        if warning and not _has_dual_class_explanation(row):
            warnings.append(warning)
        if issuer_by_ticker.get(target_ref) not in {None, issuer_by_security.get(source_ref)}:
            if issuer_by_security.get(source_ref):
                warnings.append(
                    f"ticker_security_conflict:issuer_mismatch:{source_ref}:{target_ref}"
                )
    return tuple(warnings)


def validate_market_graph_source_lanes(
    *,
    edge_tsv_paths: Iterable[str | Path],
    node_tsv_paths: Iterable[str | Path] = (),
    sample_limit: int = 50,
) -> ImpactSourceLaneValidationResult:
    resolved_paths = tuple(Path(path).expanduser().resolve() for path in edge_tsv_paths)
    if not resolved_paths:
        raise ValueError("At least one edge TSV path is required.")
    resolved_node_paths = tuple(Path(path).expanduser().resolve() for path in node_tsv_paths)
    for path in (*resolved_paths, *resolved_node_paths):
        if not path.exists():
            raise FileNotFoundError(path)

    row_count = 0
    invalid_row_count = 0
    relation_counts: dict[str, int] = {}
    node_type_counts: dict[str, int] = {}
    source_tier_counts: dict[str, int] = {}
    source_policy_counts: dict[str, int] = {}
    source_warning_counts: dict[str, int] = {}
    relation_warning_counts: dict[str, int] = {}
    node_warning_counts: dict[str, int] = {}
    canonical_schema_warning_counts: dict[str, int] = {}
    warning_samples: list[dict[str, object]] = []
    production_eligible_row_count = 0
    proposal_only_row_count = 0
    today = date.today()
    active_parent_by_child: dict[str, str] = {}
    issuer_by_ticker: dict[str, str] = {}
    issuer_by_security: dict[str, str] = {}
    security_by_ticker: dict[str, str] = {}
    ticker_by_security: dict[str, str] = {}
    tickers_by_security: dict[str, set[str]] = {}

    for path in resolved_node_paths:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row_index, row in enumerate(reader, start=2):
                row_count += 1
                entity_ref = _read_string(row, "entity_ref")
                entity_type = _read_string(row, "entity_type")
                if not entity_ref or not entity_type:
                    invalid_row_count += 1
                if entity_type:
                    _add_count(node_type_counts, entity_type)
                for warning in validate_node_type_metadata(entity_type):
                    _add_count(node_warning_counts, warning)
                    if len(warning_samples) < sample_limit:
                        warning_samples.append(
                            _warning_sample(
                                path=path,
                                line_number=row_index,
                                row=row,
                                scope="node",
                                warning=warning,
                            )
                        )

    for path in resolved_paths:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            has_canonical_source_schema = row_uses_canonical_source_schema(reader.fieldnames)
            for row_index, row in enumerate(reader, start=2):
                row_count += 1
                production_required_warnings: tuple[str, ...] = ()
                if has_canonical_source_schema:
                    canonical_warnings = validate_canonical_source_row(row)
                    production_required_warnings = validate_production_required_source_row(row)
                    for warning in canonical_warnings:
                        _add_count(canonical_schema_warning_counts, warning)
                        if len(warning_samples) < sample_limit:
                            warning_samples.append(
                                _warning_sample(
                                    path=path,
                                    line_number=row_index,
                                    row=row,
                                    scope="canonical_schema",
                                    warning=warning,
                                )
                            )

                source_ref = _read_string(row, "source_ref")
                target_ref = _read_string(row, "target_ref")
                relation = _read_string(row, "relation")
                if not source_ref or not target_ref or not relation:
                    invalid_row_count += 1
                if relation:
                    _add_count(relation_counts, relation)
                source_type = _read_string(row, "source_type")
                target_type = _read_string(row, "target_type")
                canonical_source_type = source_type.casefold().replace("-", "_")
                canonical_target_type = target_type.casefold().replace("-", "_")

                source_name = _read_string(row, "source_name")
                source_url = _read_string(row, "source_url")
                source_snapshot = _read_string(row, "source_snapshot")
                if has_canonical_source_schema:
                    source_name = source_name or _read_string(row, "source_title")
                    source_snapshot = source_snapshot or _read_string(row, "fetched_at")
                    source_tier = _read_string(row, "source_tier") or SOURCE_TIER_UNKNOWN
                    classified_source_tier = classify_source_tier(source_name, source_url)
                else:
                    source_tier = classify_source_tier(source_name, source_url)
                    classified_source_tier = source_tier
                _add_count(source_tier_counts, source_tier)
                source_policy = source_tier_policy_label(source_tier)
                _add_count(source_policy_counts, source_policy)
                if (
                    is_source_tier_production_eligible(source_tier)
                    and not production_required_warnings
                ):
                    production_eligible_row_count += 1
                else:
                    proposal_only_row_count += 1
                for warning in validate_source_attribution(
                    source_name=source_name or None,
                    source_url=source_url or None,
                    source_snapshot=source_snapshot or None,
                ):
                    _add_count(source_warning_counts, warning)
                    if len(warning_samples) < sample_limit:
                        warning_samples.append(
                            _warning_sample(
                                path=path,
                                line_number=row_index,
                                row=row,
                                scope="source",
                                warning=warning,
                            )
                        )
                if classified_source_tier == SOURCE_TIER_UNKNOWN and _host(source_url):
                    warning = "unrecognized_source_host"
                    _add_count(source_warning_counts, warning)
                    if len(warning_samples) < sample_limit:
                        warning_samples.append(
                            _warning_sample(
                                path=path,
                                line_number=row_index,
                                row=row,
                                scope="source",
                                warning=warning,
                            )
                        )

                for warning in validate_relation_metadata(
                    relation=relation,
                    configured_event_types=_parse_items(
                        _read_string(row, "compatible_event_types")
                    ),
                    configured_preconditions=_parse_items(
                        _read_string(row, "direction_preconditions")
                    ),
                    source_node_type=source_type or None,
                    target_node_type=target_type or None,
                ):
                    _append_relation_warning(
                        warning=warning,
                        path=path,
                        line_number=row_index,
                        row=row,
                        relation_warning_counts=relation_warning_counts,
                        warning_samples=warning_samples,
                        sample_limit=sample_limit,
                    )

                for warning in _relationship_conflict_warnings(
                    row=row,
                    relation=relation,
                    source_ref=source_ref,
                    target_ref=target_ref,
                    source_type=canonical_source_type,
                    target_type=canonical_target_type,
                    today=today,
                    active_parent_by_child=active_parent_by_child,
                    issuer_by_ticker=issuer_by_ticker,
                    issuer_by_security=issuer_by_security,
                    security_by_ticker=security_by_ticker,
                    ticker_by_security=ticker_by_security,
                    tickers_by_security=tickers_by_security,
                ):
                    _append_relation_warning(
                        warning=warning,
                        path=path,
                        line_number=row_index,
                        row=row,
                        relation_warning_counts=relation_warning_counts,
                        warning_samples=warning_samples,
                        sample_limit=sample_limit,
                    )

    return ImpactSourceLaneValidationResult(
        edge_tsv_paths=resolved_paths,
        node_tsv_paths=resolved_node_paths,
        row_count=row_count,
        invalid_row_count=invalid_row_count,
        relation_counts=_sorted_counts(relation_counts),
        node_type_counts=_sorted_counts(node_type_counts),
        source_tier_counts=_sorted_counts(source_tier_counts),
        source_policy_counts=_sorted_counts(source_policy_counts),
        production_eligible_row_count=production_eligible_row_count,
        proposal_only_row_count=proposal_only_row_count,
        source_warning_counts=_sorted_counts(source_warning_counts),
        relation_warning_counts=_sorted_counts(relation_warning_counts),
        node_warning_counts=_sorted_counts(node_warning_counts),
        canonical_schema_warning_counts=_sorted_counts(canonical_schema_warning_counts),
        warning_samples=tuple(warning_samples),
    )
