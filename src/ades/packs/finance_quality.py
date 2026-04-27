"""Quality validation for generated `finance-en` pack bundles."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .quality_common import (
    PackQualityCase,
    PackQualityEntity,
    PackQualityResult,
    validate_generated_pack_quality,
)


DEFAULT_FINANCE_MAX_AMBIGUOUS_ALIASES = 5000
DEFAULT_FINANCE_MAX_DROPPED_ALIAS_RATIO = 0.5
DEFAULT_FINANCE_REGIONAL_MAX_DROPPED_ALIAS_RATIO = 0.6
_FINANCE_HIGH_RISK_QUALITY_TICKERS = {
    "a",
    "aa",
    "am",
    "an",
    "as",
    "at",
    "be",
    "by",
    "can",
    "for",
    "go",
    "in",
    "it",
    "may",
    "new",
    "no",
    "now",
    "of",
    "on",
    "one",
    "or",
    "out",
    "so",
    "the",
    "to",
    "top",
    "two",
    "up",
    "us",
}
_FINANCE_QUALITY_MARKET_INDEX_PREFERENCES = (
    "S&P 500",
    "S&P 500 Index",
    "VIX",
    "Cboe Volatility Index",
)
_FINANCE_QUALITY_COMMODITY_PREFERENCES = (
    "Brent crude",
    "Brent crude oil",
)
_FINANCE_QUALITY_EXCHANGE_PREFERENCES = (
    "London Stock Exchange",
    "LSE",
    "Chicago Mercantile Exchange",
    "CME",
    "Nasdaq Stock Market",
    "NASDAQ",
    "New York Stock Exchange",
    "NYSE",
)


def validate_finance_pack_quality(
    bundle_dir: str | Path,
    *,
    output_dir: str,
    fixture_profile: str = "benchmark",
    dependency_bundle_dirs: tuple[str | Path, ...] = (),
    version: str | None = None,
    min_expected_recall: float = 1.0,
    max_unexpected_hits: int = 0,
    max_ambiguous_aliases: int = DEFAULT_FINANCE_MAX_AMBIGUOUS_ALIASES,
    max_dropped_alias_ratio: float = DEFAULT_FINANCE_MAX_DROPPED_ALIAS_RATIO,
) -> PackQualityResult:
    """Build, install, and evaluate one generated finance-domain pack bundle."""

    cases = _resolve_finance_quality_cases(bundle_dir, fixture_profile)
    return validate_generated_pack_quality(
        bundle_dir,
        output_dir=output_dir,
        quality_dir_name="finance-quality",
        cases=cases,
        fixture_profile=fixture_profile,
        dependency_bundle_dirs=dependency_bundle_dirs,
        version=version,
        min_expected_recall=min_expected_recall,
        max_unexpected_hits=max_unexpected_hits,
        max_ambiguous_aliases=max_ambiguous_aliases,
        max_dropped_alias_ratio=max_dropped_alias_ratio,
    )


def _resolve_finance_quality_cases(
    bundle_dir: str | Path,
    fixture_profile: str,
) -> tuple[PackQualityCase, ...]:
    entities_by_type = _load_finance_entities_by_type(bundle_dir)
    normalized_profile = fixture_profile.strip().casefold()
    if normalized_profile == "benchmark":
        return _build_finance_benchmark_cases(entities_by_type)
    if normalized_profile == "smoke":
        return _build_finance_smoke_cases(entities_by_type)
    if normalized_profile == "regional":
        return _build_finance_regional_cases(entities_by_type)
    raise ValueError("fixture_profile must be one of: benchmark, smoke, regional.")


def _build_finance_smoke_cases(
    entities_by_type: dict[str, list[dict[str, Any]]],
) -> tuple[PackQualityCase, ...]:
    issuer_one, issuer_one_text, ticker_one = _pick_issuer_and_ticker(
        entities_by_type,
        excluded_issuer_ids=set(),
        excluded_ticker_symbols=set(),
    )
    exchange_one = _pick_exchange_text(
        entities_by_type,
        preferred_texts=_FINANCE_QUALITY_EXCHANGE_PREFERENCES,
        excluded_texts=set(),
    )
    exchange_two = _pick_exchange_text(
        entities_by_type,
        preferred_texts=("Chicago Mercantile Exchange", "LSE", "Nasdaq Stock Market", "NYSE"),
        excluded_texts={exchange_one},
        require_alias=True,
    )
    cases = [
        PackQualityCase(
            name="earnings-update",
            text=(
                f"{issuer_one_text} said {ticker_one} traded on "
                f"{exchange_one} after USD 12.5 guidance."
            ),
            expected_entities=(
                PackQualityEntity(text=issuer_one_text, label="organization"),
                PackQualityEntity(text=ticker_one, label="ticker"),
                PackQualityEntity(text=exchange_one, label="exchange"),
                PackQualityEntity(text="USD 12.5", label="currency_amount"),
            ),
        ),
        PackQualityCase(
            name="exchange-alias",
            text=f"{exchange_two} rose after USD 22 guidance.",
            expected_entities=(
                PackQualityEntity(text=exchange_two, label="exchange"),
                PackQualityEntity(text="USD 22", label="currency_amount"),
            ),
        ),
        PackQualityCase(
            name="produce-market-non-hit",
            text="Farmers sold produce and tools at the public market on Tuesday.",
            expected_entities=(),
        ),
    ]
    person_text = _pick_person_text(entities_by_type, excluded_texts=set())
    if person_text is not None:
        cases.append(
            PackQualityCase(
                name="finance-person-name",
                text=f"{person_text} said financing conditions were stable after USD 5 guidance.",
                expected_entities=(
                    PackQualityEntity(text=person_text, label="person"),
                    PackQualityEntity(text="USD 5", label="currency_amount"),
                ),
            )
        )
    return tuple(cases)


def _build_finance_benchmark_cases(
    entities_by_type: dict[str, list[dict[str, Any]]],
) -> tuple[PackQualityCase, ...]:
    smoke_cases = _build_finance_smoke_cases(entities_by_type)
    _, _, ticker_one = _pick_issuer_and_ticker(
        entities_by_type,
        excluded_issuer_ids=set(),
        excluded_ticker_symbols=set(),
    )
    _, _, ticker_two = _pick_issuer_and_ticker(
        entities_by_type,
        excluded_issuer_ids=set(),
        excluded_ticker_symbols={ticker_one},
    )
    exchange_one = _pick_exchange_text(
        entities_by_type,
        preferred_texts=_FINANCE_QUALITY_EXCHANGE_PREFERENCES,
        excluded_texts=set(),
    )
    exchange_two = _pick_exchange_text(
        entities_by_type,
        preferred_texts=("Chicago Mercantile Exchange", "Nasdaq Stock Market", "New York Stock Exchange", "LSE"),
        excluded_texts={exchange_one},
    )
    exchange_alias = _pick_exchange_text(
        entities_by_type,
        preferred_texts=("LSE", "Chicago Mercantile Exchange", "NASDAQ", "NYSE"),
        excluded_texts={exchange_one, exchange_two},
        require_alias=True,
    )
    market_index_one = _pick_entity_text(
        entities_by_type,
        entity_type="market_index",
        preferred_texts=_FINANCE_QUALITY_MARKET_INDEX_PREFERENCES,
        excluded_texts=set(),
    )
    market_index_two = _pick_entity_text(
        entities_by_type,
        entity_type="market_index",
        preferred_texts=tuple(
            text
            for text in reversed(_FINANCE_QUALITY_MARKET_INDEX_PREFERENCES)
            if text != market_index_one
        ),
        excluded_texts={market_index_one},
    )
    commodity_one = _pick_entity_text(
        entities_by_type,
        entity_type="commodity",
        preferred_texts=_FINANCE_QUALITY_COMMODITY_PREFERENCES,
        excluded_texts=set(),
    )
    return (
        *smoke_cases[:2],
        PackQualityCase(
            name="exchange-canonical-and-symbol-mix",
            text=(
                f"{exchange_one} and {exchange_two} moved while {ticker_one} and {ticker_two} "
                "stayed active after USD 18 guidance."
            ),
            expected_entities=(
                PackQualityEntity(text=exchange_one, label="exchange"),
                PackQualityEntity(text=exchange_two, label="exchange"),
                PackQualityEntity(text=ticker_one, label="ticker"),
                PackQualityEntity(text=ticker_two, label="ticker"),
                PackQualityEntity(text="USD 18", label="currency_amount"),
            ),
        ),
        PackQualityCase(
            name="curated-exchange-alias-expansion",
            text=f"{exchange_alias} and {exchange_two} opened higher after USD 7.5 guidance.",
            expected_entities=(
                PackQualityEntity(text=exchange_alias, label="exchange"),
                PackQualityEntity(text=exchange_two, label="exchange"),
                PackQualityEntity(text="USD 7.5", label="currency_amount"),
            ),
        ),
        PackQualityCase(
            name="indices-and-rates",
            text=(
                f"The {market_index_one} slipped 2.4% while the {market_index_two} rose after "
                "€1.2 billion issuance widened spreads by 15 basis points."
            ),
            expected_entities=(
                PackQualityEntity(text=market_index_one, label="market_index"),
                PackQualityEntity(text=market_index_two, label="market_index"),
                PackQualityEntity(text="2.4%", label="percentage"),
                PackQualityEntity(text="€1.2 billion", label="currency_amount"),
                PackQualityEntity(text="15 basis points", label="basis_points"),
            ),
        ),
        PackQualityCase(
            name="commodities-and-global-exchanges",
            text=(
                f"{commodity_one} rallied while the {exchange_one} and {exchange_two} "
                "digested C$28.9m hedging flows."
            ),
            expected_entities=(
                PackQualityEntity(text=commodity_one, label="commodity"),
                PackQualityEntity(text=exchange_one, label="exchange"),
                PackQualityEntity(text=exchange_two, label="exchange"),
                PackQualityEntity(text="C$28.9m", label="currency_amount"),
            ),
        ),
        smoke_cases[2],
        *smoke_cases[3:],
    )


def _build_finance_regional_cases(
    entities_by_type: dict[str, list[dict[str, Any]]],
) -> tuple[PackQualityCase, ...]:
    organization_one = _pick_finance_organization_text(
        entities_by_type,
        excluded_texts=set(),
    )
    exchange_one = _pick_exchange_text(
        entities_by_type,
        preferred_texts=(),
        excluded_texts=set(),
    )
    cases: list[PackQualityCase] = [
        PackQualityCase(
            name="regional-organization-and-exchange",
            text=(
                f"{organization_one} coordinated disclosures with {exchange_one} "
                "after USD 10 guidance."
            ),
            expected_entities=(
                PackQualityEntity(text=organization_one, label="organization"),
                PackQualityEntity(text=exchange_one, label="exchange"),
                PackQualityEntity(text="USD 10", label="currency_amount"),
            ),
        ),
        PackQualityCase(
            name="regional-market-non-hit",
            text="Local merchants sold flowers and vegetables in the outdoor market square.",
            expected_entities=(),
        ),
    ]
    market_index = _pick_optional_entity_text(
        entities_by_type,
        entity_type="market_index",
        excluded_texts=set(),
    )
    if market_index is not None:
        cases.append(
            PackQualityCase(
                name="regional-index-case",
                text=f"The {market_index} rose 1.2% after €5 issuance.",
                expected_entities=(
                    PackQualityEntity(text=market_index, label="market_index"),
                    PackQualityEntity(text="1.2%", label="percentage"),
                    PackQualityEntity(text="€5", label="currency_amount"),
                ),
            )
        )
    person_text = _pick_person_text(entities_by_type, excluded_texts=set())
    if person_text is not None:
        cases.append(
            PackQualityCase(
                name="regional-finance-person-name",
                text=f"{person_text} told {exchange_one} that financing remained stable.",
                expected_entities=(
                    PackQualityEntity(text=person_text, label="person"),
                    PackQualityEntity(text=exchange_one, label="exchange"),
                ),
            )
        )
    return tuple(cases)


def _load_finance_entities_by_type(
    bundle_dir: str | Path,
) -> dict[str, list[dict[str, Any]]]:
    resolved_bundle_dir = Path(bundle_dir).expanduser().resolve()
    manifest = json.loads(
        (resolved_bundle_dir / "bundle.json").read_text(encoding="utf-8")
    )
    entities_path = resolved_bundle_dir / str(manifest["entities_path"])
    entities_by_type: dict[str, list[dict[str, Any]]] = {}
    for line in entities_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        entity_type = str(record.get("entity_type", "")).strip()
        if not entity_type:
            continue
        entities_by_type.setdefault(entity_type, []).append(record)
    return entities_by_type


def _pick_issuer_and_ticker(
    entities_by_type: dict[str, list[dict[str, Any]]],
    *,
    excluded_issuer_ids: set[str],
    excluded_ticker_symbols: set[str],
) -> tuple[dict[str, Any], str, str]:
    ticker_records = entities_by_type.get("ticker", [])
    ticker_symbols = {
        str(record.get("canonical_text", "")).upper(): record for record in ticker_records
    }
    issuer_candidates = entities_by_type.get("issuer", [])
    preferred_issuer: dict[str, Any] | None = None
    preferred_issuer_text: str | None = None
    preferred_ticker: str | None = None
    fallback_issuer: dict[str, Any] | None = None
    fallback_issuer_text: str | None = None
    for issuer in issuer_candidates:
        issuer_id = str(issuer.get("entity_id", ""))
        if issuer_id in excluded_issuer_ids:
            continue
        issuer_text = _issuer_quality_text(issuer)
        if not issuer_text:
            continue
        if fallback_issuer is None:
            fallback_issuer = issuer
            fallback_issuer_text = issuer_text
        metadata = issuer.get("metadata", {})
        ticker_value = ""
        if isinstance(metadata, dict):
            ticker_value = str(metadata.get("ticker", "")).upper().strip()
        if (
            ticker_value
            and ticker_value in ticker_symbols
            and ticker_value not in excluded_ticker_symbols
            and _is_safe_quality_ticker(ticker_value)
        ):
            if _is_safe_quality_issuer_text(issuer_text):
                preferred_issuer = issuer
                preferred_issuer_text = issuer_text
                preferred_ticker = ticker_value
                break
            if preferred_issuer is None:
                preferred_issuer = issuer
                preferred_issuer_text = issuer_text
                preferred_ticker = ticker_value
    if (
        preferred_issuer is not None
        and preferred_issuer_text is not None
        and preferred_ticker is not None
    ):
        return preferred_issuer, preferred_issuer_text, preferred_ticker
    for ticker_symbol in sorted(ticker_symbols):
        if ticker_symbol in excluded_ticker_symbols:
            continue
        if _is_safe_quality_ticker(ticker_symbol):
            if fallback_issuer is None or fallback_issuer_text is None:
                break
            return fallback_issuer, fallback_issuer_text, ticker_symbol
    raise ValueError("finance-en quality fixtures require at least one safe issuer+ticker pair.")


def _pick_exchange_text(
    entities_by_type: dict[str, list[dict[str, Any]]],
    *,
    preferred_texts: tuple[str, ...],
    excluded_texts: set[str],
    require_alias: bool = False,
) -> str:
    ticker_texts = {
        str(record.get("canonical_text", "")).casefold()
        for record in entities_by_type.get("ticker", [])
    }
    return _pick_entity_text(
        entities_by_type,
        entity_type="exchange",
        preferred_texts=preferred_texts,
        excluded_texts=excluded_texts,
        require_alias=require_alias,
        blocked_text_keys=ticker_texts,
    )


def _pick_entity_text(
    entities_by_type: dict[str, list[dict[str, Any]]],
    *,
    entity_type: str,
    preferred_texts: tuple[str, ...],
    excluded_texts: set[str],
    require_alias: bool = False,
    blocked_text_keys: set[str] | None = None,
) -> str:
    blocked_keys = {text.casefold() for text in excluded_texts}
    if blocked_text_keys is not None:
        blocked_keys.update(blocked_text_keys)
    records = entities_by_type.get(entity_type, [])
    for preferred_text in preferred_texts:
        match = _find_preferred_entity_text(
            records,
            preferred_text=preferred_text,
            excluded_text_keys=blocked_keys,
            require_alias=require_alias,
        )
        if match is not None:
            return match
    for record in records:
        candidate_texts = _candidate_entity_texts(record, require_alias=require_alias)
        for candidate_text in candidate_texts:
            normalized_key = candidate_text.casefold()
            if normalized_key in blocked_keys:
                continue
            if entity_type == "ticker" and not _is_safe_quality_ticker(candidate_text):
                continue
            return candidate_text
    raise ValueError(f"finance-en quality fixtures require at least one {entity_type} entity.")


def _pick_optional_entity_text(
    entities_by_type: dict[str, list[dict[str, Any]]],
    *,
    entity_type: str,
    excluded_texts: set[str],
) -> str | None:
    try:
        return _pick_entity_text(
            entities_by_type,
            entity_type=entity_type,
            preferred_texts=(),
            excluded_texts=excluded_texts,
        )
    except ValueError:
        return None


def _pick_finance_organization_text(
    entities_by_type: dict[str, list[dict[str, Any]]],
    *,
    excluded_texts: set[str],
) -> str:
    try:
        return _pick_entity_text(
            entities_by_type,
            entity_type="organization",
            preferred_texts=(),
            excluded_texts=excluded_texts,
        )
    except ValueError:
        pass
    excluded_issuer_ids: set[str] = set()
    excluded_keys = {text.casefold() for text in excluded_texts}
    for issuer in entities_by_type.get("issuer", []):
        issuer_id = str(issuer.get("entity_id", ""))
        if issuer_id in excluded_issuer_ids:
            continue
        issuer_text = _issuer_quality_text(issuer)
        if issuer_text and issuer_text.casefold() not in excluded_keys:
            return issuer_text
    raise ValueError("finance regional quality fixtures require at least one organization entity.")


def _pick_person_text(
    entities_by_type: dict[str, list[dict[str, Any]]],
    *,
    excluded_texts: set[str],
) -> str | None:
    excluded_keys = {text.casefold() for text in excluded_texts}
    for record in entities_by_type.get("person", []):
        for candidate_text in _candidate_entity_texts(record, require_alias=False):
            if " " not in candidate_text or candidate_text.casefold() in excluded_keys:
                continue
            return candidate_text
    return None


def _find_preferred_entity_text(
    records: list[dict[str, Any]],
    *,
    preferred_text: str,
    excluded_text_keys: set[str],
    require_alias: bool,
) -> str | None:
    preferred_key = preferred_text.casefold()
    for record in records:
        for candidate_text in _candidate_entity_texts(record, require_alias=require_alias):
            normalized_key = candidate_text.casefold()
            if normalized_key in excluded_text_keys:
                continue
            if normalized_key == preferred_key:
                return candidate_text
    return None


def _candidate_entity_texts(
    record: dict[str, Any],
    *,
    require_alias: bool,
) -> tuple[str, ...]:
    canonical_text = str(record.get("canonical_text", "")).strip()
    aliases = tuple(
        str(alias).strip()
        for alias in (record.get("aliases") or [])
        if str(alias).strip() and str(alias).strip().casefold() != canonical_text.casefold()
    )
    if require_alias:
        if aliases:
            return aliases
        if canonical_text:
            return (canonical_text,)
        return ()
    if canonical_text:
        return (canonical_text, *aliases)
    return aliases


def _is_safe_quality_ticker(symbol: str) -> bool:
    normalized = symbol.strip().upper()
    if not normalized or not normalized.isalnum():
        return False
    if len(normalized) < 3:
        return False
    return normalized.casefold() not in _FINANCE_HIGH_RISK_QUALITY_TICKERS


def _issuer_quality_text(record: dict[str, Any]) -> str:
    canonical_text = str(record.get("canonical_text", "")).strip()
    aliases = [
        str(alias).strip().rstrip(" .,:;")
        for alias in (record.get("aliases") or [])
        if str(alias).strip()
    ]
    for alias in aliases:
        if _is_safe_quality_issuer_text(alias):
            return alias
    return canonical_text.rstrip(" .,:;")


def _is_safe_quality_issuer_text(text: str) -> bool:
    if not text:
        return False
    if not any(character.isalpha() for character in text):
        return False
    return any(character.islower() for character in text)
