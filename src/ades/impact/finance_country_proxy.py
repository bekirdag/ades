"""Build finance-country pack proxy lanes for the market impact graph."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Iterable, Iterator

from .graph_builder import build_market_graph_store
from .starter import starter_source_paths

DEFAULT_BIG_DATA_ROOT = Path("/mnt/githubActions/ades_big_data")
DEFAULT_PACKS_ROOT = (
    DEFAULT_BIG_DATA_ROOT / "prod-deploy" / "registry-2026-04-24-finance-country-ready-r1" / "packs"
)
DEFAULT_SOURCE_OUTPUT_ROOT = (
    DEFAULT_BIG_DATA_ROOT / "pack_sources" / "impact_relationships" / "finance_country_proxy"
)
DEFAULT_ARTIFACT_OUTPUT_ROOT = DEFAULT_BIG_DATA_ROOT / "artifacts" / "market-graph"

COUNTRY_CURRENCY = {
    "ar": "ARS",
    "au": "AUD",
    "br": "BRL",
    "ca": "CAD",
    "cn": "CNY",
    "de": "EUR",
    "fr": "EUR",
    "id": "IDR",
    "in": "INR",
    "it": "EUR",
    "jp": "JPY",
    "kr": "KRW",
    "mx": "MXN",
    "ru": "RUB",
    "sa": "SAR",
    "tr": "TRY",
    "uk": "GBP",
    "us": "USD",
    "za": "ZAR",
}

COUNTRY_DISPLAY_NAME = {
    "ar": "Argentina",
    "au": "Australia",
    "br": "Brazil",
    "ca": "Canada",
    "cn": "China",
    "de": "Germany",
    "fr": "France",
    "id": "Indonesia",
    "in": "India",
    "it": "Italy",
    "jp": "Japan",
    "kr": "South Korea",
    "mx": "Mexico",
    "ru": "Russia",
    "sa": "Saudi Arabia",
    "tr": "Turkey",
    "uk": "United Kingdom",
    "us": "United States",
    "za": "South Africa",
}

COUNTRY_IDENTITY_REFS = {
    "ar": {"wikidata_qid": "Q414", "geonames_id": "3865483"},
    "au": {"wikidata_qid": "Q408", "geonames_id": "2077456"},
    "br": {"wikidata_qid": "Q155", "geonames_id": "3469034"},
    "ca": {"wikidata_qid": "Q16", "geonames_id": "6251999"},
    "cn": {"wikidata_qid": "Q148", "geonames_id": "1814991"},
    "de": {"wikidata_qid": "Q183", "geonames_id": "2921044"},
    "fr": {"wikidata_qid": "Q142", "geonames_id": "3017382"},
    "id": {"wikidata_qid": "Q252", "geonames_id": "1643084"},
    "in": {"wikidata_qid": "Q668", "geonames_id": "1269750"},
    "it": {"wikidata_qid": "Q38", "geonames_id": "3175395"},
    "jp": {"wikidata_qid": "Q17", "geonames_id": "1861060"},
    "kr": {"wikidata_qid": "Q884", "geonames_id": "1835841"},
    "mx": {"wikidata_qid": "Q96", "geonames_id": "3996063"},
    "ru": {"wikidata_qid": "Q159", "geonames_id": "2017370"},
    "sa": {"wikidata_qid": "Q851", "geonames_id": "102358"},
    "tr": {"wikidata_qid": "Q43", "geonames_id": "298795"},
    "uk": {"wikidata_qid": "Q145", "geonames_id": "2635167"},
    "us": {"wikidata_qid": "Q30", "geonames_id": "6252001"},
    "za": {"wikidata_qid": "Q258", "geonames_id": "953987"},
}

DEFAULT_EXTRA_PROXY_PACK_IDS = (
    "business-vector-en",
    "economics-vector-en",
    "finance-en",
    "politics-vector-en",
)

NODE_COLUMNS = [
    "entity_ref",
    "canonical_name",
    "entity_type",
    "library_id",
    "is_tradable",
    "is_seed_eligible",
    "identifiers_json",
    "pack_ids",
]

EDGE_COLUMNS = [
    "source_ref",
    "target_ref",
    "relation",
    "evidence_level",
    "confidence",
    "direction_hint",
    "source_name",
    "source_url",
    "source_snapshot",
    "source_year",
    "refresh_policy",
    "pack_ids",
    "notes",
    "compatible_event_types",
    "direction_preconditions",
]

TRADABLE_ENTITY_TYPES = {
    "ticker",
    "market_index",
    "currency",
    "commodity",
    "fund",
    "etf",
    "rate",
    "indicator",
    "risk_indicator",
}

POLITICAL_OR_PUBLIC_ENTITY_TYPES = {
    "person",
    "politician",
    "public_official",
    "organization",
    "regulator",
    "central_bank",
    "government_body",
}

_DXY_EVENT_TYPES = (
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
_COUNTRY_RISK_EVENT_TYPES = (
    "sanctions",
    "tariff",
    "export_control",
    "fiscal_expansion",
    "fiscal_austerity",
    "default",
    "war_escalation",
    "ceasefire_risk_relief",
)
_GLOBAL_EQUITY_EVENT_TYPES = (
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
_POLICY_RATE_EVENT_TYPES = (
    "policy_rate_cut",
    "policy_rate_hike",
    "policy_rate_hold",
    "inflation_shock",
    "future_policy_expectation",
    "fiscal_expansion",
    "fiscal_austerity",
)
_EQUITY_EVENT_TYPES = (
    "tariff",
    "export_control",
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

_PERSON_TO_ISSUER_RELATIONS = {
    "person_is_ceo_of_issuer",
    "person_is_founder_of_issuer",
    "person_is_chair_of_issuer",
    "person_is_board_member_of_issuer",
    "person_is_major_shareholder_of_issuer",
    "person_controls_issuer",
    "person_affects_employer_issuer",
}

_ORGANIZATION_TO_ISSUER_RELATIONS = {
    "private_company_controls_public_issuer",
    "holding_company_controls_public_issuer",
    "issuer_supplier_to_issuer",
    "issuer_customer_of_issuer",
    "issuer_partner_of_issuer",
}

_ROLE_RELATIONS = _PERSON_TO_ISSUER_RELATIONS | _ORGANIZATION_TO_ISSUER_RELATIONS

_SUPPLY_CHAIN_EVENT_TYPES = (
    "supply_disruption",
    "production_decrease",
    "production_increase",
    "shipping_chokepoint_disruption",
    "tariff",
    "export_control",
    "sanctions",
    "strike_labor_disruption",
)

_ISSUER_SECTOR_KEYS = (
    "sector",
    "sectors",
    "industry",
    "industries",
    "gics_sector",
    "gics_industry",
    "icb_sector",
    "icb_industry",
    "business_sector",
    "business_segments",
)

_ISSUER_INDEX_KEYS = (
    "index",
    "indexes",
    "indices",
    "market_index",
    "market_indices",
    "index_constituent",
    "index_constituents",
    "constituent_indices",
    "benchmark_index",
)

_COUNTERPARTY_FIELD_GROUPS = (
    (
        "issuer_supplier_to_issuer",
        ("supplier", "suppliers", "vendor", "vendors"),
        "supplier_to_issuer",
    ),
    (
        "issuer_customer_of_issuer",
        ("customer", "customers", "client", "clients"),
        "customer_to_issuer",
    ),
    (
        "issuer_partner_of_issuer",
        ("partner", "partners", "joint_venture_partner", "strategic_partner"),
        "issuer_partner",
    ),
)

_LOOKUP_NON_WORD_RE = re.compile(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ]+")
_LOOKUP_NON_ALNUM_RE = re.compile(r"[^0-9A-Za-z]+")
_URLISH_RE = re.compile(r"(https?://|www\.|\.com\b|\.org\b|\.net\b|@)", re.IGNORECASE)
_ARTIFACT_NAME_RE = re.compile(
    r"\b("
    r"about\s+us|investor\s+relations|press\s+release|forward[-\s]+looking|"
    r"article\s+[ivxlcdm0-9]+|read\s+more|cookie|privacy|terms\s+of\s+use|"
    r"board\s+committees?|continuing\s+directors?|corporate\s+governance|"
    r"executive\s+officers?|non[-\s]+employee\s+directors?|beneficial\s+owners?"
    r")\b",
    re.IGNORECASE,
)
_LEGAL_ENTITY_RE = re.compile(
    r"\b("
    r"inc|incorporated|corp|corporation|company|co|ltd|limited|llc|llp|plc|"
    r"holdings?|group|sa|s\.a|ag|nv|bv|pty|gmbh|sirketi|şirketi|anonim|a\.s|a\.ş"
    r")\b",
    re.IGNORECASE,
)
_PERSON_ALPHA_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]+")


@dataclass(frozen=True)
class FinanceCountryEntity:
    entity_ref: str
    canonical_name: str
    entity_type: str
    pack_id: str
    metadata: dict[str, object]

    @property
    def category(self) -> str:
        raw = self.metadata.get("category") or self.entity_type
        return str(raw).strip().casefold()

    @property
    def country_code(self) -> str | None:
        raw = self.metadata.get("country_code")
        if raw:
            return str(raw).strip().casefold()
        return country_code_from_pack_id(self.pack_id)

    @property
    def is_tradable_terminal(self) -> bool:
        return (
            self.entity_type.casefold() in TRADABLE_ENTITY_TYPES
            or self.category in TRADABLE_ENTITY_TYPES
        )


@dataclass
class _RelationshipLookup:
    issuers_by_ticker: dict[str, list[FinanceCountryEntity]]
    issuers_by_isin: dict[str, list[FinanceCountryEntity]]
    issuers_by_cik: dict[str, list[FinanceCountryEntity]]
    issuers_by_company_number: dict[str, list[FinanceCountryEntity]]
    issuers_by_company_code: dict[str, list[FinanceCountryEntity]]
    issuers_by_name: dict[str, list[FinanceCountryEntity]]
    tickers_by_symbol: dict[str, list[FinanceCountryEntity]]
    tickers_by_isin: dict[str, list[FinanceCountryEntity]]
    tickers_by_company_number: dict[str, list[FinanceCountryEntity]]
    tickers_by_company_code: dict[str, list[FinanceCountryEntity]]
    tickers_by_issuer_name: dict[str, list[FinanceCountryEntity]]


@dataclass(frozen=True)
class _Edge:
    source_ref: str
    target_ref: str
    relation: str
    evidence_level: str
    confidence: float
    direction_hint: str
    source_name: str
    source_url: str
    source_snapshot: str
    source_year: int
    refresh_policy: str
    pack_ids: str
    notes: str
    compatible_event_types: tuple[str, ...] = ()
    direction_preconditions: tuple[str, ...] = ()


@dataclass(frozen=True)
class FinanceCountryProxyBuildResult:
    run_id: str
    output_dir: Path
    node_tsv_path: Path
    edge_tsv_path: Path
    manifest_path: Path
    artifact_path: Path | None
    artifact_node_count: int | None
    artifact_edge_count: int | None
    pack_count: int
    extra_proxy_pack_count: int
    entity_count: int
    extra_proxy_entity_count: int
    node_count: int
    edge_count: int
    terminal_node_count: int
    uncovered_entity_count: int
    relation_counts: dict[str, int]
    edge_family_counts: dict[str, int]

    def to_json_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "output_dir": str(self.output_dir),
            "node_tsv_path": str(self.node_tsv_path),
            "edge_tsv_path": str(self.edge_tsv_path),
            "manifest_path": str(self.manifest_path),
            "artifact_path": str(self.artifact_path) if self.artifact_path else None,
            "artifact_node_count": self.artifact_node_count,
            "artifact_edge_count": self.artifact_edge_count,
            "pack_count": self.pack_count,
            "extra_proxy_pack_count": self.extra_proxy_pack_count,
            "entity_count": self.entity_count,
            "extra_proxy_entity_count": self.extra_proxy_entity_count,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "terminal_node_count": self.terminal_node_count,
            "uncovered_entity_count": self.uncovered_entity_count,
            "relation_counts": self.relation_counts,
            "edge_family_counts": self.edge_family_counts,
        }


def country_code_from_pack_id(pack_id: str) -> str | None:
    parts = pack_id.split("-")
    if len(parts) >= 3 and parts[0] == "finance" and parts[-1] == "en":
        return parts[1].casefold()
    return None


def discover_finance_country_pack_dirs(packs_root: str | Path) -> list[Path]:
    root = Path(packs_root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Finance-country packs root not found: {root}")
    return [
        path
        for path in sorted(root.iterdir())
        if path.is_dir()
        and path.name.startswith("finance-")
        and path.name.endswith("-en")
        and (path / "aliases.json").exists()
    ]


def discover_extra_proxy_pack_dirs(
    packs_root: str | Path,
    *,
    pack_ids: Iterable[str] = DEFAULT_EXTRA_PROXY_PACK_IDS,
) -> list[Path]:
    root = Path(packs_root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Packs root not found: {root}")
    wanted = {str(pack_id).strip() for pack_id in pack_ids if str(pack_id).strip()}
    return [
        root / pack_id
        for pack_id in sorted(wanted)
        if (root / pack_id).is_dir() and (root / pack_id / "aliases.json").exists()
    ]


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _source_snapshot_for_pack(pack_dir: Path) -> str:
    build_path = pack_dir / "build.json"
    if build_path.exists():
        payload = _load_json(build_path)
        if isinstance(payload, dict):
            raw = payload.get("generated_at")
            if raw:
                return str(raw)[:10]
    sources_path = pack_dir / "sources.json"
    if sources_path.exists():
        payload = _load_json(sources_path)
        if isinstance(payload, dict):
            for source in payload.get("sources") or []:
                if isinstance(source, dict) and source.get("retrieved_at"):
                    return str(source["retrieved_at"])[:10]
    return datetime.now(timezone.utc).date().isoformat()


def _source_url_for_pack(pack_dir: Path) -> str:
    sources_path = pack_dir / "sources.json"
    if sources_path.exists():
        return sources_path.resolve().as_uri()
    return pack_dir.resolve().as_uri()


def _entity_path_from_sources(pack_dir: Path) -> Path | None:
    sources_path = pack_dir / "sources.json"
    if not sources_path.exists():
        return None
    payload = _load_json(sources_path)
    if not isinstance(payload, dict):
        return None
    raw_path = payload.get("entities_path")
    if not raw_path:
        return None
    path = Path(str(raw_path)).expanduser()
    return path if path.exists() else None


def _iter_entities_from_jsonl(path: Path, *, pack_id: str) -> Iterator[FinanceCountryEntity]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                continue
            entity_ref = str(payload.get("entity_id") or "").strip()
            canonical_name = str(payload.get("canonical_text") or "").strip()
            entity_type = str(payload.get("entity_type") or "").strip()
            if not entity_ref or not canonical_name or not entity_type:
                continue
            metadata = payload.get("metadata")
            yield FinanceCountryEntity(
                entity_ref=entity_ref,
                canonical_name=canonical_name,
                entity_type=entity_type,
                pack_id=pack_id,
                metadata=metadata if isinstance(metadata, dict) else {},
            )


def _iter_entities_from_aliases(pack_dir: Path, *, pack_id: str) -> Iterator[FinanceCountryEntity]:
    payload = _load_json(pack_dir / "aliases.json")
    aliases = payload.get("aliases") if isinstance(payload, dict) else []
    by_ref: dict[str, FinanceCountryEntity] = {}
    for alias in aliases or []:
        if not isinstance(alias, dict):
            continue
        entity_ref = str(alias.get("entity_id") or "").strip()
        if not entity_ref or entity_ref in by_ref:
            continue
        canonical_name = str(alias.get("canonical_text") or alias.get("text") or entity_ref).strip()
        entity_type = str(alias.get("label") or "unknown").strip()
        by_ref[entity_ref] = FinanceCountryEntity(
            entity_ref=entity_ref,
            canonical_name=canonical_name,
            entity_type=entity_type,
            pack_id=pack_id,
            metadata={
                "country_code": country_code_from_pack_id(pack_id) or "",
                "category": entity_type,
            },
        )
    yield from by_ref.values()


def load_finance_country_pack_entities(pack_dir: str | Path) -> list[FinanceCountryEntity]:
    resolved = Path(pack_dir).expanduser().resolve()
    pack_id = resolved.name
    entity_path = _entity_path_from_sources(resolved)
    iterator: Iterable[FinanceCountryEntity]
    if entity_path is not None:
        iterator = _iter_entities_from_jsonl(entity_path, pack_id=pack_id)
    else:
        iterator = _iter_entities_from_aliases(resolved, pack_id=pack_id)
    entities: dict[str, FinanceCountryEntity] = {}
    for entity in iterator:
        entities.setdefault(entity.entity_ref, entity)
    return list(entities.values())


def _json_text(payload: dict[str, object]) -> str:
    clean_payload = {
        key: value for key, value in payload.items() if value not in (None, "", [], {})
    }
    return json.dumps(clean_payload, sort_keys=True, separators=(",", ":"))


def _node_row(
    *,
    entity_ref: str,
    canonical_name: str,
    entity_type: str,
    library_id: str,
    is_tradable: bool,
    is_seed_eligible: bool,
    identifiers: dict[str, object] | None = None,
    pack_ids: str,
) -> dict[str, object]:
    return {
        "entity_ref": entity_ref,
        "canonical_name": canonical_name,
        "entity_type": entity_type,
        "library_id": library_id,
        "is_tradable": "true" if is_tradable else "false",
        "is_seed_eligible": "true" if is_seed_eligible else "false",
        "identifiers_json": _json_text(identifiers or {}),
        "pack_ids": pack_ids,
    }


def _country_identifiers(country_code: str) -> dict[str, object]:
    identifiers: dict[str, object] = {"country_code": country_code}
    known_identity = COUNTRY_IDENTITY_REFS.get(country_code)
    if known_identity is None:
        return identifiers

    identifiers.update(known_identity)
    same_as_refs = [f"country:{country_code}"]
    wikidata_qid = known_identity.get("wikidata_qid")
    geonames_id = known_identity.get("geonames_id")
    if wikidata_qid:
        same_as_refs.append(f"wikidata:{wikidata_qid}")
    if geonames_id:
        same_as_refs.append(f"geonames:{geonames_id}")
    identifiers["same_as_refs"] = same_as_refs
    return identifiers


def _entity_identifiers(
    entity: FinanceCountryEntity,
    *,
    pack_id: str,
    relationship_lookup: _RelationshipLookup | None = None,
) -> dict[str, object]:
    identifiers = dict(entity.metadata)
    ticker_ref = None
    if entity.category == "issuer" and relationship_lookup is not None:
        ticker_refs = _ticker_refs_for_issuer(entity, pack_id=pack_id, lookup=relationship_lookup)
        ticker_ref = ticker_refs[0] if ticker_refs else None
    if ticker_ref is None:
        ticker_ref = _ticker_ref(pack_id, identifiers.get("ticker"))
    if entity.category == "issuer" and ticker_ref:
        identifiers.setdefault("ticker_ref", ticker_ref)
    return identifiers


def _synthetic_ticker_identifiers(
    *,
    country_code: str,
    edge: _Edge,
    source_entity: FinanceCountryEntity,
) -> dict[str, object]:
    ticker_symbol = edge.target_ref.rsplit(":", 1)[-1]
    identifiers: dict[str, object] = {
        "country_code": country_code,
        "ticker_ref": edge.target_ref,
        "ticker_symbol": ticker_symbol,
    }
    if edge.relation == "issuer_has_listed_ticker":
        identifiers["issuer_ref"] = source_entity.entity_ref
    return identifiers


def _country_proxy_nodes(pack_id: str, country_code: str, currency: str) -> list[dict[str, object]]:
    upper_country = country_code.upper()
    return [
        _node_row(
            entity_ref=f"country:{country_code}",
            canonical_name=COUNTRY_DISPLAY_NAME.get(country_code, upper_country),
            entity_type="country",
            library_id=pack_id,
            is_tradable=False,
            is_seed_eligible=True,
            identifiers=_country_identifiers(country_code),
            pack_ids=pack_id,
        ),
        _node_row(
            entity_ref=f"ades:impact:currency:{currency.casefold()}",
            canonical_name=f"{currency} currency proxy",
            entity_type="currency",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"currency": currency},
            pack_ids=pack_id,
        ),
        _node_row(
            entity_ref="ades:impact:indicator:dxy",
            canonical_name="US Dollar Index proxy",
            entity_type="indicator",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"symbol": "DXY"},
            pack_ids=pack_id,
        ),
        _node_row(
            entity_ref=f"ades:impact:index:{country_code}-market",
            canonical_name=f"{upper_country} broad equity market proxy",
            entity_type="market_index",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"country_code": country_code},
            pack_ids=pack_id,
        ),
        _node_row(
            entity_ref=f"ades:impact:rates:{country_code}-policy-rate",
            canonical_name=f"{upper_country} policy-rate proxy",
            entity_type="rate",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"country_code": country_code},
            pack_ids=pack_id,
        ),
        _node_row(
            entity_ref=f"ades:impact:risk:{country_code}-country-risk",
            canonical_name=f"{upper_country} country-risk proxy",
            entity_type="risk_indicator",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"country_code": country_code},
            pack_ids=pack_id,
        ),
        _node_row(
            entity_ref="ades:impact:index:global-equities",
            canonical_name="Global equities proxy",
            entity_type="market_index",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"scope": "global"},
            pack_ids=pack_id,
        ),
    ]


def _global_proxy_nodes(pack_id: str) -> list[dict[str, object]]:
    return [
        _node_row(
            entity_ref="ades:impact:currency:usd",
            canonical_name="USD currency proxy",
            entity_type="currency",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"currency": "USD"},
            pack_ids=pack_id,
        ),
        _node_row(
            entity_ref="ades:impact:indicator:dxy",
            canonical_name="US Dollar Index proxy",
            entity_type="indicator",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"symbol": "DXY"},
            pack_ids=pack_id,
        ),
        _node_row(
            entity_ref="ades:impact:index:global-equities",
            canonical_name="Global equities proxy",
            entity_type="market_index",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"scope": "global"},
            pack_ids=pack_id,
        ),
        _node_row(
            entity_ref="ades:impact:risk:global-policy-risk",
            canonical_name="Global policy-risk proxy",
            entity_type="risk_indicator",
            library_id=pack_id,
            is_tradable=True,
            is_seed_eligible=False,
            identifiers={"scope": "global"},
            pack_ids=pack_id,
        ),
    ]


def _relation_event_types(relation: str) -> tuple[str, ...]:
    if relation.endswith("_dxy_proxy"):
        return _DXY_EVENT_TYPES
    if relation.endswith("_country_risk_proxy") or relation.endswith("_global_policy_risk_proxy"):
        return _COUNTRY_RISK_EVENT_TYPES
    if relation.endswith("_global_equity_proxy"):
        return _GLOBAL_EQUITY_EVENT_TYPES
    if relation.endswith("_policy_rate_proxy"):
        return _POLICY_RATE_EVENT_TYPES
    if relation in {
        "issuer_supplier_to_issuer",
        "issuer_customer_of_issuer",
        "issuer_partner_of_issuer",
    }:
        return _SUPPLY_CHAIN_EVENT_TYPES
    if relation in {
        "issuer_in_sector",
        "issuer_in_index",
        "sector_affects_index",
        "index_affects_country_index_proxy",
    }:
        return _GLOBAL_EQUITY_EVENT_TYPES
    if relation in {"issuer_has_listed_ticker", "person_affects_employer_ticker"} | _ROLE_RELATIONS:
        return _EQUITY_EVENT_TYPES
    return ()


def _edge_family_for_relation(relation: str) -> str:
    if relation in {
        "issuer_has_listed_ticker",
        "ticker_represents_issuer",
        "issuer_has_exchange_listing",
        "ticker_listed_on_exchange",
    }:
        return "identity_listing"
    if relation in _PERSON_TO_ISSUER_RELATIONS or relation == "person_affects_employer_ticker":
        return "person_to_issuer"
    if relation in {
        "issuer_supplier_to_issuer",
        "issuer_customer_of_issuer",
        "issuer_partner_of_issuer",
    }:
        return "supply_chain_counterparty"
    if relation in _ORGANIZATION_TO_ISSUER_RELATIONS:
        return "organization_control"
    if relation in {"issuer_in_sector", "sector_affects_index"}:
        return "sector_exposure"
    if relation in {"issuer_in_index", "index_affects_country_index_proxy"}:
        return "index_membership"
    if relation.startswith("country_affects_") or relation.endswith("_policy_rate_proxy"):
        return "country_macro_policy"
    if relation.endswith("_currency_proxy") or relation.endswith("_usd_proxy"):
        return "currency_proxy"
    if relation.endswith("_dxy_proxy"):
        return "dxy_proxy"
    if relation.endswith("_country_risk_proxy") or relation.endswith("_global_policy_risk_proxy"):
        return "risk_proxy"
    if relation.endswith("_global_equity_proxy"):
        return "global_equity_proxy"
    return "other"


def _edge_family_counts_from_relations(relation_counts: dict[str, int]) -> dict[str, int]:
    family_counts: dict[str, int] = {}
    for relation, count in relation_counts.items():
        family = _edge_family_for_relation(relation)
        family_counts[family] = family_counts.get(family, 0) + count
    return dict(sorted(family_counts.items()))


def _relation_direction_preconditions(relation: str) -> tuple[str, ...]:
    if relation.endswith("_dxy_proxy"):
        return ("usd_or_rates_context", "risk_or_safe_haven_context")
    if relation.endswith("_country_risk_proxy") or relation.endswith("_global_policy_risk_proxy"):
        return ("explicit_risk_event", "fiscal_or_credit_signal")
    if relation.endswith("_global_equity_proxy"):
        return ("macro_policy_or_risk_signal", "equity_event_signal")
    if relation.endswith("_policy_rate_proxy"):
        return ("rate_inflation_central_bank_or_fiscal_signal",)
    if relation == "issuer_has_listed_ticker":
        return ("direct_issuer_or_security_mention",)
    if relation == "person_affects_employer_ticker":
        return ("strong_person_employer_event",)
    if relation in _PERSON_TO_ISSUER_RELATIONS:
        return ("strong_key_person_or_ownership_event",)
    if relation in {
        "issuer_supplier_to_issuer",
        "issuer_customer_of_issuer",
        "issuer_partner_of_issuer",
    }:
        return ("supply_chain_or_counterparty_event",)
    if relation in {"issuer_in_sector", "issuer_in_index"}:
        return ("direct_issuer_or_sector_membership_evidence",)
    if relation in {"sector_affects_index", "index_affects_country_index_proxy"}:
        return ("sector_or_index_event_signal",)
    if relation in _ORGANIZATION_TO_ISSUER_RELATIONS:
        return ("strong_ownership_or_control_event",)
    return ()


def _edge(
    *,
    source_ref: str,
    target_ref: str,
    relation: str,
    evidence_level: str,
    confidence: float,
    direction_hint: str,
    source_name: str,
    source_url: str,
    source_snapshot: str,
    source_year: int,
    pack_id: str,
    notes: str,
    compatible_event_types: tuple[str, ...] | None = None,
    direction_preconditions: tuple[str, ...] | None = None,
) -> _Edge | None:
    if source_ref == target_ref:
        return None
    resolved_event_types = compatible_event_types
    if resolved_event_types is None:
        resolved_event_types = _relation_event_types(relation)
    resolved_preconditions = direction_preconditions
    if resolved_preconditions is None:
        resolved_preconditions = _relation_direction_preconditions(relation)
    return _Edge(
        source_ref=source_ref,
        target_ref=target_ref,
        relation=relation,
        evidence_level=evidence_level,
        confidence=confidence,
        direction_hint=direction_hint,
        source_name=source_name,
        source_url=source_url,
        source_snapshot=source_snapshot,
        source_year=source_year,
        refresh_policy="on_pack_refresh",
        pack_ids=pack_id,
        notes=notes,
        compatible_event_types=tuple(dict.fromkeys(resolved_event_types)),
        direction_preconditions=tuple(dict.fromkeys(resolved_preconditions)),
    )


def _ticker_ref(pack_id: str, ticker: object) -> str | None:
    raw = str(ticker or "").strip().upper()
    if not raw:
        return None
    return f"{pack_id.removesuffix('-en')}-ticker:{raw}"


def _is_pack_ticker_ref(pack_id: str, entity_ref: str) -> bool:
    return entity_ref.startswith(f"{pack_id.removesuffix('-en')}-ticker:")


def _clean_metadata_string(value: object) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.casefold() in {"none", "null", "nan", "n/a", "na", "-"}:
        return None
    return raw


def _iter_metadata_values(metadata: dict[str, object], *keys: str) -> Iterator[str]:
    for key in keys:
        raw = metadata.get(key)
        if isinstance(raw, (list, tuple, set)):
            for item in raw:
                cleaned = _clean_metadata_string(item)
                if cleaned:
                    yield cleaned
            continue
        cleaned = _clean_metadata_string(raw)
        if cleaned is None:
            continue
        if any(separator in cleaned for separator in ("|", ";")):
            for item in re.split(r"[|;]", cleaned):
                part = _clean_metadata_string(item)
                if part:
                    yield part
            continue
        yield cleaned


def _lookup_name_key(value: object) -> str | None:
    cleaned = _clean_metadata_string(value)
    if cleaned is None:
        return None
    parts = [part for part in _LOOKUP_NON_WORD_RE.split(cleaned.casefold()) if part]
    if not parts:
        return None
    return " ".join(parts)


def _lookup_identifier_key(value: object) -> str | None:
    cleaned = _clean_metadata_string(value)
    if cleaned is None:
        return None
    key = _LOOKUP_NON_ALNUM_RE.sub("", cleaned.casefold())
    return key or None


def _ref_segment(value: object, *, max_length: int = 80) -> str | None:
    key = _lookup_name_key(value)
    if key is None:
        return None
    segment = re.sub(r"[^a-z0-9]+", "-", key).strip("-")
    if not segment:
        return None
    return segment[:max_length].strip("-") or None


def _cik_key(value: object) -> str | None:
    key = _lookup_identifier_key(value)
    if key is None:
        return None
    stripped = key.lstrip("0")
    return stripped or key


def _ticker_symbol_key(value: object) -> str | None:
    cleaned = _clean_metadata_string(value)
    if cleaned is None:
        return None
    if ":" in cleaned:
        cleaned = cleaned.rsplit(":", 1)[-1]
    key = _LOOKUP_NON_ALNUM_RE.sub("", cleaned.casefold())
    return key or None


def _exchange_key(value: object) -> str | None:
    return _lookup_identifier_key(value)


def _ticker_lookup_keys(ticker: object, exchange_values: Iterable[object] = ()) -> list[str]:
    symbol_key = _ticker_symbol_key(ticker)
    if symbol_key is None:
        return []
    keys: list[str] = []
    for exchange in exchange_values:
        exchange_key = _exchange_key(exchange)
        if exchange_key:
            keys.append(f"{exchange_key}:{symbol_key}")
    keys.append(symbol_key)
    return list(dict.fromkeys(keys))


def _entity_exchange_values(
    entity: FinanceCountryEntity, *, include_employer: bool = False
) -> list[str]:
    keys = [
        "exchange",
        "exchange_code",
        "exchanges",
        "instrument_exchange",
        "primary_exchange",
        "trading_venue",
        "venue",
        "market",
    ]
    if include_employer:
        keys.extend(["employer_exchange", "employer_exchange_code", "employer_market"])
    return list(dict.fromkeys(_iter_metadata_values(entity.metadata, *keys)))


def _add_index_value(
    index: dict[str, list[FinanceCountryEntity]],
    key: str | None,
    entity: FinanceCountryEntity,
) -> None:
    if key is None:
        return
    bucket = index.setdefault(key, [])
    if all(existing.entity_ref != entity.entity_ref for existing in bucket):
        bucket.append(entity)


def _lookup_unique(
    index: dict[str, list[FinanceCountryEntity]],
    key: str | None,
) -> FinanceCountryEntity | None:
    if key is None:
        return None
    candidates = index.get(key, [])
    by_ref = {candidate.entity_ref: candidate for candidate in candidates}
    if len(by_ref) != 1:
        return None
    return next(iter(by_ref.values()))


def _append_unique_entity(
    target: list[FinanceCountryEntity],
    entity: FinanceCountryEntity | None,
) -> None:
    if entity is None:
        return
    if all(existing.entity_ref != entity.entity_ref for existing in target):
        target.append(entity)


def _append_unique_ref(target: list[str], ref: str | None) -> None:
    if ref and ref not in target:
        target.append(ref)


def _relationship_metadata_keys(
    prefixes: Iterable[str], suffixes: Iterable[str]
) -> tuple[str, ...]:
    keys: list[str] = []
    for prefix in prefixes:
        normalized_prefix = str(prefix).strip()
        if not normalized_prefix:
            continue
        for suffix in suffixes:
            normalized_suffix = str(suffix).strip()
            if normalized_suffix:
                keys.append(f"{normalized_prefix}_{normalized_suffix}")
    return tuple(dict.fromkeys(keys))


def _lookup_issuers_from_metadata_fields(
    metadata: dict[str, object],
    *,
    lookup: _RelationshipLookup,
    ticker_keys: Iterable[str] = (),
    isin_keys: Iterable[str] = (),
    cik_keys: Iterable[str] = (),
    company_number_keys: Iterable[str] = (),
    company_code_keys: Iterable[str] = (),
    name_keys: Iterable[str] = (),
    exchange_values: Iterable[object] = (),
) -> tuple[FinanceCountryEntity, ...]:
    issuers: list[FinanceCountryEntity] = []
    for ticker in _iter_metadata_values(metadata, *ticker_keys):
        for key in _ticker_lookup_keys(ticker, exchange_values):
            _append_unique_entity(issuers, _lookup_unique(lookup.issuers_by_ticker, key))
    for isin in _iter_metadata_values(metadata, *isin_keys):
        _append_unique_entity(
            issuers, _lookup_unique(lookup.issuers_by_isin, _lookup_identifier_key(isin))
        )
    for cik in _iter_metadata_values(metadata, *cik_keys):
        _append_unique_entity(issuers, _lookup_unique(lookup.issuers_by_cik, _cik_key(cik)))
    for company_number in _iter_metadata_values(metadata, *company_number_keys):
        _append_unique_entity(
            issuers,
            _lookup_unique(
                lookup.issuers_by_company_number, _lookup_identifier_key(company_number)
            ),
        )
    for company_code in _iter_metadata_values(metadata, *company_code_keys):
        _append_unique_entity(
            issuers,
            _lookup_unique(lookup.issuers_by_company_code, _lookup_identifier_key(company_code)),
        )
    for name in _iter_metadata_values(metadata, *name_keys):
        _append_unique_entity(
            issuers, _lookup_unique(lookup.issuers_by_name, _lookup_name_key(name))
        )
    return tuple(issuers)


def _build_relationship_lookup(entities: Iterable[FinanceCountryEntity]) -> _RelationshipLookup:
    lookup = _RelationshipLookup(
        issuers_by_ticker={},
        issuers_by_isin={},
        issuers_by_cik={},
        issuers_by_company_number={},
        issuers_by_company_code={},
        issuers_by_name={},
        tickers_by_symbol={},
        tickers_by_isin={},
        tickers_by_company_number={},
        tickers_by_company_code={},
        tickers_by_issuer_name={},
    )

    for entity in entities:
        metadata = entity.metadata
        if entity.category == "issuer":
            exchange_values = _entity_exchange_values(entity)
            for ticker in _iter_metadata_values(metadata, "ticker", "tickers", "symbol", "symbols"):
                for key in _ticker_lookup_keys(ticker, exchange_values):
                    _add_index_value(lookup.issuers_by_ticker, key, entity)
            for isin in _iter_metadata_values(metadata, "isin", "isins", "instrument_isin"):
                _add_index_value(lookup.issuers_by_isin, _lookup_identifier_key(isin), entity)
            for cik in _iter_metadata_values(metadata, "cik", "sec_cik", "issuer_cik"):
                _add_index_value(lookup.issuers_by_cik, _cik_key(cik), entity)
            for company_number in _iter_metadata_values(
                metadata,
                "company_number",
                "company_no",
                "registration_number",
                "employer_company_number",
            ):
                _add_index_value(
                    lookup.issuers_by_company_number,
                    _lookup_identifier_key(company_number),
                    entity,
                )
            for company_code in _iter_metadata_values(
                metadata,
                "company_code",
                "employer_company_code",
                "member_oid",
                "mkk_member_oid",
            ):
                _add_index_value(
                    lookup.issuers_by_company_code,
                    _lookup_identifier_key(company_code),
                    entity,
                )
            for name in (
                entity.canonical_name,
                *_iter_metadata_values(
                    metadata, "issuer_name", "company_name", "legal_name", "name"
                ),
            ):
                _add_index_value(lookup.issuers_by_name, _lookup_name_key(name), entity)

        if entity.entity_type.casefold() == "ticker" or entity.category == "ticker":
            exchange_values = _entity_exchange_values(entity)
            ticker_values = list(
                _iter_metadata_values(metadata, "ticker", "tickers", "symbol", "symbols")
            )
            ticker_values.append(entity.canonical_name)
            if ":" in entity.entity_ref:
                ticker_values.append(entity.entity_ref.rsplit(":", 1)[-1])
            for ticker in ticker_values:
                for key in _ticker_lookup_keys(ticker, exchange_values):
                    _add_index_value(lookup.tickers_by_symbol, key, entity)
            for isin in _iter_metadata_values(metadata, "isin", "isins", "instrument_isin"):
                _add_index_value(lookup.tickers_by_isin, _lookup_identifier_key(isin), entity)
            for company_number in _iter_metadata_values(
                metadata,
                "company_number",
                "company_no",
                "registration_number",
                "issuer_company_number",
            ):
                _add_index_value(
                    lookup.tickers_by_company_number,
                    _lookup_identifier_key(company_number),
                    entity,
                )
            for company_code in _iter_metadata_values(
                metadata,
                "company_code",
                "issuer_company_code",
                "member_oid",
                "mkk_member_oid",
            ):
                _add_index_value(
                    lookup.tickers_by_company_code,
                    _lookup_identifier_key(company_code),
                    entity,
                )
            for issuer_name in _iter_metadata_values(
                metadata,
                "issuer_name",
                "company_name",
                "legal_name",
                "name",
            ):
                _add_index_value(
                    lookup.tickers_by_issuer_name,
                    _lookup_name_key(issuer_name),
                    entity,
                )

    return lookup


def _lookup_issuers_for_entity(
    entity: FinanceCountryEntity,
    *,
    lookup: _RelationshipLookup,
) -> tuple[FinanceCountryEntity, ...]:
    metadata = entity.metadata
    exchange_values = _entity_exchange_values(entity, include_employer=True)
    counterparty_prefixes = tuple(
        prefix for _, prefixes, _ in _COUNTERPARTY_FIELD_GROUPS for prefix in prefixes
    )
    return _lookup_issuers_from_metadata_fields(
        metadata,
        lookup=lookup,
        ticker_keys=(
            "employer_ticker",
            "employer_tickers",
            "related_ticker",
            "related_tickers",
            *_relationship_metadata_keys(
                counterparty_prefixes, ("ticker", "tickers", "symbol", "symbols")
            ),
        ),
        isin_keys=(
            "employer_isin",
            "employer_isins",
            "related_isin",
            *_relationship_metadata_keys(counterparty_prefixes, ("isin", "isins")),
        ),
        cik_keys=(
            "employer_cik",
            "employer_ciks",
            "related_cik",
            *_relationship_metadata_keys(counterparty_prefixes, ("cik", "ciks")),
        ),
        company_number_keys=(
            "employer_company_number",
            "employer_company_numbers",
            "related_company_number",
            "company_number",
            *_relationship_metadata_keys(
                counterparty_prefixes, ("company_number", "company_numbers")
            ),
        ),
        company_code_keys=(
            "employer_company_code",
            "employer_company_codes",
            "related_company_code",
            "company_code",
            *_relationship_metadata_keys(counterparty_prefixes, ("company_code", "company_codes")),
        ),
        name_keys=(
            "employer_name",
            "related_issuer_name",
            "issuer_name",
            *_relationship_metadata_keys(
                counterparty_prefixes, ("name", "names", "issuer_name", "issuer_names")
            ),
        ),
        exchange_values=exchange_values,
    )


def _ticker_refs_for_issuer(
    issuer: FinanceCountryEntity,
    *,
    pack_id: str,
    lookup: _RelationshipLookup,
) -> tuple[str, ...]:
    refs: list[str] = []
    metadata = issuer.metadata
    exchange_values = _entity_exchange_values(issuer)
    for ticker in _iter_metadata_values(metadata, "ticker", "tickers", "symbol", "symbols"):
        for key in _ticker_lookup_keys(ticker, exchange_values):
            ticker_entity = _lookup_unique(lookup.tickers_by_symbol, key)
            _append_unique_ref(refs, ticker_entity.entity_ref if ticker_entity else None)
        if not refs:
            _append_unique_ref(refs, _ticker_ref(pack_id, ticker))
    for isin in _iter_metadata_values(metadata, "isin", "isins", "instrument_isin"):
        ticker_entity = _lookup_unique(lookup.tickers_by_isin, _lookup_identifier_key(isin))
        _append_unique_ref(refs, ticker_entity.entity_ref if ticker_entity else None)
    for company_number in _iter_metadata_values(
        metadata,
        "company_number",
        "company_no",
        "registration_number",
    ):
        ticker_entity = _lookup_unique(
            lookup.tickers_by_company_number,
            _lookup_identifier_key(company_number),
        )
        _append_unique_ref(refs, ticker_entity.entity_ref if ticker_entity else None)
    for company_code in _iter_metadata_values(
        metadata,
        "company_code",
        "member_oid",
        "mkk_member_oid",
    ):
        ticker_entity = _lookup_unique(
            lookup.tickers_by_company_code,
            _lookup_identifier_key(company_code),
        )
        _append_unique_ref(refs, ticker_entity.entity_ref if ticker_entity else None)
    for name in (
        issuer.canonical_name,
        *_iter_metadata_values(metadata, "issuer_name", "company_name", "legal_name", "name"),
    ):
        ticker_entity = _lookup_unique(lookup.tickers_by_issuer_name, _lookup_name_key(name))
        _append_unique_ref(refs, ticker_entity.entity_ref if ticker_entity else None)
    return tuple(refs)


def _ticker_refs_for_related_entity(
    entity: FinanceCountryEntity,
    *,
    pack_id: str,
    lookup: _RelationshipLookup,
    issuers: Iterable[FinanceCountryEntity],
) -> tuple[str, ...]:
    refs: list[str] = []
    exchange_values = _entity_exchange_values(entity, include_employer=True)
    for ticker in _iter_metadata_values(
        entity.metadata,
        "employer_ticker",
        "employer_tickers",
        "related_ticker",
        "related_tickers",
    ):
        for key in _ticker_lookup_keys(ticker, exchange_values):
            ticker_entity = _lookup_unique(lookup.tickers_by_symbol, key)
            _append_unique_ref(refs, ticker_entity.entity_ref if ticker_entity else None)
        if not refs:
            _append_unique_ref(refs, _ticker_ref(pack_id, ticker))
    for issuer in issuers:
        for ticker_ref in _ticker_refs_for_issuer(issuer, pack_id=pack_id, lookup=lookup):
            _append_unique_ref(refs, ticker_ref)
    return tuple(refs)


def _looks_like_legal_entity_name(name: str) -> bool:
    return bool(_LEGAL_ENTITY_RE.search(name))


def _looks_like_person_name(name: str) -> bool:
    cleaned = _clean_metadata_string(name)
    if cleaned is None:
        return False
    if len(cleaned) > 96 or _URLISH_RE.search(cleaned) or _ARTIFACT_NAME_RE.search(cleaned):
        return False
    if _looks_like_legal_entity_name(cleaned):
        return False
    alpha_tokens = _PERSON_ALPHA_RE.findall(cleaned)
    if len(alpha_tokens) < 2 or len(alpha_tokens) > 7:
        return False
    if sum(len(token) > 1 for token in alpha_tokens) < 2:
        return False
    return True


def _relationship_actor_kind(entity: FinanceCountryEntity) -> str | None:
    entity_type = entity.entity_type.casefold()
    category = entity.category
    name = entity.canonical_name
    if _URLISH_RE.search(name) or _ARTIFACT_NAME_RE.search(name):
        return None
    if entity_type == "person":
        if _looks_like_person_name(name):
            return "person"
        if category in {
            "shareholder",
            "beneficial_owner",
            "owner",
            "controller",
        } and _looks_like_legal_entity_name(name):
            return "organization"
        return None
    if entity_type == "organization" and category not in {"issuer", "ticker"}:
        return "organization"
    return None


def _role_text(entity: FinanceCountryEntity) -> str:
    pieces = [
        entity.category,
        entity.entity_type,
        *_iter_metadata_values(
            entity.metadata,
            "role_class",
            "role_title",
            "title",
            "position",
            "relationship",
            "relation",
            "nature_of_control",
            "source_form",
        ),
    ]
    return " ".join(piece.casefold() for piece in pieces if piece)


def _person_role_relation(entity: FinanceCountryEntity) -> str:
    text = _role_text(entity)
    if "founder" in text or "co founder" in text or "co-founder" in text:
        return "person_is_founder_of_issuer"
    if "chief executive" in text or re.search(r"\bceo\b", text):
        return "person_is_ceo_of_issuer"
    if "chair" in text or "chairman" in text or "chairwoman" in text:
        return "person_is_chair_of_issuer"
    if "shareholder" in text or "beneficial owner" in text or "ownership" in text:
        return "person_is_major_shareholder_of_issuer"
    if "controller" in text or "control" in text or "owner" in text:
        return "person_controls_issuer"
    if "director" in text or "board" in text:
        return "person_is_board_member_of_issuer"
    return "person_affects_employer_issuer"


def _organization_role_relation(entity: FinanceCountryEntity) -> str | None:
    text = _role_text(entity)
    if re.search(r"\b(supplier|supplies|vendor|upstream|input\s+provider)\b", text):
        return "issuer_supplier_to_issuer"
    if re.search(r"\b(customer|client|buyer|offtaker|off-taker|downstream)\b", text):
        return "issuer_customer_of_issuer"
    if re.search(
        r"\b(partner|partnership|joint\s+venture|alliance|strategic\s+relationship)\b", text
    ):
        return "issuer_partner_of_issuer"
    if (
        "shareholder" in text
        or "beneficial owner" in text
        or "ownership" in text
        or "controller" in text
        or "control" in text
        or "owner" in text
        or entity.entity_ref.casefold().startswith("finance-")
        and ":shareholder:" in entity.entity_ref.casefold()
    ):
        return "holding_company_controls_public_issuer"
    return None


def _role_confidence(relation: str, *, source_url: str) -> float:
    base = {
        "person_is_ceo_of_issuer": 0.89,
        "person_is_founder_of_issuer": 0.87,
        "person_is_chair_of_issuer": 0.86,
        "person_controls_issuer": 0.86,
        "person_is_major_shareholder_of_issuer": 0.84,
        "person_is_board_member_of_issuer": 0.82,
        "person_affects_employer_issuer": 0.78,
        "holding_company_controls_public_issuer": 0.84,
        "private_company_controls_public_issuer": 0.82,
        "issuer_supplier_to_issuer": 0.78,
        "issuer_customer_of_issuer": 0.77,
        "issuer_partner_of_issuer": 0.76,
    }.get(relation, 0.76)
    if source_url:
        return base
    return max(0.70, base - 0.04)


def _metadata_source_url(metadata: dict[str, object], fallback: str) -> str:
    return str(
        metadata.get("source_url") or metadata.get("url") or metadata.get("source") or fallback
    )


def _role_edges(
    entity: FinanceCountryEntity,
    *,
    pack_id: str,
    source_url: str,
    source_snapshot: str,
    source_year: int,
    lookup: _RelationshipLookup,
) -> Iterator[_Edge]:
    if entity.category in {"issuer", "ticker"}:
        return
    actor_kind = _relationship_actor_kind(entity)
    if actor_kind is None:
        return
    issuers = _lookup_issuers_for_entity(entity, lookup=lookup)
    if not issuers:
        return
    metadata_source_url = _metadata_source_url(entity.metadata, source_url)
    evidence_level = "direct" if metadata_source_url != source_url else "shallow"
    if actor_kind == "person":
        relation = _person_role_relation(entity)
    else:
        relation = _organization_role_relation(entity)
    if relation:
        for issuer in issuers:
            maybe_edge = _edge(
                source_ref=entity.entity_ref,
                target_ref=issuer.entity_ref,
                relation=relation,
                evidence_level=evidence_level,
                confidence=_role_confidence(relation, source_url=metadata_source_url),
                direction_hint="issuer_relationship",
                source_name="ades-finance-country-pack-relationship-metadata",
                source_url=metadata_source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
                pack_id=pack_id,
                notes=(
                    "Source-backed finance-country role relation to issuer "
                    f"({entity.category}; {issuer.canonical_name})."
                ),
            )
            if maybe_edge is not None:
                yield maybe_edge

    if actor_kind == "person":
        for ticker_ref in _ticker_refs_for_related_entity(
            entity,
            pack_id=pack_id,
            lookup=lookup,
            issuers=issuers,
        ):
            maybe_edge = _edge(
                source_ref=entity.entity_ref,
                target_ref=ticker_ref,
                relation="person_affects_employer_ticker",
                evidence_level=evidence_level,
                confidence=0.82 if metadata_source_url else 0.78,
                direction_hint="employer_equity_proxy",
                source_name="ades-finance-country-pack-relationship-metadata",
                source_url=metadata_source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
                pack_id=pack_id,
                notes="Compatibility shortcut from source-backed person-role relationship to employer ticker.",
            )
            if maybe_edge is not None:
                yield maybe_edge


def _default_proxy_edges(
    entity: FinanceCountryEntity,
    *,
    pack_id: str,
    country_code: str,
    currency: str,
    source_url: str,
    source_snapshot: str,
    source_year: int,
) -> Iterator[_Edge]:
    targets = [
        (
            f"ades:impact:currency:{currency.casefold()}",
            "entity_affects_currency_proxy",
            0.60,
            "currency_proxy",
        ),
        (
            "ades:impact:indicator:dxy",
            "entity_affects_dxy_proxy",
            0.56 if entity.entity_type.casefold() in POLITICAL_OR_PUBLIC_ENTITY_TYPES else 0.52,
            "dxy_proxy",
        ),
        (
            f"ades:impact:index:{country_code}-market",
            "entity_affects_country_index_proxy",
            0.62,
            "country_equity_proxy",
        ),
        (
            f"ades:impact:rates:{country_code}-policy-rate",
            "entity_affects_policy_rate_proxy",
            0.58,
            "rates_proxy",
        ),
        (
            f"ades:impact:risk:{country_code}-country-risk",
            "entity_affects_country_risk_proxy",
            0.57,
            "country_risk_proxy",
        ),
        (
            "ades:impact:index:global-equities",
            "entity_affects_global_equity_proxy",
            0.54,
            "global_equity_proxy",
        ),
    ]
    for target_ref, relation, confidence, direction_hint in targets:
        maybe_edge = _edge(
            source_ref=entity.entity_ref,
            target_ref=target_ref,
            relation=relation,
            evidence_level="inferred",
            confidence=confidence,
            direction_hint=direction_hint,
            source_name="ades-finance-country-pack-proxy",
            source_url=source_url,
            source_snapshot=source_snapshot,
            source_year=source_year,
            pack_id=pack_id,
            notes="Coverage proxy generated from finance-country pack membership.",
        )
        if maybe_edge is not None:
            yield maybe_edge


def _country_context_edges(
    *,
    pack_id: str,
    country_code: str,
    currency: str,
    source_url: str,
    source_snapshot: str,
    source_year: int,
) -> Iterator[_Edge]:
    source_ref = f"country:{country_code}"
    targets = [
        (
            f"ades:impact:currency:{currency.casefold()}",
            "country_affects_currency_proxy",
            0.66,
            "currency_proxy",
        ),
        (
            "ades:impact:indicator:dxy",
            "country_affects_dxy_proxy",
            0.57,
            "dxy_proxy",
        ),
        (
            f"ades:impact:index:{country_code}-market",
            "country_affects_country_index_proxy",
            0.60,
            "country_equity_proxy",
        ),
        (
            f"ades:impact:rates:{country_code}-policy-rate",
            "country_affects_policy_rate_proxy",
            0.64,
            "rates_proxy",
        ),
        (
            f"ades:impact:risk:{country_code}-country-risk",
            "country_affects_country_risk_proxy",
            0.62,
            "country_risk_proxy",
        ),
        (
            "ades:impact:index:global-equities",
            "country_affects_global_equity_proxy",
            0.52,
            "global_equity_proxy",
        ),
    ]
    for target_ref, relation, confidence, direction_hint in targets:
        maybe_edge = _edge(
            source_ref=source_ref,
            target_ref=target_ref,
            relation=relation,
            evidence_level="shallow",
            confidence=confidence,
            direction_hint=direction_hint,
            source_name="ades-finance-country-context-proxy",
            source_url=source_url,
            source_snapshot=source_snapshot,
            source_year=source_year,
            pack_id=pack_id,
            notes="Generic country context proxy generated from finance-country pack coverage.",
        )
        if maybe_edge is not None:
            yield maybe_edge


def _global_proxy_edges(
    entity: FinanceCountryEntity,
    *,
    pack_id: str,
    source_url: str,
    source_snapshot: str,
    source_year: int,
) -> Iterator[_Edge]:
    political_or_public = entity.entity_type.casefold() in POLITICAL_OR_PUBLIC_ENTITY_TYPES
    targets = [
        (
            "ades:impact:currency:usd",
            "entity_affects_usd_proxy",
            0.55 if political_or_public else 0.52,
            "usd_proxy",
        ),
        (
            "ades:impact:indicator:dxy",
            "entity_affects_dxy_proxy",
            0.57 if political_or_public else 0.53,
            "dxy_proxy",
        ),
        (
            "ades:impact:index:global-equities",
            "entity_affects_global_equity_proxy",
            0.54,
            "global_equity_proxy",
        ),
        (
            "ades:impact:risk:global-policy-risk",
            "entity_affects_global_policy_risk_proxy",
            0.56 if political_or_public else 0.51,
            "global_policy_risk_proxy",
        ),
    ]
    for target_ref, relation, confidence, direction_hint in targets:
        maybe_edge = _edge(
            source_ref=entity.entity_ref,
            target_ref=target_ref,
            relation=relation,
            evidence_level="inferred",
            confidence=confidence,
            direction_hint=direction_hint,
            source_name="ades-domain-pack-global-proxy",
            source_url=source_url,
            source_snapshot=source_snapshot,
            source_year=source_year,
            pack_id=pack_id,
            notes="Coverage proxy generated from non-country domain pack membership.",
        )
        if maybe_edge is not None:
            yield maybe_edge


def _issuer_context_nodes(
    entity: FinanceCountryEntity,
    *,
    pack_id: str,
    country_code: str,
) -> Iterator[dict[str, object]]:
    if entity.category != "issuer":
        return
    base_pack = pack_id.removesuffix("-en")
    for sector in _iter_metadata_values(entity.metadata, *_ISSUER_SECTOR_KEYS):
        segment = _ref_segment(sector)
        if segment is None:
            continue
        yield _node_row(
            entity_ref=f"{base_pack}-sector:{segment}",
            canonical_name=sector,
            entity_type="sector",
            library_id=pack_id,
            is_tradable=False,
            is_seed_eligible=True,
            identifiers={"country_code": country_code, "sector": sector},
            pack_ids=pack_id,
        )
    for index_name in _iter_metadata_values(entity.metadata, *_ISSUER_INDEX_KEYS):
        segment = _ref_segment(index_name)
        if segment is None:
            continue
        yield _node_row(
            entity_ref=f"{base_pack}-index:{segment}",
            canonical_name=index_name,
            entity_type="market_index",
            library_id=pack_id,
            is_tradable=False,
            is_seed_eligible=True,
            identifiers={"country_code": country_code, "index": index_name},
            pack_ids=pack_id,
        )


def _issuer_context_edges(
    entity: FinanceCountryEntity,
    *,
    pack_id: str,
    country_code: str,
    source_url: str,
    source_snapshot: str,
    source_year: int,
    relationship_lookup: _RelationshipLookup,
) -> Iterator[_Edge]:
    if entity.category != "issuer":
        return
    base_pack = pack_id.removesuffix("-en")
    metadata_source_url = _metadata_source_url(entity.metadata, source_url)
    evidence_level = "direct" if metadata_source_url != source_url else "shallow"

    for sector in _iter_metadata_values(entity.metadata, *_ISSUER_SECTOR_KEYS):
        segment = _ref_segment(sector)
        if segment is None:
            continue
        sector_ref = f"{base_pack}-sector:{segment}"
        maybe_edge = _edge(
            source_ref=entity.entity_ref,
            target_ref=sector_ref,
            relation="issuer_in_sector",
            evidence_level=evidence_level,
            confidence=0.78,
            direction_hint="issuer_sector_membership",
            source_name="ades-finance-country-pack-sector-metadata",
            source_url=metadata_source_url,
            source_snapshot=source_snapshot,
            source_year=source_year,
            pack_id=pack_id,
            notes=f"Issuer sector membership from finance-country metadata ({sector}).",
        )
        if maybe_edge is not None:
            yield maybe_edge
        maybe_edge = _edge(
            source_ref=sector_ref,
            target_ref=f"ades:impact:index:{country_code}-market",
            relation="sector_affects_index",
            evidence_level="inferred",
            confidence=0.64,
            direction_hint="sector_country_equity_proxy",
            source_name="ades-finance-country-pack-sector-metadata",
            source_url=metadata_source_url,
            source_snapshot=source_snapshot,
            source_year=source_year,
            pack_id=pack_id,
            notes="Sector node maps to the country broad equity proxy for sector-level news.",
        )
        if maybe_edge is not None:
            yield maybe_edge

    for index_name in _iter_metadata_values(entity.metadata, *_ISSUER_INDEX_KEYS):
        segment = _ref_segment(index_name)
        if segment is None:
            continue
        index_ref = f"{base_pack}-index:{segment}"
        maybe_edge = _edge(
            source_ref=entity.entity_ref,
            target_ref=index_ref,
            relation="issuer_in_index",
            evidence_level=evidence_level,
            confidence=0.80,
            direction_hint="issuer_index_membership",
            source_name="ades-finance-country-pack-index-metadata",
            source_url=metadata_source_url,
            source_snapshot=source_snapshot,
            source_year=source_year,
            pack_id=pack_id,
            notes=f"Issuer index membership from finance-country metadata ({index_name}).",
        )
        if maybe_edge is not None:
            yield maybe_edge
        maybe_edge = _edge(
            source_ref=index_ref,
            target_ref=f"ades:impact:index:{country_code}-market",
            relation="index_affects_country_index_proxy",
            evidence_level="inferred",
            confidence=0.67,
            direction_hint="country_equity_proxy",
            source_name="ades-finance-country-pack-index-metadata",
            source_url=metadata_source_url,
            source_snapshot=source_snapshot,
            source_year=source_year,
            pack_id=pack_id,
            notes="Index node maps to the country broad equity proxy for index-level news.",
        )
        if maybe_edge is not None:
            yield maybe_edge

    for relation, prefixes, direction_hint in _COUNTERPARTY_FIELD_GROUPS:
        counterparties = _lookup_issuers_from_metadata_fields(
            entity.metadata,
            lookup=relationship_lookup,
            ticker_keys=_relationship_metadata_keys(
                prefixes, ("ticker", "tickers", "symbol", "symbols")
            ),
            isin_keys=_relationship_metadata_keys(prefixes, ("isin", "isins")),
            cik_keys=_relationship_metadata_keys(prefixes, ("cik", "ciks")),
            company_number_keys=_relationship_metadata_keys(
                prefixes, ("company_number", "company_numbers")
            ),
            company_code_keys=_relationship_metadata_keys(
                prefixes, ("company_code", "company_codes")
            ),
            name_keys=_relationship_metadata_keys(
                prefixes, ("name", "names", "issuer_name", "issuer_names")
            ),
            exchange_values=_entity_exchange_values(entity),
        )
        for counterparty in counterparties:
            source_ref = (
                entity.entity_ref
                if relation == "issuer_partner_of_issuer"
                else counterparty.entity_ref
            )
            target_ref = (
                counterparty.entity_ref
                if relation == "issuer_partner_of_issuer"
                else entity.entity_ref
            )
            if source_ref == target_ref:
                continue
            maybe_edge = _edge(
                source_ref=source_ref,
                target_ref=target_ref,
                relation=relation,
                evidence_level=evidence_level,
                confidence=0.78 if relation != "issuer_partner_of_issuer" else 0.76,
                direction_hint=direction_hint,
                source_name="ades-finance-country-pack-counterparty-metadata",
                source_url=metadata_source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
                pack_id=pack_id,
                notes=(
                    "Issuer counterparty relationship from finance-country metadata "
                    f"({counterparty.canonical_name}; {entity.canonical_name})."
                ),
            )
            if maybe_edge is not None:
                yield maybe_edge


def _metadata_edges(
    entity: FinanceCountryEntity,
    *,
    pack_id: str,
    source_url: str,
    source_snapshot: str,
    source_year: int,
    relationship_lookup: _RelationshipLookup,
) -> Iterator[_Edge]:
    metadata = entity.metadata
    if entity.category == "issuer":
        for ticker_target in _ticker_refs_for_issuer(
            entity,
            pack_id=pack_id,
            lookup=relationship_lookup,
        ):
            maybe_edge = _edge(
                source_ref=entity.entity_ref,
                target_ref=ticker_target,
                relation="issuer_has_listed_ticker",
                evidence_level="direct",
                confidence=0.90,
                direction_hint="issuer_equity_security",
                source_name="ades-finance-country-pack-metadata",
                source_url=source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
                pack_id=pack_id,
                notes="Issuer ticker relation from finance-country entity metadata and identifier lookup.",
            )
            if maybe_edge is not None:
                yield maybe_edge

    yield from _role_edges(
        entity,
        pack_id=pack_id,
        source_url=source_url,
        source_snapshot=source_snapshot,
        source_year=source_year,
        lookup=relationship_lookup,
    )

    exchanges = metadata.get("exchanges")
    if entity.entity_type.casefold() == "ticker" and isinstance(exchanges, list):
        for exchange in exchanges:
            exchange_slug = str(exchange or "").strip().casefold().replace(" ", "-")
            if not exchange_slug:
                continue
            maybe_edge = _edge(
                source_ref=entity.entity_ref,
                target_ref=f"{pack_id.removesuffix('-en')}:{exchange_slug}",
                relation="ticker_trades_on_exchange",
                evidence_level="shallow",
                confidence=0.82,
                direction_hint="listing_venue",
                source_name="ades-finance-country-pack-metadata",
                source_url=source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
                pack_id=pack_id,
                notes="Ticker exchange relation from finance-country entity metadata.",
            )
            if maybe_edge is not None:
                yield maybe_edge


def _write_tsv(path: Path, *, columns: list[str], rows: Iterable[dict[str, object]]) -> int:
    count = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})
            count += 1
    return count


def _edge_row(edge: _Edge) -> dict[str, object]:
    return {
        "source_ref": edge.source_ref,
        "target_ref": edge.target_ref,
        "relation": edge.relation,
        "evidence_level": edge.evidence_level,
        "confidence": f"{edge.confidence:.2f}",
        "direction_hint": edge.direction_hint,
        "source_name": edge.source_name,
        "source_url": edge.source_url,
        "source_snapshot": edge.source_snapshot,
        "source_year": str(edge.source_year),
        "refresh_policy": edge.refresh_policy,
        "pack_ids": edge.pack_ids,
        "notes": edge.notes,
        "compatible_event_types": ",".join(edge.compatible_event_types),
        "direction_preconditions": ",".join(edge.direction_preconditions),
    }


def _write_edge_tsv(path: Path, edges: Iterable[_Edge]) -> tuple[int, dict[str, int]]:
    count = 0
    relation_counts: dict[str, int] = {}
    seen: set[tuple[str, str, str, str]] = set()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=EDGE_COLUMNS, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for edge in edges:
            key = (edge.source_ref, edge.target_ref, edge.relation, edge.source_snapshot)
            if key in seen:
                continue
            seen.add(key)
            writer.writerow(_edge_row(edge))
            count += 1
            relation_counts[edge.relation] = relation_counts.get(edge.relation, 0) + 1
    return count, dict(sorted(relation_counts.items()))


def build_finance_country_proxy_source_lane(
    *,
    packs_root: str | Path = DEFAULT_PACKS_ROOT,
    output_root: str | Path = DEFAULT_SOURCE_OUTPUT_ROOT,
    run_id: str | None = None,
    artifact_output_root: str | Path | None = None,
    build_artifact: bool = False,
    include_starter_graph: bool = True,
    extra_proxy_pack_ids: Iterable[str] = DEFAULT_EXTRA_PROXY_PACK_IDS,
    minimum_edge_count: int = 0,
) -> FinanceCountryProxyBuildResult:
    resolved_run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(output_root).expanduser().resolve() / resolved_run_id
    node_tsv_path = output_dir / "impact_nodes.tsv"
    edge_tsv_path = output_dir / "impact_edges.tsv"
    manifest_path = output_dir / "manifest.json"

    nodes: dict[str, dict[str, object]] = {}
    all_edges: list[_Edge] = []
    generated_relation_counts: dict[str, int] = {}
    source_entity_refs: set[str] = set()
    outgoing_sources: set[str] = set()
    terminal_node_refs: set[str] = set()
    pack_summaries: list[dict[str, object]] = []
    extra_pack_summaries: list[dict[str, object]] = []
    extra_proxy_entity_refs: set[str] = set()

    pack_dirs = discover_finance_country_pack_dirs(packs_root)
    source_year = datetime.now(timezone.utc).year
    for pack_dir in pack_dirs:
        pack_id = pack_dir.name
        country_code = country_code_from_pack_id(pack_id)
        if country_code is None or country_code not in COUNTRY_CURRENCY:
            continue
        currency = COUNTRY_CURRENCY[country_code]
        source_url = _source_url_for_pack(pack_dir)
        source_snapshot = _source_snapshot_for_pack(pack_dir)
        entities = load_finance_country_pack_entities(pack_dir)
        relationship_lookup = _build_relationship_lookup(entities)
        pack_edge_start = len(all_edges)

        for proxy_node in _country_proxy_nodes(pack_id, country_code, currency):
            existing = nodes.get(str(proxy_node["entity_ref"]))
            if existing is None:
                nodes[str(proxy_node["entity_ref"])] = proxy_node
            if str(proxy_node.get("is_tradable")) == "true":
                terminal_node_refs.add(str(proxy_node["entity_ref"]))

        for edge in _country_context_edges(
            pack_id=pack_id,
            country_code=country_code,
            currency=currency,
            source_url=source_url,
            source_snapshot=source_snapshot,
            source_year=source_year,
        ):
            all_edges.append(edge)
            outgoing_sources.add(edge.source_ref)
            generated_relation_counts[edge.relation] = (
                generated_relation_counts.get(edge.relation, 0) + 1
            )

        for entity in entities:
            source_entity_refs.add(entity.entity_ref)
            node = _node_row(
                entity_ref=entity.entity_ref,
                canonical_name=entity.canonical_name,
                entity_type=entity.entity_type,
                library_id=pack_id,
                is_tradable=entity.is_tradable_terminal,
                is_seed_eligible=True,
                identifiers=_entity_identifiers(
                    entity,
                    pack_id=pack_id,
                    relationship_lookup=relationship_lookup,
                ),
                pack_ids=pack_id,
            )
            nodes.setdefault(entity.entity_ref, node)
            if entity.is_tradable_terminal:
                terminal_node_refs.add(entity.entity_ref)

            for context_node in _issuer_context_nodes(
                entity,
                pack_id=pack_id,
                country_code=country_code,
            ):
                nodes.setdefault(str(context_node["entity_ref"]), context_node)

            for edge in _issuer_context_edges(
                entity,
                pack_id=pack_id,
                country_code=country_code,
                source_url=source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
                relationship_lookup=relationship_lookup,
            ):
                all_edges.append(edge)
                outgoing_sources.add(edge.source_ref)
                generated_relation_counts[edge.relation] = (
                    generated_relation_counts.get(edge.relation, 0) + 1
                )

            for edge in _metadata_edges(
                entity,
                pack_id=pack_id,
                source_url=source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
                relationship_lookup=relationship_lookup,
            ):
                all_edges.append(edge)
                outgoing_sources.add(edge.source_ref)
                generated_relation_counts[edge.relation] = (
                    generated_relation_counts.get(edge.relation, 0) + 1
                )
                if edge.target_ref not in nodes and _is_pack_ticker_ref(pack_id, edge.target_ref):
                    nodes[edge.target_ref] = _node_row(
                        entity_ref=edge.target_ref,
                        canonical_name=edge.target_ref.rsplit(":", 1)[-1],
                        entity_type="ticker",
                        library_id=pack_id,
                        is_tradable=True,
                        is_seed_eligible=True,
                        identifiers=_synthetic_ticker_identifiers(
                            country_code=country_code,
                            edge=edge,
                            source_entity=entity,
                        ),
                        pack_ids=pack_id,
                    )
                    terminal_node_refs.add(edge.target_ref)

            for edge in _default_proxy_edges(
                entity,
                pack_id=pack_id,
                country_code=country_code,
                currency=currency,
                source_url=source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
            ):
                all_edges.append(edge)
                outgoing_sources.add(edge.source_ref)
                generated_relation_counts[edge.relation] = (
                    generated_relation_counts.get(edge.relation, 0) + 1
                )

        pack_summaries.append(
            {
                "pack_id": pack_id,
                "country_code": country_code,
                "entity_count": len(entities),
                "edge_count": len(all_edges) - pack_edge_start,
                "source_snapshot": source_snapshot,
            }
        )

    for extra_pack_dir in discover_extra_proxy_pack_dirs(
        packs_root,
        pack_ids=extra_proxy_pack_ids,
    ):
        pack_id = extra_pack_dir.name
        source_url = _source_url_for_pack(extra_pack_dir)
        source_snapshot = _source_snapshot_for_pack(extra_pack_dir)
        entities = load_finance_country_pack_entities(extra_pack_dir)
        pack_edge_start = len(all_edges)
        pack_entity_count = 0

        for proxy_node in _global_proxy_nodes(pack_id):
            existing = nodes.get(str(proxy_node["entity_ref"]))
            if existing is None:
                nodes[str(proxy_node["entity_ref"])] = proxy_node
            terminal_node_refs.add(str(proxy_node["entity_ref"]))

        for entity in entities:
            if entity.entity_ref in source_entity_refs:
                continue
            source_entity_refs.add(entity.entity_ref)
            extra_proxy_entity_refs.add(entity.entity_ref)
            pack_entity_count += 1
            node = _node_row(
                entity_ref=entity.entity_ref,
                canonical_name=entity.canonical_name,
                entity_type=entity.entity_type,
                library_id=pack_id,
                is_tradable=entity.is_tradable_terminal,
                is_seed_eligible=True,
                identifiers=_entity_identifiers(entity, pack_id=pack_id),
                pack_ids=pack_id,
            )
            nodes.setdefault(entity.entity_ref, node)
            if entity.is_tradable_terminal:
                terminal_node_refs.add(entity.entity_ref)

            for edge in _global_proxy_edges(
                entity,
                pack_id=pack_id,
                source_url=source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
            ):
                all_edges.append(edge)
                outgoing_sources.add(edge.source_ref)
                generated_relation_counts[edge.relation] = (
                    generated_relation_counts.get(edge.relation, 0) + 1
                )

        extra_pack_summaries.append(
            {
                "pack_id": pack_id,
                "entity_count": pack_entity_count,
                "edge_count": len(all_edges) - pack_edge_start,
                "source_snapshot": source_snapshot,
            }
        )

    uncovered = sorted(source_entity_refs - outgoing_sources)
    node_count = _write_tsv(
        node_tsv_path,
        columns=NODE_COLUMNS,
        rows=(nodes[key] for key in sorted(nodes)),
    )
    edge_count, relation_counts = _write_edge_tsv(edge_tsv_path, all_edges)
    edge_family_counts = _edge_family_counts_from_relations(relation_counts)
    if edge_count < minimum_edge_count:
        raise ValueError(
            f"Generated {edge_count} impact edges, below minimum_edge_count={minimum_edge_count}"
        )

    artifact_path: Path | None = None
    artifact_node_count: int | None = None
    artifact_edge_count: int | None = None
    artifact_warnings: list[str] = []
    if build_artifact:
        artifact_root = (
            Path(artifact_output_root or DEFAULT_ARTIFACT_OUTPUT_ROOT).expanduser().resolve()
        )
        artifact_dir = artifact_root / resolved_run_id
        if include_starter_graph:
            with starter_source_paths() as (starter_node_path, starter_edge_path):
                response = build_market_graph_store(
                    node_tsv_paths=[starter_node_path, node_tsv_path],
                    edge_tsv_paths=[starter_edge_path, edge_tsv_path],
                    output_dir=artifact_dir,
                    artifact_version=resolved_run_id,
                )
        else:
            response = build_market_graph_store(
                node_tsv_paths=[node_tsv_path],
                edge_tsv_paths=[edge_tsv_path],
                output_dir=artifact_dir,
                artifact_version=resolved_run_id,
            )
        artifact_path = Path(response.artifact_path)
        artifact_node_count = response.node_count
        artifact_edge_count = response.edge_count
        artifact_warnings = list(response.warnings)

    manifest = {
        "schema_version": 1,
        "run_id": resolved_run_id,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "packs_root": str(Path(packs_root).expanduser().resolve()),
        "output_dir": str(output_dir),
        "node_tsv_path": str(node_tsv_path),
        "edge_tsv_path": str(edge_tsv_path),
        "artifact_path": str(artifact_path) if artifact_path else None,
        "artifact_node_count": artifact_node_count,
        "artifact_edge_count": artifact_edge_count,
        "artifact_warnings": artifact_warnings,
        "include_starter_graph": bool(include_starter_graph),
        "pack_count": len(pack_summaries),
        "extra_proxy_pack_count": len(extra_pack_summaries),
        "entity_count": len(source_entity_refs),
        "extra_proxy_entity_count": len(extra_proxy_entity_refs),
        "node_count": node_count,
        "edge_count": edge_count,
        "edge_family_counts": edge_family_counts,
        "terminal_node_count": len(terminal_node_refs),
        "uncovered_entity_count": len(uncovered),
        "uncovered_entity_refs_sample": uncovered[:25],
        "generated_relation_counts_before_dedupe": dict(sorted(generated_relation_counts.items())),
        "relation_counts": relation_counts,
        "pack_summaries": pack_summaries,
        "extra_proxy_pack_summaries": extra_pack_summaries,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    return FinanceCountryProxyBuildResult(
        run_id=resolved_run_id,
        output_dir=output_dir,
        node_tsv_path=node_tsv_path,
        edge_tsv_path=edge_tsv_path,
        manifest_path=manifest_path,
        artifact_path=artifact_path,
        artifact_node_count=artifact_node_count,
        artifact_edge_count=artifact_edge_count,
        pack_count=len(pack_summaries),
        extra_proxy_pack_count=len(extra_pack_summaries),
        entity_count=len(source_entity_refs),
        extra_proxy_entity_count=len(extra_proxy_entity_refs),
        node_count=node_count,
        edge_count=edge_count,
        terminal_node_count=len(terminal_node_refs),
        uncovered_entity_count=len(uncovered),
        relation_counts=dict(sorted(relation_counts.items())),
        edge_family_counts=edge_family_counts,
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packs-root", default=str(DEFAULT_PACKS_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_SOURCE_OUTPUT_ROOT))
    parser.add_argument("--run-id")
    parser.add_argument("--build-artifact", action="store_true")
    parser.add_argument(
        "--no-include-starter-graph",
        action="store_true",
        help="Build the SQLite artifact from only the generated finance-country proxy lane.",
    )
    parser.add_argument("--artifact-output-root", default=str(DEFAULT_ARTIFACT_OUTPUT_ROOT))
    parser.add_argument(
        "--extra-proxy-pack-id",
        action="append",
        dest="extra_proxy_pack_ids",
        help="Additional non-country pack id to proxy to global tradable terminals. Can be repeated.",
    )
    parser.add_argument(
        "--no-extra-proxy-packs",
        action="store_true",
        help="Disable the default non-country domain pack proxy enrichment.",
    )
    parser.add_argument("--minimum-edge-count", type=int, default=0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = build_finance_country_proxy_source_lane(
        packs_root=args.packs_root,
        output_root=args.output_root,
        run_id=args.run_id,
        artifact_output_root=args.artifact_output_root,
        build_artifact=args.build_artifact,
        include_starter_graph=not args.no_include_starter_graph,
        extra_proxy_pack_ids=()
        if args.no_extra_proxy_packs
        else (args.extra_proxy_pack_ids or DEFAULT_EXTRA_PROXY_PACK_IDS),
        minimum_edge_count=args.minimum_edge_count,
    )
    print(json.dumps(result.to_json_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
