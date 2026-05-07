"""Build finance-country pack proxy lanes for the market impact graph."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Iterable, Iterator

from .graph_builder import build_market_graph_store
from .starter import starter_source_paths

DEFAULT_BIG_DATA_ROOT = Path("/mnt/githubActions/ades_big_data")
DEFAULT_PACKS_ROOT = (
    DEFAULT_BIG_DATA_ROOT
    / "prod-deploy"
    / "registry-2026-04-24-finance-country-ready-r1"
    / "packs"
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
        return self.entity_type.casefold() in TRADABLE_ENTITY_TYPES or self.category in TRADABLE_ENTITY_TYPES


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
            metadata={"country_code": country_code_from_pack_id(pack_id) or "", "category": entity_type},
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
    clean_payload = {key: value for key, value in payload.items() if value not in (None, "", [], {})}
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
            identifiers={"country_code": country_code},
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
) -> _Edge | None:
    if source_ref == target_ref:
        return None
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
    )


def _ticker_ref(pack_id: str, ticker: object) -> str | None:
    raw = str(ticker or "").strip().upper()
    if not raw:
        return None
    return f"{pack_id.removesuffix('-en')}-ticker:{raw}"


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


def _metadata_edges(
    entity: FinanceCountryEntity,
    *,
    pack_id: str,
    source_url: str,
    source_snapshot: str,
    source_year: int,
) -> Iterator[_Edge]:
    metadata = entity.metadata
    ticker_target = _ticker_ref(pack_id, metadata.get("ticker"))
    if entity.category == "issuer" and ticker_target:
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
            notes="Issuer ticker relation from finance-country entity metadata.",
        )
        if maybe_edge is not None:
            yield maybe_edge

    employer_ticker_target = _ticker_ref(pack_id, metadata.get("employer_ticker"))
    if entity.entity_type.casefold() == "person" and employer_ticker_target:
        maybe_edge = _edge(
            source_ref=entity.entity_ref,
            target_ref=employer_ticker_target,
            relation="person_affects_employer_ticker",
            evidence_level="shallow",
            confidence=0.84,
            direction_hint="employer_equity_proxy",
            source_name="ades-finance-country-pack-metadata",
            source_url=str(metadata.get("source_url") or source_url),
            source_snapshot=source_snapshot,
            source_year=source_year,
            pack_id=pack_id,
            notes="Person employer ticker relation from finance-country public-person metadata.",
        )
        if maybe_edge is not None:
            yield maybe_edge

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
    }


def _write_edge_tsv(path: Path, edges: Iterable[_Edge]) -> tuple[int, dict[str, int]]:
    count = 0
    relation_counts: dict[str, int] = {}
    seen: set[tuple[str, str, str, str]] = set()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EDGE_COLUMNS, delimiter="\t", lineterminator="\n")
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
            generated_relation_counts[edge.relation] = generated_relation_counts.get(edge.relation, 0) + 1

        for entity in entities:
            source_entity_refs.add(entity.entity_ref)
            node = _node_row(
                entity_ref=entity.entity_ref,
                canonical_name=entity.canonical_name,
                entity_type=entity.entity_type,
                library_id=pack_id,
                is_tradable=entity.is_tradable_terminal,
                is_seed_eligible=True,
                identifiers=entity.metadata,
                pack_ids=pack_id,
            )
            nodes.setdefault(entity.entity_ref, node)
            if entity.is_tradable_terminal:
                terminal_node_refs.add(entity.entity_ref)

            for edge in _metadata_edges(
                entity,
                pack_id=pack_id,
                source_url=source_url,
                source_snapshot=source_snapshot,
                source_year=source_year,
            ):
                all_edges.append(edge)
                outgoing_sources.add(edge.source_ref)
                generated_relation_counts[edge.relation] = generated_relation_counts.get(edge.relation, 0) + 1
                if edge.target_ref not in nodes and edge.target_ref.startswith(pack_id.removesuffix("-en")):
                    nodes[edge.target_ref] = _node_row(
                        entity_ref=edge.target_ref,
                        canonical_name=edge.target_ref.rsplit(":", 1)[-1],
                        entity_type="ticker",
                        library_id=pack_id,
                        is_tradable=True,
                        is_seed_eligible=True,
                        identifiers={"country_code": country_code},
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
                generated_relation_counts[edge.relation] = generated_relation_counts.get(edge.relation, 0) + 1

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
                identifiers=entity.metadata,
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
                generated_relation_counts[edge.relation] = generated_relation_counts.get(edge.relation, 0) + 1

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
    if edge_count < minimum_edge_count:
        raise ValueError(
            f"Generated {edge_count} impact edges, below minimum_edge_count={minimum_edge_count}"
        )

    artifact_path: Path | None = None
    artifact_node_count: int | None = None
    artifact_edge_count: int | None = None
    artifact_warnings: list[str] = []
    if build_artifact:
        artifact_root = Path(artifact_output_root or DEFAULT_ARTIFACT_OUTPUT_ROOT).expanduser().resolve()
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
        "terminal_node_count": len(terminal_node_refs),
        "uncovered_entity_count": len(uncovered),
        "uncovered_entity_refs_sample": uncovered[:25],
        "generated_relation_counts_before_dedupe": dict(sorted(generated_relation_counts.items())),
        "relation_counts": relation_counts,
        "pack_summaries": pack_summaries,
        "extra_proxy_pack_summaries": extra_pack_summaries,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

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
