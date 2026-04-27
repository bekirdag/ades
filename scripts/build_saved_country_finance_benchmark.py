#!/usr/bin/env python3
"""Build a fixed saved-RSS benchmark slice for country-finance routing checks."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

from ades.config import Settings
from ades.vector.service import _collection_alias_candidates, _query_filter_payload


LIVE_REVIEW_ROOT = Path("docs/planning/live-rss-vector-reviews")


@dataclass(frozen=True)
class _BenchmarkCaseSpec:
    benchmark_id: str
    summary_path: Path
    selector_kind: Literal["case_slug", "article_index"]
    selector_value: str | int
    domain_hint: str
    country_hint: str
    pack_id: str = "general-en"


DEFAULT_CASE_SPECS = (
    _BenchmarkCaseSpec(
        benchmark_id="uk-business-cityam-intel",
        summary_path=LIVE_REVIEW_ROOT
        / "unseen-hybrid-live-curated-20260425T075139Z"
        / "summary.json",
        selector_kind="case_slug",
        selector_value=(
            "business-uk-cityam-intel-hails-highest-share-price-in-decade-as-"
            "earnings-smash-forecasts"
        ),
        domain_hint="business",
        country_hint="uk",
    ),
    _BenchmarkCaseSpec(
        benchmark_id="us-economics-dw-powell",
        summary_path=LIVE_REVIEW_ROOT
        / "unseen-hybrid-live-curated-20260425T075139Z"
        / "summary.json",
        selector_kind="case_slug",
        selector_value=(
            "economics-us-dw-us-justice-department-drops-investigation-into-fed-"
            "chair-powell"
        ),
        domain_hint="economics",
        country_hint="us",
    ),
    _BenchmarkCaseSpec(
        benchmark_id="de-economics-dw-growth-forecast",
        summary_path=LIVE_REVIEW_ROOT
        / "unseen-hint-routing-service-20260424T185406Z"
        / "summary.json",
        selector_kind="case_slug",
        selector_value="economics-de-dw-growth-forecast",
        domain_hint="economics",
        country_hint="de",
    ),
    _BenchmarkCaseSpec(
        benchmark_id="kr-economics-koreatimes-kospi",
        summary_path=LIVE_REVIEW_ROOT
        / "korea-times-unseen-20260424T082847Z"
        / "summary.json",
        selector_kind="article_index",
        selector_value=2,
        domain_hint="economics",
        country_hint="kr",
    ),
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build one fixed saved-RSS benchmark slice for country-finance "
            "routing checks."
        )
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Optional output directory. Defaults under "
            "docs/planning/live-rss-vector-reviews/."
        ),
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"Expected one JSON object in {path}.")
    return payload


def _resolve_output_dir(requested: str | None) -> Path:
    if requested:
        return Path(requested).expanduser().resolve()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (LIVE_REVIEW_ROOT / f"country-finance-benchmark-{timestamp}").resolve()


def _routing_snapshot(spec: _BenchmarkCaseSpec) -> tuple[list[str], list[str]]:
    response = SimpleNamespace(pack=spec.pack_id, entities=[])
    filter_payload = _query_filter_payload(
        response,
        settings=Settings(),
        domain_hint=spec.domain_hint,
        country_hint=spec.country_hint,
    )
    collection_aliases = _collection_alias_candidates(
        response,
        settings=Settings(),
        domain_hint=spec.domain_hint,
        country_hint=spec.country_hint,
    )
    routed_pack_ids = list(filter_payload.get("packs", [])) if isinstance(filter_payload, dict) else []
    return routed_pack_ids, collection_aliases


def _normalize_case_entry(case: dict[str, Any], *, spec: _BenchmarkCaseSpec) -> dict[str, Any]:
    plain = case.get("plain") if isinstance(case.get("plain"), dict) else {}
    hinted = case.get("hinted") if isinstance(case.get("hinted"), dict) else {}
    routed_pack_ids, collection_aliases = _routing_snapshot(spec)
    return {
        "benchmark_id": spec.benchmark_id,
        "summary_path": str(spec.summary_path.resolve()),
        "selector_kind": spec.selector_kind,
        "selector_value": spec.selector_value,
        "source": case.get("source"),
        "title": case.get("title"),
        "url": case.get("url"),
        "domain_hint": spec.domain_hint,
        "country_hint": spec.country_hint,
        "word_count": case.get("word_count"),
        "issue_types": list(case.get("issue_types", []))
        if isinstance(case.get("issue_types"), list)
        else [],
        "plain_entity_count": plain.get("entity_count"),
        "plain_related_entity_count": plain.get("related_entity_count"),
        "plain_warning_count": len(plain.get("warnings", []))
        if isinstance(plain.get("warnings"), list)
        else 0,
        "hinted_entity_count": hinted.get("entity_count"),
        "hinted_related_entity_count": hinted.get("related_entity_count"),
        "hinted_warning_count": len(hinted.get("warnings", []))
        if isinstance(hinted.get("warnings"), list)
        else 0,
        "routed_pack_ids": routed_pack_ids,
        "collection_alias_candidates": collection_aliases,
    }


def _normalize_article_entry(article: dict[str, Any], *, spec: _BenchmarkCaseSpec) -> dict[str, Any]:
    routed_pack_ids, collection_aliases = _routing_snapshot(spec)
    warnings = article.get("warnings", [])
    warning_count = len(warnings) if isinstance(warnings, list) else 0
    return {
        "benchmark_id": spec.benchmark_id,
        "summary_path": str(spec.summary_path.resolve()),
        "selector_kind": spec.selector_kind,
        "selector_value": spec.selector_value,
        "source": "korea_times",
        "title": article.get("title"),
        "url": article.get("url"),
        "domain_hint": spec.domain_hint,
        "country_hint": spec.country_hint,
        "word_count": article.get("word_count"),
        "issue_types": list(article.get("issue_types", []))
        if isinstance(article.get("issue_types"), list)
        else [],
        "plain_entity_count": article.get("entity_count"),
        "plain_related_entity_count": 0,
        "plain_warning_count": warning_count,
        "hinted_entity_count": article.get("entity_count"),
        "hinted_related_entity_count": 0,
        "hinted_warning_count": warning_count,
        "routed_pack_ids": routed_pack_ids,
        "collection_alias_candidates": collection_aliases,
    }


def _select_case(spec: _BenchmarkCaseSpec) -> dict[str, Any]:
    summary = _load_json(spec.summary_path.resolve())
    if spec.selector_kind == "case_slug":
        cases = summary.get("cases")
        if not isinstance(cases, list):
            raise SystemExit(f"Summary does not contain cases: {spec.summary_path}")
        for item in cases:
            if not isinstance(item, dict):
                continue
            if item.get("slug") == spec.selector_value:
                return _normalize_case_entry(item, spec=spec)
        raise SystemExit(
            f"Could not find case slug {spec.selector_value!r} in {spec.summary_path}"
        )
    articles = summary.get("articles")
    if not isinstance(articles, list):
        raise SystemExit(f"Summary does not contain articles: {spec.summary_path}")
    for item in articles:
        if not isinstance(item, dict):
            continue
        if item.get("index") == spec.selector_value:
            return _normalize_article_entry(item, spec=spec)
    raise SystemExit(
        f"Could not find article index {spec.selector_value!r} in {spec.summary_path}"
    )


def _markdown_summary(report: dict[str, Any]) -> str:
    aggregate = report["aggregate"]
    lines = [
        "# Country Finance Benchmark",
        "",
        f"- generated_at: `{report['generated_at']}`",
        f"- case_count: `{aggregate['case_count']}`",
        f"- countries_seen: `{', '.join(aggregate['countries_seen'])}`",
        f"- missing_required_countries: `{', '.join(aggregate['missing_required_countries']) or 'none'}`",
        f"- domains_seen: `{', '.join(aggregate['domains_seen'])}`",
        f"- missing_required_domains: `{', '.join(aggregate['missing_required_domains']) or 'none'}`",
        "",
        "## Routing Snapshot",
        "",
        f"- domain_routes: `{report['settings']['vector_search_domain_pack_routes']}`",
        f"- pack_aliases: `{report['settings']['vector_search_pack_collection_aliases']}`",
        "",
        "## Cases",
        "",
    ]
    for case in report["cases"]:
        lines.append(
            "- "
            f"`{case['benchmark_id']}` | "
            f"{case['domain_hint']}/{case['country_hint']} | "
            f"packs=`{', '.join(case['routed_pack_ids'])}` | "
            f"aliases=`{', '.join(case['collection_alias_candidates'])}` | "
            f"hinted_related=`{case['hinted_related_entity_count']}` | "
            f"title={case['title']}"
        )
    lines.append("")
    return "\n".join(lines)


def _serializable_case_spec(spec: _BenchmarkCaseSpec) -> dict[str, object]:
    payload = asdict(spec)
    payload["summary_path"] = str(spec.summary_path.resolve())
    return payload


def main() -> int:
    args = _parse_args()
    output_dir = _resolve_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    settings = Settings()
    cases = [_select_case(spec) for spec in DEFAULT_CASE_SPECS]
    countries_seen = sorted({case["country_hint"] for case in cases})
    domains_seen = sorted({case["domain_hint"] for case in cases})
    required_countries = ["de", "kr", "uk", "us"]
    required_domains = ["business", "economics"]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "settings": {
            "vector_search_collection_alias": settings.vector_search_collection_alias,
            "vector_search_domain_pack_routes": dict(settings.vector_search_domain_pack_routes),
            "vector_search_pack_collection_aliases": dict(
                settings.vector_search_pack_collection_aliases
            ),
            "vector_search_country_aliases": dict(settings.vector_search_country_aliases),
        },
        "aggregate": {
            "case_count": len(cases),
            "countries_seen": countries_seen,
            "domains_seen": domains_seen,
            "missing_required_countries": [
                country for country in required_countries if country not in countries_seen
            ],
            "missing_required_domains": [
                domain for domain in required_domains if domain not in domains_seen
            ],
        },
        "default_case_specs": [_serializable_case_spec(spec) for spec in DEFAULT_CASE_SPECS],
        "cases": cases,
    }

    summary_path = output_dir / "summary.json"
    summary_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    index_path = output_dir / "index.md"
    index_path.write_text(_markdown_summary(report), encoding="utf-8")
    print(summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
