#!/usr/bin/env python3
"""Read-only smoke checks for the public ADES news-analysis endpoint."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import sys
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "https://api.adestool.com"
DEFAULT_NEWS_CASE = {
    "name": "default-news-analyze-smoke",
    "title": "Central bank rate smoke",
    "text": "The central bank cut interest rates as FX markets moved.",
    "packs": ["finance-en"],
    "options": {
        "include_relationship_paths": True,
        "include_terminal_candidates": True,
        "include_passive_entities": True,
        "include_tag_responses": False,
        "max_terminal_candidates": 12,
    },
}

JsonClient = Callable[[str, str, dict[str, Any] | None, float], dict[str, Any]]


@dataclass(frozen=True)
class SmokeConfig:
    base_url: str = DEFAULT_BASE_URL
    expected_artifact_hash: str | None = None
    expected_artifact_version: str | None = None
    golden_cases: tuple[dict[str, Any], ...] = (DEFAULT_NEWS_CASE,)
    required_changed_relations: tuple[str, ...] = ()
    timeout_seconds: float = 30.0


def _json_request(
    base_url: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None,
    timeout_seconds: float,
) -> dict[str, Any]:
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    body = None
    headers = {"accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["content-type"] = "application/json"
    request = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} returned HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"{method} {path} failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise RuntimeError(f"{method} {path} timed out") from exc
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{method} {path} returned invalid JSON") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError(f"{method} {path} returned JSON {type(decoded).__name__}, expected object")
    return decoded


def _extract_status_artifact(status_payload: dict[str, Any]) -> dict[str, Any]:
    impact_artifact = status_payload.get("impact_artifact")
    if not isinstance(impact_artifact, dict):
        return {}
    metadata = impact_artifact.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    return {
        "artifact_hash": metadata.get("artifact_hash") or impact_artifact.get("artifact_hash"),
        "artifact_version": metadata.get("artifact_version")
        or impact_artifact.get("artifact_version"),
        "generated_at": impact_artifact.get("generated_at"),
        "modified_at": impact_artifact.get("modified_at"),
        "configured_path": impact_artifact.get("configured_path"),
        "exists": impact_artifact.get("exists"),
        "readable": impact_artifact.get("readable"),
        "warnings": impact_artifact.get("warnings") or [],
    }


def _extract_analyze_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    artifact_metadata = payload.get("artifact_metadata")
    if not isinstance(artifact_metadata, dict):
        artifact_metadata = {}
    artifact_versions = payload.get("artifact_versions")
    if not isinstance(artifact_versions, dict):
        artifact_versions = {}
    return {
        "artifact_hash": artifact_metadata.get("artifact_hash")
        or artifact_versions.get("impact_artifact_hash"),
        "artifact_version": artifact_metadata.get("artifact_version")
        or artifact_versions.get("impact_artifact_version"),
        "graph_version": artifact_metadata.get("graph_version")
        or artifact_versions.get("impact_graph_version"),
        "artifact_id": artifact_metadata.get("artifact_id"),
    }


def _check(
    checks: list[dict[str, Any]],
    name: str,
    ok: bool,
    *,
    observed: Any = None,
    expected: Any = None,
    code: str | None = None,
) -> None:
    result: dict[str, Any] = {"name": name, "ok": ok}
    if code is not None:
        result["code"] = code
    if expected is not None:
        result["expected"] = expected
    if observed is not None:
        result["observed"] = observed
    checks.append(result)


def _terminal_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = payload.get("terminal_impact_candidates", payload.get("terminal_candidates", []))
    if not isinstance(candidates, list):
        return []
    return [candidate for candidate in candidates if isinstance(candidate, dict)]


def _candidate_paths(payload: dict[str, Any]) -> list[dict[str, Any]]:
    paths = payload.get("candidate_paths", [])
    if not isinstance(paths, list):
        return []
    return [path for path in paths if isinstance(path, dict)]


def _event_types(payload: dict[str, Any]) -> set[str]:
    event_signals = payload.get("event_signals", [])
    if not isinstance(event_signals, list):
        return set()
    observed: set[str] = set()
    for signal in event_signals:
        if isinstance(signal, dict) and isinstance(signal.get("event_type"), str):
            observed.add(signal["event_type"])
        elif isinstance(signal, str):
            observed.add(signal)
    return observed


def _relationship_edges(payload: dict[str, Any]) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    for candidate_path in _candidate_paths(payload):
        relationship_path = candidate_path.get("relationship_path")
        if not isinstance(relationship_path, dict):
            continue
        raw_edges = relationship_path.get("edges", [])
        if not isinstance(raw_edges, list):
            continue
        edges.extend(edge for edge in raw_edges if isinstance(edge, dict))
    return edges


def _relationship_labels(payload: dict[str, Any]) -> set[str]:
    return {
        str(edge["relation"])
        for edge in _relationship_edges(payload)
        if isinstance(edge.get("relation"), str)
    }


def _relation_edge_matches(edge: dict[str, Any], expected: dict[str, Any]) -> bool:
    for key in ("relation", "source_ref", "target_ref"):
        if key in expected and edge.get(key) != expected[key]:
            return False
    return isinstance(expected.get("relation"), str)


def _load_golden_cases(path: Path | None) -> tuple[dict[str, Any], ...]:
    if path is None:
        return (DEFAULT_NEWS_CASE,)
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        raw_cases = raw.get("cases")
    else:
        raw_cases = raw
    if not isinstance(raw_cases, list):
        raise ValueError("--goldens must be a JSON list or an object with a cases list")
    cases = [case for case in raw_cases if isinstance(case, dict)]
    if len(cases) != len(raw_cases):
        raise ValueError("all golden cases must be JSON objects")
    return tuple(cases)


def _case_request(case: dict[str, Any]) -> dict[str, Any]:
    request = {
        "title": case.get("title") or case.get("name") or "ADES public smoke",
        "text": case.get("text") or case.get("article") or "",
        "packs": case.get("packs") or ["finance-en"],
        "options": case.get("options") or DEFAULT_NEWS_CASE["options"],
    }
    extra_payload = case.get("request")
    if isinstance(extra_payload, dict):
        request.update(extra_payload)
    return request


def _check_expected_artifact(
    checks: list[dict[str, Any]],
    *,
    name_prefix: str,
    observed: dict[str, Any],
    expected_hash: str | None,
    expected_version: str | None,
) -> None:
    if expected_hash is not None:
        _check(
            checks,
            f"{name_prefix}:artifact_hash",
            observed.get("artifact_hash") == expected_hash,
            observed=observed.get("artifact_hash"),
            expected=expected_hash,
            code="stale_endpoint",
        )
    if expected_version is not None:
        _check(
            checks,
            f"{name_prefix}:artifact_version",
            observed.get("artifact_version") == expected_version,
            observed=observed.get("artifact_version"),
            expected=expected_version,
            code="stale_endpoint",
        )


def _check_golden_case(
    checks: list[dict[str, Any]],
    *,
    case: dict[str, Any],
    payload: dict[str, Any],
    status_artifact: dict[str, Any],
    expected_artifact_hash: str | None,
    expected_artifact_version: str | None,
) -> set[str]:
    case_name = str(case.get("name") or case.get("title") or "golden")
    artifact = _extract_analyze_artifact(payload)
    case_expected_hash = case.get("expected_artifact_hash") or expected_artifact_hash
    case_expected_version = case.get("expected_artifact_version") or expected_artifact_version
    _check_expected_artifact(
        checks,
        name_prefix=f"news:{case_name}",
        observed=artifact,
        expected_hash=str(case_expected_hash) if case_expected_hash else None,
        expected_version=str(case_expected_version) if case_expected_version else None,
    )
    if status_artifact.get("artifact_hash") and artifact.get("artifact_hash"):
        _check(
            checks,
            f"news:{case_name}:status_hash_match",
            artifact.get("artifact_hash") == status_artifact.get("artifact_hash"),
            observed=artifact.get("artifact_hash"),
            expected=status_artifact.get("artifact_hash"),
            code="stale_endpoint",
        )

    candidate_refs = {
        str(candidate.get("entity_ref") or candidate.get("terminal_ref"))
        for candidate in _terminal_candidates(payload)
        if candidate.get("entity_ref") or candidate.get("terminal_ref")
    }
    for terminal_ref in case.get("expected_terminal_refs") or ():
        _check(
            checks,
            f"news:{case_name}:terminal:{terminal_ref}",
            terminal_ref in candidate_refs,
            observed=sorted(candidate_refs),
            expected=terminal_ref,
            code="missing_terminal_candidate",
        )

    observed_events = _event_types(payload)
    for event_type in case.get("expected_event_types") or ():
        _check(
            checks,
            f"news:{case_name}:event:{event_type}",
            event_type in observed_events,
            observed=sorted(observed_events),
            expected=event_type,
            code="missing_event_signal",
        )

    relationship_edges = _relationship_edges(payload)
    observed_relations = _relationship_labels(payload)
    for relation in case.get("expected_relations") or ():
        _check(
            checks,
            f"news:{case_name}:relation:{relation}",
            relation in observed_relations,
            observed=sorted(observed_relations),
            expected=relation,
            code="missing_relation",
        )
    for relation in case.get("changed_relations") or ():
        relation_name = relation.get("relation") if isinstance(relation, dict) else relation
        if not isinstance(relation_name, str):
            continue
        if isinstance(relation, dict):
            relation_ok = any(_relation_edge_matches(edge, relation) for edge in relationship_edges)
            observed = relationship_edges
        else:
            relation_ok = relation_name in observed_relations
            observed = sorted(observed_relations)
        _check(
            checks,
            f"news:{case_name}:changed_relation:{relation_name}",
            relation_ok,
            observed=observed,
            expected=relation,
            code="missing_changed_relation",
        )

    quality_flags = payload.get("quality_flags", [])
    if not isinstance(quality_flags, list):
        quality_flags = []
    forbidden_flags = case.get("forbidden_quality_flags")
    if forbidden_flags is None:
        has_terminal_expectations = any(
            case.get(key)
            for key in (
                "expected_terminal_refs",
                "expected_relations",
                "changed_relations",
            )
        )
        forbidden_flags = ["NO_TERMINAL_IMPACT_CANDIDATES"] if has_terminal_expectations else []
    for flag in forbidden_flags:
        _check(
            checks,
            f"news:{case_name}:forbidden_flag:{flag}",
            flag not in quality_flags,
            observed=quality_flags,
            expected=f"without {flag}",
            code="forbidden_quality_flag",
        )
    return observed_relations


def run_public_smoke(config: SmokeConfig, client: JsonClient | None = None) -> dict[str, Any]:
    client = client or (
        lambda method, path, payload, timeout: _json_request(
            config.base_url,
            method,
            path,
            payload,
            timeout,
        )
    )
    checks: list[dict[str, Any]] = []
    observed_relations: set[str] = set()

    try:
        health = client("GET", "/healthz", None, config.timeout_seconds)
        health_status = health.get("status")
        _check(
            checks,
            "healthz",
            health_status == "ok",
            observed=health,
            expected={"status": "ok"},
            code="health_failed",
        )

        status_payload = client("GET", "/v0/status", None, config.timeout_seconds)
        status_artifact = _extract_status_artifact(status_payload)
        _check(
            checks,
            "status:impact_artifact_readable",
            bool(status_artifact.get("artifact_hash")) and status_artifact.get("readable") is not False,
            observed=status_artifact,
            expected="impact artifact hash and readable=true",
            code="status_artifact_unavailable",
        )
        _check_expected_artifact(
            checks,
            name_prefix="status",
            observed=status_artifact,
            expected_hash=config.expected_artifact_hash,
            expected_version=config.expected_artifact_version,
        )

        for case in config.golden_cases:
            case_payload = client(
                "POST",
                "/v0/news/analyze",
                _case_request(case),
                config.timeout_seconds,
            )
            _check(
                checks,
                f"news:{case.get('name') or case.get('title') or 'golden'}:response",
                isinstance(case_payload, dict),
                observed=sorted(case_payload),
                expected="JSON object",
                code="news_analyze_failed",
            )
            observed_relations.update(
                _check_golden_case(
                    checks,
                    case=case,
                    payload=case_payload,
                    status_artifact=status_artifact,
                    expected_artifact_hash=config.expected_artifact_hash,
                    expected_artifact_version=config.expected_artifact_version,
                )
            )

        for relation in config.required_changed_relations:
            _check(
                checks,
                f"changed_relation:{relation}",
                relation in observed_relations,
                observed=sorted(observed_relations),
                expected=relation,
                code="missing_changed_relation",
            )
    except Exception as exc:  # noqa: BLE001 - smoke scripts should report structured failures.
        _check(checks, "runtime", False, observed=str(exc), code="runtime_error")

    failures = [check for check in checks if not check["ok"]]
    return {
        "ok": not failures,
        "base_url": config.base_url,
        "checks": checks,
        "failure_codes": sorted(
            {str(check["code"]) for check in failures if "code" in check}
        ),
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run read-only public ADES /v0/news/analyze smoke checks.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--expected-artifact-hash")
    parser.add_argument("--expected-artifact-version")
    parser.add_argument(
        "--goldens",
        type=Path,
        help="JSON list, or object with a cases list, of news/analyze golden examples.",
    )
    parser.add_argument(
        "--require-changed-relation",
        action="append",
        default=[],
        help="Relation name that must be observed in at least one golden candidate path.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        golden_cases = _load_golden_cases(args.goldens)
    except Exception as exc:  # noqa: BLE001 - CLI should return JSON on input errors.
        print(
            json.dumps(
                {
                    "ok": False,
                    "base_url": args.base_url,
                    "checks": [
                        {
                            "name": "goldens",
                            "ok": False,
                            "code": "invalid_goldens",
                            "observed": str(exc),
                        }
                    ],
                    "failure_codes": ["invalid_goldens"],
                },
                sort_keys=True,
            )
        )
        return 2
    report = run_public_smoke(
        SmokeConfig(
            base_url=args.base_url,
            expected_artifact_hash=args.expected_artifact_hash,
            expected_artifact_version=args.expected_artifact_version,
            golden_cases=golden_cases,
            required_changed_relations=tuple(args.require_changed_relation),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
