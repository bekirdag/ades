import importlib.util
import json
from pathlib import Path
import sys


def _load_script_module() -> object:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "public_ades_news_smoke.py"
    spec = importlib.util.spec_from_file_location(
        "public_ades_news_smoke_script",
        script_path,
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _status_payload(
    *,
    artifact_hash: str = "sha256:new",
    artifact_version: str = "artifact-v2",
) -> dict[str, object]:
    return {
        "service": "ades",
        "impact_artifact": {
            "readable": True,
            "metadata": {
                "artifact_hash": artifact_hash,
                "artifact_version": artifact_version,
            },
        },
        "bdya_impact_readiness": {"ready": True},
    }


def _news_payload(
    *,
    artifact_hash: str = "sha256:new",
    artifact_version: str = "artifact-v2",
    relation: str = "issuer_has_listed_ticker",
) -> dict[str, object]:
    return {
        "artifact_versions": {
            "impact_artifact_hash": artifact_hash,
            "impact_artifact_version": artifact_version,
        },
        "artifact_metadata": {
            "artifact_hash": artifact_hash,
            "artifact_version": artifact_version,
        },
        "event_signals": [{"event_type": "earnings_beat"}],
        "terminal_impact_candidates": [
            {
                "entity_ref": "finance-us-ticker:EXH",
                "source_entity_refs": ["finance-us-issuer:example-holdings"],
            }
        ],
        "candidate_paths": [
            {
                "terminal_ref": "finance-us-ticker:EXH",
                "relationship_path": {
                    "edges": [
                        {
                            "source_ref": "finance-us-issuer:example-holdings",
                            "target_ref": "finance-us-ticker:EXH",
                            "relation": relation,
                        }
                    ]
                },
            }
        ],
        "quality_flags": [],
    }


class _FakeClient:
    def __init__(
        self,
        *,
        status_payload: dict[str, object] | None = None,
        news_payload: dict[str, object] | None = None,
    ) -> None:
        self.status_payload = status_payload or _status_payload()
        self.news_payload = news_payload or _news_payload()
        self.calls: list[tuple[str, str, dict[str, object] | None]] = []

    def __call__(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> dict[str, object]:
        assert timeout_seconds == 7.0
        self.calls.append((method, path, payload))
        if (method, path) == ("GET", "/healthz"):
            return {"status": "ok", "version": "0.3.1"}
        if (method, path) == ("GET", "/v0/status"):
            return self.status_payload
        if (method, path) == ("POST", "/v0/news/analyze"):
            assert payload is not None
            assert payload["text"]
            return self.news_payload
        raise AssertionError(f"unexpected request: {method} {path}")


def test_public_smoke_verifies_expected_artifact_and_changed_relation() -> None:
    module = _load_script_module()
    client = _FakeClient()
    config = module.SmokeConfig(
        base_url="https://api.example.test",
        expected_artifact_hash="sha256:new",
        expected_artifact_version="artifact-v2",
        golden_cases=(
            {
                "name": "issuer",
                "title": "Issuer smoke",
                "text": "Example Holdings beat earnings estimates.",
                "packs": ["finance-us-en"],
                "expected_terminal_refs": ["finance-us-ticker:EXH"],
                "expected_event_types": ["earnings_beat"],
                "expected_relations": ["issuer_has_listed_ticker"],
                "changed_relations": [
                    {
                        "source_ref": "finance-us-issuer:example-holdings",
                        "target_ref": "finance-us-ticker:EXH",
                        "relation": "issuer_has_listed_ticker",
                    }
                ],
            },
        ),
        required_changed_relations=("issuer_has_listed_ticker",),
        timeout_seconds=7.0,
    )

    report = module.run_public_smoke(config, client=client)

    assert report["ok"] is True
    assert report["failure_codes"] == []
    assert ("GET", "/healthz", None) in client.calls
    assert ("GET", "/v0/status", None) in client.calls
    assert any(call[0:2] == ("POST", "/v0/news/analyze") for call in client.calls)


def test_public_smoke_reports_stale_endpoint_for_artifact_mismatch() -> None:
    module = _load_script_module()
    client = _FakeClient(
        status_payload=_status_payload(artifact_hash="sha256:old"),
        news_payload=_news_payload(artifact_hash="sha256:old"),
    )
    config = module.SmokeConfig(
        expected_artifact_hash="sha256:new",
        golden_cases=({"name": "default", "text": "Example Holdings beat earnings."},),
        timeout_seconds=7.0,
    )

    report = module.run_public_smoke(config, client=client)

    assert report["ok"] is False
    assert "stale_endpoint" in report["failure_codes"]
    stale_checks = [
        check
        for check in report["checks"]
        if check.get("code") == "stale_endpoint" and not check["ok"]
    ]
    assert stale_checks


def test_public_smoke_loads_golden_cases_from_cases_object(tmp_path: Path) -> None:
    module = _load_script_module()
    goldens_path = tmp_path / "goldens.json"
    goldens_path.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "name": "issuer",
                        "text": "Example Holdings beat earnings estimates.",
                        "expected_terminal_refs": ["finance-us-ticker:EXH"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    cases = module._load_golden_cases(goldens_path)

    assert cases == (
        {
            "name": "issuer",
            "text": "Example Holdings beat earnings estimates.",
            "expected_terminal_refs": ["finance-us-ticker:EXH"],
        },
    )
