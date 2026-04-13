import json
from pathlib import Path

from fastapi.testclient import TestClient

from ades import build_finance_source_bundle
from ades.service.app import create_app
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_refresh_generated_packs_endpoint_builds_candidates_by_default(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    client = TestClient(create_app())

    response = client.post(
        "/v0/registry/refresh-generated-packs",
        json={
            "bundle_dirs": [bundle.bundle_dir],
            "output_dir": str(tmp_path / "refresh-output"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["passed"] is True
    assert payload["pack_count"] == 1
    assert payload["materialize_registry"] is False
    assert payload["registry_materialized"] is False
    assert payload["registry"] is None
    assert payload["packs"][0]["pack_id"] == "finance-en"
    assert payload["packs"][0]["report"]["publishable_sources_only"] is True
    assert "registry_build_skipped:candidate_only" in payload["warnings"]


def test_refresh_generated_packs_endpoint_can_materialize_registry_release(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    client = TestClient(create_app())

    response = client.post(
        "/v0/registry/refresh-generated-packs",
        json={
            "bundle_dirs": [bundle.bundle_dir],
            "output_dir": str(tmp_path / "refresh-output"),
            "materialize_registry": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["registry"]["pack_count"] == 1


def test_refresh_generated_packs_endpoint_reports_source_governance_failure(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    bundle_manifest_path = Path(bundle.bundle_dir) / "bundle.json"
    bundle_manifest = json.loads(bundle_manifest_path.read_text(encoding="utf-8"))
    bundle_manifest["sources"][0]["license_class"] = "build-only"
    bundle_manifest_path.write_text(
        json.dumps(bundle_manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    client = TestClient(create_app())

    response = client.post(
        "/v0/registry/refresh-generated-packs",
        json={
            "bundle_dirs": [bundle.bundle_dir],
            "output_dir": str(tmp_path / "refresh-output"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["passed"] is False
    assert payload["registry"] is None
    assert "registry_build_skipped:source_governance_failed" in payload["warnings"]
    assert payload["packs"][0]["report"]["publishable_sources_only"] is False
