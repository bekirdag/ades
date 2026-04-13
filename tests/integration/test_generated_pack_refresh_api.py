import json
from pathlib import Path

from ades import build_finance_source_bundle, refresh_generated_packs
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_public_api_refreshes_generated_finance_pack_candidates_by_default(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    response = refresh_generated_packs(
        [bundle.bundle_dir],
        output_dir=tmp_path / "refresh-output",
    )

    assert response.passed is True
    assert response.materialize_registry is False
    assert response.registry_materialized is False
    assert response.registry is None
    assert response.packs[0].pack_id == "finance-en"
    assert response.packs[0].quality.passed is True
    assert response.packs[0].report.publishable_sources_only is True
    assert "registry_build_skipped:candidate_only" in response.warnings


def test_public_api_can_refresh_and_materialize_generated_finance_pack_release(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    response = refresh_generated_packs(
        [bundle.bundle_dir],
        output_dir=tmp_path / "refresh-output",
        materialize_registry=True,
    )

    assert response.passed is True
    assert response.registry is not None
    assert response.registry.pack_count == 1


def test_public_api_skips_generated_pack_publication_on_source_governance_failure(
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

    response = refresh_generated_packs(
        [bundle.bundle_dir],
        output_dir=tmp_path / "refresh-output",
    )

    assert response.passed is False
    assert response.registry is None
    assert "registry_build_skipped:source_governance_failed" in response.warnings
    assert response.packs[0].report.publishable_sources_only is False
