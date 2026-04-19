import json
from pathlib import Path

import ades.packs.finance_country as finance_country_module
from ades.packs.finance_bundle import build_finance_source_bundle
from ades.packs.finance_country import (
    build_finance_country_source_bundles,
    fetch_finance_country_source_snapshots,
)
from ades.packs.general_bundle import build_general_source_bundle
from ades.packs.refresh import refresh_generated_pack_registry
from tests.finance_bundle_helpers import (
    create_finance_country_profiles,
    create_finance_raw_snapshots,
)
from tests.general_bundle_helpers import create_general_raw_snapshots


def test_finance_country_download_source_follows_http_redirects(
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "source.html"
    captured: dict[str, object] = {}

    class _FakeResponse:
        content = b"<html>redirect target</html>\n"

        def raise_for_status(self) -> None:
            return None

    def fake_get(url: str, *, headers: dict[str, str], follow_redirects: bool, timeout: float):
        captured["url"] = url
        captured["headers"] = headers
        captured["follow_redirects"] = follow_redirects
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(finance_country_module.httpx, "get", fake_get)

    resolved_url = finance_country_module._download_source(
        "https://example.com/country-registry",
        destination,
        user_agent="ades-test/0.1.0",
    )

    assert resolved_url == "https://example.com/country-registry"
    assert destination.read_text(encoding="utf-8") == "<html>redirect target</html>\n"
    assert captured["headers"] == {
        "User-Agent": "ades-test/0.1.0",
        "Accept": "text/html,application/json,text/plain;q=0.9,*/*;q=0.1",
    }
    assert captured["follow_redirects"] is True
    assert (
        captured["timeout"]
        == finance_country_module.DEFAULT_COUNTRY_SOURCE_FETCH_TIMEOUT_SECONDS
    )


def test_finance_country_fetch_and_build_round_trip(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = create_finance_country_profiles(tmp_path / "remote-country-sources")
    monkeypatch.setattr(finance_country_module, "FINANCE_COUNTRY_PROFILES", profiles)

    fetch_result = fetch_finance_country_source_snapshots(
        output_dir=tmp_path / "raw" / "finance-country-en",
        snapshot="2026-04-19",
        country_codes=["us", "uk"],
    )

    assert fetch_result.snapshot == "2026-04-19"
    assert fetch_result.country_count == 2
    assert {item.pack_id for item in fetch_result.countries} == {
        "finance-us-en",
        "finance-uk-en",
    }
    for item in fetch_result.countries:
        assert Path(item.snapshot_dir).exists()
        assert Path(item.profile_path).exists()
        assert Path(item.curated_entities_path).exists()
        manifest = json.loads(Path(item.source_manifest_path).read_text(encoding="utf-8"))
        assert manifest["pack_id"] == item.pack_id
        assert manifest["country_code"] == item.country_code

    build_result = build_finance_country_source_bundles(
        snapshot_dir=fetch_result.snapshot_dir,
        output_dir=tmp_path / "bundles" / "finance-country-en",
        country_codes=["us", "uk"],
        version="0.2.0",
    )

    assert build_result.country_count == 2
    assert {item.pack_id for item in build_result.bundles} == {
        "finance-us-en",
        "finance-uk-en",
    }
    us_bundle = next(item for item in build_result.bundles if item.country_code == "us")
    assert us_bundle.organization_count >= 2
    assert us_bundle.exchange_count == 1
    assert us_bundle.market_index_count == 1
    bundle_manifest = json.loads(
        Path(us_bundle.bundle_manifest_path).read_text(encoding="utf-8")
    )
    assert bundle_manifest["pack_id"] == "finance-us-en"
    assert bundle_manifest["dependencies"] == ["general-en", "finance-en"]
    assert bundle_manifest["coverage"]["country_code"] == "us"


def test_finance_country_fetch_continues_when_one_source_download_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = create_finance_country_profiles(tmp_path / "remote-country-sources")
    monkeypatch.setattr(finance_country_module, "FINANCE_COUNTRY_PROFILES", profiles)
    original_download_source = finance_country_module._download_source

    def flaky_download(source_url: str | Path, destination: Path, *, user_agent: str) -> str:
        if str(source_url).endswith("uk-fca.html"):
            raise RuntimeError("javascript redirect wall")
        return original_download_source(source_url, destination, user_agent=user_agent)

    monkeypatch.setattr(finance_country_module, "_download_source", flaky_download)

    fetch_result = fetch_finance_country_source_snapshots(
        output_dir=tmp_path / "raw" / "finance-country-en",
        snapshot="2026-04-19",
        country_codes=["us", "uk"],
    )

    assert fetch_result.country_count == 2
    uk_item = next(item for item in fetch_result.countries if item.country_code == "uk")
    assert any("javascript redirect wall" in warning for warning in uk_item.warnings)
    manifest = json.loads(Path(uk_item.source_manifest_path).read_text(encoding="utf-8"))
    assert len(manifest["sources"]) == 1
    assert Path(uk_item.curated_entities_path).exists()


def test_finance_country_refresh_passes_regional_quality(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = create_finance_country_profiles(tmp_path / "remote-country-sources")
    monkeypatch.setattr(finance_country_module, "FINANCE_COUNTRY_PROFILES", profiles)
    general_snapshots = create_general_raw_snapshots(tmp_path / "general-snapshots")
    finance_snapshots = create_finance_raw_snapshots(tmp_path / "finance-snapshots")
    general_bundle = build_general_source_bundle(
        wikidata_entities_path=general_snapshots["wikidata_entities"],
        geonames_places_path=general_snapshots["geonames_places"],
        curated_entities_path=general_snapshots["curated_entities"],
        output_dir=tmp_path / "general-bundles",
    )
    finance_bundle = build_finance_source_bundle(
        sec_companies_path=finance_snapshots["sec_companies"],
        sec_submissions_path=finance_snapshots["sec_submissions"],
        sec_companyfacts_path=finance_snapshots["sec_companyfacts"],
        symbol_directory_path=finance_snapshots["symbol_directory"],
        other_listed_path=finance_snapshots["other_listed"],
        finance_people_path=finance_snapshots["finance_people"],
        curated_entities_path=finance_snapshots["curated_entities"],
        output_dir=tmp_path / "finance-bundles",
    )

    fetch_result = fetch_finance_country_source_snapshots(
        output_dir=tmp_path / "raw" / "finance-country-en",
        snapshot="2026-04-19",
        country_codes=["us"],
    )
    build_result = build_finance_country_source_bundles(
        snapshot_dir=fetch_result.snapshot_dir,
        output_dir=tmp_path / "bundles" / "finance-country-en",
        country_codes=["us"],
        version="0.2.0",
    )

    refresh_result = refresh_generated_pack_registry(
        [
            finance_bundle.bundle_dir,
            build_result.bundles[0].bundle_dir,
        ],
        dependency_bundle_dirs=[general_bundle.bundle_dir],
        output_dir=tmp_path / "refresh-output",
        materialize_registry=True,
    )

    assert refresh_result.passed is True
    assert refresh_result.registry_materialized is True
    assert refresh_result.registry is not None
    regional_result = next(
        item for item in refresh_result.packs if item.pack_id == "finance-us-en"
    )
    assert all(item.pack_id != "general-en" for item in refresh_result.packs)
    assert regional_result.quality.fixture_profile == "regional"
    index_payload = json.loads(
        Path(refresh_result.registry.index_path).read_text(encoding="utf-8")
    )
    assert sorted(index_payload["packs"]) == ["finance-en", "finance-us-en"]
