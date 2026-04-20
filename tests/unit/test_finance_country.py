import json
from pathlib import Path
from urllib.parse import urlparse

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


_GERMANY_WIKIDATA_SEARCH_FIXTURES = {
    "ABO Energy GmbH & Co. KGaA": [
        {"id": "Q33354510", "label": "ABO Wind", "description": "German renewable energy company"},
        {"id": "Q131641235", "label": "Abo Energy Gmbh & Co. Kgaa", "description": None},
    ],
    "Advanced Blockchain AG": [
        {"id": "Q119889189", "label": "Advanced Blockchain AG", "description": None},
    ],
    "Deutsche Boerse AG": [
        {"id": "Q157852", "label": "Deutsche Börse", "description": "financial services company of Germany"},
    ],
    "Mercedes-Benz Group AG": [
        {"id": "Q27530", "label": "Mercedes-Benz Group", "description": "German automotive manufacturer"},
    ],
    "Britta Seeger": [
        {"id": "Q50387551", "label": "Britta Seeger", "description": "German business executive"},
    ],
    "Harald Wilhelm": [
        {"id": "Q62090160", "label": "Harald Wilhelm", "description": "German manager"},
    ],
    "Martin Brudermuller": [
        {
            "id": "Q15834173",
            "label": "Martin Brudermüller",
            "description": "German top manager in the chemical industry and trained chemist",
        },
    ],
    "Ola Kallenius": [
        {"id": "Q19297571", "label": "Ola Källenius", "description": "Swedish manager"},
    ],
    "Sabine Kohleisen": [
        {"id": "Q111281602", "label": "Sabine Kohleisen", "description": "German manager"},
    ],
}

_GERMANY_WIKIDATA_ENTITY_FIXTURES = {
    "Q111281602": {
        "labels": {
            "en": {"value": "Sabine Kohleisen"},
            "de": {"value": "Sabine Kohleisen"},
        },
        "aliases": {},
        "descriptions": {"en": {"value": "German manager"}},
        "claims": {},
    },
    "Q119889189": {
        "labels": {
            "en": {"value": "Advanced Blockchain AG"},
            "de": {"value": "Advanced Blockchain AG"},
        },
        "aliases": {},
        "descriptions": {},
        "claims": {
            "P946": [{"mainsnak": {"datavalue": {"value": "DE000A0M93V6"}}}],
        },
    },
    "Q131641235": {
        "labels": {
            "en": {"value": "Abo Energy Gmbh & Co. Kgaa"},
            "de": {"value": "ABO Energy GmbH & Co. KGaA"},
        },
        "aliases": {},
        "descriptions": {},
        "claims": {},
    },
    "Q157852": {
        "labels": {
            "en": {"value": "Deutsche Börse"},
            "de": {"value": "Deutsche Börse"},
        },
        "aliases": {
            "en": [{"value": "Deutsche Börse AG"}, {"value": "Deutsche Boerse AG"}],
        },
        "descriptions": {"en": {"value": "financial services company of Germany"}},
        "claims": {
            "P946": [{"mainsnak": {"datavalue": {"value": "DE0005810055"}}}],
        },
    },
    "Q19297571": {
        "labels": {
            "en": {"value": "Ola Källenius"},
            "de": {"value": "Ola Källenius"},
        },
        "aliases": {},
        "descriptions": {"en": {"value": "Swedish manager"}},
        "claims": {
            "P108": [{"mainsnak": {"datavalue": {"value": {"id": "Q27530"}}}}],
        },
    },
    "Q27530": {
        "labels": {
            "en": {"value": "Mercedes-Benz Group"},
            "de": {"value": "Mercedes-Benz Group"},
        },
        "aliases": {
            "en": [{"value": "Daimler AG"}, {"value": "MBG"}],
        },
        "descriptions": {"en": {"value": "German automotive manufacturer"}},
        "claims": {
            "P946": [{"mainsnak": {"datavalue": {"value": "DE0007100000"}}}],
        },
    },
    "Q33354510": {
        "labels": {
            "en": {"value": "ABO Wind"},
            "de": {"value": "ABO Wind"},
        },
        "aliases": {"en": [{"value": "ABO Wind AG"}]},
        "descriptions": {"en": {"value": "German renewable energy company"}},
        "claims": {
            "P946": [{"mainsnak": {"datavalue": {"value": "DE0005760029"}}}],
        },
    },
    "Q50387551": {
        "labels": {
            "en": {"value": "Britta Seeger"},
            "de": {"value": "Britta Seeger"},
        },
        "aliases": {},
        "descriptions": {"en": {"value": "German business executive"}},
        "claims": {
            "P108": [{"mainsnak": {"datavalue": {"value": {"id": "Q27530"}}}}],
        },
    },
    "Q62090160": {
        "labels": {
            "en": {"value": "Harald Wilhelm"},
            "de": {"value": "Harald Wilhelm"},
        },
        "aliases": {},
        "descriptions": {"en": {"value": "German manager"}},
        "claims": {
            "P108": [{"mainsnak": {"datavalue": {"value": {"id": "Q27530"}}}}],
        },
    },
    "Q15834173": {
        "labels": {
            "en": {"value": "Martin Brudermüller"},
            "de": {"value": "Martin Brudermüller"},
        },
        "aliases": {
            "de": [{"value": "Dr. Martin Brudermüller"}],
        },
        "descriptions": {
            "en": {
                "value": "German top manager in the chemical industry and trained chemist"
            }
        },
        "claims": {},
    },
}


def _fake_germany_wikidata_api_json(*, params: dict[str, str], user_agent: str) -> dict[str, object]:
    del user_agent
    action = params["action"]
    if action == "wbsearchentities":
        return {
            "search": _GERMANY_WIKIDATA_SEARCH_FIXTURES.get(
                str(params.get("search", "")),
                [],
            )
        }
    if action == "wbgetentities":
        entity_ids = [
            entity_id.strip()
            for entity_id in str(params.get("ids", "")).split("|")
            if entity_id.strip()
        ]
        return {
            "entities": {
                entity_id: _GERMANY_WIKIDATA_ENTITY_FIXTURES[entity_id]
                for entity_id in entity_ids
                if entity_id in _GERMANY_WIKIDATA_ENTITY_FIXTURES
            }
        }
    raise AssertionError(f"Unexpected Wikidata action in test: {action}")


def test_finance_country_download_source_follows_http_redirects(
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "source.html"
    captured: dict[str, object] = {}

    class _FakeResponse:
        headers = {"content-type": "text/html"}
        content = b"<html>redirect target</html>\n"
        text = "<html>redirect target</html>\n"

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


def test_finance_country_download_source_follows_html_js_redirect(
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "official-list.csv"
    requested_urls: list[str] = []

    class _FakeHtmlRedirectResponse:
        headers = {"content-type": "text/html;charset=UTF-8"}
        content = (
            b"<html><script>window.location.replace('https://example.com/live.csv');</script></html>\n"
        )
        text = (
            "<html><script>window.location.replace('https://example.com/live.csv');</script></html>\n"
        )

        def raise_for_status(self) -> None:
            return None

    class _FakeCsvResponse:
        headers = {"content-type": "text/csv; charset=UTF-8"}
        content = b"Company Name,Country of Inc.\n3I GROUP PLC,United Kingdom\n"
        text = "Company Name,Country of Inc.\n3I GROUP PLC,United Kingdom\n"

        def raise_for_status(self) -> None:
            return None

    def fake_get(url: str, *, headers: dict[str, str], follow_redirects: bool, timeout: float):
        requested_urls.append(url)
        if url == "https://example.com/download":
            return _FakeHtmlRedirectResponse()
        if url == "https://example.com/live.csv":
            return _FakeCsvResponse()
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(finance_country_module.httpx, "get", fake_get)

    resolved_url = finance_country_module._download_source(
        "https://example.com/download",
        destination,
        user_agent="ades-test/0.1.0",
    )

    assert resolved_url == "https://example.com/live.csv"
    assert destination.read_text(encoding="utf-8") == (
        "Company Name,Country of Inc.\n3I GROUP PLC,United Kingdom\n"
    )
    assert requested_urls == [
        "https://example.com/download",
        "https://example.com/live.csv",
    ]


def test_finance_country_source_destination_filename_keeps_xlsx_extension() -> None:
    assert finance_country_module._source_destination_filename(
        source_name="deutsche-boerse-listed-companies",
        source_url="https://example.com/data/Listed-companies.xlsx",
    ) == "deutsche-boerse-listed-companies.xlsx"


def test_finance_country_fetch_and_build_round_trip(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = create_finance_country_profiles(tmp_path / "remote-country-sources")
    monkeypatch.setattr(finance_country_module, "FINANCE_COUNTRY_PROFILES", profiles)

    fetch_result = fetch_finance_country_source_snapshots(
        output_dir=tmp_path / "raw" / "finance-country-en",
        snapshot="2026-04-19",
        country_codes=["us", "au", "uk"],
    )

    assert fetch_result.snapshot == "2026-04-19"
    assert fetch_result.country_count == 3
    assert {item.pack_id for item in fetch_result.countries} == {
        "finance-au-en",
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
        people_source_plan = manifest["people_source_plan"]
        people_source_plan_path = Path(item.snapshot_dir) / people_source_plan["path"]
        assert people_source_plan_path.exists()
        people_source_payload = json.loads(
            people_source_plan_path.read_text(encoding="utf-8")
        )
        assert people_source_payload["country_code"] == item.country_code
        assert people_source_payload["sources"]
        if item.country_code == "us":
            derived_entities = manifest["derived_entities"]
            assert derived_entities
            derived_people_path = Path(item.snapshot_dir) / derived_entities[0]["path"]
            derived_people_payload = json.loads(
                derived_people_path.read_text(encoding="utf-8")
            )
            derived_names = {
                entity["canonical_text"]
                for entity in derived_people_payload["entities"]
            }
            assert {"Jane Doe", "Mark Smith", "Mary Major"} <= derived_names
        if item.country_code == "au":
            derived_entities = manifest["derived_entities"]
            derived_names: set[str] = set()
            for derived_entity in derived_entities:
                derived_payload = json.loads(
                    (Path(item.snapshot_dir) / derived_entity["path"]).read_text(
                        encoding="utf-8"
                    )
                )
                derived_names.update(
                    entity["canonical_text"] for entity in derived_payload["entities"]
                )
            assert {
                "ASX Limited",
                "WTC",
                "Vicki Carter",
                "Helen Lofthouse",
            } <= derived_names

    build_result = build_finance_country_source_bundles(
        snapshot_dir=fetch_result.snapshot_dir,
        output_dir=tmp_path / "bundles" / "finance-country-en",
        country_codes=["us", "au", "uk"],
        version="0.2.0",
    )

    assert build_result.country_count == 3
    assert {item.pack_id for item in build_result.bundles} == {
        "finance-au-en",
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
    assert bundle_manifest["coverage"]["person_count"] >= 4
    normalized_entities = Path(us_bundle.entities_path).read_text(encoding="utf-8")
    assert "Jane Doe" in normalized_entities
    assert "Mark Smith" in normalized_entities
    au_bundle = next(item for item in build_result.bundles if item.country_code == "au")
    au_manifest = json.loads(
        Path(au_bundle.bundle_manifest_path).read_text(encoding="utf-8")
    )
    assert au_manifest["coverage"]["person_count"] >= 4
    au_entities = Path(au_bundle.entities_path).read_text(encoding="utf-8")
    assert "Vicki Carter" in au_entities
    assert "ASX Limited" in au_entities


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


def test_extract_kap_company_directory_records_dedupes_permalink_entries() -> None:
    html = r"""
    {\"permaLink\":\"5369-vbt-yazilim-a-s\",\"title\":\"VBT YAZILIM A.Ş.\",\"fundCode\":\"5369\",\"fundOid\":null}
    {\"permaLink\":\"5369-vbt-yazilim-a-s\",\"title\":\"VBT YAZILIM A.Ş.\",\"fundCode\":\"5369\",\"fundOid\":null}
    {\"permaLink\":\"1234-arcelik-a-s\",\"title\":\"ARÇELİK A.Ş.\",\"fundCode\":\"1234\",\"fundOid\":null}
    """
    records = finance_country_module._extract_kap_company_directory_records(html)

    assert records == [
        {
            "permalink": "5369-vbt-yazilim-a-s",
            "title": "VBT YAZILIM A.Ş.",
            "fund_code": "5369",
        },
        {
            "permalink": "1234-arcelik-a-s",
            "title": "ARÇELİK A.Ş.",
            "fund_code": "1234",
        },
    ]


def test_finance_country_people_source_plan_covers_g20_and_marks_turkiye_implemented() -> None:
    plans = finance_country_module.finance_country_people_source_plan()

    assert len(plans) == 19
    assert set(plans) == set(finance_country_module.available_finance_country_codes())
    turkiye_plan = finance_country_module.finance_country_people_source_plan("tr")
    assert turkiye_plan["automation_status"] == "implemented"
    germany_plan = finance_country_module.finance_country_people_source_plan("de")
    assert germany_plan["automation_status"] == "partial"
    assert any(
        source["source_url"]
        == "https://www.cashmarket.deutsche-boerse.com/cash-en/Data-Tech/statistics/listed-companies"
        for source in germany_plan["sources"]
    )
    assert any(
        source["source_url"]
        == (
            "https://portal.mvp.bafin.de/database/InstInfo/"
            "sucheForm.do?6578706f7274=1&sucheButtonInstitut=Suche&institutName=&d-4012550-e=1"
        )
        for source in germany_plan["sources"]
    )
    assert any(
        source["source_url"] == "https://www.kap.org.tr/en/bist-sirketler"
        for source in turkiye_plan["sources"]
    )


def test_extract_kap_people_entities_parses_board_and_management_sections() -> None:
    detail_html = r"""
    {\"stockCode\":\"VBTYZ\"}
    [\"$\",\"$L3e\",null,{\"title\":\"Investor Relations Department or Contact People\",\"itemObject\":{\"no\":6,\"itemName\":\"Yatırımcı İlişkileri Bölümü veya Bağlantı Kurulacak Şirket Yetkilileri\",\"itemKey\":\"kpy41_acc1_yatirimci_iliskileri\",\"value\":[{\"nameSurname\":\"Zahide Koçyiğit\",\"position\":\"Chief Financial Officer\"}]}}]
    [\"$\",\"$L3e\",null,{\"title\":\"Board Members\",\"itemObject\":{\"no\":11,\"itemName\":\"Yönetim Kurulu Üyeleri\",\"itemKey\":\"kpy41_acc6_yonetim_kurulu_uyeleri\",\"value\":[{\"nameSurname\":\"BİROL BAŞARAN\",\"title\":{\"text\":\"Chairman of the Board\"}}]}}]
    [\"$\",\"$L3e\",null,{\"title\":\"Managers\",\"itemObject\":{\"no\":12,\"itemName\":\"Yönetimde Söz Sahibi Olan Personel\",\"itemKey\":\"kpy41_acc6_yonetimde_soz_sahibi\",\"value\":[{\"nameSurname\":\"ZAHİDE KOÇYİĞİT\",\"position\":\"Chief Financial Officer\"}]}}]
    """

    entities = finance_country_module._extract_kap_people_entities(
        detail_html=detail_html,
        company_title="VBT YAZILIM A.Ş.",
        stock_code="VBTYZ",
    )

    canonical_names = {entity["canonical_text"] for entity in entities}
    assert canonical_names == {"Birol Başaran", "Zahide Koçyiğit"}

    birol = next(entity for entity in entities if "Bi" in str(entity["canonical_text"]))
    assert birol["metadata"]["category"] == "director"
    assert birol["metadata"]["role_title"] == "Chairman of the Board"
    assert "Birol Basaran" in birol["aliases"]

    zahide = next(
        entity for entity in entities if entity["canonical_text"] == "Zahide Koçyiğit"
    )
    assert zahide["metadata"]["employer_ticker"] == "VBTYZ"
    assert "Zahide Kocyigit" in zahide["aliases"]


def test_extract_asx_people_entities_parses_board_and_executive_cards() -> None:
    html = """
    <div class="bio__heading"><h3>Vicki Carter</h3><p>Independent, Non-Executive Director<br />BA (Social Sciences), GradDipMgmt, GAICD</p></div>
    <div class="bio__heading"><h3>Helen Lofthouse</h3><p>Managing Director and CEO<br />BSc (Hons), GAICD</p></div>
    """

    entities = finance_country_module._extract_asx_people_entities(
        html,
        employer_name="ASX Limited",
        employer_ticker="ASX",
        role_class="director",
    )

    canonical_names = {entity["canonical_text"] for entity in entities}
    assert canonical_names == {"Vicki Carter", "Helen Lofthouse"}
    vicki = next(entity for entity in entities if entity["canonical_text"] == "Vicki Carter")
    assert vicki["metadata"]["employer_ticker"] == "ASX"
    assert vicki["metadata"]["role_title"].startswith("Independent, Non-Executive Director")


def test_extract_asx_listed_company_entities_builds_issuers_and_tickers(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "asx-listed-companies.csv"
    csv_path.write_text(
        "\n".join(
            [
                "ASX listed companies as at Mon Apr 20 06:17:09 AEST 2026",
                "",
                "Company name,ASX code,GICS industry group",
                "ASX Limited,ASX,Diversified Financials",
                "WiseTech Global Limited,WTC,Software",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    issuers, tickers = finance_country_module._extract_asx_listed_company_entities(csv_path)

    assert {entity["canonical_text"] for entity in issuers} == {
        "ASX Limited",
        "WiseTech Global Limited",
    }
    assert {entity["canonical_text"] for entity in tickers} == {"ASX", "WTC"}
    asx_issuer = next(entity for entity in issuers if entity["canonical_text"] == "ASX Limited")
    assert "ASX" in asx_issuer["aliases"]
    assert asx_issuer["metadata"]["industry_group"] == "Diversified Financials"


def test_extract_deutsche_boerse_listed_company_records_filters_german_rows(
    tmp_path: Path,
) -> None:
    profile = create_finance_country_profiles(tmp_path / "remote-country-sources")["de"]
    workbook_source = next(
        source
        for source in profile["sources"]
        if source["name"] == "deutsche-boerse-listed-companies"
    )
    workbook_path = Path(urlparse(str(workbook_source["source_url"])).path)

    records = finance_country_module._extract_deutsche_boerse_listed_company_records(
        workbook_path
    )

    assert [record["company_name"] for record in records] == [
        "Adcapital AG",
        "Advanced Blockchain AG",
        "Deutsche Boerse AG",
        "Mercedes-Benz Group AG",
    ]
    mercedes = next(record for record in records if record["ticker"] == "MBG")
    assert mercedes["isin"] == "DE0007100000"
    assert mercedes["listing_segment"] == "Prime Standard"
    assert mercedes["indices"] == ["DAX"]


def test_extract_boerse_muenchen_maccess_issuer_records_parses_isin_and_wkn(
    tmp_path: Path,
) -> None:
    profile = create_finance_country_profiles(tmp_path / "remote-country-sources")["de"]
    source = next(
        source
        for source in profile["sources"]
        if source["name"] == "boerse-muenchen-maccess-listed-companies"
    )
    html_text = Path(urlparse(str(source["source_url"])).path).read_text(encoding="utf-8")

    records = finance_country_module._extract_boerse_muenchen_maccess_issuer_records(
        html_text
    )

    assert [record["company_name"] for record in records] == [
        "ABO Energy GmbH & Co. KGaA",
        "Advanced Blockchain AG",
    ]
    abo_energy = next(
        record for record in records if record["company_name"] == "ABO Energy GmbH & Co. KGaA"
    )
    assert abo_energy["isin"] == "DE0005760029"
    assert abo_energy["wkn"] == "576002"
    assert abo_energy["listing_segment"] == "m:access"
    assert abo_energy["sector"] == "Technologie"
    assert abo_energy["issuer_website"] == "https://www.aboenergy.com/de/index.php"


def test_extract_boerse_duesseldorf_primary_market_issuer_records_parses_issuer_rows(
    tmp_path: Path,
) -> None:
    profile = create_finance_country_profiles(tmp_path / "remote-country-sources")["de"]
    source = next(
        source
        for source in profile["sources"]
        if source["name"] == "boerse-duesseldorf-primary-market"
    )
    html_text = Path(urlparse(str(source["source_url"])).path).read_text(encoding="utf-8")

    records = (
        finance_country_module._extract_boerse_duesseldorf_primary_market_issuer_records(
            html_text
        )
    )

    assert [record["company_name"] for record in records] == [
        "123fahrschule SE",
        "Advanced Blockchain AG",
    ]
    driving_school = next(
        record for record in records if record["company_name"] == "123fahrschule SE"
    )
    assert driving_school["isin"] == "DE000A2P4HL9"
    assert driving_school["listing_segment"] == "Primärmarkt"
    assert driving_school["instrument_exchange"] == "Börse Düsseldorf"


def test_extract_tradegate_order_book_identifiers_parses_code_and_wkn(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    html_text = (fixture_root / "de-tradegate-abo-energy-order-book.html").read_text(
        encoding="utf-8"
    )

    identifiers = finance_country_module._extract_tradegate_order_book_identifiers(
        html_text
    )

    assert identifiers == {
        "wkn": "576002",
        "code": "AB9",
        "isin": "DE0005760029",
    }


def test_build_germany_company_aliases_emits_short_forms_and_variants() -> None:
    aliases = finance_country_module._build_germany_company_aliases(
        "Mercedes-Benz Group AG",
        "ING BANK N.V.",
        "ABO Energy GmbH & Co. KGaA",
    )

    assert "Mercedes-Benz Group AG" in aliases
    assert "Mercedes Benz Group AG" in aliases
    assert "Mercedes-Benz Group" in aliases
    assert "ING BANK N.V." in aliases
    assert "ING BANK NV" in aliases
    assert "ING BANK" in aliases
    assert "ABO Energy GmbH & Co. KGaA" in aliases
    assert "ABO Energy GmbH & Co KGaA" in aliases
    assert "ABO Energy" in aliases


def test_extract_bafin_company_database_records_filters_to_germany_scope(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    profile = create_finance_country_profiles(fixture_root)["de"]
    source = next(
        source
        for source in profile["sources"]
        if source["name"] == "bafin-company-database-export"
    )
    csv_path = Path(urlparse(str(source["source_url"])).path)

    records = finance_country_module._extract_bafin_company_database_records(csv_path)

    assert [str(record["company_name"]) for record in records] == [
        "Commerzbank AG",
        "Ing Bank N.V.",
    ]
    commerzbank = next(
        record for record in records if record["company_name"] == "Commerzbank AG"
    )
    assert commerzbank["country"] == "Deutschland"
    assert commerzbank["trade_names"] == ["Commerzbank"]
    assert commerzbank["institution_types"] == [
        "Kreditinstitut",
        "Wertpapierinstitut",
    ]
    assert commerzbank["cross_border_credit_service_countries"] == [
        "Österreich",
        "Frankreich",
    ]
    assert commerzbank["detail_url"] == (
        "https://portal.mvp.bafin.de/database/InstInfo/"
        "institutDetails.do?cmd=loadInstitutAction&institutId=10010010"
    )
    assert commerzbank["activities"] == [
        {"activity": "Kreditgeschäft", "granted_on": "01.01.1970"},
        {"activity": "Depotgeschäft", "granted_on": "01.01.1970"},
    ]
    ing = next(record for record in records if record["company_name"] == "Ing Bank N.V.")
    assert ing["country"] == "Niederlande"
    assert ing["germany_branch"] == "ING Bank N.V. Frankfurt Branch"
    assert ing["trade_names"] == ["ING-DiBa AG"]
    assert not any(record["company_name"] == "Banque Exemple SA" for record in records)


def test_merge_germany_bafin_institutions_enriches_matching_issuer_and_ticker(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "bafin-company-database.csv"
    csv_path.write_text(
        "\n".join(
            [
                (
                    "NAME;BAK NR;REG NR;BAFIN-ID;LEI;"
                    "NATIONALE IDENTIFIKATIONSNUMMER DER BEHÖRDE DES HERKUNFTSMITGLIEDSTAATES;"
                    "PLZ;ORT;STRASSE;LAND;GATTUNG;SCHLICHTUNGSSTELLE;HANDELSNAMEN;"
                    "ZWEIGNIEDERLASSUNG IN DEUTSCHLAND;KONTAKTDATEN FÜR VERBRAUCHERBESCHWERDEN;"
                    "GRENZÜBERSCHREITENDE ERBRINGUNG VON KREDITDIENSTLEISTUNGEN IN;"
                    "ERLAUBNISSE/ZULASSUNG/TÄTIGKEITEN;ERTEILUNGSDATUM;ENDE AM;ENDEGRUND"
                ),
                (
                    "COMMERZBANK AG;10010010;HRB12345;50070001;851WYGNLUQLFZBSYGB56;;60311;"
                    "Frankfurt am Main;Kaiserplatz 16;Deutschland;Kreditinstitut;"
                    "Ombudsmann der privaten Banken;Commerzbank;;;Österreich;Kreditgeschäft;"
                    "01.01.1970;;"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    issuer_entities = [
        {
            "entity_type": "organization",
            "canonical_text": "Commerzbank AG",
            "aliases": ["CBK"],
            "entity_id": "finance-de-issuer:DE000CBK1001",
            "metadata": {
                "country_code": "de",
                "category": "issuer",
                "ticker": "CBK",
                "isin": "DE000CBK1001",
            },
        }
    ]
    ticker_entities = [
        {
            "entity_type": "ticker",
            "canonical_text": "CBK",
            "aliases": ["Commerzbank AG"],
            "entity_id": "finance-de-ticker:CBK",
            "metadata": {
                "country_code": "de",
                "category": "ticker",
                "ticker": "CBK",
                "isin": "DE000CBK1001",
            },
        }
    ]
    warnings: list[str] = []

    finance_country_module._merge_germany_bafin_institutions(
        bafin_csv_path=csv_path,
        issuer_entities=issuer_entities,
        ticker_entities=ticker_entities,
        warnings=warnings,
    )

    assert warnings == []
    assert len(issuer_entities) == 1
    assert len(ticker_entities) == 1
    issuer = issuer_entities[0]
    ticker = ticker_entities[0]
    assert issuer["metadata"]["regulated_institution"] is True
    assert issuer["metadata"]["lei"] == "851WYGNLUQLFZBSYGB56"
    assert issuer["metadata"]["bafin_bak_nrs"] == ["10010010"]
    assert issuer["metadata"]["bafin_ids"] == ["50070001"]
    assert issuer["metadata"]["bafin_activities"] == ["Kreditgeschäft"]
    assert "Commerzbank" in issuer["aliases"]
    assert ticker["metadata"]["regulated_institution"] is True
    assert ticker["metadata"]["bafin_categories"] == ["Kreditinstitut"]


def test_score_germany_issuer_wikidata_candidate_prefers_isin_backed_match() -> None:
    entity = {
        "entity_type": "organization",
        "canonical_text": "ABO Energy GmbH & Co. KGaA",
        "aliases": ["ABO Energy GmbH & Co. KGaA", "576002"],
        "entity_id": "finance-de-issuer:DE0005760029",
        "metadata": {
            "country_code": "de",
            "category": "issuer",
            "isin": "DE0005760029",
            "wkn": "576002",
        },
    }

    exact_name_without_isin_score = finance_country_module._score_germany_issuer_wikidata_candidate(
        entity=entity,
        candidate_payload=_GERMANY_WIKIDATA_ENTITY_FIXTURES["Q131641235"],
    )
    old_name_with_isin_score = finance_country_module._score_germany_issuer_wikidata_candidate(
        entity=entity,
        candidate_payload=_GERMANY_WIKIDATA_ENTITY_FIXTURES["Q33354510"],
    )

    assert old_name_with_isin_score > exact_name_without_isin_score


def test_build_germany_wikidata_entity_aliases_filters_ticker_like_org_aliases() -> None:
    aliases = finance_country_module._build_germany_wikidata_entity_aliases(
        _GERMANY_WIKIDATA_ENTITY_FIXTURES["Q27530"],
        entity_type="organization",
    )

    assert "Daimler AG" in aliases
    assert "Daimler" in aliases
    assert "Mercedes-Benz Group" in aliases
    assert "MBG" not in aliases


def test_extract_german_governance_people_entities_parses_sections_and_matches_issuer() -> None:
    issuer_lookup = finance_country_module._build_germany_issuer_lookup(
        [
            {
                "isin": "DE0007100000",
                "ticker": "MBG",
                "company_name": "Mercedes-Benz Group AG",
                "listing_segment": "Prime Standard",
                "sector": "Automobile",
                "subsector": "Automobiles",
                "instrument_exchange": "XETRA + FRANKFURT",
                "indices": ["DAX"],
            }
        ]
    )
    html = """
    <html>
      <head><title>Supervisory Board | Mercedes-Benz Group AG</title></head>
      <body>
        <h1>Supervisory Board</h1>
        <table>
          <tr><th>Dr. Martin Brudermuller</th><td>Chairman</td></tr>
        </table>
        <h1>Board of Management</h1>
        <h2>Ola Kallenius</h2>
        <p>Chairman of the Board of Management</p>
      </body>
    </html>
    """

    entities = finance_country_module._extract_german_governance_people_entities(
        html,
        issuer_lookup=issuer_lookup,
        source_name="mercedes-governance",
    )

    assert {entity["canonical_text"] for entity in entities} == {
        "Martin Brudermuller",
        "Ola Kallenius",
    }
    martin = next(
        entity for entity in entities if entity["canonical_text"] == "Martin Brudermuller"
    )
    assert "Dr. Martin Brudermuller" in martin["aliases"]
    assert martin["metadata"]["category"] == "director"
    assert martin["metadata"]["employer_ticker"] == "MBG"
    ola = next(entity for entity in entities if entity["canonical_text"] == "Ola Kallenius")
    assert ola["metadata"]["category"] == "executive_officer"
    assert ola["metadata"]["source_name"] == "mercedes-governance"


def test_extract_deutsche_boerse_company_details_people_entities_parses_board_rows(
    tmp_path: Path,
) -> None:
    profile = create_finance_country_profiles(tmp_path / "remote-country-sources")["de"]
    company_details_source = next(
        source for source in profile["sources"] if source["name"] == "mercedes-company-details"
    )
    company_details_html = Path(
        urlparse(str(company_details_source["source_url"])).path
    ).read_text(encoding="utf-8")

    entities = finance_country_module._extract_deutsche_boerse_company_details_people_entities(
        company_details_html,
        issuer_record={
            "isin": "DE0007100000",
            "ticker": "MBG",
            "company_name": "Mercedes-Benz Group AG",
            "listing_segment": "Prime Standard",
            "sector": "Automobile",
            "subsector": "Automobiles",
            "instrument_exchange": "XETRA + FRANKFURT",
            "indices": ["DAX"],
        },
    )

    assert {entity["canonical_text"] for entity in entities} == {
        "Harald Wilhelm",
        "Martin Brudermuller",
        "Ola Kallenius",
        "Sabine Kohleisen",
    }
    harald = next(
        entity for entity in entities if entity["canonical_text"] == "Harald Wilhelm"
    )
    assert harald["metadata"]["category"] == "executive_officer"
    assert harald["metadata"]["role_title"] == "Finance & Controlling"
    martin = next(
        entity for entity in entities if entity["canonical_text"] == "Martin Brudermuller"
    )
    assert "Dr. Martin Brudermuller" in martin["aliases"]
    sabine = next(
        entity for entity in entities if entity["canonical_text"] == "Sabine Kohleisen"
    )
    assert sabine["metadata"]["category"] == "director"
    assert sabine["metadata"]["employee_representative"] is True


def test_extract_deutsche_boerse_company_details_aliases_parses_former_names(
    tmp_path: Path,
) -> None:
    profile = create_finance_country_profiles(tmp_path / "remote-country-sources")["de"]
    company_details_source = next(
        source for source in profile["sources"] if source["name"] == "mercedes-company-details"
    )
    company_details_html = Path(
        urlparse(str(company_details_source["source_url"])).path
    ).read_text(encoding="utf-8")

    aliases = finance_country_module._extract_deutsche_boerse_company_details_aliases(
        company_details_html,
        company_name="Mercedes-Benz Group AG",
    )

    assert "Mercedes-Benz Group AG" in aliases
    assert "Mercedes Benz Group AG" in aliases
    assert "Mercedes-Benz Group" in aliases
    assert "Daimler AG" in aliases
    assert "Daimler" in aliases


def test_extract_unternehmensregister_register_information_search_result_matches_company_row(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    search_html = (fixture_root / "de-unternehmensregister-search-results.html").read_text(
        encoding="utf-8"
    )

    result = finance_country_module._extract_unternehmensregister_register_information_search_result(
        search_html,
        company_name="Mercedes-Benz Group AG",
    )

    assert result == {
        "company_name": "Mercedes-Benz Group AG",
        "detail_url": (
            "https://www.unternehmensregister.de/en/register-information/company/"
            "mercedes-benz-group-ag?court=Stuttgart&number=19360&type=HRB"
        ),
        "register_court": "Stuttgart",
        "register_number": "19360",
        "register_type": "HRB",
    }


def test_extract_german_governance_people_entities_parses_register_information_lists_and_definitions(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    issuer_lookup = finance_country_module._build_germany_issuer_lookup(
        [
            {
                "isin": "DE0007100000",
                "ticker": "MBG",
                "company_name": "Mercedes-Benz Group AG",
                "listing_segment": "Prime Standard",
                "sector": "Automobile",
                "subsector": "Automobiles",
                "instrument_exchange": "XETRA + FRANKFURT",
                "indices": ["DAX"],
            }
        ]
    )
    register_information_html = (
        fixture_root / "de-unternehmensregister-register-information.html"
    ).read_text(encoding="utf-8")

    entities = finance_country_module._extract_german_governance_people_entities(
        register_information_html,
        issuer_lookup=issuer_lookup,
        source_name="unternehmensregister-register-information",
    )

    assert {entity["canonical_text"] for entity in entities} == {
        "Britta Seeger",
        "Martin Brudermuller",
        "Ola Kallenius",
    }
    britta = next(
        entity for entity in entities if entity["canonical_text"] == "Britta Seeger"
    )
    assert britta["metadata"]["category"] == "executive_officer"
    assert britta["metadata"]["employer_ticker"] == "MBG"
    martin = next(
        entity
        for entity in entities
        if entity["canonical_text"] == "Martin Brudermuller"
    )
    assert martin["metadata"]["category"] == "director"
    assert martin["metadata"]["role_title"] == "Chairman"


def test_derive_germany_deutsche_boerse_entities_emits_issuers_tickers_and_people(
    tmp_path: Path,
    monkeypatch,
) -> None:
    country_dir = tmp_path / "de"
    country_dir.mkdir(parents=True, exist_ok=True)
    fixture_root = tmp_path / "remote-country-sources"
    profile = create_finance_country_profiles(fixture_root)["de"]
    company_details_source = next(
        source for source in profile["sources"] if source["name"] == "mercedes-company-details"
    )
    company_details_fixture_path = Path(
        urlparse(str(company_details_source["source_url"])).path
    )
    company_registry_search_fixture_path = (
        fixture_root / "de-unternehmensregister-search-results.html"
    )
    company_registry_detail_fixture_path = (
        fixture_root / "de-unternehmensregister-register-information.html"
    )
    company_registry_search_abo_fixture_path = (
        fixture_root / "de-unternehmensregister-search-results-abo-energy.html"
    )
    company_registry_detail_abo_fixture_path = (
        fixture_root / "de-unternehmensregister-register-information-abo-energy.html"
    )
    tradegate_abo_fixture_path = fixture_root / "de-tradegate-abo-energy-order-book.html"
    downloaded_sources: list[dict[str, object]] = []
    for source in profile["sources"]:
        source_url = str(source["source_url"])
        source_path = Path(urlparse(source_url).path)
        destination = country_dir / finance_country_module._source_destination_filename(
            source_name=str(source["name"]),
            source_url=source_url,
        )
        destination.write_bytes(source_path.read_bytes())
        downloaded_sources.append(
            {
                "name": str(source["name"]),
                "source_url": source_url,
                "path": destination.name,
                "category": str(source["category"]),
            }
        )

    requested_company_details_urls: list[str] = []
    requested_register_information_urls: list[str] = []
    requested_tradegate_urls: list[str] = []

    def fake_download_company_details(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        del user_agent
        requested_company_details_urls.append(str(source_url))
        if str(source_url).endswith("/DE0007100000/company-details"):
            destination.write_bytes(company_details_fixture_path.read_bytes())
        else:
            destination.write_text("<html><body>Company details unavailable</body></html>\n", encoding="utf-8")
        return str(source_url)

    def fake_download_register_information_search(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        del user_agent
        source_url_text = str(source_url)
        requested_register_information_urls.append(source_url_text)
        if "abo+energy+gmbh+%26+co.+kgaa" in source_url_text.casefold():
            destination.write_bytes(company_registry_search_abo_fixture_path.read_bytes())
        else:
            destination.write_bytes(company_registry_search_fixture_path.read_bytes())
        return str(source_url)

    def fake_download_register_information_detail(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        del user_agent
        source_url_text = str(source_url)
        requested_register_information_urls.append(source_url_text)
        if "abo-energy-gmbh-co-kgaa" in source_url_text.casefold():
            destination.write_bytes(company_registry_detail_abo_fixture_path.read_bytes())
        else:
            destination.write_bytes(company_registry_detail_fixture_path.read_bytes())
        return str(source_url)

    def fake_download_source(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        del user_agent
        source_url_text = str(source_url)
        if source_url_text.startswith("https://www.tradegate.de/orderbuch.php?isin="):
            requested_tradegate_urls.append(source_url_text)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if "DE0005760029" in source_url_text:
                destination.write_bytes(tradegate_abo_fixture_path.read_bytes())
            else:
                destination.write_text(
                    "<html><body>Tradegate order book unavailable</body></html>\n",
                    encoding="utf-8",
                )
            return source_url_text
        raise AssertionError(f"Unexpected source download in test: {source_url_text}")

    monkeypatch.setattr(
        finance_country_module,
        "_download_deutsche_boerse_company_details_page",
        fake_download_company_details,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_download_unternehmensregister_register_information_search_page",
        fake_download_register_information_search,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_download_unternehmensregister_register_information_detail_page",
        fake_download_register_information_detail,
    )
    monkeypatch.setattr(finance_country_module, "_download_source", fake_download_source)
    monkeypatch.setattr(
        finance_country_module,
        "_fetch_wikidata_api_json",
        _fake_germany_wikidata_api_json,
    )

    derived_files = finance_country_module._derive_germany_deutsche_boerse_entities(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent="ades-test/0.1.0",
    )

    assert {item["name"] for item in derived_files} == {
        "derived-finance-de-issuers",
        "derived-finance-de-tickers",
        "derived-finance-de-people",
    }
    issuers = finance_country_module._load_curated_entities(
        country_dir / "derived_issuer_entities.json"
    )
    assert {entity["canonical_text"] for entity in issuers} == {
        "123fahrschule SE",
        "ABO Energy GmbH & Co. KGaA",
        "Adcapital AG",
        "Advanced Blockchain AG",
        "Commerzbank AG",
        "Deutsche Boerse AG",
        "Ing Bank N.V.",
        "Mercedes-Benz Group AG",
    }
    mercedes_issuer = next(
        entity for entity in issuers if entity["canonical_text"] == "Mercedes-Benz Group AG"
    )
    assert "Daimler AG" in mercedes_issuer["aliases"]
    assert mercedes_issuer["metadata"]["register_court"] == "Stuttgart"
    assert mercedes_issuer["metadata"]["register_number"] == "19360"
    assert mercedes_issuer["metadata"]["register_type"] == "HRB"
    assert mercedes_issuer["metadata"]["wikidata_qid"] == "Q27530"
    assert mercedes_issuer["metadata"]["wikidata_entity_id"] == "wikidata:Q27530"
    assert mercedes_issuer["metadata"]["wikidata_url"] == "https://www.wikidata.org/wiki/Q27530"
    abo_energy_issuer = next(
        entity
        for entity in issuers
        if entity["canonical_text"] == "ABO Energy GmbH & Co. KGaA"
    )
    assert "576002" in abo_energy_issuer["aliases"]
    assert "ABO Energy" in abo_energy_issuer["aliases"]
    assert abo_energy_issuer["metadata"]["ticker"] == "AB9"
    assert abo_energy_issuer["metadata"]["wkn"] == "576002"
    assert abo_energy_issuer["metadata"]["issuer_website"] == (
        "https://www.aboenergy.com/de/index.php"
    )
    assert abo_energy_issuer["metadata"]["wikidata_qid"] == "Q33354510"
    assert "ABO Wind AG" in abo_energy_issuer["aliases"]
    assert "ABO Wind" in abo_energy_issuer["aliases"]
    assert "Tradegate Exchange" in abo_energy_issuer["metadata"]["instrument_exchanges"]
    assert "m:access" in abo_energy_issuer["metadata"]["listing_segments"]
    commerzbank_issuer = next(
        entity for entity in issuers if entity["canonical_text"] == "Commerzbank AG"
    )
    assert commerzbank_issuer["metadata"]["regulated_institution"] is True
    assert commerzbank_issuer["metadata"]["bafin_bak_nrs"] == ["10010010"]
    assert commerzbank_issuer["metadata"]["lei"] == "851WYGNLUQLFZBSYGB56"
    assert commerzbank_issuer["metadata"]["bafin_activities"] == [
        "Kreditgeschäft",
        "Depotgeschäft",
    ]
    assert "Commerzbank" in commerzbank_issuer["aliases"]
    assert "Mercedes-Benz Group" in mercedes_issuer["aliases"]
    assert "Mercedes Benz Group AG" in mercedes_issuer["aliases"]
    ing_issuer = next(entity for entity in issuers if entity["canonical_text"] == "Ing Bank N.V.")
    assert ing_issuer["metadata"]["regulated_institution"] is True
    assert ing_issuer["metadata"]["country"] == "Niederlande"
    assert ing_issuer["metadata"]["germany_branch"] == "ING Bank N.V. Frankfurt Branch"
    assert "Ing Bank" in ing_issuer["aliases"]
    assert "ING-DiBa AG" in ing_issuer["aliases"]
    tickers = finance_country_module._load_curated_entities(
        country_dir / "derived_ticker_entities.json"
    )
    assert {entity["canonical_text"] for entity in tickers} == {
        "AB9",
        "ADC",
        "ABX",
        "DB1",
        "MBG",
    }
    mercedes_ticker = next(entity for entity in tickers if entity["canonical_text"] == "MBG")
    assert "Daimler AG" in mercedes_ticker["aliases"]
    assert mercedes_ticker["metadata"]["register_court"] == "Stuttgart"
    assert mercedes_ticker["metadata"]["wikidata_qid"] == "Q27530"
    people = finance_country_module._load_curated_entities(
        country_dir / "derived_people_entities.json"
    )
    assert {entity["canonical_text"] for entity in people} == {
        "Alexander Koffka",
        "Andreas Höllinger",
        "Britta Seeger",
        "Harald Wilhelm",
        "Jochen Stotmeister",
        "Martin Brudermuller",
        "Ola Kallenius",
        "Sabine Kohleisen",
    }
    martin = next(
        entity for entity in people if entity["canonical_text"] == "Martin Brudermuller"
    )
    assert martin["metadata"]["category"] == "director"
    assert martin["metadata"]["employer_ticker"] == "MBG"
    assert martin["metadata"]["wikidata_qid"] == "Q15834173"
    assert "Martin Brudermüller" in martin["aliases"]
    assert "Dr. Martin Brudermüller" in martin["aliases"]
    jochen = next(
        entity for entity in people if entity["canonical_text"] == "Jochen Stotmeister"
    )
    assert jochen["metadata"]["category"] == "executive_officer"
    assert jochen["metadata"]["employer_ticker"] == "AB9"
    ola = next(entity for entity in people if entity["canonical_text"] == "Ola Kallenius")
    assert ola["metadata"]["wikidata_qid"] == "Q19297571"
    sabine = next(entity for entity in people if entity["canonical_text"] == "Sabine Kohleisen")
    assert sabine["metadata"]["wikidata_qid"] == "Q111281602"
    assert any(
        url.endswith("/DE0007100000/company-details")
        for url in requested_company_details_urls
    )
    assert any("DE0005760029" in url for url in requested_tradegate_urls)
    assert any(
        "registerPortalAdvice?" in url
        for url in requested_register_information_urls
    )
    assert any(
        "abo+energy+gmbh+%26+co.+kgaa" in url.casefold()
        for url in requested_register_information_urls
    )
    assert any(
        "register-information/company/mercedes-benz-group-ag" in url
        for url in requested_register_information_urls
    )
    assert any(
        str(source["name"]).startswith("deutsche-boerse-company-details-")
        and str(source["path"]).startswith("deutsche-boerse-company-details/")
        for source in downloaded_sources
    )
    assert any(
        str(source["name"]).startswith("unternehmensregister-register-information-search-")
        and str(source["path"]).startswith("unternehmensregister-register-information-search/")
        for source in downloaded_sources
    )
    assert any(
        str(source["name"]).startswith("unternehmensregister-register-information-")
        and str(source["path"]).startswith("unternehmensregister-register-information/")
        for source in downloaded_sources
    )
    assert any(
        str(source["name"]).startswith("tradegate-order-book-")
        and str(source["path"]).startswith("tradegate-order-book/")
        for source in downloaded_sources
    )
    assert any(
        str(source["name"]) == "wikidata-germany-entity-resolution"
        and str(source["path"]) == "wikidata-germany-entity-resolution.json"
        for source in downloaded_sources
    )


def test_fetch_finance_country_snapshot_records_deutsche_boerse_company_details_sources(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    profiles = create_finance_country_profiles(fixture_root)
    monkeypatch.setattr(finance_country_module, "FINANCE_COUNTRY_PROFILES", profiles)
    profile = profiles["de"]
    company_details_source = next(
        source for source in profile["sources"] if source["name"] == "mercedes-company-details"
    )
    company_details_fixture_path = Path(
        urlparse(str(company_details_source["source_url"])).path
    )
    company_registry_search_fixture_path = (
        fixture_root / "de-unternehmensregister-search-results.html"
    )
    company_registry_detail_fixture_path = (
        fixture_root / "de-unternehmensregister-register-information.html"
    )
    company_registry_search_abo_fixture_path = (
        fixture_root / "de-unternehmensregister-search-results-abo-energy.html"
    )
    company_registry_detail_abo_fixture_path = (
        fixture_root / "de-unternehmensregister-register-information-abo-energy.html"
    )
    tradegate_abo_fixture_path = fixture_root / "de-tradegate-abo-energy-order-book.html"
    original_download_source = finance_country_module._download_source

    def fake_download_company_details(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        del user_agent
        if str(source_url).endswith("/DE0007100000/company-details"):
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(company_details_fixture_path.read_bytes())
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text("<html><body>Company details unavailable</body></html>\n", encoding="utf-8")
        return str(source_url)

    def fake_download_register_information_search(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        del user_agent
        destination.parent.mkdir(parents=True, exist_ok=True)
        source_url_text = str(source_url)
        if "abo+energy+gmbh+%26+co.+kgaa" in source_url_text.casefold():
            destination.write_bytes(company_registry_search_abo_fixture_path.read_bytes())
        else:
            destination.write_bytes(company_registry_search_fixture_path.read_bytes())
        return source_url_text

    def fake_download_register_information_detail(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        del user_agent
        destination.parent.mkdir(parents=True, exist_ok=True)
        source_url_text = str(source_url)
        if "abo-energy-gmbh-co-kgaa" in source_url_text.casefold():
            destination.write_bytes(company_registry_detail_abo_fixture_path.read_bytes())
        else:
            destination.write_bytes(company_registry_detail_fixture_path.read_bytes())
        return source_url_text

    def wrapped_download_source(source_url: str | Path, destination: Path, *, user_agent: str) -> str:
        source_url_text = str(source_url)
        if source_url_text.startswith("https://live.deutsche-boerse.com/equity/"):
            return fake_download_company_details(source_url, destination, user_agent=user_agent)
        if source_url_text.startswith("https://www.tradegate.de/orderbuch.php?isin="):
            destination.parent.mkdir(parents=True, exist_ok=True)
            if "DE0005760029" in source_url_text:
                destination.write_bytes(tradegate_abo_fixture_path.read_bytes())
            else:
                destination.write_text(
                    "<html><body>Tradegate order book unavailable</body></html>\n",
                    encoding="utf-8",
                )
            return source_url_text
        return original_download_source(source_url, destination, user_agent=user_agent)

    monkeypatch.setattr(
        finance_country_module,
        "_download_deutsche_boerse_company_details_page",
        fake_download_company_details,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_download_unternehmensregister_register_information_search_page",
        fake_download_register_information_search,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_download_unternehmensregister_register_information_detail_page",
        fake_download_register_information_detail,
    )
    monkeypatch.setattr(finance_country_module, "_download_source", wrapped_download_source)
    monkeypatch.setattr(
        finance_country_module,
        "_fetch_wikidata_api_json",
        _fake_germany_wikidata_api_json,
    )

    fetch_result = fetch_finance_country_source_snapshots(
        output_dir=tmp_path / "raw" / "finance-country-en",
        snapshot="2026-04-19",
        country_codes=["de"],
    )

    assert fetch_result.country_count == 1
    de_item = fetch_result.countries[0]
    manifest = json.loads(Path(de_item.source_manifest_path).read_text(encoding="utf-8"))
    company_detail_sources = [
        source
        for source in manifest["sources"]
        if str(source["name"]).startswith("deutsche-boerse-company-details-")
    ]
    assert company_detail_sources
    assert any(
        str(source["path"]).startswith("deutsche-boerse-company-details/de0007100000")
        for source in company_detail_sources
    )
    company_register_sources = [
        source
        for source in manifest["sources"]
        if str(source["name"]).startswith("unternehmensregister-register-information-")
    ]
    assert company_register_sources
    assert any(
        str(source["name"]).startswith("unternehmensregister-register-information-search-")
        and str(source["path"]).startswith(
            "unternehmensregister-register-information-search/de0007100000"
        )
        for source in company_register_sources
    )
    assert any(
        str(source["path"]).startswith("unternehmensregister-register-information/de0007100000")
        for source in company_register_sources
    )
    tradegate_sources = [
        source
        for source in manifest["sources"]
        if str(source["name"]).startswith("tradegate-order-book-")
    ]
    assert tradegate_sources
    assert any(
        str(source["path"]).startswith("tradegate-order-book/de0005760029")
        for source in tradegate_sources
    )
    wikidata_sources = [
        source
        for source in manifest["sources"]
        if str(source["name"]) == "wikidata-germany-entity-resolution"
    ]
    assert len(wikidata_sources) == 1
    assert wikidata_sources[0]["path"] == "wikidata-germany-entity-resolution.json"
    bafin_sources = [
        source
        for source in manifest["sources"]
        if str(source["name"]) == "bafin-company-database-export"
    ]
    assert len(bafin_sources) == 1
    assert str(bafin_sources[0]["path"]).endswith(".csv")



def test_extract_uk_official_list_commercial_company_records_filters_company_rows(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "official-list.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Company Name,Country of Inc.,Description of Listed Security,Listing Category,Market Status,Trading Venue,ISIN(S)",
                "3I GROUP PLC,United Kingdom,Ordinary shares of 73 19/22p each; fully paid,Equity shares (commercial companies),RM,LSE,GB00B1YW4409,",
                "ABERDEEN GROUP PLC,United Kingdom,Ordinary shares of 13 61/63; fully paid,Equity shares (commercial companies),RM,LSE,GB00BF8Q6K64,",
                "ABERDEEN GROUP PLC,United Kingdom,4.25% Reset Subordinated Notes 30/06/2048,Debt and debt-like securities,RM,LSE,XS1698906259,",
                "AB IGNITIS GRUPĖ,Lithuania,Global Depositary Receipts,Certificates representing certain securities (depository receipts),RM,LSE,US66981G2075,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    records = finance_country_module._extract_uk_official_list_commercial_company_records(
        csv_path
    )

    assert records == [
        {
            "raw_company_name": "3I GROUP PLC",
            "company_name": "3I Group PLC",
            "listing_category": "Equity shares (commercial companies)",
            "market_status": "RM",
            "trading_venue": "LSE",
            "isin": "GB00B1YW4409",
        },
        {
            "raw_company_name": "ABERDEEN GROUP PLC",
            "company_name": "Aberdeen Group PLC",
            "listing_category": "Equity shares (commercial companies)",
            "market_status": "RM",
            "trading_venue": "LSE",
            "isin": "GB00BF8Q6K64",
        },
    ]


def test_extract_companies_house_search_result_company_number_matches_company() -> None:
    search_html = """
    <ul id='results' class="results-list">
      <li class="type-company">
        <h3><a class="govuk-link" href="/company/01142830">3I GROUP PLC</a></h3>
        <p class="meta crumbtrail"> 01142830 - Incorporated on 1 November 1973 </p>
      </li>
      <li class="type-company">
        <h3><a class="govuk-link" href="/company/LP006504">3I GROUP INVESTMENTS LP</a></h3>
      </li>
    </ul>
    """

    company_number = (
        finance_country_module._extract_companies_house_search_result_company_number(
            search_html,
            company_name="3I Group PLC",
            raw_company_name="3I GROUP PLC",
        )
    )

    assert company_number == "01142830"


def test_extract_companies_house_search_result_company_number_matches_json_payload() -> None:
    search_payload = {
        "page_number": 1,
        "total_results": 2,
        "items": [
            {
                "title": "ADMIRAL GROUP PLC",
                "company_number": "03849958",
            },
            {
                "title": "ADMIRAL COMMERCIAL CLEANING GROUP LTD",
                "company_number": "12446538",
            },
        ],
    }

    company_number = (
        finance_country_module._extract_companies_house_search_result_company_number(
            json.dumps(search_payload),
            company_name="Admiral Group PLC",
            raw_company_name="ADMIRAL GROUP PLC",
        )
    )

    assert company_number == "03849958"


def test_extract_companies_house_officers_entities_parses_active_people() -> None:
    officers_html = """
    <div class="appointment-1">
      <h2 class="heading-medium heading-with-border">
        <span id="officer-name-1">
          <a class="govuk-link" href="/officers/x/appointments">DUNN, Kevin John</a>
        </span>
      </h2>
      <div class="grid-row">
        <dl class="column-quarter">
          <dt>Role <span id="officer-status-tag-1" class="status-tag font-xsmall">Active</span></dt>
          <dd id="officer-role-1" class="data">Secretary</dd>
        </dl>
      </div>
    </div>
    <div class="appointment-2">
      <h2 class="heading-medium heading-with-border">
        <span id="officer-name-2">
          <a class="govuk-link" href="/officers/y/appointments">BORROWS, Simon Alexander</a>
        </span>
      </h2>
      <div class="grid-row">
        <dl class="column-quarter">
          <dt>Role <span id="officer-status-tag-2" class="status-tag font-xsmall">Active</span></dt>
          <dd id="officer-role-2" class="data">Director</dd>
        </dl>
      </div>
    </div>
    <div class="appointment-3">
      <h2 class="heading-medium heading-with-border">
        <span id="officer-name-3">
          <a class="govuk-link" href="/officers/z/appointments">E P S SECRETARIES LIMITED</a>
        </span>
      </h2>
      <div class="grid-row">
        <dl class="column-quarter">
          <dt>Role <span id="officer-status-tag-3" class="status-tag font-xsmall">Active</span></dt>
          <dd id="officer-role-3" class="data">Secretary</dd>
        </dl>
      </div>
    </div>
    <div class="appointment-4">
      <h2 class="heading-medium heading-with-border">
        <span id="officer-name-4">
          <a class="govuk-link" href="/officers/q/appointments">BROWN, Peter Charles</a>
        </span>
      </h2>
      <div class="grid-row">
        <dl class="column-quarter">
          <dt>Role <span id="officer-status-tag-4" class="status-tag font-xsmall">Resigned</span></dt>
          <dd id="officer-role-4" class="data">Director</dd>
        </dl>
      </div>
    </div>
    """

    entities = finance_country_module._extract_companies_house_officers_entities(
        officers_html,
        employer_name="3I Group PLC",
        employer_company_number="01142830",
    )

    canonical_names = {entity["canonical_text"] for entity in entities}
    assert canonical_names == {"Kevin John Dunn", "Simon Alexander Borrows"}
    kevin = next(entity for entity in entities if entity["canonical_text"] == "Kevin John Dunn")
    assert kevin["metadata"]["category"] == "secretary"
    assert "DUNN, Kevin John" in kevin["aliases"]
    simon = next(
        entity for entity in entities if entity["canonical_text"] == "Simon Alexander Borrows"
    )
    assert simon["metadata"]["category"] == "director"
    assert simon["metadata"]["employer_company_number"] == "01142830"


def test_derive_united_states_sec_entities_emits_issuers_and_tickers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    country_dir = tmp_path / "us"
    country_dir.mkdir(parents=True, exist_ok=True)
    (country_dir / "sec_company_tickers.json").write_text("{}\n", encoding="utf-8")
    (country_dir / "sec_submissions.zip").write_bytes(b"placeholder")

    monkeypatch.setattr(
        finance_country_module,
        "_load_sec_company_items",
        lambda _path: [{"cik_str": "1730168", "ticker": "AVGO", "title": "Broadcom Inc."}],
    )
    monkeypatch.setattr(
        finance_country_module,
        "_collect_sec_source_ids",
        lambda _items: {"0001730168"},
    )
    monkeypatch.setattr(
        finance_country_module,
        "_load_sec_submission_profiles",
        lambda _path, *, target_source_ids: {
            "0001730168": {
                "tickers": ["AVGO"],
                "exchanges": ["NASDAQ"],
                "former_names": ["Broadcom Limited"],
            }
        },
    )
    monkeypatch.setattr(
        finance_country_module,
        "_build_sec_company_issuers",
        lambda _items, *, submission_profiles, companyfacts_profiles, symbol_profiles: (
            [
                {
                    "entity_id": "sec-cik:0001730168",
                    "source_id": "0001730168",
                    "canonical_text": "Broadcom Inc.",
                    "aliases": ["Broadcom Limited"],
                    "metadata": {"ticker": "AVGO", "exchanges": ["NASDAQ"]},
                }
            ],
            1,
        ),
    )

    derived_files = finance_country_module._derive_united_states_sec_entities(
        country_dir=country_dir,
        downloaded_sources=[
            {"name": "sec-company-tickers", "path": "sec_company_tickers.json"},
            {"name": "sec-submissions", "path": "sec_submissions.zip"},
        ],
        user_agent="ades-test/0.1.0",
    )

    assert derived_files == [
        {
            "name": "derived-finance-us-sec-issuers",
            "path": "derived_sec_issuer_entities.json",
            "record_count": 2,
            "adapter": "sec_company_tickers_country_derivation",
        }
    ]
    entities = finance_country_module._load_curated_entities(
        country_dir / "derived_sec_issuer_entities.json"
    )
    broadcom = next(
        entity for entity in entities if entity["canonical_text"] == "Broadcom Inc."
    )
    assert broadcom["entity_type"] == "organization"
    assert broadcom["entity_id"] == "finance-us-issuer:0001730168"
    assert "AVGO" in broadcom["aliases"]
    ticker = next(entity for entity in entities if entity["canonical_text"] == "AVGO")
    assert ticker["entity_type"] == "ticker"
    assert ticker["entity_id"] == "finance-us-ticker:AVGO"
    assert ticker["aliases"] == ["AVGO"]


def test_finance_country_build_includes_derived_entity_files(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "raw" / "2026-04-19"
    country_dir = snapshot_dir / "us"
    country_dir.mkdir(parents=True, exist_ok=True)
    profile = create_finance_country_profiles(tmp_path / "remote-country-sources")["us"]
    (country_dir / "country_profile.json").write_text(
        json.dumps(profile, indent=2) + "\n",
        encoding="utf-8",
    )
    (country_dir / "curated_finance_entities.json").write_text(
        json.dumps({"entities": profile["entities"]}, indent=2) + "\n",
        encoding="utf-8",
    )
    derived_path = country_dir / "derived_people_entities.json"
    derived_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "entity_type": "person",
                        "canonical_text": "Jane Doe",
                        "aliases": ["Jane A. Doe"],
                        "entity_id": "finance-us:jane-doe",
                        "metadata": {"country_code": "us", "category": "executive_officer"},
                    },
                    {
                        "entity_type": "ticker",
                        "canonical_text": "TICKA",
                        "aliases": ["Issuer Alpha Holdings"],
                        "entity_id": "finance-us:ticka",
                        "metadata": {"country_code": "us", "category": "ticker"},
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (country_dir / "sources.fetch.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": "finance-us-en",
                "country_code": "us",
                "country_name": "United States",
                "generated_at": "2026-04-19T00:00:00Z",
                "user_agent": "ades-test/0.1.0",
                "sources": [],
                "curated_entities": {
                    "path": "curated_finance_entities.json",
                    "sha256": "ignored",
                    "size_bytes": 0,
                },
                "people_source_plan": {
                    "path": "country_people_source_plan.json",
                    "sha256": "ignored",
                    "size_bytes": 0,
                },
                "derived_entities": [
                    {
                        "name": "derived-finance-us-people",
                        "path": "derived_people_entities.json",
                        "record_count": 2,
                        "adapter": "test_adapter",
                        "sha256": "ignored",
                        "size_bytes": 0,
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (country_dir / "country_people_source_plan.json").write_text(
        json.dumps(finance_country_module.finance_country_people_source_plan("us"), indent=2)
        + "\n",
        encoding="utf-8",
    )

    build_result = build_finance_country_source_bundles(
        snapshot_dir=snapshot_dir,
        output_dir=tmp_path / "bundles",
        country_codes=["us"],
        version="0.2.0",
    )

    bundle = build_result.bundles[0]
    bundle_manifest = json.loads(Path(bundle.bundle_manifest_path).read_text(encoding="utf-8"))
    assert bundle_manifest["coverage"]["person_count"] == 2
    assert bundle_manifest["coverage"]["ticker_count"] == 1
    normalized_entities = Path(bundle.entities_path).read_text(encoding="utf-8")
    assert "Jane Doe" in normalized_entities
    assert "\"TICKA\"" in normalized_entities
