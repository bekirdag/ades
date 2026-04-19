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
