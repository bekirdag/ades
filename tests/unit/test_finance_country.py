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
    _write_test_xlsx_workbook,
    create_finance_country_profiles,
    create_finance_raw_snapshots,
)
from tests.general_bundle_helpers import create_general_raw_snapshots


_GERMANY_WIKIDATA_SEARCH_FIXTURES = {
    "Commerzbank AG": [
        {"id": "Q157617", "label": "Commerzbank AG", "description": "major commercial bank"},
    ],
    "Ing Bank N.V.": [
        {"id": "Q2304977", "label": "ING Bank N.V.", "description": "Dutch bank"},
        {"id": "Q131553738", "label": "Ing Bank N.V.", "description": None},
    ],
    "ING Bank N.V.": [
        {"id": "Q2304977", "label": "ING Bank N.V.", "description": "Dutch bank"},
        {"id": "Q131553738", "label": "Ing Bank N.V.", "description": None},
    ],
    "Kuwait Investment Authority": [
        {
            "id": "Q1794766",
            "label": "Kuwait Investment Authority",
            "description": "sovereign wealth fund of Kuwait",
        },
    ],
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
    "Q131553738": {
        "labels": {
            "en": {"value": "Ing Bank N.V."},
            "de": {"value": "ING Bank N.V."},
        },
        "aliases": {},
        "descriptions": {},
        "claims": {},
    },
    "Q111281602": {
        "labels": {
            "en": {"value": "Sabine Kohleisen"},
            "de": {"value": "Sabine Kohleisen"},
        },
        "aliases": {},
        "descriptions": {"en": {"value": "German manager"}},
        "claims": {
            "P31": [{"mainsnak": {"datavalue": {"value": {"id": "Q5"}}}}],
        },
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
    "Q157617": {
        "labels": {
            "en": {"value": "Commerzbank AG"},
            "de": {"value": "Commerzbank AG"},
        },
        "aliases": {
            "en": [{"value": "Commerzbank"}],
        },
        "descriptions": {"en": {"value": "major commercial bank"}},
        "claims": {
            "P1278": [{"mainsnak": {"datavalue": {"value": "851WYGNLUQLFZBSYGB56"}}}],
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
            "P31": [{"mainsnak": {"datavalue": {"value": {"id": "Q5"}}}}],
            "P108": [{"mainsnak": {"datavalue": {"value": {"id": "Q27530"}}}}],
        },
    },
    "Q1794766": {
        "labels": {
            "en": {"value": "Kuwait Investment Authority"},
            "de": {"value": "Kuwait Investment Authority"},
        },
        "aliases": {},
        "descriptions": {"en": {"value": "sovereign wealth fund of Kuwait"}},
        "claims": {},
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
            "P169": [{"mainsnak": {"datavalue": {"value": {"id": "Q19297571"}}}}],
            "P488": [{"mainsnak": {"datavalue": {"value": {"id": "Q15834173"}}}}],
            "P3320": [
                {"mainsnak": {"datavalue": {"value": {"id": "Q50387551"}}}},
                {"mainsnak": {"datavalue": {"value": {"id": "Q62090160"}}}},
                {"mainsnak": {"datavalue": {"value": {"id": "Q111281602"}}}},
            ],
            "P127": [{"mainsnak": {"datavalue": {"value": {"id": "Q1794766"}}}}],
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
    "Q2304977": {
        "labels": {
            "en": {"value": "ING Bank N.V."},
            "de": {"value": "ING Bank N.V."},
        },
        "aliases": {
            "en": [{"value": "ING-DiBa AG"}],
        },
        "descriptions": {"en": {"value": "Dutch bank"}},
        "claims": {
            "P1278": [{"mainsnak": {"datavalue": {"value": "724500A2PXFMEQ9D8H38"}}}],
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
            "P31": [{"mainsnak": {"datavalue": {"value": {"id": "Q5"}}}}],
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
            "P31": [{"mainsnak": {"datavalue": {"value": {"id": "Q5"}}}}],
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
        "claims": {
            "P31": [{"mainsnak": {"datavalue": {"value": {"id": "Q5"}}}}],
        },
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


def _fake_germany_wikidata_property_query(
    *,
    property_id: str,
    values: list[str],
    user_agent: str,
    warnings: list[str],
) -> dict[str, list[str]]:
    del user_agent, warnings
    property_matches = {
        "P946": {
            "DE000A0M93V6": ["Q119889189"],
            "DE0005760029": ["Q33354510"],
            "DE0005810055": ["Q157852"],
            "DE0007100000": ["Q27530"],
        },
        "P1278": {
            "724500A2PXFMEQ9D8H38": ["Q2304977"],
            "851WYGNLUQLFZBSYGB56": ["Q157617"],
        },
    }
    resolved_matches = property_matches.get(property_id, {})
    return {
        value: resolved_matches[value]
        for value in values
        if value in resolved_matches
    }


def _fake_germany_wikidata_public_people_rows(
    *,
    organization_qids: list[str],
    user_agent: str,
    warnings: list[str],
) -> list[dict[str, str]]:
    del user_agent, warnings
    rows: list[dict[str, str]] = []
    if "Q27530" in organization_qids:
        rows.extend(
            [
                {
                    "organization_qid": "Q27530",
                    "property_id": "P169",
                    "person_qid": "Q19297571",
                    "person_label": "Ola Källenius",
                },
                {
                    "organization_qid": "Q27530",
                    "property_id": "P488",
                    "person_qid": "Q15834173",
                    "person_label": "Martin Brudermüller",
                },
                {
                    "organization_qid": "Q27530",
                    "property_id": "P3320",
                    "person_qid": "Q50387551",
                    "person_label": "Britta Seeger",
                },
                {
                    "organization_qid": "Q27530",
                    "property_id": "P3320",
                    "person_qid": "Q62090160",
                    "person_label": "Harald Wilhelm",
                },
                {
                    "organization_qid": "Q27530",
                    "property_id": "P3320",
                    "person_qid": "Q111281602",
                    "person_label": "Sabine Kohleisen",
                },
            ]
        )
    return rows


_ARGENTINA_WIKIDATA_SEARCH_FIXTURES = {
    "YPF S.A.": [
        {"id": "Q2006989", "label": "YPF", "description": "Argentine energy company"},
    ],
    "GRUPO FINANCIERO GALICIA S.A.": [
        {
            "id": "Q3118234",
            "label": "Galicia Financial Group",
            "description": "Argentine banking group",
        },
    ],
    "Grupo Financiero Galicia S.A.": [
        {
            "id": "Q3118234",
            "label": "Galicia Financial Group",
            "description": "Argentine banking group",
        },
    ],
    "BANCO BBVA ARGENTINA S.A.": [
        {
            "id": "Q130001011",
            "label": "Banco BBVA Argentina",
            "description": "Argentine bank",
        },
    ],
    "Marin Horacio Daniel": [],
    "Horacio Daniel Marin": [
        {
            "id": "Q130001001",
            "label": "Horacio Daniel Marín",
            "description": "Argentine business executive",
        },
    ],
    "Escasany Eduardo J.": [],
    "Eduardo J. Escasany": [
        {
            "id": "Q130001002",
            "label": "Eduardo J. Escasany",
            "description": "Argentine businessman",
        },
    ],
}

_ARGENTINA_WIKIDATA_ENTITY_FIXTURES = {
    "Q2006989": {
        "labels": {
            "en": {"value": "YPF"},
            "es": {"value": "YPF"},
        },
        "aliases": {
            "en": [
                {"value": "YPF S. A."},
                {"value": "YPF SOCIEDAD ANONIMA"},
            ],
            "es": [
                {"value": "Yacimientos Petrolíferos Fiscales"},
            ],
        },
        "descriptions": {"en": {"value": "Argentine energy company"}},
        "claims": {
            "P946": [{"mainsnak": {"datavalue": {"value": "ARP9897X1319"}}}],
            "P249": [{"mainsnak": {"datavalue": {"value": "YPFD"}}}],
        },
    },
    "Q3118234": {
        "labels": {
            "en": {"value": "Galicia Financial Group"},
            "es": {"value": "Grupo Financiero Galicia"},
        },
        "aliases": {
            "en": [
                {"value": "Grupo Financiero Galicia S.A."},
                {"value": "Grupo Galicia"},
            ],
            "es": [
                {"value": "Grupo Financiero Galicia S.A."},
            ],
        },
        "descriptions": {"en": {"value": "Argentine banking group"}},
        "claims": {
            "P946": [{"mainsnak": {"datavalue": {"value": "ARP495251018"}}}],
            "P249": [{"mainsnak": {"datavalue": {"value": "GGAL"}}}],
        },
    },
    "Q130001001": {
        "labels": {
            "en": {"value": "Horacio Daniel Marín"},
            "es": {"value": "Horacio Daniel Marín"},
        },
        "aliases": {
            "es": [{"value": "Horacio Marín"}],
        },
        "descriptions": {"en": {"value": "Argentine business executive"}},
        "claims": {
            "P108": [{"mainsnak": {"datavalue": {"value": {"id": "Q2006989"}}}}],
        },
    },
    "Q130001002": {
        "labels": {
            "en": {"value": "Eduardo J. Escasany"},
            "es": {"value": "Eduardo J. Escasany"},
        },
        "aliases": {
            "en": [{"value": "Eduardo Escasany"}],
        },
        "descriptions": {"en": {"value": "Argentine businessman"}},
        "claims": {
            "P108": [{"mainsnak": {"datavalue": {"value": {"id": "Q3118234"}}}}],
        },
    },
    "Q130001011": {
        "labels": {
            "en": {"value": "Banco BBVA Argentina"},
            "es": {"value": "Banco BBVA Argentina"},
        },
        "aliases": {
            "en": [
                {"value": "Banco BBVA Argentina S.A."},
                {"value": "BBVA Argentina"},
            ],
        },
        "descriptions": {"en": {"value": "Argentine bank"}},
        "claims": {
            "P159": [{"mainsnak": {"datavalue": {"value": {"text": "Buenos Aires"}}}}],
        },
    },
}


def _fake_argentina_wikidata_api_json(
    *, params: dict[str, str], user_agent: str
) -> dict[str, object]:
    del user_agent
    action = params["action"]
    if action == "wbsearchentities":
        return {
            "search": _ARGENTINA_WIKIDATA_SEARCH_FIXTURES.get(
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
                entity_id: _ARGENTINA_WIKIDATA_ENTITY_FIXTURES[entity_id]
                for entity_id in entity_ids
                if entity_id in _ARGENTINA_WIKIDATA_ENTITY_FIXTURES
            }
        }
    raise AssertionError(f"Unexpected Wikidata action in test: {action}")


_FRANCE_WIKIDATA_SEARCH_FIXTURES = {
    "Roche Bobois": [
        {
            "id": "Q130010001",
            "label": "Roche Bobois",
            "description": "French furniture company",
        },
    ],
    "Anne-Pauline Petureaux": [
        {
            "id": "Q130010011",
            "label": "Anne-Pauline Pétureaux",
            "description": "French investor relations adviser",
        },
    ],
    "Jean-Eric Chouchan": [
        {
            "id": "Q130010012",
            "label": "Jean-Éric Chouchan",
            "description": "French business executive",
        },
    ],
}

_FRANCE_WIKIDATA_ENTITY_FIXTURES = {
    "Q130010001": {
        "labels": {
            "en": {"value": "Roche Bobois"},
            "fr": {"value": "Roche Bobois"},
        },
        "aliases": {
            "en": [{"value": "Roche Bobois SA"}],
            "fr": [{"value": "Roche Bobois S.A."}],
        },
        "descriptions": {"en": {"value": "French furniture company"}},
        "claims": {
            "P946": [{"mainsnak": {"datavalue": {"value": "FR0013344173"}}}],
            "P249": [{"mainsnak": {"datavalue": {"value": "RBO"}}}],
        },
    },
    "Q130010011": {
        "labels": {
            "en": {"value": "Anne-Pauline Pétureaux"},
            "fr": {"value": "Anne-Pauline Pétureaux"},
        },
        "aliases": {
            "fr": [{"value": "Anne Pauline Pétureaux"}],
        },
        "descriptions": {"en": {"value": "French investor relations adviser"}},
        "claims": {},
    },
    "Q130010012": {
        "labels": {
            "en": {"value": "Jean-Éric Chouchan"},
            "fr": {"value": "Jean-Éric Chouchan"},
        },
        "aliases": {
            "fr": [{"value": "Jean Eric Chouchan"}],
        },
        "descriptions": {"en": {"value": "French business executive"}},
        "claims": {
            "P108": [{"mainsnak": {"datavalue": {"value": {"id": "Q130010001"}}}}],
        },
    },
}


def _fake_france_wikidata_api_json(
    *, params: dict[str, str], user_agent: str
) -> dict[str, object]:
    del user_agent
    action = params["action"]
    if action == "wbsearchentities":
        return {
            "search": _FRANCE_WIKIDATA_SEARCH_FIXTURES.get(
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
                entity_id: _FRANCE_WIKIDATA_ENTITY_FIXTURES[entity_id]
                for entity_id in entity_ids
                if entity_id in _FRANCE_WIKIDATA_ENTITY_FIXTURES
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

    def fake_get(
        url: str,
        *,
        headers: dict[str, str],
        follow_redirects: bool,
        timeout: float,
        verify: bool,
    ):
        captured["url"] = url
        captured["headers"] = headers
        captured["follow_redirects"] = follow_redirects
        captured["timeout"] = timeout
        captured["verify"] = verify
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
    assert captured["verify"] is True


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

    def fake_get(
        url: str,
        *,
        headers: dict[str, str],
        follow_redirects: bool,
        timeout: float,
        verify: bool,
    ):
        requested_urls.append(url)
        assert verify is True
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


def test_finance_country_download_source_relaxes_tls_for_bymadata_host(
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "source.json"
    captured: dict[str, object] = {}

    class _FakeResponse:
        headers = {"content-type": "application/json"}
        content = b'{"ok":true}\n'
        text = '{"ok":true}\n'

        def raise_for_status(self) -> None:
            return None

    def fake_get(
        url: str,
        *,
        headers: dict[str, str],
        follow_redirects: bool,
        timeout: float,
        verify: bool,
    ):
        captured["url"] = url
        captured["verify"] = verify
        return _FakeResponse()

    monkeypatch.setattr(finance_country_module.httpx, "get", fake_get)

    resolved_url = finance_country_module._download_source(
        "https://open.bymadata.com.ar/example.json",
        destination,
        user_agent="ades-test/0.1.0",
    )

    assert resolved_url == "https://open.bymadata.com.ar/example.json"
    assert captured["url"] == "https://open.bymadata.com.ar/example.json"
    assert captured["verify"] is False
    assert destination.read_text(encoding="utf-8") == '{"ok":true}\n'


def test_finance_country_fetch_json_post_source_relaxes_tls_for_bymadata_host(
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "payload.json"
    captured: dict[str, object] = {}

    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> object:
            return {"data": [{"ticker": "GGAL"}]}

    def fake_post(
        url: str,
        *,
        json: dict[str, object],
        headers: dict[str, str],
        follow_redirects: bool,
        timeout: float,
        verify: bool,
    ):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["verify"] = verify
        return _FakeResponse()

    monkeypatch.setattr(finance_country_module.httpx, "post", fake_post)

    payload = finance_country_module._fetch_json_post_source(
        finance_country_module._BYMA_DATA_SOCIETY_GENERAL_URL,
        destination,
        payload={"symbol": "GGAL"},
        user_agent="ades-test/0.1.0",
    )

    assert captured["url"] == finance_country_module._BYMA_DATA_SOCIETY_GENERAL_URL
    assert captured["json"] == {"symbol": "GGAL"}
    assert captured["verify"] is False
    assert payload == {"data": [{"ticker": "GGAL"}]}
    assert json.loads(destination.read_text(encoding="utf-8")) == {"data": [{"ticker": "GGAL"}]}


def test_finance_country_source_destination_filename_keeps_xlsx_extension() -> None:
    assert finance_country_module._source_destination_filename(
        source_name="deutsche-boerse-listed-companies",
        source_url="https://example.com/data/Listed-companies.xlsx",
    ) == "deutsche-boerse-listed-companies.xlsx"


def test_download_euronext_directory_page_renders_live_pages(
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "fr-euronext-paris-regulated.html"
    captured: dict[str, object] = {}

    def fake_render_dynamic_page_with_headless_chrome(**kwargs: object) -> str:
        captured.update(kwargs)
        return "<html><body>rendered france directory</body></html>\n"

    monkeypatch.setattr(
        finance_country_module,
        "_find_headless_chrome_binary",
        lambda: "/usr/bin/google-chrome",
    )
    monkeypatch.setattr(
        finance_country_module,
        "_render_dynamic_page_with_headless_chrome",
        fake_render_dynamic_page_with_headless_chrome,
    )

    resolved_url = finance_country_module._download_euronext_directory_page(
        finance_country_module._EURONEXT_PARIS_REGULATED_LIST_URL,
        destination,
        user_agent="ades-test/0.1.0",
    )

    assert resolved_url == finance_country_module._EURONEXT_PARIS_REGULATED_LIST_URL
    assert destination.read_text(encoding="utf-8") == (
        "<html><body>rendered france directory</body></html>\n"
    )
    assert captured["page_label"] == "live.euronext directory page"
    assert (
        captured["render_budget_ms"]
        == finance_country_module._EURONEXT_DIRECTORY_RENDER_BUDGET_MS
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
        country_codes=["us", "au", "uk", "fr"],
    )

    assert fetch_result.snapshot == "2026-04-19"
    assert fetch_result.country_count == 4
    assert {item.pack_id for item in fetch_result.countries} == {
        "finance-au-en",
        "finance-fr-en",
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
        if item.country_code == "fr":
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
            assert {"74SW", "RBO", "AL2SI", "MLACT", "Activium Group"} <= derived_names

    build_result = build_finance_country_source_bundles(
        snapshot_dir=fetch_result.snapshot_dir,
        output_dir=tmp_path / "bundles" / "finance-country-en",
        country_codes=["us", "au", "uk", "fr"],
        version="0.2.0",
    )

    assert build_result.country_count == 4
    assert {item.pack_id for item in build_result.bundles} == {
        "finance-au-en",
        "finance-fr-en",
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
    fr_bundle = next(item for item in build_result.bundles if item.country_code == "fr")
    fr_manifest = json.loads(
        Path(fr_bundle.bundle_manifest_path).read_text(encoding="utf-8")
    )
    assert fr_manifest["coverage"]["organization_count"] >= 5
    fr_entities = Path(fr_bundle.entities_path).read_text(encoding="utf-8")
    assert "MLACT" in fr_entities
    assert "Activium Group" in fr_entities


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
    argentina_plan = finance_country_module.finance_country_people_source_plan("ar")
    assert argentina_plan["automation_status"] == "partial"
    assert any(
        source["source_url"]
        == "https://www.byma.com.ar/financiarse/para-emisoras/empresas-listadas"
        for source in argentina_plan["sources"]
    )
    assert any(
        source["source_url"]
        == "https://www.cnv.gov.ar/SitioWeb/Reportes/ListadoSociedadesEmisorasPorRegimen"
        for source in argentina_plan["sources"]
    )
    assert any(
        source["source_url"]
        == "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/instrumentproduct/instrument/all-instrument-select"
        for source in argentina_plan["sources"]
    )
    assert any(
        source["source_url"]
        == "https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/"
        for source in argentina_plan["sources"]
    )
    turkiye_plan = finance_country_module.finance_country_people_source_plan("tr")
    assert turkiye_plan["automation_status"] == "implemented"
    germany_plan = finance_country_module.finance_country_people_source_plan("de")
    assert germany_plan["automation_status"] == "partial"
    france_plan = finance_country_module.finance_country_people_source_plan("fr")
    assert france_plan["automation_status"] == "partial"
    japan_plan = finance_country_module.finance_country_people_source_plan("jp")
    assert japan_plan["automation_status"] == "partial"
    assert any(
        source["source_url"]
        == "https://www.jpx.co.jp/english/equities/listed-co/disclosure-gate/availability/"
        for source in japan_plan["sources"]
    )
    assert any(
        source["source_url"] == "https://www.jpx.co.jp/english/listing/co-search/"
        for source in japan_plan["sources"]
    )
    assert any(
        source["source_url"] == "https://www.jpx.co.jp/english/listing/cg-search/"
        for source in japan_plan["sources"]
    )
    assert any(
        source["source_url"]
        == "https://live.euronext.com/en/markets/paris/equities/euronext/list"
        for source in france_plan["sources"]
    )
    assert any(
        source["source_url"]
        == "https://live.euronext.com/en/markets/paris/equities/growth/list"
        for source in france_plan["sources"]
    )
    assert any(
        source["source_url"]
        == "https://live.euronext.com/en/markets/paris/equities/access/list"
        for source in france_plan["sources"]
    )
    assert any(
        source["source_url"]
        == "https://live.euronext.com/en/markets/paris/company-news"
        for source in france_plan["sources"]
    )
    assert any(
        source["source_url"]
        == "https://live.euronext.com/en/markets/paris/company-news-archive"
        for source in france_plan["sources"]
    )
    assert any(
        source["source_url"] == "https://www.wikidata.org/w/api.php"
        for source in france_plan["sources"]
    )
    assert any(
        source["source_url"] == "https://www.info-financiere.fr/"
        for source in france_plan["sources"]
    )
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


def test_finance_country_profile_includes_all_authorized_argentina_markets() -> None:
    argentina_profile = finance_country_module.FINANCE_COUNTRY_PROFILES["ar"]

    exchange_entities = [
        entity
        for entity in argentina_profile["entities"]
        if entity["entity_type"] == "exchange"
    ]

    assert {entity["entity_id"] for entity in exchange_entities} == {
        "finance-ar:byma",
        "finance-ar:a3",
        "finance-ar:mav",
    }
    assert {
        entity["canonical_text"]: tuple(entity["aliases"]) for entity in exchange_entities
    } == {
        "Bolsas y Mercados Argentinos": (
            "BYMA",
            "Bolsas y Mercados Argentinos S.A.",
        ),
        "A3 Mercados": ("A3", "A3 Mercados S.A."),
        "Mercado Argentino de Valores": (
            "MAV",
            "Mercado Argentino de Valores S.A.",
        ),
    }


def test_finance_country_profile_includes_all_authorized_france_markets() -> None:
    france_profile = finance_country_module.FINANCE_COUNTRY_PROFILES["fr"]

    exchange_entities = [
        entity
        for entity in france_profile["entities"]
        if entity["entity_type"] == "exchange"
    ]

    assert {entity["entity_id"] for entity in exchange_entities} == {
        "finance-fr:euronext-paris",
        "finance-fr:euronext-growth-paris",
        "finance-fr:euronext-access-paris",
    }
    assert {
        entity["canonical_text"]: tuple(entity["aliases"]) for entity in exchange_entities
    } == {
        "Euronext Paris": (),
        "Euronext Growth Paris": ("Paris Growth Market",),
        "Euronext Access Paris": ("Paris Access Market",),
    }
    assert {
        entity["entity_id"]: entity["metadata"]["wikidata_slug"]
        for entity in exchange_entities
    } == {
        "finance-fr:euronext-paris": "Q2385849",
        "finance-fr:euronext-growth-paris": "Q107188657",
        "finance-fr:euronext-access-paris": "Q107190270",
    }


def test_finance_country_profile_includes_japan_baseline_market_entities() -> None:
    japan_profile = finance_country_module.FINANCE_COUNTRY_PROFILES["jp"]

    exchange_entities = [
        entity
        for entity in japan_profile["entities"]
        if entity["entity_type"] == "exchange"
    ]

    assert {entity["entity_id"] for entity in exchange_entities} == {
        "finance-jp:jpx",
        "finance-jp:tse",
    }
    assert {
        entity["canonical_text"]: tuple(entity["aliases"]) for entity in exchange_entities
    } == {
        "Japan Exchange Group": ("JPX",),
        "Tokyo Stock Exchange": ("TSE",),
    }
    market_index = next(
        entity
        for entity in japan_profile["entities"]
        if entity["entity_id"] == "finance-jp:nikkei-225"
    )
    assert market_index["canonical_text"] == "Nikkei 225"
    assert market_index["metadata"]["country_code"] == "jp"


def test_finance_country_profile_includes_all_core_germany_markets() -> None:
    germany_profile = finance_country_module.FINANCE_COUNTRY_PROFILES["de"]

    exchange_entities = [
        entity
        for entity in germany_profile["entities"]
        if entity["entity_type"] == "exchange"
    ]

    assert {entity["entity_id"] for entity in exchange_entities} == {
        "finance-de:boerse-frankfurt",
        "finance-de:xetra",
        "finance-de:boerse-stuttgart",
        "finance-de:boerse-berlin",
        "finance-de:boerse-hamburg",
        "finance-de:boerse-hannover",
        "finance-de:boerse-muenchen",
        "finance-de:boerse-duesseldorf",
        "finance-de:tradegate-exchange",
    }
    assert {
        entity["canonical_text"]: tuple(entity["aliases"]) for entity in exchange_entities
    } == {
        "Börse Frankfurt": ("Frankfurt Stock Exchange",),
        "Xetra": (),
        "Börse Stuttgart": ("Stuttgart Stock Exchange", "Borse Stuttgart"),
        "Börse Berlin": ("Berlin Stock Exchange", "Borse Berlin"),
        "Börse Hamburg": ("Hamburg Stock Exchange", "Borse Hamburg"),
        "Börse Hannover": (
            "Hannover Stock Exchange",
            "Hanover Stock Exchange",
            "Borse Hannover",
        ),
        "Börse München": ("Munich Stock Exchange", "Borse Munchen"),
        "Börse Düsseldorf": ("Dusseldorf Stock Exchange", "Borse Dusseldorf"),
        "Tradegate Exchange": ("Tradegate",),
    }


def test_finance_country_profile_assigns_wikidata_slugs_to_all_france_static_entities() -> None:
    france_profile = finance_country_module.FINANCE_COUNTRY_PROFILES["fr"]

    metadata_by_entity_id = {
        str(entity["entity_id"]): dict(entity.get("metadata", {}))
        for entity in france_profile["entities"]
    }

    assert {
        entity_id: metadata["wikidata_slug"]
        for entity_id, metadata in metadata_by_entity_id.items()
    } == {
        "finance-fr:amf": "Q788606",
        "finance-fr:euronext-paris": "Q2385849",
        "finance-fr:euronext-growth-paris": "Q107188657",
        "finance-fr:euronext-access-paris": "Q107190270",
        "finance-fr:banque-de-france": "Q806950",
        "finance-fr:acpr": "Q2872781",
        "finance-fr:cac-40": "Q648828",
    }
    for metadata in metadata_by_entity_id.values():
        assert metadata["wikidata_qid"] == metadata["wikidata_slug"]
        assert metadata["wikidata_entity_id"] == f"wikidata:{metadata['wikidata_qid']}"
        assert metadata["wikidata_url"] == (
            f"https://www.wikidata.org/wiki/{metadata['wikidata_qid']}"
        )


def test_finance_country_profile_assigns_wikidata_slugs_to_all_united_states_static_entities() -> None:
    united_states_profile = finance_country_module.FINANCE_COUNTRY_PROFILES["us"]

    metadata_by_entity_id = {
        str(entity["entity_id"]): dict(entity.get("metadata", {}))
        for entity in united_states_profile["entities"]
    }

    assert {
        entity_id: metadata["wikidata_slug"]
        for entity_id, metadata in metadata_by_entity_id.items()
    } == {
        "finance-us:sec": "Q953944",
        "finance-us:edgar": "Q3050604",
        "finance-us:nasdaq": "Q82059",
        "finance-us:nyse": "Q13677",
        "finance-us:fdic": "Q1345065",
        "finance-us:sp-500": "Q242345",
    }
    for metadata in metadata_by_entity_id.values():
        assert metadata["wikidata_qid"] == metadata["wikidata_slug"]
        assert metadata["wikidata_entity_id"] == f"wikidata:{metadata['wikidata_qid']}"
        assert metadata["wikidata_url"] == (
            f"https://www.wikidata.org/wiki/{metadata['wikidata_qid']}"
        )


def test_finance_country_profile_assigns_wikidata_slugs_to_selected_united_kingdom_static_entities() -> None:
    united_kingdom_profile = finance_country_module.FINANCE_COUNTRY_PROFILES["uk"]

    metadata_by_entity_id = {
        str(entity["entity_id"]): dict(entity.get("metadata", {}))
        for entity in united_kingdom_profile["entities"]
    }

    assert {
        entity_id: metadata["wikidata_slug"]
        for entity_id, metadata in metadata_by_entity_id.items()
        if "wikidata_slug" in metadata
    } == {
        "finance-uk:fca": "Q5449586",
        "finance-uk:rns": "Q7309686",
        "finance-uk:lse": "Q171240",
        "finance-uk:aim": "Q438511",
        "finance-uk:ftse-100": "Q466496",
        "finance-uk:ftse-aim-all-share": "Q5427224",
    }
    assert "wikidata_slug" not in metadata_by_entity_id["finance-uk:official-list"]
    for metadata in metadata_by_entity_id.values():
        if "wikidata_slug" not in metadata:
            continue
        assert metadata["wikidata_qid"] == metadata["wikidata_slug"]
        assert metadata["wikidata_entity_id"] == f"wikidata:{metadata['wikidata_qid']}"
        assert metadata["wikidata_url"] == (
            f"https://www.wikidata.org/wiki/{metadata['wikidata_qid']}"
        )


def test_fetch_wikidata_api_json_retries_429(monkeypatch) -> None:
    request = finance_country_module.httpx.Request(
        "GET",
        finance_country_module._WIKIDATA_API_URL,
    )
    responses = [
        finance_country_module.httpx.Response(
            429,
            request=request,
            headers={"Retry-After": "0"},
        ),
        finance_country_module.httpx.Response(
            200,
            request=request,
            json={"search": []},
        ),
    ]
    recorded_calls: list[dict[str, object]] = []
    sleep_calls: list[float] = []

    class _FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            del exc_type, exc, tb
            return False

        def get(self, url: str, *, params: dict[str, str]):
            recorded_calls.append({"url": url, "params": dict(params)})
            return responses[len(recorded_calls) - 1]

    monkeypatch.setattr(finance_country_module.httpx, "Client", _FakeClient)
    monkeypatch.setattr(
        finance_country_module.time,
        "sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    payload = finance_country_module._fetch_wikidata_api_json(
        params={"action": "wbsearchentities", "search": "Commerzbank AG"},
        user_agent="ades-test/0.1.0",
    )

    assert payload == {"search": []}
    assert len(recorded_calls) == 2
    assert sleep_calls == [finance_country_module._WIKIDATA_API_RETRY_BASE_SECONDS]


def test_extract_france_euronext_directory_records_parses_rendered_rows(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    html_text = (fixture_root / "fr-euronext-paris-growth.html").read_text(
        encoding="utf-8"
    )

    records = finance_country_module._extract_france_euronext_directory_records(
        html_text,
        listing_segment="Euronext Growth Paris",
    )

    assert len(records) == 1
    record = records[0]
    assert record["raw_company_name"] == "2CRSI"
    assert record["isin"] == "FR0013341781"
    assert record["symbol"] == "AL2SI"
    assert record["market_code"] == "ALXP"
    assert record["listing_segment"] == "Euronext Growth Paris"
    assert record["product_url"] == (
        "https://live.euronext.com/en/product/equities/FR0013341781-ALXP"
    )


def test_extract_france_company_news_records_parses_rows(tmp_path: Path) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)

    records = finance_country_module._extract_france_company_news_records(
        (fixture_root / "fr-euronext-paris-company-news.html").read_text(
            encoding="utf-8"
        )
    )

    assert records == [
        {
            "node_nid": "12878420",
            "issuer_name": "Roche Bobois",
            "title": "Mise à disposition du Document d'enregistrement universel au titre de l'exercice 2025",
            "category": "Legal",
            "published_at": "20 Apr 2026, 19:00 CEST",
            "sequence": "0",
        }
    ]
    archive_page_count = finance_country_module._extract_france_company_news_last_page(
        (fixture_root / "fr-euronext-paris-company-news-archive.html").read_text(
            encoding="utf-8"
        )
    )
    assert archive_page_count == 8


def test_extract_france_company_press_release_people_entities_parses_contacts(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)

    entities = finance_country_module._extract_france_company_press_release_people_entities(
        (fixture_root / "fr-euronext-company-press-release-12878420.html").read_text(
            encoding="utf-8"
        ),
        issuer_record={
            "company_name": "Roche Bobois",
            "symbol": "RBO",
            "isin": "FR0013344173",
        },
    )

    entities_by_name = {
        entity["canonical_text"]: entity for entity in entities
    }
    assert set(entities_by_name) == {"Anne-Pauline Petureaux", "Serena Boni"}
    anne_pauline = entities_by_name["Anne-Pauline Petureaux"]
    assert anne_pauline["metadata"]["category"] == "investor_relations"
    assert anne_pauline["metadata"]["contact_email"] == "apetureaux@actus.fr"
    assert anne_pauline["metadata"]["contact_phone"] == "01 53 67 36 72"
    serena = entities_by_name["Serena Boni"]
    assert serena["metadata"]["category"] == "press_contact"
    assert serena["metadata"]["contact_email"] == "sboni@actus.fr"


def test_extract_france_filing_candidate_urls_prefers_official_disclosure_docs(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)

    urls = finance_country_module._extract_france_filing_candidate_urls(
        (fixture_root / "fr-euronext-company-press-release-12878420.html").read_text(
            encoding="utf-8"
        ),
        base_url=(
            finance_country_module._FRANCE_COMPANY_PRESS_RELEASE_MODAL_URL_TEMPLATE.format(
                node_nid="12878420"
            )
        ),
    )

    assert urls == [
        (
            "https://www.info-financiere.fr/upload/"
            "roche-bobois-document-denregistrement-universel-2025.xhtml"
        )
    ]


def test_extract_france_filing_people_entities_parses_board_and_executives(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)

    issuer_lookup = finance_country_module._build_france_issuer_lookup(
        [
            {
                "company_name": "Roche Bobois",
                "raw_company_name": "ROCHE BOBOIS",
                "isin": "FR0013344173",
                "symbol": "RBO",
            }
        ]
    )
    entities = finance_country_module._extract_france_filing_people_entities(
        (
            fixture_root
            / "fr-roche-bobois-document-denregistrement-universel-2025.xhtml"
        ).read_text(encoding="utf-8"),
        issuer_lookup=issuer_lookup,
        issuer_record={
            "company_name": "Roche Bobois",
            "isin": "FR0013344173",
            "symbol": "RBO",
        },
        source_name="france-filing-document-test",
    )

    entities_by_name = {
        entity["canonical_text"]: entity for entity in entities
    }
    assert entities_by_name["Veronique Langlois"]["metadata"]["category"] == "director"
    assert (
        entities_by_name["Paul Mercier"]["metadata"]["category"]
        == "executive_officer"
    )


def test_extract_france_governance_people_entities_parses_board_and_executives(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)

    issuer_lookup = finance_country_module._build_france_issuer_lookup(
        [
            {
                "company_name": "Roche Bobois",
                "raw_company_name": "ROCHE BOBOIS",
                "isin": "FR0013344173",
                "symbol": "RBO",
            }
        ]
    )
    entities = finance_country_module._extract_france_governance_people_entities(
        (fixture_root / "fr-roche-bobois-governance.html").read_text(
            encoding="utf-8"
        ),
        issuer_lookup=issuer_lookup,
    )

    entities_by_name = {
        entity["canonical_text"]: entity for entity in entities
    }
    assert {
        "Jean-Eric Chouchan",
        "Nicolas Roche",
        "Guillaume Demulier",
        "Stéphanie Berson",
    } <= set(entities_by_name)
    assert entities_by_name["Jean-Eric Chouchan"]["metadata"]["category"] == "director"
    assert (
        entities_by_name["Guillaume Demulier"]["metadata"]["category"]
        == "executive_officer"
    )


def test_derive_france_euronext_entities_emits_issuers_and_tickers(
    tmp_path: Path,
) -> None:
    country_dir = tmp_path / "fr"
    country_dir.mkdir(parents=True, exist_ok=True)
    fixture_root = tmp_path / "remote-country-sources"
    profile = create_finance_country_profiles(fixture_root)["fr"]
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

    derived_files = finance_country_module._derive_france_euronext_entities(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent="ades-test/0.1.0",
    )

    assert {item["name"] for item in derived_files} == {
        "derived-finance-fr-issuers",
        "derived-finance-fr-tickers",
    }
    issuer_payload = json.loads(
        (country_dir / "derived_issuer_entities.json").read_text(encoding="utf-8")
    )
    ticker_payload = json.loads(
        (country_dir / "derived_ticker_entities.json").read_text(encoding="utf-8")
    )
    issuer_by_isin = {
        entity["metadata"]["isin"]: entity for entity in issuer_payload["entities"]
    }
    assert set(issuer_by_isin) == {
        "FR0010979377",
        "FR0011040500",
        "FR0013344173",
        "FR0013341781",
    }
    activium = issuer_by_isin["FR0010979377"]
    assert activium["metadata"]["listing_segment"] == "Euronext Access Paris"
    assert "ACTIVIUM GROUP" in activium["aliases"]
    ticker_by_symbol = {
        entity["canonical_text"]: entity for entity in ticker_payload["entities"]
    }
    assert set(ticker_by_symbol) == {"74SW", "RBO", "AL2SI", "MLACT"}
    mlact = ticker_by_symbol["MLACT"]
    assert mlact["metadata"]["instrument_exchange"] == "XMLI"
    assert "ACTIVIUM GROUP" in mlact["aliases"]
    assert "FR0010979377" in mlact["aliases"]


def test_derive_france_euronext_entities_emits_people_from_company_news_and_governance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    country_dir = tmp_path / "fr"
    country_dir.mkdir(parents=True, exist_ok=True)
    fixture_root = tmp_path / "remote-country-sources"
    profile = create_finance_country_profiles(fixture_root)["fr"]
    downloaded_sources: list[dict[str, object]] = []
    for source in profile["sources"]:
        source_url = str(source["source_url"])
        destination = country_dir / finance_country_module._source_destination_filename(
            source_name=str(source["name"]),
            source_url=source_url,
        )
        destination.write_bytes(Path(urlparse(source_url).path).read_bytes())
        downloaded_source = {
            "name": str(source["name"]),
            "source_url": source_url,
            "path": destination.name,
            "category": str(source["category"]),
        }
        if downloaded_source["name"] == "euronext-paris-company-news":
            downloaded_source["source_url"] = (
                finance_country_module._EURONEXT_PARIS_COMPANY_NEWS_URL
            )
        if downloaded_source["name"] == "euronext-paris-company-news-archive":
            downloaded_source["source_url"] = (
                finance_country_module._EURONEXT_PARIS_COMPANY_NEWS_ARCHIVE_URL
            )
        downloaded_sources.append(downloaded_source)

    modal_fixture = fixture_root / "fr-euronext-company-press-release-12878420.html"
    filing_fixture = (
        fixture_root / "fr-roche-bobois-document-denregistrement-universel-2025.xhtml"
    )
    governance_fixture = fixture_root / "fr-roche-bobois-governance.html"

    def fake_download_france_company_press_release_modal(
        node_nid: str,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        assert node_nid == "12878420"
        destination.write_bytes(modal_fixture.read_bytes())
        return (
            finance_country_module._FRANCE_COMPANY_PRESS_RELEASE_MODAL_URL_TEMPLATE.format(
                node_nid=node_nid
            )
        )

    def fake_download_france_governance_page(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        normalized_source_url = str(source_url)
        if normalized_source_url.rstrip("/") == "https://www.finance-roche-bobois.com/fr":
            destination.write_text(
                '<html><body><a href="https://www.finance-roche-bobois.com/fr/societe/gouvernance.html">Gouvernance</a></body></html>\n',
                encoding="utf-8",
            )
        else:
            destination.write_bytes(governance_fixture.read_bytes())
        return normalized_source_url

    def fake_download_france_filing_document(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        normalized_source_url = str(source_url)
        assert normalized_source_url == (
            "https://www.info-financiere.fr/upload/"
            "roche-bobois-document-denregistrement-universel-2025.xhtml"
        )
        destination.write_bytes(filing_fixture.read_bytes())
        return normalized_source_url

    monkeypatch.setattr(
        finance_country_module,
        "_download_france_company_press_release_modal",
        fake_download_france_company_press_release_modal,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_download_france_governance_page",
        fake_download_france_governance_page,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_download_france_filing_document",
        fake_download_france_filing_document,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_fetch_wikidata_api_json",
        _fake_france_wikidata_api_json,
    )

    derived_files = finance_country_module._derive_france_euronext_entities(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent="ades-test/0.1.0",
    )

    assert {item["name"] for item in derived_files} == {
        "derived-finance-fr-issuers",
        "derived-finance-fr-tickers",
        "derived-finance-fr-people",
    }
    issuer_payload = json.loads(
        (country_dir / "derived_issuer_entities.json").read_text(encoding="utf-8")
    )
    ticker_payload = json.loads(
        (country_dir / "derived_ticker_entities.json").read_text(encoding="utf-8")
    )
    people_payload = json.loads(
        (country_dir / "derived_people_entities.json").read_text(encoding="utf-8")
    )
    issuer_by_name = {
        entity["canonical_text"]: entity for entity in issuer_payload["entities"]
    }
    ticker_by_symbol = {
        entity["canonical_text"]: entity for entity in ticker_payload["entities"]
    }
    people_by_name = {
        entity["canonical_text"]: entity for entity in people_payload["entities"]
    }
    assert {
        "Anne-Pauline Petureaux",
        "Serena Boni",
        "Jean-Eric Chouchan",
        "Guillaume Demulier",
        "Veronique Langlois",
        "Paul Mercier",
    } <= set(people_by_name)
    roche_bobois = issuer_by_name["Roche Bobois"]
    assert roche_bobois["metadata"]["wikidata_qid"] == "Q130010001"
    assert roche_bobois["metadata"]["wikidata_slug"] == "Q130010001"
    assert "Roche Bobois SA" in roche_bobois["aliases"]
    rbo = ticker_by_symbol["RBO"]
    assert rbo["metadata"]["wikidata_qid"] == "Q130010001"
    assert "Roche Bobois SA" in rbo["aliases"]
    assert (
        people_by_name["Anne-Pauline Petureaux"]["metadata"]["category"]
        == "investor_relations"
    )
    assert people_by_name["Anne-Pauline Petureaux"]["metadata"]["wikidata_qid"] == "Q130010011"
    assert "Anne-Pauline Pétureaux" in people_by_name["Anne-Pauline Petureaux"]["aliases"]
    assert people_by_name["Jean-Eric Chouchan"]["metadata"]["category"] == "director"
    assert people_by_name["Jean-Eric Chouchan"]["metadata"]["wikidata_qid"] == "Q130010012"
    assert "Jean-Éric Chouchan" in people_by_name["Jean-Eric Chouchan"]["aliases"]
    assert people_by_name["Veronique Langlois"]["metadata"]["category"] == "director"
    assert (
        people_by_name["Paul Mercier"]["metadata"]["category"]
        == "executive_officer"
    )
    assert any(
        str(source["name"]) == "wikidata-france-entity-resolution"
        and str(source["path"]) == "wikidata-france-entity-resolution.json"
        for source in downloaded_sources
    )
    assert any(
        str(source.get("source_url"))
        == (
            "https://www.info-financiere.fr/upload/"
            "roche-bobois-document-denregistrement-universel-2025.xhtml"
        )
        for source in downloaded_sources
    )


def test_extract_argentina_byma_equity_symbols_filters_equities(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "ar-bymadata-all-instrument-select.json"
    source_path.write_text(
        json.dumps(
            [
                {"code": "YPFD", "description": "EQUITYINSTRUMENT", "deleted": False},
                {"code": "GGAL", "description": "EQUITYINSTRUMENT", "deleted": False},
                {"code": "BONO1", "description": "BONDINSTRUMENT", "deleted": False},
                {"code": "TEST", "description": "EQUITYINSTRUMENT", "deleted": True},
            ],
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    symbols = finance_country_module._extract_argentina_byma_equity_symbols(source_path)

    assert symbols == ["YPFD", "GGAL"]


def test_extract_argentina_byma_listed_company_records_parses_cards(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    html_path = fixture_root / "ar-byma-listed-companies.html"

    records = finance_country_module._extract_argentina_byma_listed_company_records(
        html_path.read_text(encoding="utf-8")
    )

    assert [record["symbol"] for record in records] == ["YPFD", "GGAL", "BPAT", "CEPU2"]
    bpat = next(record for record in records if record["symbol"] == "BPAT")
    assert bpat["company_name"] == "Banco Patagonia S.A."
    assert bpat["isin"] == "ARMERI013163"
    assert bpat["website"] == "https://www.bancopatagonia.com.ar/personas/index.php"
    assert bpat["sector"] == "Finanzas"
    assert bpat["country"] == "Argentina"
    cepu2 = next(record for record in records if record["symbol"] == "CEPU2")
    assert cepu2["company_name"] == "Central Puerto S.A."
    assert cepu2["panel_label"] == "S&P Merval"


def test_extract_argentina_cnv_emitter_regime_records_parses_workbook(
    tmp_path: Path,
) -> None:
    profile = create_finance_country_profiles(tmp_path / "remote-country-sources")["ar"]
    workbook_source = next(
        source
        for source in profile["sources"]
        if source["name"] == "cnv-emitter-regimes"
    )
    workbook_path = Path(urlparse(str(workbook_source["source_url"])).path)

    records = finance_country_module._extract_argentina_cnv_emitter_regime_records(
        workbook_path
    )

    assert [record["company_name"] for record in records] == [
        "Banco Patagonia SA",
        "Central Puerto SA",
        "Grupo Financiero Galicia S.A.",
        "YPF Sociedad Anónima",
    ]
    banco_patagonia = next(
        record for record in records if record["company_name"] == "Banco Patagonia SA"
    )
    assert banco_patagonia["cuit"] == "30-50000661-3"
    assert banco_patagonia["cnv_category"] == "Régimen General"
    assert banco_patagonia["cnv_category_status"] == "En Oferta Pública"


def test_extract_argentina_byma_people_entities_parses_board_and_market_representatives(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    administration_payload = json.loads(
        (fixture_root / "ar-bymadata-sociedades-administracion-ggal.json").read_text(
            encoding="utf-8"
        )
    )
    responsables_payload = json.loads(
        (fixture_root / "ar-bymadata-sociedades-responsables-ggal.json").read_text(
            encoding="utf-8"
        )
    )
    species_payload = json.loads(
        (fixture_root / "ar-bymadata-especies-general-ggal.json").read_text(
            encoding="utf-8"
        )
    )
    society_general_payload = json.loads(
        (fixture_root / "ar-bymadata-sociedades-general-ggal.json").read_text(
            encoding="utf-8"
        )
    )
    species_snapshot = finance_country_module._extract_argentina_byma_species_snapshot(
        species_payload,
        symbol="GGAL",
    )
    society_general_snapshot = (
        finance_country_module._extract_argentina_byma_society_general_snapshot(
            society_general_payload
        )
    )
    assert species_snapshot is not None
    assert society_general_snapshot is not None
    issuer_record = finance_country_module._build_argentina_byma_issuer_record(
        symbol="GGAL",
        species_snapshot=species_snapshot,
        society_general_snapshot=society_general_snapshot,
        listed_company_record=None,
    )
    assert issuer_record is not None

    entities = finance_country_module._extract_argentina_byma_people_entities(
        administration_payload=administration_payload,
        responsables_payload=responsables_payload,
        issuer_record=issuer_record,
    )

    canonical_names = {entity["canonical_text"] for entity in entities}
    assert canonical_names == {
        "Escasany Eduardo J.",
        "Gutierrez Pablo",
        "Estecho Claudia Raquel",
        "Enrique Pedemonte",
    }
    enrique = next(
        entity for entity in entities if entity["canonical_text"] == "Enrique Pedemonte"
    )
    assert enrique["metadata"]["category"] == "market_representative"
    assert "Pedemonte, Enrique" in enrique["aliases"]
    president = next(
        entity for entity in entities if entity["canonical_text"] == "Escasany Eduardo J."
    )
    assert president["metadata"]["category"] == "director"
    assert president["metadata"]["role_title"] == "Presidente"
    assert "Eduardo J. Escasany" in president["aliases"]


def test_extract_bcra_financial_institutions_archive_url_finds_latest_archive() -> None:
    html = """
    <html><body>
    <a href="/archivos/Pdfs/PublicacionesEstadisticas/Entidades/202512d.7z">202512d.7z</a>
    <a href="/archivos/Pdfs/PublicacionesEstadisticas/Entidades/202601d.7z">202601d.7z</a>
    </body></html>
    """

    archive_url = finance_country_module._extract_bcra_financial_institutions_archive_url(
        html,
        base_url="https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/",
    )

    assert (
        archive_url
        == "https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/Entidades/202601d.7z"
    )


def test_extract_bcra_financial_institutions_period_page_url_finds_latest_page() -> None:
    html = """
    <html><body>
    <select name="fecha">
      <option value="https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/?fecha=202512">Diciembre 2025</option>
      <option value="https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/?fecha=202601">Enero 2026</option>
    </select>
    </body></html>
    """

    period_page_url = (
        finance_country_module._extract_bcra_financial_institutions_period_page_url(
            html,
            base_url="https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/",
        )
    )

    assert (
        period_page_url
        == "https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/?fecha=202601"
    )


def test_extract_argentina_bcra_institution_records_parses_entity_metadata(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)

    records = finance_country_module._extract_argentina_bcra_institution_records(
        fixture_root / "ar-bcra-entidad-completo.txt"
    )

    assert [record["company_name"] for record in records] == [
        "BANCO DE GALICIA Y BUENOS AIRES S.A.",
        "BANCO BBVA ARGENTINA S.A.",
    ]
    galicia = next(record for record in records if record["entity_code"] == "00007")
    assert galicia["cuit"] == "30-50000173-5"
    assert galicia["institution_group"] == "Bancos Locales de Capital Nacional"
    assert galicia["website"] == "https://www.galicia.ar"
    assert galicia["email"] == "relacionesinstitucionales@bancogalicia.com.ar"


def test_extract_argentina_bcra_people_entities_parses_directors_and_customer_service(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    institution_records = finance_country_module._extract_argentina_bcra_institution_records(
        fixture_root / "ar-bcra-entidad-completo.txt"
    )
    records_by_code = {
        str(record["entity_code"]): record for record in institution_records
    }
    employer_entities_by_code = {
        str(record["entity_code"]): finance_country_module._build_argentina_bcra_institution_entity(
            record
        )
        for record in institution_records
    }

    director_entities = finance_country_module._extract_argentina_bcra_people_entities(
        rows_path=fixture_root / "ar-bcra-direct-completo.txt",
        source_name="bcra-financial-institutions-directors",
        institution_records_by_code=records_by_code,
        employer_entities_by_code=employer_entities_by_code,
    )
    customer_entities = finance_country_module._extract_argentina_bcra_people_entities(
        rows_path=fixture_root / "ar-bcra-respclie-completo.txt",
        source_name="bcra-financial-institutions-customer-service",
        institution_records_by_code=records_by_code,
        employer_entities_by_code=employer_entities_by_code,
    )

    canonical_names = {
        entity["canonical_text"] for entity in [*director_entities, *customer_entities]
    }
    assert canonical_names == {
        "Francia Guerrero Beatriz",
        "Grinenco Sergio",
        "Pando Guillermo Juan",
        "Rojas Leonardo Daniel",
        "Roizis Lautaro",
    }
    grinenco = next(
        entity for entity in director_entities if entity["canonical_text"] == "Grinenco Sergio"
    )
    assert grinenco["metadata"]["category"] == "director"
    assert "Sergio Grinenco" in grinenco["aliases"]
    roizis = next(
        entity for entity in customer_entities if entity["canonical_text"] == "Roizis Lautaro"
    )
    assert roizis["metadata"]["category"] == "customer_service_officer"
    assert roizis["metadata"]["contact_email"] == "circulodeservicios.edb.bcra@bancogalicia.com.ar"


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


def test_extract_japan_jpx_english_disclosure_records_parses_workbook(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "jpx-english-disclosure-availability.xlsx"
    _write_test_xlsx_workbook(
        workbook_path,
        sheets={
            "English Disclosure": [
                ["Availability of English Disclosure Information by Listed Companies"],
                [],
                ["as of March 31, 2026"],
                [],
                [
                    "Code",
                    "Company Name",
                    "Market Segment",
                    "Sector",
                    "Market Capitalization (JPY mil.)",
                    "Corporate Governance Reports disclosure status",
                    "Annual Securities Reports disclosure status",
                    "IR Presentations disclosure status",
                    "Other English Documents disclosed on IR Website",
                    "IR Website English Links",
                ],
                [
                    "1301",
                    "KYOKUYO CO.,LTD.",
                    "Prime",
                    "Fishery, Agriculture and Forestry",
                    "86,409",
                    "Available",
                    "Available",
                    "Available",
                    "Factsheet",
                    "https://www.kyokuyo.co.jp/en/index.html",
                ],
                [
                    "130A",
                    "Veritas In Silico Inc.",
                    "Growth",
                    "Services",
                    "11,245",
                    "Available",
                    "Available",
                    "Available",
                    "",
                    "https://www.veritasinsilico.com/en/",
                ],
                [
                    "1332",
                    "Nissui Corporation",
                    "Prime",
                    "Fishery, Agriculture and Forestry",
                    "287,517",
                    "Available",
                    "Available",
                    "Available",
                    "Integrated report",
                    "https://www.nissui.co.jp/english/index.html",
                ],
            ]
        },
    )

    records = finance_country_module._extract_japan_jpx_english_disclosure_records(
        workbook_path
    )

    assert [record["code"] for record in records] == ["1301", "130A", "1332"]
    assert [record["company_name"] for record in records] == [
        "Kyokuyo Co., Ltd.",
        "Veritas In Silico Inc.",
        "Nissui Corporation",
    ]
    assert records[0]["market_segment"] == "Prime"
    assert records[1]["market_segment"] == "Growth"
    assert records[0]["ir_website_url"] == "https://www.kyokuyo.co.jp/en/index.html"
    assert records[2]["annual_securities_reports_status"] == "Available"
    assert records[0]["as_of"] == "as of March 31, 2026"


def test_derive_japan_jpx_entities_emits_issuers_tickers_and_detail_people(
    tmp_path: Path,
) -> None:
    country_dir = tmp_path / "jp"
    country_dir.mkdir(parents=True, exist_ok=True)
    fixture_root = tmp_path / "remote-country-sources"
    profile = create_finance_country_profiles(fixture_root)["jp"]
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

    listed_company_results_fixture = (
        fixture_root / "jp-jpx-listed-company-search-results-page-1.html"
    )
    listed_company_results_dir = country_dir / "jpx-listed-company-search"
    listed_company_results_dir.mkdir(parents=True, exist_ok=True)
    listed_company_results_path = listed_company_results_dir / "page-1.html"
    listed_company_results_path.write_bytes(listed_company_results_fixture.read_bytes())
    downloaded_sources.append(
        {
            "name": "jpx-listed-company-search-results-page-1",
            "source_url": "https://www2.jpx.co.jp/tseHpFront/JJK020030Action.do",
            "path": str(listed_company_results_path.relative_to(country_dir)),
            "category": "issuer_directory",
        }
    )
    listed_company_detail_dir = country_dir / "jpx-listed-company-details"
    listed_company_detail_dir.mkdir(parents=True, exist_ok=True)
    for code in ("1301", "1332"):
        detail_fixture = fixture_root / f"jp-jpx-listed-company-detail-{code}.html"
        detail_path = listed_company_detail_dir / f"{code}.html"
        detail_path.write_bytes(detail_fixture.read_bytes())
        downloaded_sources.append(
            {
                "name": f"jpx-listed-company-detail-{code}",
                "source_url": "https://www2.jpx.co.jp/tseHpFront/JJK020030Action.do",
                "path": str(detail_path.relative_to(country_dir)),
                "category": "issuer_profile",
            }
        )

    derived_files = finance_country_module._derive_japan_jpx_entities(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent="ades-test/0.1.0",
    )

    assert {item["name"] for item in derived_files} == {
        "derived-finance-jp-issuers",
        "derived-finance-jp-tickers",
        "derived-finance-jp-people",
    }
    derived_file_by_name = {item["name"]: item for item in derived_files}
    assert (
        derived_file_by_name["derived-finance-jp-issuers"]["adapter"]
        == "jpx_listed_company_search_plus_workbook"
    )
    assert (
        derived_file_by_name["derived-finance-jp-people"]["adapter"]
        == "jpx_corporate_governance_reports_plus_detail_pages"
    )
    assert any(
        str(source["name"]) == "jpx-english-disclosure-availability-workbook"
        for source in downloaded_sources
    )
    issuer_payload = json.loads(
        (country_dir / "derived_issuer_entities.json").read_text(encoding="utf-8")
    )
    ticker_payload = json.loads(
        (country_dir / "derived_ticker_entities.json").read_text(encoding="utf-8")
    )
    people_payload = json.loads(
        (country_dir / "derived_people_entities.json").read_text(encoding="utf-8")
    )
    issuer_by_ticker = {
        entity["metadata"]["ticker"]: entity for entity in issuer_payload["entities"]
    }
    ticker_by_symbol = {
        entity["canonical_text"]: entity for entity in ticker_payload["entities"]
    }
    assert set(issuer_by_ticker) == {"1301", "130A", "1332", "7203"}
    assert set(ticker_by_symbol) == {"1301", "130A", "1332", "7203"}
    kyokuyo = issuer_by_ticker["1301"]
    assert kyokuyo["canonical_text"] == "Kyokuyo Co., Ltd."
    assert kyokuyo["metadata"]["market_segment"] == "Prime"
    assert kyokuyo["metadata"]["industry"] == "Fishery, Agriculture & Forestry"
    assert kyokuyo["metadata"]["fiscal_year_end"] == "March"
    assert kyokuyo["metadata"]["market_capitalization_jpy_millions"] == "86,409"
    assert kyokuyo["metadata"]["ir_website_url"] == (
        "https://www.kyokuyo.co.jp/en/index.html"
    )
    assert kyokuyo["metadata"]["source_as_of"] == "as of March 31, 2026"
    assert kyokuyo["metadata"]["ir_presentations_status"] == "Available"
    assert kyokuyo["metadata"]["stock_price_url"] == (
        "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=e_stock_detail&disptype=information&qcode=1301"
    )
    assert kyokuyo["metadata"]["isin_code"] == "JP3256800007"
    assert kyokuyo["metadata"]["date_of_incorporation"] == "1937-09-03"
    assert kyokuyo["metadata"]["date_of_listing"] == "1949-05-16"
    assert kyokuyo["metadata"]["representative_name"] == "Tetsuya Inoue"
    assert "KYOKUYO CO.,LTD." in kyokuyo["aliases"]
    assert "1301" in kyokuyo["aliases"]
    veritas_ticker = ticker_by_symbol["130A"]
    assert veritas_ticker["metadata"]["issuer_name"] == "Veritas In Silico Inc."
    assert veritas_ticker["metadata"]["market_segment"] == "Growth"
    assert veritas_ticker["metadata"]["industry"] == "Services"
    assert veritas_ticker["metadata"]["fiscal_year_end"] == "December"
    assert veritas_ticker["metadata"]["source_as_of"] == "as of March 31, 2026"
    assert veritas_ticker["metadata"]["annual_securities_reports_status"] == "Available"
    assert "isin_code" not in veritas_ticker["metadata"]
    assert "Veritas In Silico Inc." in veritas_ticker["aliases"]
    nissui_ticker = ticker_by_symbol["1332"]
    assert nissui_ticker["metadata"]["market_capitalization_jpy_millions"] == "287,517"
    assert nissui_ticker["metadata"]["ir_presentations_status"] == "Available"
    assert nissui_ticker["metadata"]["other_english_documents"] == "Integrated report"
    assert nissui_ticker["metadata"]["isin_code"] == "JP3718800000"
    assert nissui_ticker["metadata"]["representative_title"] == (
        "Representative Director and President"
    )
    assert nissui_ticker["metadata"]["representative_name"] == "Teru Tanaka"
    assert nissui_ticker["metadata"]["date_of_listing"] == "1949-05-16"
    toyota = issuer_by_ticker["7203"]
    assert toyota["canonical_text"] == "Toyota Motor Corporation"
    assert toyota["metadata"]["market_segment"] == "Prime"
    assert toyota["metadata"]["industry"] == "Transportation Equipment"
    assert toyota["metadata"]["fiscal_year_end"] == "March"
    assert toyota["metadata"]["stock_price_url"] == (
        "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=e_stock_detail&disptype=information&qcode=7203"
    )
    assert "isin_code" not in toyota["metadata"]
    assert {entity["canonical_text"] for entity in people_payload["entities"]} == {
        "Tetsuya Inoue",
        "Teru Tanaka",
    }


def test_extract_japan_listed_company_search_records_parses_results_page(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    results_path = fixture_root / "jp-jpx-listed-company-search-results-page-1.html"

    records = finance_country_module._extract_japan_listed_company_search_records(
        results_path.read_text(encoding="utf-8")
    )

    assert [record["code"] for record in records] == ["1301", "130A", "7203"]
    assert [record["company_name"] for record in records] == [
        "Kyokuyo Co., Ltd.",
        "Veritas In Silico Inc.",
        "Toyota Motor Corporation",
    ]
    assert records[0]["industry"] == "Fishery, Agriculture & Forestry"
    assert records[1]["fiscal_year_end"] == "December"
    assert records[0]["listed_company_search_code"] == "13010"
    assert records[1]["listed_company_search_delisted_flag"] == "1"
    assert records[2]["stock_price_url"] == (
        "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=e_stock_detail&disptype=information&qcode=7203"
    )


def test_download_japan_listed_company_search_pages_posts_segment_list(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    results_html = (
        fixture_root / "jp-jpx-listed-company-search-results-page-1.html"
    ).read_text(encoding="utf-8")

    class _FakeRequest:
        def __init__(self, url: str) -> None:
            self.url = url

    class _FakeResponse:
        def __init__(self, *, text: str, url: str) -> None:
            self.text = text
            self.request = _FakeRequest(url)

        def raise_for_status(self) -> None:
            return None

    class _FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def __enter__(self) -> "_FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb
            return None

        def get(self, url: str) -> _FakeResponse:
            assert url == finance_country_module._JPX_LISTED_COMPANY_SEARCH_APP_URL
            return _FakeResponse(
                text=(
                    '<html><body><form name="JJK020010Form" '
                    'action="/tseHpFront/JJK020010Action.do"></form></body></html>'
                ),
                url=url,
            )

        def post(self, url: str, *, data: dict[str, object]) -> _FakeResponse:
            assert url == "https://www2.jpx.co.jp/tseHpFront/JJK020010Action.do"
            assert data["ListShow"] == "ListShow"
            assert data["dspSsuPd"] == finance_country_module._JPX_LISTED_COMPANY_SEARCH_PAGE_SIZE
            assert data["szkbuChkbx"] == list(
                finance_country_module._JPX_LISTED_COMPANY_SEARCH_SEGMENT_VALUES
            )
            return _FakeResponse(text=results_html, url=url)

    monkeypatch.setattr(finance_country_module.httpx, "Client", _FakeClient)

    country_dir = tmp_path / "jp"
    downloaded_sources: list[dict[str, object]] = []
    warnings: list[str] = []

    page_paths = finance_country_module._download_japan_listed_company_search_pages(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent="ades-test/0.1.0",
        warnings=warnings,
    )

    assert warnings == []
    assert [path.relative_to(country_dir).as_posix() for path in page_paths] == [
        "jpx-listed-company-search/page-1.html"
    ]
    assert downloaded_sources == [
        {
            "name": "jpx-listed-company-search-results-page-1",
            "source_url": "https://www2.jpx.co.jp/tseHpFront/JJK020010Action.do",
            "path": "jpx-listed-company-search/page-1.html",
            "category": "issuer_directory",
            "notes": "page=1; market_segments=011,012,013",
        }
    ]


def test_extract_japan_listed_company_detail_record_parses_detail_page(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    detail_path = fixture_root / "jp-jpx-listed-company-detail-1332.html"

    record = finance_country_module._extract_japan_listed_company_detail_record(
        detail_path.read_text(encoding="utf-8")
    )

    assert record["code"] == "1332"
    assert record["listed_company_search_code"] == "13320"
    assert record["company_name"] == "Nissui Corporation"
    assert record["isin_code"] == "JP3718800000"
    assert record["date_of_incorporation"] == "1943-03-31"
    assert record["date_of_listing"] == "1949-05-16"
    assert record["representative_name"] == "Teru Tanaka"
    assert record["listed_shares_count"] == "312,430,277"
    assert record["controlling_shareholders_information"] == "None"


def test_extract_japan_corporate_governance_report_records_parses_results_page(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    results_path = fixture_root / "jp-jpx-corporate-governance-results-page-1.html"

    records = finance_country_module._extract_japan_corporate_governance_report_records(
        results_path.read_text(encoding="utf-8")
    )

    assert [record["code"] for record in records] == ["1301", "1332"]
    assert [record["company_name"] for record in records] == [
        "Kyokuyo Co., Ltd.",
        "Nissui Corporation",
    ]
    assert records[0]["report_date"] == "2026-03-31"
    assert records[1]["report_url"] == (
        "https://www2.jpx.co.jp/disc/13320/140120250903552493.pdf"
    )
    assert records[1]["heading"] == "[Delayed] Corporate Governance Report (June 26,2025)"


def test_extract_people_from_japan_corporate_governance_report_parses_public_people(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    report_text = (
        fixture_root / "jp-jpx-corporate-governance-report-1332.txt"
    ).read_text(encoding="utf-8")

    entities = finance_country_module._extract_people_from_japan_corporate_governance_report(
        text=report_text,
        issuer_record={"company_name": "Nissui Corporation", "code": "1332"},
        source_name="jpx-corporate-governance-report-1332",
        report_url="https://www2.jpx.co.jp/disc/13320/140120250903552493.pdf",
        report_date="2025-09-03",
        report_heading="[Delayed] Corporate Governance Report (June 26,2025)",
    )

    assert {entity["canonical_text"] for entity in entities} == {
        "Teru Tanaka",
        "Kunihiko Umemura",
    }
    representative = next(
        entity for entity in entities if entity["canonical_text"] == "Teru Tanaka"
    )
    assert representative["metadata"]["category"] == "executive_officer"
    assert representative["metadata"]["employer_ticker"] == "1332"
    assert representative["metadata"]["report_date"] == "2025-09-03"
    ir_contact = next(
        entity for entity in entities if entity["canonical_text"] == "Kunihiko Umemura"
    )
    assert ir_contact["metadata"]["category"] == "investor_relations"
    assert ir_contact["metadata"]["role_title"] == "Investor Relations Contact"
    assert "Investor Relations" in ir_contact["metadata"]["contact_details"]


def test_derive_japan_jpx_entities_emits_people_from_governance_reports(
    tmp_path: Path,
    monkeypatch,
) -> None:
    country_dir = tmp_path / "jp"
    country_dir.mkdir(parents=True, exist_ok=True)
    fixture_root = tmp_path / "remote-country-sources"
    profile = create_finance_country_profiles(fixture_root)["jp"]
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

    governance_results_fixture = (
        fixture_root / "jp-jpx-corporate-governance-results-page-1.html"
    )
    governance_results_dir = country_dir / "jpx-corporate-governance-search"
    governance_results_dir.mkdir(parents=True, exist_ok=True)
    governance_results_path = governance_results_dir / "page-1.html"
    governance_results_path.write_bytes(governance_results_fixture.read_bytes())
    downloaded_sources.append(
        {
            "name": "jpx-corporate-governance-search-results-page-1",
            "source_url": "https://www2.jpx.co.jp/tseHpFront/CGK020010Action.do?Show=Show",
            "path": str(governance_results_path.relative_to(country_dir)),
            "category": "issuer_profile",
        }
    )
    listed_company_detail_dir = country_dir / "jpx-listed-company-details"
    listed_company_detail_dir.mkdir(parents=True, exist_ok=True)
    veritas_detail_fixture = fixture_root / "jp-jpx-listed-company-detail-130a.html"
    veritas_detail_path = listed_company_detail_dir / "130a.html"
    veritas_detail_path.write_bytes(veritas_detail_fixture.read_bytes())
    downloaded_sources.append(
        {
            "name": "jpx-listed-company-detail-130a",
            "source_url": "https://www2.jpx.co.jp/tseHpFront/JJK020030Action.do",
            "path": str(veritas_detail_path.relative_to(country_dir)),
            "category": "issuer_profile",
        }
    )

    original_download_source = finance_country_module._download_source
    governance_report_fixtures = {
        "https://www2.jpx.co.jp/disc/13010/140120260331123456.pdf": (
            fixture_root / "jp-jpx-corporate-governance-report-1301.txt"
        ),
        "https://www2.jpx.co.jp/disc/13320/140120250903552493.pdf": (
            fixture_root / "jp-jpx-corporate-governance-report-1332.txt"
        ),
    }

    def fake_download_source(
        source_url: str | Path,
        destination: Path,
        *,
        user_agent: str,
    ) -> str:
        source_url_text = str(source_url)
        fixture_path = governance_report_fixtures.get(source_url_text)
        if fixture_path is not None:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(fixture_path.read_bytes())
            return source_url_text
        return original_download_source(source_url, destination, user_agent=user_agent)

    monkeypatch.setattr(finance_country_module, "_download_source", fake_download_source)

    derived_files = finance_country_module._derive_japan_jpx_entities(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent="ades-test/0.1.0",
    )

    assert {item["name"] for item in derived_files} == {
        "derived-finance-jp-issuers",
        "derived-finance-jp-tickers",
        "derived-finance-jp-people",
    }
    derived_file_by_name = {item["name"]: item for item in derived_files}
    assert (
        derived_file_by_name["derived-finance-jp-people"]["adapter"]
        == "jpx_corporate_governance_reports_plus_detail_pages"
    )
    issuers = finance_country_module._load_curated_entities(
        country_dir / "derived_issuer_entities.json"
    )
    kyokuyo = next(entity for entity in issuers if entity["canonical_text"] == "Kyokuyo Co., Ltd.")
    assert kyokuyo["metadata"]["corporate_governance_report_url"] == (
        "https://www2.jpx.co.jp/disc/13010/140120260331123456.pdf"
    )
    assert kyokuyo["metadata"]["corporate_governance_report_date"] == "2026-03-31"
    tickers = finance_country_module._load_curated_entities(
        country_dir / "derived_ticker_entities.json"
    )
    nissui_ticker = next(entity for entity in tickers if entity["canonical_text"] == "1332")
    assert nissui_ticker["metadata"]["corporate_governance_report_heading"] == (
        "[Delayed] Corporate Governance Report (June 26,2025)"
    )
    people = finance_country_module._load_curated_entities(
        country_dir / "derived_people_entities.json"
    )
    assert {entity["canonical_text"] for entity in people} == {
        "Atsushi Nakamura",
        "Tetsuya Inoue",
        "Haruka Sato",
        "Teru Tanaka",
        "Kunihiko Umemura",
    }
    atsushi = next(entity for entity in people if entity["canonical_text"] == "Atsushi Nakamura")
    assert atsushi["metadata"]["category"] == "executive_officer"
    assert atsushi["metadata"]["employer_ticker"] == "130A"
    assert atsushi["metadata"]["role_title"] == "Representative Director and CEO"
    assert atsushi["metadata"]["section_name"] == "listed company detail page"
    assert atsushi["metadata"]["source_name"] == "jpx-listed-company-detail-130a"
    assert atsushi["metadata"]["listed_company_detail_url"] == (
        "https://www2.jpx.co.jp/tseHpFront/JJK020030Action.do"
    )
    haruka = next(entity for entity in people if entity["canonical_text"] == "Haruka Sato")
    assert haruka["metadata"]["category"] == "investor_relations"
    assert haruka["metadata"]["employer_ticker"] == "1301"
    assert any(
        str(source["name"]).startswith("jpx-corporate-governance-report-")
        and str(source["path"]).startswith("jpx-corporate-governance-reports/")
        for source in downloaded_sources
    )


def test_derive_argentina_byma_entities_emits_issuers_tickers_and_people(
    tmp_path: Path,
    monkeypatch,
) -> None:
    country_dir = tmp_path / "ar"
    country_dir.mkdir(parents=True, exist_ok=True)
    fixture_root = tmp_path / "remote-country-sources"
    profile = create_finance_country_profiles(fixture_root)["ar"]
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

    fixture_paths = {
        ("species", "YPFD"): fixture_root / "ar-bymadata-especies-general-ypfd.json",
        ("society_general", "YPFD"): fixture_root / "ar-bymadata-sociedades-general-ypfd.json",
        (
            "society_administration",
            "YPFD",
        ): fixture_root / "ar-bymadata-sociedades-administracion-ypfd.json",
        (
            "society_responsables",
            "YPFD",
        ): fixture_root / "ar-bymadata-sociedades-responsables-ypfd.json",
        ("species", "GGAL"): fixture_root / "ar-bymadata-especies-general-ggal.json",
        ("society_general", "GGAL"): fixture_root / "ar-bymadata-sociedades-general-ggal.json",
        (
            "society_administration",
            "GGAL",
        ): fixture_root / "ar-bymadata-sociedades-administracion-ggal.json",
        (
            "society_responsables",
            "GGAL",
        ): fixture_root / "ar-bymadata-sociedades-responsables-ggal.json",
        ("species", "BPAT"): fixture_root / "ar-bymadata-especies-general-bpat.json",
        (
            "society_administration",
            "BPAT",
        ): fixture_root / "ar-bymadata-sociedades-administracion-bpat.json",
        (
            "society_responsables",
            "BPAT",
        ): fixture_root / "ar-bymadata-sociedades-responsables-bpat.json",
        ("species", "CEPU2"): fixture_root / "ar-bymadata-especies-general-cepu2.json",
        ("society_general", "CEPU2"): fixture_root / "ar-bymadata-sociedades-general-cepu2.json",
        (
            "society_administration",
            "CEPU2",
        ): fixture_root / "ar-bymadata-sociedades-administracion-cepu2.json",
        (
            "society_responsables",
            "CEPU2",
        ): fixture_root / "ar-bymadata-sociedades-responsables-cepu2.json",
    }

    def fake_extract_bcra_7z_members(
        archive_path: Path,
        *,
        output_dir: Path,
        members: tuple[str, ...],
    ) -> dict[str, Path]:
        del archive_path
        fixture_map = {
            "Entfin/Tec_Cont/entidad/COMPLETO.TXT": fixture_root / "ar-bcra-entidad-completo.txt",
            "Entfin/Direct/COMPLETO.TXT": fixture_root / "ar-bcra-direct-completo.txt",
            "Entfin/Gerent/COMPLETO.TXT": fixture_root / "ar-bcra-gerent-completo.txt",
            "Entfin/Respclie/completo.txt": fixture_root / "ar-bcra-respclie-completo.txt",
        }
        extracted: dict[str, Path] = {}
        for member in members:
            fixture_path = fixture_map.get(member)
            if fixture_path is None:
                continue
            destination = output_dir / Path(member)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(fixture_path.read_bytes())
            extracted[member] = destination
        return extracted

    def fake_fetch_json_post_source(
        source_url: str,
        destination: Path,
        *,
        payload: dict[str, object],
        user_agent: str,
    ) -> object:
        del user_agent
        symbol = str(payload["symbol"])
        source_kind = {
            finance_country_module._BYMA_DATA_SPECIES_GENERAL_URL: "species",
            finance_country_module._BYMA_DATA_SOCIETY_GENERAL_URL: "society_general",
            finance_country_module._BYMA_DATA_SOCIETY_ADMINISTRATION_URL: "society_administration",
            finance_country_module._BYMA_DATA_SOCIETY_RESPONSIBLES_URL: "society_responsables",
        }.get(source_url)
        if source_kind == "society_general" and symbol in {"AYPF", "BPAT"}:
            request = finance_country_module.httpx.Request("POST", source_url)
            response = finance_country_module.httpx.Response(400, request=request)
            raise finance_country_module.httpx.HTTPStatusError(
                "400 Bad Request",
                request=request,
                response=response,
            )
        fixture_path = fixture_paths.get((str(source_kind), symbol))
        if fixture_path is None:
            raise AssertionError(f"Unexpected BYMA fetch in test: {source_url} {payload}")
        response_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(response_payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        return response_payload

    monkeypatch.setattr(
        finance_country_module,
        "_fetch_json_post_source",
        fake_fetch_json_post_source,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_fetch_wikidata_api_json",
        _fake_argentina_wikidata_api_json,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_extract_bcra_7z_members",
        fake_extract_bcra_7z_members,
    )

    derived_files = finance_country_module._derive_argentina_byma_entities(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
        user_agent="ades-test/0.1.0",
    )

    assert {item["name"] for item in derived_files} == {
        "derived-finance-ar-issuers",
        "derived-finance-ar-institutions",
        "derived-finance-ar-tickers",
        "derived-finance-ar-people",
    }
    issuer_payload = json.loads(
        (country_dir / "derived_issuer_entities.json").read_text(encoding="utf-8")
    )
    institution_payload = json.loads(
        (country_dir / "derived_institution_entities.json").read_text(encoding="utf-8")
    )
    ticker_payload = json.loads(
        (country_dir / "derived_ticker_entities.json").read_text(encoding="utf-8")
    )
    people_payload = json.loads(
        (country_dir / "derived_people_entities.json").read_text(encoding="utf-8")
    )

    issuer_names = {
        entity["canonical_text"] for entity in issuer_payload["entities"]
    }
    assert issuer_names == {
        "Banco Patagonia S.A.",
        "Central Puerto S.A.",
        "Grupo Financiero Galicia S.A.",
        "YPF S.A.",
    }
    bpat_issuer = next(
        entity
        for entity in issuer_payload["entities"]
        if entity["canonical_text"] == "Banco Patagonia S.A."
    )
    assert bpat_issuer["metadata"]["ticker"] == "BPAT"
    assert bpat_issuer["metadata"]["isin"] == "ARMERI013163"
    assert bpat_issuer["metadata"]["issuer_website"] == (
        "https://www.bancopatagonia.com.ar/personas/index.php"
    )
    assert bpat_issuer["metadata"]["sector"] == "Finanzas"
    assert bpat_issuer["metadata"]["byma_panel"] == "S&P BYMA General"
    assert bpat_issuer["metadata"]["cuit"] == "30-50000661-3"
    assert bpat_issuer["metadata"]["cnv_category"] == "Régimen General"
    assert bpat_issuer["metadata"]["cnv_category_status"] == "En Oferta Pública"
    cepu2_issuer = next(
        entity
        for entity in issuer_payload["entities"]
        if entity["canonical_text"] == "Central Puerto S.A."
    )
    assert cepu2_issuer["metadata"]["ticker"] == "CEPU2"
    assert cepu2_issuer["metadata"]["isin"] == "ARP2354Y1011"
    assert cepu2_issuer["metadata"]["issuer_website"] == "https://www.centralpuerto.com/"
    assert cepu2_issuer["metadata"]["sector"] == "Utilities"
    ggal_issuer = next(
        entity
        for entity in issuer_payload["entities"]
        if entity["canonical_text"] == "Grupo Financiero Galicia S.A."
    )
    assert "Grupo Financiero Galicia" in ggal_issuer["aliases"]
    assert "Grupo Galicia" in ggal_issuer["aliases"]
    assert ggal_issuer["metadata"]["ticker"] == "GGAL"
    assert ggal_issuer["metadata"]["isin"] == "ARP495251018"
    assert ggal_issuer["metadata"]["issuer_website"] == "www.gfgsa.com"
    assert ggal_issuer["metadata"]["wikidata_qid"] == "Q3118234"
    assert ggal_issuer["metadata"]["wikidata_slug"] == "Q3118234"
    institution_names = {
        entity["canonical_text"] for entity in institution_payload["entities"]
    }
    assert institution_names == {
        "BANCO BBVA ARGENTINA S.A.",
        "BANCO DE GALICIA Y BUENOS AIRES S.A.",
    }
    bbva = next(
        entity
        for entity in institution_payload["entities"]
        if entity["canonical_text"] == "BANCO BBVA ARGENTINA S.A."
    )
    assert bbva["metadata"]["category"] == "regulated_institution"
    assert bbva["metadata"]["cuit"] == "30-50001008-0"
    assert bbva["metadata"]["wikidata_qid"] == "Q130001011"
    assert bbva["metadata"]["wikidata_slug"] == "Q130001011"
    ticker_symbols = {
        entity["canonical_text"] for entity in ticker_payload["entities"]
    }
    assert ticker_symbols == {"BPAT", "CEPU2", "GGAL", "YPFD"}
    ypfd = next(
        entity for entity in ticker_payload["entities"] if entity["canonical_text"] == "YPFD"
    )
    assert ypfd["metadata"]["leader_panel"] is True
    assert ypfd["metadata"]["issuer_name"] == "YPF S.A."
    assert ypfd["metadata"]["wikidata_qid"] == "Q2006989"
    assert ypfd["metadata"]["wikidata_slug"] == "Q2006989"
    assert ypfd["metadata"]["issuer_cuit"] == "30-54668929-7"
    assert ypfd["metadata"]["cnv_category"] == "Régimen General"
    cepu2 = next(
        entity for entity in ticker_payload["entities"] if entity["canonical_text"] == "CEPU2"
    )
    assert cepu2["metadata"]["issuer_name"] == "Central Puerto S.A."
    assert cepu2["metadata"]["byma_panel"] == "S&P Merval"
    assert cepu2["metadata"]["issuer_cuit"] == "30-60950650-7"
    people_names = {
        entity["canonical_text"] for entity in people_payload["entities"]
    }
    assert {
        "Diego Andres Ferreyra",
        "Margarita Chun",
        "Parre Dos Santos Oswaldo",
        "Reca Osvaldo Arturo",
        "Escasany Eduardo J.",
        "Francia Guerrero Beatriz",
        "Marin Horacio Daniel",
        "Roizis Lautaro",
        "Enrique Pedemonte",
        "Rivas Diego",
    } <= people_names
    market_representative = next(
        entity
        for entity in people_payload["entities"]
        if entity["canonical_text"] == "Margarita Chun"
    )
    assert market_representative["metadata"]["category"] == "market_representative"
    assert "CHUN, MARGARITA" in market_representative["aliases"]
    marin = next(
        entity
        for entity in people_payload["entities"]
        if entity["canonical_text"] == "Marin Horacio Daniel"
    )
    assert "Horacio Daniel Marin" in marin["aliases"]
    assert marin["metadata"]["wikidata_qid"] == "Q130001001"
    assert marin["metadata"]["wikidata_slug"] == "Q130001001"
    escasany = next(
        entity
        for entity in people_payload["entities"]
        if entity["canonical_text"] == "Escasany Eduardo J."
    )
    assert "Eduardo J. Escasany" in escasany["aliases"]
    assert escasany["metadata"]["wikidata_qid"] == "Q130001002"
    assert escasany["metadata"]["wikidata_slug"] == "Q130001002"
    parre = next(
        entity
        for entity in people_payload["entities"]
        if entity["canonical_text"] == "Parre Dos Santos Oswaldo"
    )
    assert parre["metadata"]["employer_ticker"] == "BPAT"
    assert parre["metadata"]["category"] == "director"
    assert parre["metadata"]["employer_cuit"] == "30-50000661-3"
    reca = next(
        entity
        for entity in people_payload["entities"]
        if entity["canonical_text"] == "Reca Osvaldo Arturo"
    )
    assert reca["metadata"]["employer_ticker"] == "CEPU2"
    assert reca["metadata"]["category"] == "director"
    assert reca["metadata"]["employer_cuit"] == "30-60950650-7"
    ferreyra = next(
        entity
        for entity in people_payload["entities"]
        if entity["canonical_text"] == "Diego Andres Ferreyra"
    )
    assert ferreyra["metadata"]["category"] == "market_representative"
    assert ferreyra["metadata"]["employer_ticker"] == "BPAT"
    assert ferreyra["metadata"]["employer_cuit"] == "30-50000661-3"
    roizis = next(
        entity
        for entity in people_payload["entities"]
        if entity["canonical_text"] == "Roizis Lautaro"
    )
    assert roizis["metadata"]["category"] == "customer_service_officer"
    assert roizis["metadata"]["employer_cuit"] == "30-50000173-5"
    rivas = next(
        entity
        for entity in people_payload["entities"]
        if entity["canonical_text"] == "Rivas Diego"
    )
    assert rivas["metadata"]["category"] == "executive_officer"
    assert any(
        str(source["name"]).startswith("bymadata-sociedades-general-ypfd")
        for source in downloaded_sources
    )
    assert any(
        str(source["name"]) == "bcra-financial-institutions-archive"
        for source in downloaded_sources
    )
    assert any(
        str(source["name"]) == "bcra-financial-institutions-entidad-completo"
        for source in downloaded_sources
    )
    assert any(
        str(source["name"]) == "wikidata-argentina-entity-resolution"
        and str(source["path"]) == "wikidata-argentina-entity-resolution.json"
        for source in downloaded_sources
    )
    assert any(
        str(source["name"]) == "cnv-emitter-regimes"
        for source in downloaded_sources
    )
    assert not any(
        entity["canonical_text"] == "AYPF" for entity in ticker_payload["entities"]
    )


def test_find_argentina_bcra_matching_organization_prefers_cuit_match() -> None:
    matched_entity = finance_country_module._find_argentina_bcra_matching_organization(
        record={
            "company_name": "Banco Patagonia Sociedad Anonima",
            "reduced_name": "Banco Patagonia",
            "abbreviated_name": "BPAT",
            "cuit": "30-50000661-3",
        },
        entities=[
            {
                "entity_type": "organization",
                "canonical_text": "Banco Patagonia S.A.",
                "aliases": ["Banco Patagonia"],
                "entity_id": "finance-ar-issuer:ARMERI013163",
                "metadata": {
                    "country_code": "ar",
                    "category": "issuer",
                    "ticker": "BPAT",
                    "isin": "ARMERI013163",
                    "cuit": "30-50000661-3",
                },
            }
        ],
    )

    assert matched_entity is not None
    assert matched_entity["canonical_text"] == "Banco Patagonia S.A."


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

    branch_entities = finance_country_module._merge_germany_bafin_institutions(
        bafin_csv_path=csv_path,
        issuer_entities=issuer_entities,
        ticker_entities=ticker_entities,
        warnings=warnings,
    )

    assert warnings == []
    assert branch_entities == []
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


def test_merge_germany_bafin_institutions_emits_germany_branch_entity(
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
                    "ING BANK N.V.;30030030;KVK88888;70090003;724500A2PXFMEQ9D8H38;;60439;"
                    "Frankfurt am Main;Theodor-Heuss-Allee 2;Niederlande;"
                    "Kreditinstitut, Zweigniederlassung in Deutschland;"
                    "Ombudsman Finanzdienstleister;ING-DiBa AG;"
                    "ING Bank N.V. Frankfurt Branch;complaints@ing.example;Deutschland;"
                    "Kreditgeschäft;03.03.2015;;"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    issuer_entities: list[dict[str, object]] = []
    ticker_entities: list[dict[str, object]] = []
    warnings: list[str] = []

    branch_entities = finance_country_module._merge_germany_bafin_institutions(
        bafin_csv_path=csv_path,
        issuer_entities=issuer_entities,
        ticker_entities=ticker_entities,
        warnings=warnings,
    )

    assert warnings == []
    assert len(issuer_entities) == 1
    parent = issuer_entities[0]
    assert parent["canonical_text"] == "Ing Bank N.V."
    assert parent["metadata"]["germany_branch"] == "ING Bank N.V. Frankfurt Branch"
    assert len(branch_entities) == 1
    branch = branch_entities[0]
    assert branch["canonical_text"] == "ING Bank N.V. Frankfurt Branch"
    assert branch["metadata"]["category"] == "regulated_institution_branch"
    assert branch["metadata"]["parent_institution_name"] == "Ing Bank N.V."
    assert branch["metadata"]["parent_institution_entity_id"] == (
        "finance-de-institution:bafin:30030030"
    )
    assert branch["metadata"]["country"] == "Deutschland"
    assert branch["metadata"]["home_country"] == "Niederlande"
    assert branch["metadata"]["consumer_complaint_contacts"] == "complaints@ing.example"
    assert branch["metadata"]["bafin_activities"] == ["Kreditgeschäft"]
    assert "ING Bank N.V. Frankfurt Branch" in branch["aliases"]
    assert "ING Bank NV Frankfurt Branch" in branch["aliases"]


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


def test_score_germany_organization_wikidata_candidate_prefers_lei_backed_match() -> None:
    entity = {
        "entity_type": "organization",
        "canonical_text": "ING Bank N.V.",
        "aliases": ["ING-DiBa AG"],
        "entity_id": "finance-de-institution:bafin:30030030",
        "metadata": {
            "country_code": "de",
            "category": "regulated_institution",
            "lei": "724500A2PXFMEQ9D8H38",
        },
    }

    weak_exact_name_score = finance_country_module._score_germany_organization_wikidata_candidate(
        entity=entity,
        candidate_payload=_GERMANY_WIKIDATA_ENTITY_FIXTURES["Q131553738"],
    )
    lei_backed_score = finance_country_module._score_germany_organization_wikidata_candidate(
        entity=entity,
        candidate_payload=_GERMANY_WIKIDATA_ENTITY_FIXTURES["Q2304977"],
    )

    assert lei_backed_score > weak_exact_name_score


def test_resolve_germany_organization_wikidata_matches_uses_lei_candidates(
    monkeypatch,
) -> None:
    entity = {
        "entity_type": "organization",
        "canonical_text": "Ing Bank N.V.",
        "aliases": ["ING-DiBa AG"],
        "entity_id": "finance-de-institution:bafin:30030030",
        "metadata": {
            "country_code": "de",
            "category": "regulated_institution",
            "lei": "724500A2PXFMEQ9D8H38",
        },
    }

    monkeypatch.setattr(
        finance_country_module,
        "_query_wikidata_entity_ids_by_string_property",
        lambda *, property_id, values, user_agent, warnings: {
            "724500A2PXFMEQ9D8H38": ["Q2304977"]
        },
    )
    monkeypatch.setattr(
        finance_country_module,
        "_search_wikidata_entities",
        lambda query, *, user_agent: [],
    )
    monkeypatch.setattr(
        finance_country_module,
        "_fetch_wikidata_entity_payloads",
        lambda *, entity_ids, user_agent, warnings: {
            "Q2304977": _GERMANY_WIKIDATA_ENTITY_FIXTURES["Q2304977"]
        },
    )

    matches, warnings = finance_country_module._resolve_germany_organization_wikidata_matches(
        organization_entities=[entity],
        user_agent="ades-test/0.1.0",
    )

    assert warnings == []
    match = matches["finance-de-institution:bafin:30030030"]
    assert match["qid"] == "Q2304977"
    assert match["slug"] == "Q2304977"
    assert match["label"] == "Ing Bank N.V."
    assert match["url"] == "https://www.wikidata.org/wiki/Q2304977"
    assert "Ing Bank N.V." in match["aliases"]
    assert "ING-DiBa AG" in match["aliases"]


def test_resolve_germany_issuer_wikidata_matches_uses_isin_candidates(
    monkeypatch,
) -> None:
    entity = {
        "entity_type": "organization",
        "canonical_text": "Mercedes-Benz Group AG",
        "aliases": ["Mercedes-Benz Group", "Daimler AG"],
        "entity_id": "finance-de-issuer:DE0007100000",
        "metadata": {
            "country_code": "de",
            "category": "issuer",
            "isin": "DE0007100000",
            "ticker": "MBG",
            "wkn": "710000",
        },
    }

    monkeypatch.setattr(
        finance_country_module,
        "_query_wikidata_entity_ids_by_string_property",
        lambda *, property_id, values, user_agent, warnings: {
            "DE0007100000": ["Q27530"]
        },
    )
    monkeypatch.setattr(
        finance_country_module,
        "_search_wikidata_entities",
        lambda query, *, user_agent: [],
    )
    monkeypatch.setattr(
        finance_country_module,
        "_fetch_wikidata_entity_payloads",
        lambda *, entity_ids, user_agent, warnings: {
            "Q27530": _GERMANY_WIKIDATA_ENTITY_FIXTURES["Q27530"]
        },
    )

    matches, warnings = finance_country_module._resolve_germany_issuer_wikidata_matches(
        issuer_entities=[entity],
        user_agent="ades-test/0.1.0",
    )

    assert warnings == []
    match = matches["DE0007100000"]
    assert match["qid"] == "Q27530"
    assert match["slug"] == "Q27530"
    assert match["label"] == "Mercedes-Benz Group AG"
    assert match["url"] == "https://www.wikidata.org/wiki/Q27530"
    assert "Daimler AG" in match["aliases"]


def test_build_germany_wikidata_public_people_snapshot_extracts_human_claims(
    monkeypatch,
) -> None:
    issuer_entities = [
        {
            "entity_type": "organization",
            "canonical_text": "Mercedes-Benz Group AG",
            "aliases": ["Mercedes-Benz Group", "Daimler AG"],
            "entity_id": "finance-de-issuer:DE0007100000",
            "metadata": {
                "country_code": "de",
                "category": "issuer",
                "isin": "DE0007100000",
                "ticker": "MBG",
            },
        }
    ]
    monkeypatch.setattr(
        finance_country_module,
        "_query_germany_wikidata_public_people_rows",
        _fake_germany_wikidata_public_people_rows,
    )

    organization_people, warnings = (
        finance_country_module._build_germany_wikidata_public_people_snapshot(
            issuer_entities=issuer_entities,
            issuer_matches={
                "DE0007100000": {
                    "qid": "Q27530",
                    "slug": "Q27530",
                    "entity_id": "wikidata:Q27530",
                    "url": "https://www.wikidata.org/wiki/Q27530",
                    "label": "Mercedes-Benz Group",
                    "aliases": ["Daimler AG"],
                }
            },
            organization_matches={},
            user_agent="ades-test/0.1.0",
        )
    )

    assert warnings == []
    people = organization_people["finance-de-issuer:DE0007100000"]
    assert {item["canonical_text"] for item in people} == {
        "Britta Seeger",
        "Harald Wilhelm",
        "Martin Brudermüller",
        "Ola Källenius",
        "Sabine Kohleisen",
    }
    ola = next(item for item in people if item["canonical_text"] == "Ola Källenius")
    assert ola["qid"] == "Q19297571"
    assert ola["role_class"] == "executive_officer"
    assert ola["role_title"] == "Chief Executive Officer"
    martin = next(
        item for item in people if item["canonical_text"] == "Martin Brudermüller"
    )
    assert martin["role_class"] == "director"
    assert martin["role_title"] == "Chairperson"
    assert "Martin Brudermuller" in martin["aliases"]
    assert all(item["qid"] != "Q1794766" for item in people)


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


def test_extract_deutsche_boerse_company_details_contact_and_shareholder_metadata(
    tmp_path: Path,
) -> None:
    fixture_root = tmp_path / "remote-country-sources"
    create_finance_country_profiles(fixture_root)
    company_details_html = (fixture_root / "de-mercedes-company-details.html").read_text(
        encoding="utf-8"
    )

    contact_details = (
        finance_country_module._extract_deutsche_boerse_company_details_contact_details(
            company_details_html
        )
    )
    shareholder_records = (
        finance_country_module._extract_deutsche_boerse_company_details_shareholder_records(
            company_details_html
        )
    )

    assert contact_details == {
        "phone": "+49 711 17-0",
        "website": "https://group.mercedes-benz.com/",
        "contact": "ir@mercedes-benz.com",
    }
    assert shareholder_records == [
        {
            "holder_name": "Geely Sweden Holdings AB",
            "ownership_percent": "9.69",
        },
        {
            "holder_name": "Kuwait Investment Authority",
            "ownership_percent": "6.84",
        },
    ]


def test_extract_deutsche_boerse_company_details_shareholder_entities_split_people_and_orgs() -> None:
    html_text = """
    <html><body>
      <div class="widget">
        <h2 class="widget-table-headline">Shareholder structure Example AG *</h2>
        <table class="table widget-table">
          <tbody>
            <tr>
              <td class="widget-table-cell">Robin Laik (CEO)</td>
              <td class="widget-table-cell">12.40</td>
            </tr>
            <tr>
              <td class="widget-table-cell">Wellington Management Group LLP</td>
              <td class="widget-table-cell">5.10</td>
            </tr>
            <tr>
              <td class="widget-table-cell">Freefloat</td>
              <td class="widget-table-cell">38.00</td>
            </tr>
          </tbody>
        </table>
      </div>
    </body></html>
    """
    issuer_record = {
        "company_name": "Example AG",
        "ticker": "EXA",
        "isin": "DE000EXAMPLE0",
    }

    people_entities = (
        finance_country_module._extract_deutsche_boerse_company_details_shareholder_people_entities(
            html_text,
            issuer_record=issuer_record,
        )
    )
    organization_records = (
        finance_country_module._extract_deutsche_boerse_company_details_shareholder_organization_records(
            html_text,
            issuer_record=issuer_record,
        )
    )

    assert [entity["canonical_text"] for entity in people_entities] == ["Robin Laik"]
    assert "Robin Laik (CEO)" in people_entities[0]["aliases"]
    assert organization_records == [
        {
            "holder_name": "Wellington Management Group LLP",
            "ownership_percent": "5.10",
            "employer_name": "Example AG",
            "employer_ticker": "EXA",
            "employer_isin": "DE000EXAMPLE0",
            "source_name": "deutsche-boerse-company-details",
        }
    ]


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
    monkeypatch.setattr(
        finance_country_module,
        "_query_wikidata_entity_ids_by_string_property",
        _fake_germany_wikidata_property_query,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_query_germany_wikidata_public_people_rows",
        _fake_germany_wikidata_public_people_rows,
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
        "Geely Sweden Holdings AB",
        "Ing Bank N.V.",
        "ING Bank N.V. Frankfurt Branch",
        "Kuwait Investment Authority",
        "Mercedes-Benz Group AG",
    }
    mercedes_issuer = next(
        entity for entity in issuers if entity["canonical_text"] == "Mercedes-Benz Group AG"
    )
    assert "Daimler AG" in mercedes_issuer["aliases"]
    assert mercedes_issuer["metadata"]["register_court"] == "Stuttgart"
    assert mercedes_issuer["metadata"]["register_number"] == "19360"
    assert mercedes_issuer["metadata"]["register_type"] == "HRB"
    assert mercedes_issuer["metadata"]["contact_phone"] == "+49 711 17-0"
    assert mercedes_issuer["metadata"]["contact_email"] == "ir@mercedes-benz.com"
    assert mercedes_issuer["metadata"]["issuer_website"] == "https://group.mercedes-benz.com/"
    assert mercedes_issuer["metadata"]["major_shareholder_names"] == [
        "Geely Sweden Holdings AB",
        "Kuwait Investment Authority",
    ]
    assert mercedes_issuer["metadata"]["major_shareholders"] == [
        {
            "holder_name": "Geely Sweden Holdings AB",
            "ownership_percent": "9.69",
        },
        {
            "holder_name": "Kuwait Investment Authority",
            "ownership_percent": "6.84",
        },
    ]
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
    assert commerzbank_issuer["metadata"]["wikidata_qid"] == "Q157617"
    assert commerzbank_issuer["metadata"]["wikidata_slug"] == "Q157617"
    assert "Commerzbank" in commerzbank_issuer["aliases"]
    assert "Mercedes-Benz Group" in mercedes_issuer["aliases"]
    assert "Mercedes Benz Group AG" in mercedes_issuer["aliases"]
    geely = next(
        entity
        for entity in issuers
        if entity["canonical_text"] == "Geely Sweden Holdings AB"
    )
    assert geely["metadata"]["category"] == "major_shareholder_organization"
    assert geely["metadata"]["major_shareholder_of_tickers"] == ["MBG"]
    assert geely["metadata"]["major_shareholder_of_issuers"] == [
        "Mercedes-Benz Group AG"
    ]
    assert geely["metadata"]["major_shareholding_records"] == [
        {
            "employer_name": "Mercedes-Benz Group AG",
            "employer_ticker": "MBG",
            "employer_isin": "DE0007100000",
            "ownership_percent": "9.69",
            "source_name": "deutsche-boerse-company-details",
        }
    ]
    kuwait = next(
        entity
        for entity in issuers
        if entity["canonical_text"] == "Kuwait Investment Authority"
    )
    assert "wikidata_qid" not in kuwait["metadata"]
    ing_issuer = next(entity for entity in issuers if entity["canonical_text"] == "Ing Bank N.V.")
    assert ing_issuer["metadata"]["regulated_institution"] is True
    assert ing_issuer["metadata"]["country"] == "Niederlande"
    assert ing_issuer["metadata"]["germany_branch"] == "ING Bank N.V. Frankfurt Branch"
    assert ing_issuer["metadata"]["wikidata_qid"] == "Q2304977"
    assert ing_issuer["metadata"]["wikidata_slug"] == "Q2304977"
    assert "Ing Bank" in ing_issuer["aliases"]
    assert "ING-DiBa AG" in ing_issuer["aliases"]
    ing_branch = next(
        entity
        for entity in issuers
        if entity["canonical_text"] == "ING Bank N.V. Frankfurt Branch"
    )
    assert ing_branch["metadata"]["category"] == "regulated_institution_branch"
    assert ing_branch["metadata"]["parent_institution_name"] == "Ing Bank N.V."
    assert ing_branch["metadata"]["country"] == "Deutschland"
    assert ing_branch["metadata"]["home_country"] == "Niederlande"
    assert ing_branch["metadata"]["consumer_complaint_contacts"] == (
        "complaints@ing.example"
    )
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
    assert mercedes_ticker["metadata"]["contact_email"] == "ir@mercedes-benz.com"
    assert mercedes_ticker["metadata"]["major_shareholder_names"] == [
        "Geely Sweden Holdings AB",
        "Kuwait Investment Authority",
    ]
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
    assert any(alias.startswith("Dr. Martin Bruderm") for alias in martin["aliases"])
    jochen = next(
        entity for entity in people if entity["canonical_text"] == "Jochen Stotmeister"
    )
    assert jochen["metadata"]["category"] == "executive_officer"
    assert jochen["metadata"]["employer_ticker"] == "AB9"
    ola = next(entity for entity in people if entity["canonical_text"] == "Ola Kallenius")
    assert ola["metadata"]["wikidata_qid"] == "Q19297571"
    britta_entities = [
        entity for entity in people if entity["canonical_text"] == "Britta Seeger"
    ]
    assert any(
        entity.get("metadata", {}).get("wikidata_qid") == "Q50387551"
        for entity in britta_entities
    )
    harald_entities = [
        entity for entity in people if entity["canonical_text"] == "Harald Wilhelm"
    ]
    assert any(
        entity.get("metadata", {}).get("wikidata_qid") == "Q62090160"
        for entity in harald_entities
    )
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
    monkeypatch.setattr(
        finance_country_module,
        "_query_wikidata_entity_ids_by_string_property",
        _fake_germany_wikidata_property_query,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_query_germany_wikidata_public_people_rows",
        _fake_germany_wikidata_public_people_rows,
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


def test_extract_companies_house_search_result_company_number_matches_clean_company_name() -> None:
    search_payload = {
        "items": [
            {
                "title": "DELTIC ENERGY PLC",
                "company_number": "07958581",
            }
        ]
    }

    company_number = (
        finance_country_module._extract_companies_house_search_result_company_number(
            json.dumps(search_payload),
            company_name="Deltic Energy PLC",
            raw_company_name="DELTIC ENERGY PLC ORD 10P",
        )
    )

    assert company_number == "07958581"


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


def test_extract_uk_aim_constituent_records_parses_rows() -> None:
    aim_html = """
    <table class="full-width ftse-index-table-table">
      <tbody>
        <tr class="medium-font-weight slide-panel">
          <td class="instrument-tidm">MTC</td>
          <td class="instrument-name"><span class="ellipsed">MOTHERCARE PLC ORD 1P</span></td>
          <td class="instrument-currency">GBX</td>
          <td class="instrument-marketcapitalization">6.17</td>
          <td class="instrument-lastprice">1.39</td>
          <td class="instrument-netchange">0.30</td>
          <td class="instrument-percentualchange">26.94%</td>
        </tr>
        <tr class="medium-font-weight slide-panel">
          <td class="instrument-tidm">DELT</td>
          <td class="instrument-name"><span class="ellipsed">DELTIC ENERGY PLC ORD 10P</span></td>
          <td class="instrument-currency">GBX</td>
          <td class="instrument-marketcapitalization">2.79</td>
          <td class="instrument-lastprice">3.50</td>
          <td class="instrument-netchange">0.50</td>
          <td class="instrument-percentualchange">16.67%</td>
        </tr>
      </tbody>
    </table>
    <div class="paginator">
      <a class="page-number" href="/indices/ftse-aim-all-share/constituents/table?page=2">2</a>
      <a class="page-number page-last" href="/indices/ftse-aim-all-share/constituents/table?page=28">Last page</a>
    </div>
    """

    records = finance_country_module._extract_uk_aim_constituent_records(aim_html)

    assert records == [
        {
            "raw_company_name": "DELTIC ENERGY PLC ORD 10P",
            "company_name": "Deltic Energy PLC",
            "listing_category": "Equity shares (AIM companies)",
            "market_status": "AIM",
            "trading_venue": "LSE AIM",
            "isin": "",
            "ticker": "DELT",
            "market_segment": "AIM",
            "currency": "GBX",
            "market_cap_millions": "2.79",
            "share_price": "3.50",
            "price_change": "0.50",
            "price_change_percent": "16.67%",
            "source_name": "lse-aim-all-share-constituents",
        },
        {
            "raw_company_name": "MOTHERCARE PLC ORD 1P",
            "company_name": "Mothercare PLC",
            "listing_category": "Equity shares (AIM companies)",
            "market_status": "AIM",
            "trading_venue": "LSE AIM",
            "isin": "",
            "ticker": "MTC",
            "market_segment": "AIM",
            "currency": "GBX",
            "market_cap_millions": "6.17",
            "share_price": "1.39",
            "price_change": "0.30",
            "price_change_percent": "26.94%",
            "source_name": "lse-aim-all-share-constituents",
        },
    ]
    assert finance_country_module._extract_uk_lse_directory_last_page(aim_html) == 28


def test_extract_companies_house_psc_people_entities_parses_active_person() -> None:
    psc_html = """
    <div class="appointments-list">
      <div class="appointment-1">
        <h2 class="heading-medium">
          <span id="psc-name-1"><span><b>Mr Roger Mainwaring</b></span></span>
          <span id="psc-status-tag-1" class="status-tag font-xsmall">Active</span>
        </h2>
        <div class="grid-row">
          <dl class="column-quarter">
            <dt>Notified on</dt>
            <dd id="psc-notified-on-1" class="data">30 January 2023</dd>
          </dl>
          <dl class="column-quarter">
            <dt>Nationality</dt>
            <dd id="psc-nationality-1" class="data">British</dd>
          </dl>
        </div>
        <div class="grid-row">
          <dl class="column-quarter">
            <dt>Country of residence</dt>
            <dd id="psc-country-of-residence-1" class="data">England</dd>
          </dl>
        </div>
        <div class="grid-row">
          <dl class="column-full">
            <dt>Nature of control</dt>
            <dd id="psc-noc-1-ownership-of-shares-75-to-100-percent" class="data">Ownership of shares – 75% or more</dd>
            <dd id="psc-noc-1-voting-rights-75-to-100-percent" class="data">Ownership of voting rights - 75% or more</dd>
          </dl>
        </div>
      </div>
    </div>
    """

    entities = finance_country_module._extract_companies_house_psc_people_entities(
        psc_html,
        employer_name="UK Access Limited",
        employer_company_number="14624173",
        employer_ticker="UKA",
    )

    assert len(entities) == 1
    entity = entities[0]
    assert entity["canonical_text"] == "Roger Mainwaring"
    assert entity["metadata"]["category"] == "shareholder"
    assert entity["metadata"]["employer_company_number"] == "14624173"
    assert entity["metadata"]["employer_ticker"] == "UKA"
    assert entity["metadata"]["nature_of_control"] == [
        "Ownership of shares – 75% or more",
        "Ownership of voting rights - 75% or more",
    ]


def test_extract_companies_house_psc_organization_records_parses_active_company() -> None:
    psc_html = """
    <div class="appointments-list">
      <div class="appointment-1">
        <h2 class="heading-medium">
          <span id="psc-name-1"><span><b>Hq 200 Limited</b></span></span>
          <span id="psc-status-tag-1" class="status-tag font-xsmall">Active</span>
        </h2>
        <div class="grid-row">
          <dl class="column-quarter">
            <dt>Notified on</dt>
            <dd id="psc-notified-on-1" class="data">29 April 2024</dd>
          </dl>
        </div>
        <div class="grid-row">
          <dl class="column-half">
            <dt>Governing law</dt>
            <dd id="psc-legal-authority-1" class="data">Companies Act 2006</dd>
          </dl>
          <dl class="column-half">
            <dt>Legal form</dt>
            <dd id="psc-legal-form-1" class="data">Limited Company</dd>
          </dl>
        </div>
        <div class="grid-row">
          <dl class="column-half">
            <dt>Registration number</dt>
            <dd id="psc-registration-number-1" class="data">15279598</dd>
          </dl>
        </div>
        <div class="grid-row">
          <dl class="column-full">
            <dt>Nature of control</dt>
            <dd id="psc-noc-1-right-to-appoint-and-remove-directors" class="data">Right to appoint or remove directors</dd>
          </dl>
        </div>
      </div>
    </div>
    """

    records = finance_country_module._extract_companies_house_psc_organization_records(
        psc_html,
        employer_name="HQ 001 Limited",
        employer_company_number="15691706",
        employer_ticker="HQ1",
    )

    assert records == [
        {
            "holder_name": "Hq 200 Limited",
            "employer_name": "HQ 001 Limited",
            "employer_company_number": "15691706",
            "employer_ticker": "HQ1",
            "notified_on": "29 April 2024",
            "nature_of_control": "Right to appoint or remove directors",
            "registration_number": "15279598",
            "legal_form": "Limited Company",
            "governing_law": "Companies Act 2006",
            "source_name": "companies-house-psc",
        }
    ]


def test_extract_companies_house_psc_records_skips_exempt_and_empty_pages() -> None:
    exempt_html = """
    <p id="exemption-info" class="heading-small">
      The PSC information is not available because:
    </p>
    """
    empty_html = """
    <p class="text" id="no-pscs">
      There are no persons with significant control or statements available for this company.
    </p>
    """

    assert finance_country_module._extract_companies_house_psc_records(exempt_html) == []
    assert finance_country_module._extract_companies_house_psc_records(empty_html) == []


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
    def _fake_build_united_states_wikidata_resolution_snapshot(
        *,
        issuer_entities: list[dict[str, object]],
        country_dir: Path,
        user_agent: str,
    ) -> tuple[dict[str, object], Path, list[str]]:
        assert user_agent == "ades-test/0.1.0"
        assert [entity["entity_id"] for entity in issuer_entities] == [
            "finance-us-issuer:0001730168"
        ]
        payload = {
            "schema_version": 1,
            "country_code": "us",
            "generated_at": "2026-04-21T00:00:00Z",
            "issuers": {
                "0001730168": {
                    "qid": "Q130001999",
                    "slug": "Q130001999",
                    "entity_id": "wikidata:Q130001999",
                    "url": "https://www.wikidata.org/wiki/Q130001999",
                    "label": "Broadcom Inc.",
                    "aliases": ["Broadcom"],
                }
            },
            "people": {},
        }
        resolution_path = country_dir / "wikidata-united-states-entity-resolution.json"
        resolution_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return payload, resolution_path, []

    monkeypatch.setattr(
        finance_country_module,
        "_build_united_states_wikidata_resolution_snapshot",
        _fake_build_united_states_wikidata_resolution_snapshot,
    )

    downloaded_sources = [
        {"name": "sec-company-tickers", "path": "sec_company_tickers.json"},
        {"name": "sec-submissions", "path": "sec_submissions.zip"},
    ]

    derived_files = finance_country_module._derive_united_states_sec_entities(
        country_dir=country_dir,
        downloaded_sources=downloaded_sources,
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
    assert "Broadcom" in broadcom["aliases"]
    assert broadcom["metadata"]["wikidata_slug"] == "Q130001999"
    ticker = next(entity for entity in entities if entity["canonical_text"] == "AVGO")
    assert ticker["entity_type"] == "ticker"
    assert ticker["entity_id"] == "finance-us-ticker:AVGO"
    assert ticker["aliases"][0] == "AVGO"
    assert "Broadcom" in ticker["aliases"]
    assert ticker["metadata"]["wikidata_slug"] == "Q130001999"
    assert any(
        source.get("name") == "wikidata-united-states-entity-resolution"
        for source in downloaded_sources
    )


def test_query_wikidata_entity_ids_by_string_property_parses_results(monkeypatch) -> None:
    def _fake_fetch_wikidata_sparql_json(*, query: str, user_agent: str) -> dict[str, object]:
        assert user_agent == "ades-test/0.1.0"
        assert "wdt:P5531" in query
        return {
            "results": {
                "bindings": [
                    {
                        "property_value": {"value": "0001730168"},
                        "item": {"value": "http://www.wikidata.org/entity/Q230000001"},
                    },
                    {
                        "property_value": {"value": "0001730168"},
                        "item": {"value": "http://www.wikidata.org/entity/Q230000001"},
                    },
                    {
                        "property_value": {"value": "0000320193"},
                        "item": {"value": "http://www.wikidata.org/entity/Q312"},
                    },
                ]
            }
        }

    monkeypatch.setattr(
        finance_country_module,
        "_fetch_wikidata_sparql_json",
        _fake_fetch_wikidata_sparql_json,
    )

    matches = finance_country_module._query_wikidata_entity_ids_by_string_property(
        property_id="P5531",
        values=["0001730168", "0000320193"],
        user_agent="ades-test/0.1.0",
        warnings=[],
    )

    assert matches == {
        "0001730168": ["Q230000001"],
        "0000320193": ["Q312"],
    }


def test_extract_wikidata_qid_from_uri_returns_qid() -> None:
    assert (
        finance_country_module._extract_wikidata_qid_from_uri(
            "http://www.wikidata.org/entity/Q312"
        )
        == "Q312"
    )


def test_resolve_united_states_issuer_wikidata_matches_prefers_name_and_ticker(monkeypatch) -> None:
    monkeypatch.setattr(
        finance_country_module,
        "_query_wikidata_entity_ids_by_string_property",
        lambda *, property_id, values, user_agent, warnings: {
            "0001730168": ["Q230000001", "Q230000099"]
        },
    )
    monkeypatch.setattr(
        finance_country_module,
        "_fetch_wikidata_entity_payloads",
        lambda *, entity_ids, user_agent, warnings: {
            "Q230000001": {
                "labels": {"en": {"value": "Broadcom Inc."}},
                "aliases": {"en": [{"value": "Broadcom"}]},
                "descriptions": {"en": {"value": "American technology company"}},
                "claims": {
                    "P5531": [{"mainsnak": {"datavalue": {"value": "0001730168"}}}],
                    "P249": [{"mainsnak": {"datavalue": {"value": "AVGO"}}}],
                },
            },
            "Q230000099": {
                "labels": {"en": {"value": "Apple Inc."}},
                "aliases": {"en": [{"value": "Apple"}]},
                "descriptions": {"en": {"value": "American technology company"}},
                "claims": {
                    "P5531": [{"mainsnak": {"datavalue": {"value": "0001730168"}}}],
                },
            },
        },
    )

    matches, warnings = finance_country_module._resolve_united_states_issuer_wikidata_matches(
        issuer_entities=[
            {
                "entity_id": "finance-us-issuer:0001730168",
                "entity_type": "organization",
                "canonical_text": "Broadcom Inc.",
                "aliases": ["Broadcom Limited", "AVGO"],
                "metadata": {"country_code": "us", "category": "issuer", "ticker": "AVGO"},
            }
        ],
        user_agent="ades-test/0.1.0",
    )

    assert warnings == []
    assert matches["0001730168"]["qid"] == "Q230000001"
    assert "Broadcom" in matches["0001730168"]["aliases"]


def test_resolve_united_kingdom_issuer_wikidata_matches_prefers_isin_and_ticker(
    monkeypatch,
) -> None:
    def _fake_query(*, property_id, values, user_agent, warnings):
        assert user_agent == "ades-test/0.1.0"
        assert warnings == []
        if property_id == finance_country_module._WIKIDATA_ISIN_PROPERTY_ID:
            assert values == ["GB00BJVR8977"]
            return {"GB00BJVR8977": ["Q230100001"]}
        if (
            property_id
            == finance_country_module._WIKIDATA_LONDON_STOCK_EXCHANGE_COMPANY_ID_PROPERTY_ID
        ):
            assert values == ["SPA"]
            return {"SPA": ["Q230100001", "Q230100099"]}
        raise AssertionError(property_id)

    monkeypatch.setattr(
        finance_country_module,
        "_query_wikidata_entity_ids_by_string_property",
        _fake_query,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_fetch_wikidata_entity_payloads",
        lambda *, entity_ids, user_agent, warnings: {
            "Q230100001": {
                "labels": {"en": {"value": "1Spatial plc"}},
                "aliases": {"en": [{"value": "1Spatial"}]},
                "descriptions": {"en": {"value": "British software company"}},
                "claims": {
                    "P946": [{"mainsnak": {"datavalue": {"value": "GB00BJVR8977"}}}],
                    "P8676": [{"mainsnak": {"datavalue": {"value": "SPA"}}}],
                },
            },
            "Q230100099": {
                "labels": {"en": {"value": "SpaceandPeople plc"}},
                "aliases": {"en": [{"value": "SpaceandPeople"}]},
                "descriptions": {"en": {"value": "British services company"}},
                "claims": {
                    "P8676": [{"mainsnak": {"datavalue": {"value": "SPA"}}}],
                },
            },
        },
    )

    matches, warnings = (
        finance_country_module._resolve_united_kingdom_issuer_wikidata_matches(
            issuer_entities=[
                {
                    "entity_id": "finance-uk-issuer:05429800",
                    "entity_type": "organization",
                    "canonical_text": "1SPATIAL PLC",
                    "aliases": ["1SPATIAL PLC ORD 10P"],
                    "metadata": {
                        "country_code": "uk",
                        "category": "issuer",
                        "company_number": "05429800",
                        "isin": "GB00BJVR8977",
                        "ticker": "SPA",
                    },
                }
            ],
            user_agent="ades-test/0.1.0",
        )
    )

    assert warnings == []
    assert matches["company-number:05429800"]["qid"] == "Q230100001"
    assert "1Spatial" in matches["company-number:05429800"]["aliases"]


def test_enrich_united_kingdom_entities_with_wikidata_merges_issuer_ticker_and_person(
    monkeypatch,
    tmp_path: Path,
) -> None:
    country_dir = tmp_path / "uk"
    country_dir.mkdir()

    def _fake_build_snapshot(
        *,
        issuer_entities,
        people_entities,
        country_dir,
        user_agent,
    ):
        assert user_agent == "ades-test/0.1.0"
        assert len(issuer_entities) == 1
        assert len(people_entities) == 1
        resolution_path = country_dir / "wikidata-united-kingdom-entity-resolution.json"
        resolution_path.write_text('{"schema_version":1}\n', encoding="utf-8")
        return (
            {
                "issuers": {
                    "company-number:05429800": {
                        "qid": "Q230100001",
                        "slug": "Q230100001",
                        "entity_id": "wikidata:Q230100001",
                        "url": "https://www.wikidata.org/wiki/Q230100001",
                        "label": "1Spatial plc",
                        "aliases": ["1Spatial"],
                    }
                },
                "people": {
                    "finance-uk-person:05429800:aakash-vanchi-nath:director": {
                        "qid": "Q230100777",
                        "slug": "Q230100777",
                        "entity_id": "wikidata:Q230100777",
                        "url": "https://www.wikidata.org/wiki/Q230100777",
                        "label": "Aakash Vanchi Nath",
                        "aliases": ["Aakash Nath"],
                    }
                },
            },
            resolution_path,
            [],
        )

    monkeypatch.setattr(
        finance_country_module,
        "_build_united_kingdom_wikidata_resolution_snapshot",
        _fake_build_snapshot,
    )

    issuer_entities = [
        {
            "entity_id": "finance-uk-issuer:05429800",
            "entity_type": "organization",
            "canonical_text": "1SPATIAL PLC",
            "aliases": ["1SPATIAL PLC ORD 10P"],
            "metadata": {
                "country_code": "uk",
                "category": "issuer",
                "company_number": "05429800",
                "isin": "GB00BJVR8977",
                "ticker": "SPA",
            },
        }
    ]
    ticker_entities = [
        {
            "entity_id": "finance-uk-ticker:spa",
            "entity_type": "ticker",
            "canonical_text": "SPA",
            "aliases": ["SPA"],
            "metadata": {
                "country_code": "uk",
                "category": "ticker",
                "company_number": "05429800",
                "ticker": "SPA",
                "issuer_name": "1SPATIAL PLC",
            },
        }
    ]
    people_entities = [
        {
            "entity_id": "finance-uk-person:05429800:aakash-vanchi-nath:director",
            "entity_type": "person",
            "canonical_text": "Aakash Vanchi Nath",
            "aliases": ["VANCHI NATH, Aakash"],
            "metadata": {
                "country_code": "uk",
                "category": "director",
                "employer_company_number": "05429800",
                "employer_name": "1SPATIAL PLC",
            },
        }
    ]
    downloaded_sources: list[dict[str, object]] = []

    warnings = finance_country_module._enrich_united_kingdom_entities_with_wikidata(
        country_dir=country_dir,
        issuer_entities=issuer_entities,
        ticker_entities=ticker_entities,
        people_entities=people_entities,
        downloaded_sources=downloaded_sources,
        user_agent="ades-test/0.1.0",
    )

    assert warnings == []
    assert issuer_entities[0]["metadata"]["wikidata_slug"] == "Q230100001"
    assert ticker_entities[0]["metadata"]["wikidata_slug"] == "Q230100001"
    assert people_entities[0]["metadata"]["wikidata_slug"] == "Q230100777"
    assert any(
        source.get("name") == "wikidata-united-kingdom-entity-resolution"
        for source in downloaded_sources
    )


def test_extract_united_states_sec_insider_people_entity_parses_reporting_owner() -> None:
    entity = finance_country_module._extract_united_states_sec_insider_people_entity(
        text="""
        <html>
          <body>
            <table>
              <tr><td>1. Name and Address of Reporting Person *</td></tr>
              <tr><td>Borders Ben</td></tr>
              <tr><td>(Last)</td><td>(First)</td><td>(Middle)</td></tr>
              <tr><td>5. Relationship of Reporting Person(s) to Issuer</td></tr>
              <tr><td>Director</td><td></td><td>10% Owner</td></tr>
              <tr><td>X</td><td>Officer (give title below)</td><td></td><td>Other (specify below)</td></tr>
              <tr><td>Principal Accounting Officer</td></tr>
              <tr><td>3. Date of Earliest Transaction (Month/Day/Year)</td></tr>
            </table>
          </body>
        </html>
        """,
        employer_cik="0000320193",
        employer_name="Apple Inc.",
        employer_ticker="AAPL",
        filing_date="2026-04-17",
        filing_url="https://www.sec.gov/Archives/edgar/data/320193/example/form4.xml",
        source_form="4",
    )

    assert entity is not None
    assert entity["canonical_text"] == "Ben Borders"
    assert "Borders Ben" in entity["aliases"]
    assert entity["entity_id"] == "finance-person:0000320193:ben-borders"
    assert entity["source_name"] == "sec-insider-people"
    assert entity["metadata"]["category"] == "executive_officer"
    assert entity["metadata"]["role_title"] == "Principal Accounting Officer"
    assert entity["metadata"]["source_forms"] == ["4"]


def test_extract_united_states_sec_insider_people_entities_uses_multiple_recent_filings_per_issuer(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        finance_country_module,
        "_read_targeted_zip_json_records",
        lambda _path, *, target_source_ids: [
            {
                "cik": "1730168",
                "name": "Broadcom Inc.",
                "filings": {
                    "recent": {
                        "accessionNumber": [
                            "0001730168-26-000034",
                            "0001730168-26-000032",
                            "0001730168-26-000030",
                            "0001730168-26-000028",
                        ],
                        "filingDate": [
                            "2026-04-14",
                            "2026-04-13",
                            "2026-04-10",
                            "2026-04-10",
                        ],
                        "form": ["4", "4", "4", "4"],
                        "primaryDocument": [
                            "wk-form4-a.xml",
                            "wk-form4-b.xml",
                            "wk-form4-c.xml",
                            "wk-form4-d.xml",
                        ],
                        "primaryDocDescription": [
                            "Form 4",
                            "Form 4",
                            "Form 4",
                            "Form 4",
                        ],
                    }
                },
            }
        ],
    )
    captured_urls: list[str] = []

    def fake_extract(*, filing: dict[str, str], user_agent: str) -> dict[str, object] | None:
        assert user_agent == "ades-test/0.1.0"
        filing_url = str(filing["filing_url"])
        captured_urls.append(filing_url)
        person_name = {
            "wk-form4-a.xml": "Hock E. Tan",
            "wk-form4-b.xml": "Charlie B. Kawwas",
            "wk-form4-c.xml": "Henry Samueli",
        }[filing_url.rsplit("/", 1)[-1]]
        return {
            "entity_id": f"finance-person:0001730168:{finance_country_module._slugify(person_name)}",
            "entity_type": "person",
            "canonical_text": person_name,
            "aliases": [],
            "source_name": "sec-insider-people",
            "source_id": person_name,
            "metadata": {
                "country_code": "us",
                "category": "director",
                "employer_name": "Broadcom Inc.",
                "employer_ticker": "AVGO",
                "employer_cik": "0001730168",
                "role_title": "Director",
                "role_class": "director",
                "source_form": "4",
                "source_forms": ["4"],
                "source_url": filing_url,
                "source_urls": [filing_url],
                "effective_date": str(filing["filing_date"]),
            },
        }

    monkeypatch.setattr(
        finance_country_module,
        "_extract_united_states_sec_insider_people_entity_from_filing",
        fake_extract,
    )

    entities = finance_country_module._extract_united_states_sec_insider_people_entities(
        sec_submissions_path=Path("unused.zip"),
        sec_company_profiles={
            "0001730168": {
                "employer_cik": "0001730168",
                "employer_name": "Broadcom Inc.",
                "employer_ticker": "AVGO",
            }
        },
        archive_base_url="https://www.sec.gov/Archives/edgar/data",
        user_agent="ades-test/0.1.0",
    )

    assert [entity["canonical_text"] for entity in entities] == [
        "Charlie B. Kawwas",
        "Henry Samueli",
        "Hock E. Tan",
    ]
    assert len(captured_urls) == finance_country_module._UNITED_STATES_SEC_INSIDER_MAX_FILINGS_PER_ISSUER
    assert all("wk-form4-d.xml" not in url for url in captured_urls)


def test_derive_profile_people_entities_merges_us_proxy_and_insider_people(
    tmp_path: Path,
    monkeypatch,
) -> None:
    country_dir = tmp_path / "us"
    country_dir.mkdir(parents=True, exist_ok=True)
    (country_dir / "sec_company_tickers.json").write_text("{}\n", encoding="utf-8")
    (country_dir / "sec_submissions.zip").write_bytes(b"placeholder")

    def _fake_write_sec_proxy_people_entities(*, path: Path, **_: object) -> int:
        path.write_text(
            json.dumps(
                {
                    "entities": [
                        {
                            "entity_id": "finance-person:0001730168:harry-l--you",
                            "canonical_text": "Harry L. You",
                            "aliases": [],
                            "source_name": "sec-proxy-people",
                            "source_id": "0001730168:harry-l--you:def14a",
                            "metadata": {
                                "country_code": "us",
                                "category": "director",
                                "employer_name": "Broadcom Inc.",
                                "employer_ticker": "AVGO",
                                "employer_cik": "0001730168",
                                "role_title": "Director",
                                "role_class": "director",
                                "source_form": "DEF 14A",
                                "source_url": "https://example.test/proxy",
                            },
                        }
                    ]
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return 1

    monkeypatch.setattr(
        finance_country_module,
        "_write_sec_proxy_people_entities",
        _fake_write_sec_proxy_people_entities,
    )
    monkeypatch.setattr(
        finance_country_module,
        "_load_sec_company_items",
        lambda _path: [
            {"cik_str": "1730168", "ticker": "AVGO", "title": "Broadcom Inc."}
        ],
    )
    monkeypatch.setattr(
        finance_country_module,
        "_extract_united_states_sec_insider_people_entities",
        lambda **_: [
            {
                "entity_id": "finance-person:0001730168:harry-l--you",
                "entity_type": "person",
                "canonical_text": "Harry L. You",
                "aliases": ["You Harry L."],
                "source_name": "sec-insider-people",
                "source_id": "0001730168:harry-l--you:4",
                "metadata": {
                    "country_code": "us",
                    "category": "beneficial_owner",
                    "employer_name": "Broadcom Inc.",
                    "employer_ticker": "AVGO",
                    "employer_cik": "0001730168",
                    "role_title": "10% Owner",
                    "role_class": "beneficial_owner",
                    "source_form": "4",
                    "source_forms": ["4"],
                    "source_url": "https://example.test/form4",
                    "source_urls": ["https://example.test/form4"],
                },
            },
            {
                "entity_id": "finance-person:0001730168:ben-borders",
                "entity_type": "person",
                "canonical_text": "Ben Borders",
                "aliases": ["Borders Ben"],
                "source_name": "sec-insider-people",
                "source_id": "0001730168:ben-borders:4",
                "metadata": {
                    "country_code": "us",
                    "category": "executive_officer",
                    "employer_name": "Broadcom Inc.",
                    "employer_ticker": "AVGO",
                    "employer_cik": "0001730168",
                    "role_title": "Principal Accounting Officer",
                    "role_class": "executive_officer",
                    "source_form": "4",
                    "source_forms": ["4"],
                    "source_url": "https://example.test/form4-ben",
                    "source_urls": ["https://example.test/form4-ben"],
                },
            },
        ],
    )

    derived_files = finance_country_module._derive_profile_people_entities(
        profile=create_finance_country_profiles(tmp_path / "remote-country-sources")["us"],
        country_dir=country_dir,
        downloaded_sources=[
            {"name": "sec-company-tickers", "path": "sec_company_tickers.json"},
            {"name": "sec-submissions", "path": "sec_submissions.zip"},
        ],
        user_agent="ades-test/0.1.0",
    )

    assert derived_files == [
        {
            "name": "derived-finance-us-sec-people",
            "path": "derived_sec_proxy_people_entities.json",
            "record_count": 2,
            "adapter": "sec_proxy_plus_insider_people_derivation",
            "sha256": finance_country_module._sha256_file(
                country_dir / "derived_sec_proxy_people_entities.json"
            ),
            "size_bytes": (country_dir / "derived_sec_proxy_people_entities.json").stat().st_size,
        }
    ]
    entities = json.loads(
        (country_dir / "derived_sec_proxy_people_entities.json").read_text(
            encoding="utf-8"
        )
    )["entities"]
    harry = next(entity for entity in entities if entity["canonical_text"] == "Harry L. You")
    assert harry["metadata"]["category"] == "director"
    assert set(harry["metadata"]["source_forms"]) == {"DEF 14A", "4"}
    assert set(harry["metadata"]["source_urls"]) == {
        "https://example.test/proxy",
        "https://example.test/form4",
    }
    ben = next(entity for entity in entities if entity["canonical_text"] == "Ben Borders")
    assert ben["metadata"]["category"] == "executive_officer"
    assert ben["metadata"]["source_form"] == "4"


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
