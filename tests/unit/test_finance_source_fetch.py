import io
import json
from pathlib import Path
import zipfile
import urllib.error

from ades.packs.finance_bundle import build_finance_source_bundle
from ades.packs.finance_sources import (
    _extract_sec_proxy_people_entities,
    DEFAULT_CURATED_FINANCE_ENTITIES,
    _download_text,
    _extract_people_from_sec_proxy_document,
    fetch_finance_source_snapshot,
)
from tests.finance_bundle_helpers import create_finance_remote_sources


def test_fetch_finance_source_snapshot_writes_immutable_snapshot_dir(tmp_path: Path) -> None:
    remote_sources = create_finance_remote_sources(tmp_path / "remote")

    result = fetch_finance_source_snapshot(
        output_dir=tmp_path / "raw" / "finance-en",
        snapshot="2026-04-10",
        sec_companies_url=remote_sources["sec_companies_url"],
        sec_submissions_url=remote_sources["sec_submissions_url"],
        sec_companyfacts_url=remote_sources["sec_companyfacts_url"],
        symbol_directory_url=remote_sources["symbol_directory_url"],
        other_listed_url=remote_sources["other_listed_url"],
        finance_people_url=remote_sources["finance_people_url"],
    )

    assert result.pack_id == "finance-en"
    assert result.snapshot == "2026-04-10"
    assert result.source_count == 7
    assert result.curated_entity_count == len(DEFAULT_CURATED_FINANCE_ENTITIES)
    assert Path(result.snapshot_dir).exists()
    assert Path(result.sec_companies_path).exists()
    assert Path(result.sec_submissions_path).exists()
    assert Path(result.sec_companyfacts_path).exists()
    assert Path(result.symbol_directory_path).exists()
    assert Path(result.other_listed_path).exists()
    assert Path(result.finance_people_path).exists()
    assert Path(result.curated_entities_path).exists()
    assert Path(result.source_manifest_path).exists()
    assert result.warnings == []

    manifest = json.loads(Path(result.source_manifest_path).read_text(encoding="utf-8"))
    assert manifest["pack_id"] == "finance-en"
    assert manifest["snapshot"] == "2026-04-10"
    sources = {item["name"]: item for item in manifest["sources"]}
    assert sources["sec-company-tickers"]["source_url"] == remote_sources["sec_companies_url"]
    assert sources["sec-submissions"]["source_url"] == remote_sources["sec_submissions_url"]
    assert (
        sources["sec-companyfacts"]["source_url"]
        == remote_sources["sec_companyfacts_url"]
    )
    assert (
        sources["symbol-directory"]["source_url"]
        == remote_sources["symbol_directory_url"]
    )
    assert sources["other-listed"]["source_url"] == remote_sources["other_listed_url"]
    assert sources["finance-people"]["source_url"] == remote_sources["finance_people_url"]
    assert sources["curated-finance-entities"]["source_url"].startswith("operator://")
    curated_entities = json.loads(Path(result.curated_entities_path).read_text(encoding="utf-8"))
    assert any(
        item["canonical_text"] == "S&P 500"
        for item in curated_entities.get("entities", [])
    )
    assert any(
        item["canonical_text"] == "Brent crude"
        for item in curated_entities.get("entities", [])
    )

    bundle = build_finance_source_bundle(
        sec_companies_path=result.sec_companies_path,
        sec_submissions_path=result.sec_submissions_path,
        sec_companyfacts_path=result.sec_companyfacts_path,
        symbol_directory_path=result.symbol_directory_path,
        other_listed_path=result.other_listed_path,
        finance_people_path=result.finance_people_path,
        curated_entities_path=result.curated_entities_path,
        output_dir=tmp_path / "bundles",
    )
    assert bundle.sec_issuer_count == 2
    assert bundle.symbol_count == 3
    assert bundle.person_count == 2
    assert bundle.company_equity_ticker_count == 3
    assert bundle.company_name_enriched_issuer_count == 2
    assert bundle.curated_entity_count == len(DEFAULT_CURATED_FINANCE_ENTITIES)


def test_fetch_finance_source_snapshot_rejects_existing_snapshot_dir(
    tmp_path: Path,
) -> None:
    remote_sources = create_finance_remote_sources(tmp_path / "remote")
    output_root = tmp_path / "raw" / "finance-en"

    fetch_finance_source_snapshot(
        output_dir=output_root,
        snapshot="2026-04-10",
        sec_companies_url=remote_sources["sec_companies_url"],
        sec_submissions_url=remote_sources["sec_submissions_url"],
        sec_companyfacts_url=remote_sources["sec_companyfacts_url"],
        symbol_directory_url=remote_sources["symbol_directory_url"],
        other_listed_url=remote_sources["other_listed_url"],
        finance_people_url=remote_sources["finance_people_url"],
    )

    try:
        fetch_finance_source_snapshot(
            output_dir=output_root,
            snapshot="2026-04-10",
            sec_companies_url=remote_sources["sec_companies_url"],
            sec_submissions_url=remote_sources["sec_submissions_url"],
            sec_companyfacts_url=remote_sources["sec_companyfacts_url"],
            symbol_directory_url=remote_sources["symbol_directory_url"],
            other_listed_url=remote_sources["other_listed_url"],
            finance_people_url=remote_sources["finance_people_url"],
        )
    except FileExistsError as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("Expected FileExistsError for an immutable snapshot dir.")


def test_fetch_finance_source_snapshot_can_derive_people_from_sec_proxy_filings(
    tmp_path: Path,
) -> None:
    remote_sources = create_finance_remote_sources(tmp_path / "remote")

    result = fetch_finance_source_snapshot(
        output_dir=tmp_path / "raw" / "finance-en",
        snapshot="2026-04-11",
        sec_companies_url=remote_sources["sec_companies_url"],
        sec_submissions_url=remote_sources["sec_submissions_url"],
        sec_companyfacts_url=remote_sources["sec_companyfacts_url"],
        symbol_directory_url=remote_sources["symbol_directory_url"],
        other_listed_url=remote_sources["other_listed_url"],
        derive_finance_people_from_sec=True,
        finance_people_archive_base_url=remote_sources[
            "finance_people_archive_base_url"
        ],
        finance_people_max_companies=2,
    )

    assert result.finance_people_url.startswith("operator://ades/sec-proxy-people-entities")
    assert any("bounded to 2 companies" in warning for warning in result.warnings)
    people_payload = json.loads(Path(result.finance_people_path).read_text(encoding="utf-8"))
    extracted_names = {
        item["canonical_text"] for item in people_payload.get("entities", [])
    }
    assert extracted_names == {
        "Jane Doe",
        "John Roe",
        "Lucy Stone",
        "Mark Smith",
        "Mary Major",
    }


def test_sec_proxy_people_extractor_ignores_sentence_level_title_fragments() -> None:
    text = """
    <html>
      <body>
        <p>Jane Doe | Chief Executive Officer</p>
        <p>Mr. Gregoire served as Executive Director at Electronic Data Systems (EDS) and has been chair of multiple committees.</p>
        <p>Our Corporate Secretary at Advanced Micro Devices, Inc. will respond to stockholder communications.</p>
        <p>Director Skills Matrix</p>
        <p>Director | Fees earned or</p>
        <p>CHIEF EXECUTIVE OFFICER PAY RATIO</p>
        <p>Chief Financial Officer: John Roe</p>
        <p>Outside directors receive cash fees as compensation for his or her service as a director.</p>
      </body>
    </html>
    """

    entities = _extract_people_from_sec_proxy_document(
        text=text,
        employer_cik="0000000001",
        employer_name="Example Issuer",
        employer_ticker="EXMPL",
        filing_date="2026-04-19",
        filing_url="https://example.test/proxy.htm",
        source_form="DEF 14A",
    )

    assert [item["canonical_text"] for item in entities] == ["Jane Doe"]


def test_sec_proxy_people_extractor_filters_heading_and_table_false_positives() -> None:
    text = """
    <html>
      <body>
        <p>Members of the Nominating and Corporate Governance Committee:</p>
        <p>2026 Proxy Statement</p>
        <p>Annual Meeting</p>
        <p>Cash Retainers</p>
        <p>At Apple</p>
        <p>Certain Beneficial Owners</p>
        <p>Alex Gorsky</p>
        <p>Art Levinson</p>
        <p>Perkins Coie LLP</p>
        <p>Executive Compensation</p>
      </body>
    </html>
    """

    entities = _extract_people_from_sec_proxy_document(
        text=text,
        employer_cik="0000320193",
        employer_name="Apple Inc.",
        employer_ticker="AAPL",
        filing_date="2026-01-08",
        filing_url="https://www.sec.gov/Archives/edgar/data/320193/example/def14a.htm",
        source_form="DEF 14A",
    )

    assert {item["canonical_text"] for item in entities} == {
        "Alex Gorsky",
        "Art Levinson",
    }


def test_sec_proxy_people_extractor_parses_named_executives_and_committee_members() -> None:
    text = """
    <html>
      <body>
        <p>Our Named Executive Officers</p>
        <p>Boris Elisman, Former Executive Chairman and Former Chief Executive Officer(1)</p>
        <p>Thomas W. Tedford, President and Chief Executive Officer(2)</p>
        <p>Deborah A. O'Connor</p>
        <p>Executive Vice President and Chief Financial Officer</p>
        <p>Members of the Audit Committee:</p>
        <p>Ronald Lombardi (Chairperson)</p>
        <p>Joseph B. Burton</p>
        <p>Kathleen S. Dvorak</p>
        <p>E. Mark Rajkowski</p>
        <p>Executive Compensation</p>
      </body>
    </html>
    """

    entities = _extract_people_from_sec_proxy_document(
        text=text,
        employer_cik="0000712034",
        employer_name="ACCO Brands Corporation",
        employer_ticker="ACCO",
        filing_date="2024-04-01",
        filing_url="https://www.sec.gov/Archives/edgar/data/712034/example/def14a.htm",
        source_form="DEF 14A",
    )

    by_name = {entity["canonical_text"]: entity for entity in entities}
    assert {
        "Boris Elisman",
        "Thomas W. Tedford",
        "Deborah A. O'Connor",
        "Ronald Lombardi",
        "Joseph B. Burton",
        "Kathleen S. Dvorak",
        "E. Mark Rajkowski",
    } <= set(by_name)
    assert by_name["Boris Elisman"]["role_class"] == "executive_officer"
    assert (
        by_name["Deborah A. O'Connor"]["role_title"]
        == "Executive Vice President And Chief Financial Officer"
    )
    assert by_name["Joseph B. Burton"]["role_class"] == "director"


def test_extract_sec_proxy_people_entities_skips_missing_filing_documents(
    tmp_path: Path,
    monkeypatch,
) -> None:
    submissions_path = tmp_path / "sec_submissions.zip"
    with zipfile.ZipFile(submissions_path, "w") as archive:
        archive.writestr(
            "CIK0000000001.json",
            json.dumps(
                {
                    "cik": "1",
                    "name": "Example Issuer",
                    "filings": {
                        "recent": {
                            "accessionNumber": [
                                "0000000001-26-000001",
                                "0000000001-26-000002",
                            ],
                            "filingDate": ["2026-04-19", "2026-04-18"],
                            "form": ["DEF 14A", "DEFA14A"],
                            "primaryDocument": ["missing.htm", "fallback.htm"],
                            "primaryDocDescription": ["Proxy", "Proxy amendment"],
                        }
                    },
                }
            ),
        )

    def fake_download_text(source_url: str, *, user_agent: str) -> str:
        assert user_agent == "ades-test/0.1.0"
        if source_url.endswith("missing.htm"):
            raise FileNotFoundError(source_url)
        return "<html><body><p>Jane Doe | Chief Executive Officer</p></body></html>"

    monkeypatch.setattr("ades.packs.finance_sources._download_text", fake_download_text)

    entities = _extract_sec_proxy_people_entities(
        sec_submissions_path=submissions_path,
        sec_company_profiles={
            "0000000001": {
                "employer_cik": "0000000001",
                "employer_name": "Example Issuer",
                "employer_ticker": "EXMPL",
            }
        },
        archive_base_url="https://example.test/sec",
        user_agent="ades-test/0.1.0",
    )

    assert [item["canonical_text"] for item in entities] == ["Jane Doe"]
    assert entities[0]["source_form"] == "DEFA14A"


def test_extract_sec_proxy_people_entities_prefers_richer_recent_proxy_filing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    submissions_path = tmp_path / "sec_submissions.zip"
    with zipfile.ZipFile(submissions_path, "w") as archive:
        archive.writestr(
            "CIK0000000001.json",
            json.dumps(
                {
                    "cik": "1",
                    "name": "Example Issuer",
                    "filings": {
                        "recent": {
                            "accessionNumber": [
                                "0000000001-26-000001",
                                "0000000001-26-000002",
                            ],
                            "filingDate": ["2026-04-19", "2026-04-18"],
                            "form": ["DEFA14A", "DEF 14A"],
                            "primaryDocument": ["thin.htm", "rich.htm"],
                            "primaryDocDescription": ["Proxy amendment", "Proxy"],
                        }
                    },
                }
            ),
        )

    def fake_download_text(source_url: str, *, user_agent: str) -> str:
        assert user_agent == "ades-test/0.1.0"
        if source_url.endswith("thin.htm"):
            return "<html><body><p>Jane Doe | Chief Executive Officer</p></body></html>"
        if source_url.endswith("rich.htm"):
            return """
            <html><body>
              <p>Jane Doe | Chief Executive Officer</p>
              <p>John Roe | Chief Financial Officer</p>
              <p>Members of the Audit Committee:</p>
              <p>Mary Major</p>
              <p>Executive Compensation</p>
            </body></html>
            """
        raise AssertionError(source_url)

    monkeypatch.setattr("ades.packs.finance_sources._download_text", fake_download_text)

    entities = _extract_sec_proxy_people_entities(
        sec_submissions_path=submissions_path,
        sec_company_profiles={
            "0000000001": {
                "employer_cik": "0000000001",
                "employer_name": "Example Issuer",
                "employer_ticker": "EXMPL",
            }
        },
        archive_base_url="https://example.test/sec",
        user_agent="ades-test/0.1.0",
    )

    assert {item["canonical_text"] for item in entities} == {
        "Jane Doe",
        "John Roe",
        "Mary Major",
    }
    assert {item["source_form"] for item in entities} == {"DEF 14A"}


def test_download_text_retries_sec_rate_limited_requests(monkeypatch) -> None:
    attempts = {"count": 0}
    sleep_calls: list[float] = []

    class _FakeResponse:
        def __init__(self, body: bytes) -> None:
            self._body = body
            self.headers = type(
                "_Headers",
                (),
                {"get_content_charset": lambda self: "utf-8"},
            )()

        def read(self) -> bytes:
            return self._body

        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def fake_urlopen(request, timeout: float):
        del timeout
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise urllib.error.HTTPError(
                url=request.full_url,
                code=429,
                msg="Too Many Requests",
                hdrs={"Retry-After": "0"},
                fp=io.BytesIO(b"rate limited"),
            )
        return _FakeResponse(b"<html>ok</html>")

    monkeypatch.setattr("ades.packs.finance_sources.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("ades.packs.finance_sources.time.sleep", sleep_calls.append)

    text = _download_text(
        "https://www.sec.gov/Archives/edgar/data/789019/example/form4.xml",
        user_agent="ades-test/0.1.0",
    )

    assert text == "<html>ok</html>"
    assert attempts["count"] == 3
    assert sleep_calls == [1.0, 1.0]
