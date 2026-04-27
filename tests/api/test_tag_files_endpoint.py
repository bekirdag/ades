from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from ades.service.models import BatchSourceSummary, BatchTagResponse


def test_tag_files_endpoint_tags_multiple_local_documents(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    alpha_dir = tmp_path / "alpha"
    beta_dir = tmp_path / "beta"
    alpha_dir.mkdir()
    beta_dir.mkdir()
    first_input = alpha_dir / "report.html"
    second_input = beta_dir / "report.html"
    first_input.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    second_input.write_text("<p>EXCHX said Org Beta moved TICKA guidance.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "paths": [str(first_input), str(second_input)],
            "pack": "finance-en",
            "output": {
                "directory": str(output_dir),
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    saved_paths = {item["saved_output_path"] for item in payload["items"]}

    assert payload["pack"] == "finance-en"
    assert payload["item_count"] == 2
    assert saved_paths == {
        str(output_dir.resolve() / "report.finance-en.ades.json"),
        str(output_dir.resolve() / "report.finance-en.ades-2.json"),
    }


def test_tag_files_endpoint_forwards_domain_and_country_hints(
    tmp_path: Path, monkeypatch
) -> None:
    first_input = tmp_path / "report-one.txt"
    second_input = tmp_path / "report-two.txt"
    first_input.write_text("Entity Alpha Holdings moved.", encoding="utf-8")
    second_input.write_text("Entity Beta Holdings moved.", encoding="utf-8")
    client = TestClient(create_app(storage_root=tmp_path))

    def _fake_tag_files(
        inputs,
        *,
        pack=None,
        content_type=None,
        domain_hint=None,
        country_hint=None,
        **kwargs,
    ):
        assert inputs == [str(first_input), str(second_input)]
        assert pack == "finance-en"
        assert content_type is None
        assert domain_hint == "business"
        assert country_hint == "uk"
        assert "directories" in kwargs
        assert "registry" in kwargs
        return BatchTagResponse(
            pack="finance-en",
            item_count=0,
            summary=BatchSourceSummary(
                explicit_path_count=2,
                directory_match_count=0,
                glob_match_count=0,
                discovered_count=2,
                included_count=2,
                processed_count=0,
                excluded_count=0,
                skipped_count=0,
                rejected_count=0,
                duplicate_count=0,
                generated_output_skipped_count=0,
                discovered_input_bytes=0,
                included_input_bytes=0,
                processed_input_bytes=0,
                recursive=True,
            ),
            items=[],
            warnings=[],
        )

    monkeypatch.setattr("ades.service.app.tag_files", _fake_tag_files)

    response = client.post(
        "/v0/tag/files",
        json={
            "paths": [str(first_input), str(second_input)],
            "pack": "finance-en",
            "options": {
                "domain_hint": "business",
                "country_hint": "uk",
            },
        },
    )

    assert response.status_code == 200
