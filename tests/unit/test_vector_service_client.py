from pathlib import Path

from ades.service.client import tag_file_via_local_service, tag_files_via_local_service, tag_via_local_service
from ades.service.models import BatchSourceSummary, BatchTagResponse, TagResponse


def _tag_response() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[],
        topics=[],
        warnings=[],
        timing_ms=1,
    )


def _batch_response() -> BatchTagResponse:
    return BatchTagResponse(
        pack="general-en",
        item_count=0,
        summary=BatchSourceSummary(
            explicit_path_count=0,
            directory_match_count=0,
            glob_match_count=0,
            discovered_count=0,
            included_count=0,
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


def test_tag_via_local_service_serializes_vector_options(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr("ades.service.client.ensure_local_service", lambda settings: None)

    def _fake_request(method: str, url: str, *, payload=None, timeout_seconds=3600.0):
        captured["method"] = method
        captured["url"] = url
        captured["payload"] = payload
        return _tag_response().model_dump(mode="json")

    monkeypatch.setattr("ades.service.client._request_json", _fake_request)

    response = tag_via_local_service(
        "Entity Alpha Holdings moved.",
        pack="general-en",
        settings=None,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=True,
        refinement_depth="deep",
        domain_hint="finance",
        retrieval_profile="finance",
    )

    assert response.pack == "general-en"
    assert captured["method"] == "POST"
    options = captured["payload"]["options"]  # type: ignore[index]
    assert options["include_related_entities"] is True
    assert options["include_graph_support"] is True
    assert options["refine_links"] is True
    assert options["refinement_depth"] == "deep"
    assert options["domain_hint"] == "finance"
    assert options["retrieval_profile"] == "finance"
    assert captured["payload"]["domain_hint"] == "finance"  # type: ignore[index]
    assert captured["payload"]["retrieval_profile"] == "finance"  # type: ignore[index]


def test_tag_file_via_local_service_serializes_vector_options(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr("ades.service.client.ensure_local_service", lambda settings: None)

    def _fake_request(method: str, url: str, *, payload=None, timeout_seconds=3600.0):
        captured["payload"] = payload
        return _tag_response().model_dump(mode="json")

    monkeypatch.setattr("ades.service.client._request_json", _fake_request)

    response = tag_file_via_local_service(
        tmp_path / "sample.txt",
        pack="general-en",
        include_related_entities=True,
        refine_links=True,
        refinement_depth="deep",
        domain_hint="politics",
    )

    assert response.pack == "general-en"
    options = captured["payload"]["options"]  # type: ignore[index]
    assert options["include_related_entities"] is True
    assert options["refine_links"] is True
    assert options["refinement_depth"] == "deep"
    assert options["domain_hint"] == "politics"
    assert captured["payload"]["domain_hint"] == "politics"  # type: ignore[index]


def test_tag_files_via_local_service_serializes_vector_options(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr("ades.service.client.ensure_local_service", lambda settings: None)

    def _fake_request(method: str, url: str, *, payload=None, timeout_seconds=3600.0):
        captured["payload"] = payload
        return _batch_response().model_dump(mode="json")

    monkeypatch.setattr("ades.service.client._request_json", _fake_request)

    response = tag_files_via_local_service(
        [tmp_path / "sample.txt"],
        pack="general-en",
        include_related_entities=True,
        include_graph_support=True,
        refine_links=True,
        refinement_depth="deep",
        retrieval_profile="finance_politics",
    )

    assert response.pack == "general-en"
    options = captured["payload"]["options"]  # type: ignore[index]
    assert options["include_related_entities"] is True
    assert options["include_graph_support"] is True
    assert options["refine_links"] is True
    assert options["refinement_depth"] == "deep"
    assert options["retrieval_profile"] == "finance_politics"
    assert captured["payload"]["retrieval_profile"] == "finance_politics"  # type: ignore[index]
