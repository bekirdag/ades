import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller
from ades.service.client import LocalServiceUnavailableError
from ades.service.models import BatchSourceSummary, BatchTagResponse, TagResponse


def _tag_response(*, pack: str = "finance-en") -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack=pack,
        pack_version="1.2.3",
        language="en",
        content_type="text/plain",
        entities=[],
        topics=[],
        warnings=[],
        timing_ms=7,
    )


def _batch_response(*, pack: str = "finance-en") -> BatchTagResponse:
    return BatchTagResponse(
        pack=pack,
        item_count=1,
        summary=BatchSourceSummary(
            explicit_path_count=1,
            directory_match_count=0,
            glob_match_count=0,
            discovered_count=1,
            included_count=1,
            processed_count=1,
            excluded_count=0,
            skipped_count=0,
            rejected_count=0,
            duplicate_count=0,
            generated_output_skipped_count=0,
            discovered_input_bytes=32,
            included_input_bytes=32,
            processed_input_bytes=32,
            recursive=True,
        ),
        warnings=[],
        items=[_tag_response(pack=pack)],
    )


def test_cli_tag_prefers_local_service_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    PackInstaller(tmp_path).install("finance-en")
    runner = CliRunner()

    monkeypatch.setattr("ades.cli.should_use_local_service", lambda settings: True)
    monkeypatch.setattr("ades.cli.tag_via_local_service", lambda *args, **kwargs: _tag_response())
    monkeypatch.setattr(
        "ades.cli.api_tag",
        lambda *args, **kwargs: pytest.fail("in-process tag fallback should not run"),
    )

    result = runner.invoke(
        app,
        ["tag", "Org Beta said TICKA rallied.", "--pack", "finance-en"],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack"] == "finance-en"
    assert payload["pack_version"] == "1.2.3"


def test_cli_tag_defaults_to_in_process_for_custom_storage_root_in_auto_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    PackInstaller(tmp_path).install("finance-en")
    runner = CliRunner()

    monkeypatch.delenv("ADES_TAG_SERVICE_MODE", raising=False)
    monkeypatch.setattr(
        "ades.cli.tag_via_local_service",
        lambda *args, **kwargs: pytest.fail(
            "custom storage roots should bypass the warm local service in auto mode"
        ),
    )
    monkeypatch.setattr("ades.cli.api_tag", lambda *args, **kwargs: _tag_response())

    result = runner.invoke(
        app,
        ["tag", "Org Beta said TICKA rallied.", "--pack", "finance-en"],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack"] == "finance-en"


def test_cli_tag_exits_when_service_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    PackInstaller(tmp_path).install("finance-en")
    runner = CliRunner()

    monkeypatch.setattr("ades.cli.should_use_local_service", lambda settings: True)
    monkeypatch.setattr(
        "ades.cli.tag_via_local_service",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            LocalServiceUnavailableError("service unavailable")
        ),
    )
    monkeypatch.setattr(
        "ades.cli.api_tag",
        lambda *args, **kwargs: pytest.fail("in-process tag fallback should not run"),
    )

    result = runner.invoke(
        app,
        ["tag", "Org Beta said TICKA rallied.", "--pack", "finance-en"],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 1
    assert "service unavailable" in result.stderr


def test_cli_tag_files_can_use_local_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    PackInstaller(tmp_path).install("finance-en")
    input_path = tmp_path / "report.html"
    input_path.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    runner = CliRunner()

    monkeypatch.setattr("ades.cli.should_use_local_service", lambda settings: True)
    monkeypatch.setattr(
        "ades.cli.tag_files_via_local_service",
        lambda *args, **kwargs: _batch_response(),
    )
    monkeypatch.setattr(
        "ades.cli.api_tag_files",
        lambda *args, **kwargs: pytest.fail("in-process batch fallback should not run"),
    )

    result = runner.invoke(
        app,
        ["tag-files", str(input_path), "--pack", "finance-en"],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["pack"] == "finance-en"
    assert payload["item_count"] == 1
