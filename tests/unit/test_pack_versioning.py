import json
from pathlib import Path

from ades.extraction_quality import ExtractionQualityReport
from ades.packs.versioning import (
    ReleaseThresholdDecision,
    ReleaseThresholds,
    default_release_thresholds,
    diff_pack_directories,
    evaluate_release_thresholds,
)
from tests.pack_registry_helpers import create_pack_source


def test_diff_pack_directories_classifies_metadata_only_change_as_patch(tmp_path: Path) -> None:
    old_dir = create_pack_source(
        tmp_path / "old",
        pack_id="general-en",
        domain="general",
        aliases=(("Entity Alpha", "organization"),),
    )
    new_dir = create_pack_source(
        tmp_path / "new",
        pack_id="general-en",
        domain="general",
        aliases=(("Entity Alpha", "organization"),),
        description="updated description only",
    )

    diff = diff_pack_directories(old_dir, new_dir)

    assert diff.severity == "patch"
    assert diff.requires_retag is False
    assert diff.changed_metadata_fields == ["description"]


def test_diff_pack_directories_classifies_additive_alias_change_as_minor(tmp_path: Path) -> None:
    old_dir = create_pack_source(
        tmp_path / "old",
        pack_id="general-en",
        domain="general",
        aliases=(("Entity Alpha", "organization"),),
    )
    new_dir = create_pack_source(
        tmp_path / "new",
        pack_id="general-en",
        domain="general",
        aliases=(
            ("Entity Alpha", "organization"),
            ("Entity Beta", "organization"),
        ),
    )

    diff = diff_pack_directories(old_dir, new_dir)

    assert diff.severity == "minor"
    assert diff.requires_retag is True
    assert diff.added_aliases == ["entity beta:organization"]


def test_diff_pack_directories_classifies_canonical_change_as_major(tmp_path: Path) -> None:
    old_dir = create_pack_source(
        tmp_path / "old",
        pack_id="general-en",
        domain="general",
        aliases=(("Entity Alpha", "organization"),),
    )
    new_dir = create_pack_source(
        tmp_path / "new",
        pack_id="general-en",
        domain="general",
        aliases=(("Entity Alpha", "organization"),),
    )
    old_aliases_path = old_dir / "aliases.json"
    new_aliases_path = new_dir / "aliases.json"
    old_aliases = json.loads(old_aliases_path.read_text(encoding="utf-8"))
    new_aliases = json.loads(new_aliases_path.read_text(encoding="utf-8"))
    old_aliases["aliases"][0]["canonical_text"] = "Entity Alpha Holdings"
    new_aliases["aliases"][0]["canonical_text"] = "Entity Alpha Group"
    old_aliases_path.write_text(json.dumps(old_aliases, indent=2) + "\n", encoding="utf-8")
    new_aliases_path.write_text(json.dumps(new_aliases, indent=2) + "\n", encoding="utf-8")

    diff = diff_pack_directories(old_dir, new_dir)

    assert diff.severity == "major"
    assert diff.requires_retag is True
    assert diff.changed_canonical_aliases == ["entity alpha:organization"]


def test_evaluate_release_thresholds_checks_hybrid_delta() -> None:
    baseline = ExtractionQualityReport(
        pack_id="general-en",
        profile="default",
        hybrid=False,
        document_count=1,
        expected_entity_count=10,
        actual_entity_count=10,
        matched_entity_count=9,
        missing_entity_count=1,
        unexpected_entity_count=1,
        recall=0.9,
        precision=0.9,
        entities_per_100_tokens=6.0,
        overlap_drop_count=0,
        chunk_count=1,
        low_density_warning_count=0,
        p95_latency_ms=50,
        per_label_expected={"organization": 4},
        per_label_actual={"organization": 4},
        per_label_matched={"organization": 4},
        per_label_recall={"organization": 1.0},
    )
    hybrid = ExtractionQualityReport(
        pack_id="general-en",
        profile="default",
        hybrid=True,
        document_count=1,
        expected_entity_count=10,
        actual_entity_count=11,
        matched_entity_count=10,
        missing_entity_count=0,
        unexpected_entity_count=1,
        recall=1.0,
        precision=0.9091,
        entities_per_100_tokens=7.0,
        overlap_drop_count=1,
        chunk_count=1,
        low_density_warning_count=0,
        p95_latency_ms=90,
        per_label_expected={"organization": 4},
        per_label_actual={"organization": 5},
        per_label_matched={"organization": 4},
        per_label_recall={"organization": 1.0},
    )

    decision = evaluate_release_thresholds(
        hybrid,
        mode="hybrid",
        baseline_report=baseline,
        thresholds=ReleaseThresholds(
            min_recall=0.9,
            min_precision=0.85,
            min_recall_lift=0.05,
            max_precision_drop=0.05,
            max_p95_latency_ms=150,
            max_model_artifact_bytes=1_000_000_000,
            max_peak_memory_mb=500,
        ),
        model_artifact_bytes=250_000_000,
        peak_memory_mb=320,
    )

    assert isinstance(decision, ReleaseThresholdDecision)
    assert decision.passed is True
    assert decision.quality_delta is not None
    assert decision.quality_delta.recall_delta == 0.1


def test_default_release_thresholds_lock_expected_mode_defaults() -> None:
    deterministic = default_release_thresholds("deterministic")
    hybrid = default_release_thresholds("hybrid")

    assert deterministic.min_recall == 0.85
    assert deterministic.min_precision == 0.85
    assert deterministic.max_label_recall_drop == 0.0
    assert deterministic.max_p95_latency_ms == 100
    assert deterministic.max_model_artifact_bytes is None
    assert deterministic.max_peak_memory_mb is None

    assert hybrid.min_recall == 0.85
    assert hybrid.min_precision == 0.85
    assert hybrid.max_label_recall_drop == 0.0
    assert hybrid.min_recall_lift == 0.10
    assert hybrid.max_precision_drop == 0.05
    assert hybrid.max_p95_latency_ms == 150
    assert hybrid.max_model_artifact_bytes == 1_000_000_000
    assert hybrid.max_peak_memory_mb == 500
