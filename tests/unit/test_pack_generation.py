import json
from pathlib import Path

import pytest

from ades.packs.generation import generate_pack_source
from tests.pack_generation_helpers import (
    create_finance_generation_bundle,
    create_general_generation_bundle,
    create_noisy_general_generation_bundle,
    create_pre_resolved_general_generation_bundle,
)


def test_generate_pack_source_writes_runtime_compatible_pack(tmp_path: Path) -> None:
    bundle_dir = create_finance_generation_bundle(tmp_path)

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    assert result.pack_id == "finance-en"
    assert result.version == "0.2.0"
    assert result.label_count == 4
    assert result.alias_count >= 3
    assert result.rule_count == 1
    assert result.source_count == 2
    assert result.publishable_source_count == 1
    assert result.restricted_source_count == 1
    assert result.publishable_sources_only is False
    assert result.source_license_classes == {"build-only": 1, "ship-now": 1}
    assert result.included_entity_count == 3
    assert result.included_rule_count == 1
    assert result.dropped_record_count == 2
    assert result.dropped_alias_count >= 1
    assert result.ambiguous_alias_count == 1
    assert any("sec-companyfacts" in warning for warning in result.warnings)

    pack_dir = Path(result.pack_dir)
    manifest = json.loads((pack_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["pack_id"] == "finance-en"
    assert manifest["rules"] == ["rules.json"]
    assert manifest["labels"] == ["labels.json"]
    assert manifest["matcher"]["algorithm"] == "aho_corasick"
    assert manifest["matcher"]["artifact_path"] == "matcher/automaton.json"
    assert manifest["matcher"]["entries_path"] == "matcher/entries.jsonl"
    assert manifest["matcher"]["entry_count"] == result.matcher_entry_count
    assert Path(result.matcher_artifact_path).exists()
    assert Path(result.matcher_entries_path).exists()

    labels = json.loads((pack_dir / "labels.json").read_text(encoding="utf-8"))
    assert labels == ["currency_amount", "exchange", "organization", "ticker"]

    aliases = json.loads((pack_dir / "aliases.json").read_text(encoding="utf-8"))["aliases"]
    assert any(
        item["text"] == "Issuer Alpha" and item["label"] == "organization"
        for item in aliases
    )
    assert any(
        item["text"] == "Issuer Alpha Holdings" and item["label"] == "organization"
        for item in aliases
    )
    assert any(item["text"] == "TICKA" and item["label"] == "ticker" for item in aliases)
    assert any(item["text"] == "EXCHX" and item["label"] == "exchange" for item in aliases)
    assert any(
        item["text"] == "Issuer Alpha" and item.get("generated") is True
        for item in aliases
    )
    assert all("canonical_text" in item for item in aliases)
    assert all("score" in item for item in aliases)
    assert all("source_priority" in item for item in aliases)
    assert all("popularity_weight" in item for item in aliases)
    assert all(item["text"] != "N/A" for item in aliases)
    assert all(item["text"] != "PRIVATE-42" for item in aliases)

    rules = json.loads((pack_dir / "rules.json").read_text(encoding="utf-8"))["patterns"]
    assert rules == [
        {
            "kind": "regex",
            "label": "currency_amount",
            "name": "currency_amount",
            "pattern": r"(USD|EUR|GBP|TRY)\s?[0-9]+(?:\.[0-9]+)?",
        }
    ]

    build_metadata = json.loads((pack_dir / "build.json").read_text(encoding="utf-8"))
    assert build_metadata["input_entity_count"] == 4
    assert build_metadata["input_rule_count"] == 2
    assert build_metadata["publishable_source_count"] == 1
    assert build_metadata["restricted_source_count"] == 1
    assert build_metadata["publishable_sources_only"] is False
    assert build_metadata["source_license_classes"] == {"build-only": 1, "ship-now": 1}
    assert build_metadata["analysis_backend"] == "sqlite"
    assert build_metadata["candidate_alias_count"] >= result.alias_count
    assert build_metadata["matcher"]["algorithm"] == "aho_corasick"
    assert build_metadata["matcher"]["entry_count"] == result.matcher_entry_count
    assert build_metadata["matcher"]["state_count"] == result.matcher_state_count
    assert build_metadata["entity_label_distribution"] == {
        "exchange": 1,
        "organization": 1,
        "ticker": 1,
    }
    assert Path(build_metadata["analysis_db_path"]).exists()
    assert Path(build_metadata["alias_analysis_report_path"]).exists()

    alias_analysis = json.loads(
        Path(build_metadata["alias_analysis_report_path"]).read_text(encoding="utf-8")
    )
    assert alias_analysis["backend"] == "sqlite"
    assert alias_analysis["retained_alias_count"] == result.alias_count
    assert alias_analysis["candidate_alias_count"] == build_metadata["candidate_alias_count"]

    sources_metadata = json.loads((pack_dir / "sources.json").read_text(encoding="utf-8"))
    assert sources_metadata["publishable_sources_only"] is False
    assert sources_metadata["source_license_classes"] == {"build-only": 1, "ship-now": 1}


def test_generate_pack_source_drops_ambiguous_aliases_by_default(tmp_path: Path) -> None:
    bundle_dir = create_finance_generation_bundle(
        tmp_path,
        include_ambiguous_alias=True,
    )

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    aliases = json.loads(
        (Path(result.pack_dir) / "aliases.json").read_text(encoding="utf-8")
    )["aliases"]
    build_metadata = json.loads(
        (Path(result.pack_dir) / "build.json").read_text(encoding="utf-8")
    )
    alias_analysis = json.loads(
        Path(build_metadata["alias_analysis_report_path"]).read_text(encoding="utf-8")
    )

    assert result.ambiguous_alias_count == 2
    assert not any(item["text"].casefold() == "exchange alpha" for item in aliases)
    assert alias_analysis["blocked_reason_counts"]["ambiguous_natural_language_alias"] == 2
    assert alias_analysis["blocked_reason_counts"]["low_information_single_label_alias"] == 1
    assert alias_analysis["top_collision_clusters"][0]["alias_key"] == "exchange alpha"


def test_generate_pack_source_supports_record_level_blocked_aliases(tmp_path: Path) -> None:
    bundle_dir = create_finance_generation_bundle(
        tmp_path,
        include_ambiguous_alias=True,
    )
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    entity_rows = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    for row in entity_rows:
        if row.get("entity_id") == "issuer:exchange-alpha-holdings":
            row["blocked_aliases"] = ["Exchange Alpha"]
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in entity_rows),
        encoding="utf-8",
    )

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    aliases = json.loads(
        (Path(result.pack_dir) / "aliases.json").read_text(encoding="utf-8")
    )["aliases"]

    assert result.ambiguous_alias_count == 1
    assert any(item["text"] == "EXCHX" and item["label"] == "exchange" for item in aliases)


def test_generate_pack_source_rejects_invalid_regex_patterns(tmp_path: Path) -> None:
    bundle_dir = create_finance_generation_bundle(tmp_path)
    rules_path = bundle_dir / "normalized" / "rules.jsonl"
    rules_path.write_text(
        json.dumps(
            {
                "name": "broken",
                "label": "ticker",
                "kind": "regex",
                "pattern": "[",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid regex pattern"):
        generate_pack_source(
            bundle_dir,
            output_dir=tmp_path / "generated-packs",
        )


def test_generate_pack_source_adds_generic_person_and_org_variants(tmp_path: Path) -> None:
    bundle_dir = create_general_generation_bundle(tmp_path)

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    aliases = json.loads(
        (Path(result.pack_dir) / "aliases.json").read_text(encoding="utf-8")
    )["aliases"]
    pairs = {(item["text"], item["label"]) for item in aliases}

    assert ("Jordan Vale", "person") in pairs
    assert ("North Harbor", "location") in pairs
    assert ("Beacon", "organization") in pairs
    assert ("Beacon", "location") not in pairs

    jordan = next(item for item in aliases if item["text"] == "Jordan Vale")
    north_harbor = next(item for item in aliases if item["text"] == "North Harbor")

    assert jordan["generated"] is True
    assert north_harbor["label"] == "location"


def test_generate_pack_source_prefers_stronger_natural_alias_candidate(tmp_path: Path) -> None:
    bundle_dir = create_general_generation_bundle(tmp_path)

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    build_metadata = json.loads(
        (Path(result.pack_dir) / "build.json").read_text(encoding="utf-8")
    )
    alias_analysis = json.loads(
        Path(build_metadata["alias_analysis_report_path"]).read_text(encoding="utf-8")
    )
    clusters = {
        cluster["alias_key"]: cluster
        for cluster in alias_analysis["top_collision_clusters"]
    }

    north_harbor = clusters["north harbor"]
    beacon = clusters["beacon"]

    assert north_harbor["ambiguous"] is False
    assert north_harbor["retained_labels"] == ["location"]
    assert north_harbor["reason"] == "dominant_location_alias"

    assert beacon["ambiguous"] is False
    assert beacon["retained_labels"] == ["organization"]
    assert beacon["reason"] == "lower_authority_cross_label"


def test_generate_pack_source_drops_low_information_general_single_token_aliases(
    tmp_path: Path,
) -> None:
    bundle_dir = create_noisy_general_generation_bundle(tmp_path)

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    aliases = json.loads(
        (Path(result.pack_dir) / "aliases.json").read_text(encoding="utf-8")
    )["aliases"]
    alias_pairs = {(item["text"], item["label"]) for item in aliases}

    assert ("Daniel", "person") not in alias_pairs
    assert ("Third", "organization") not in alias_pairs
    assert ("York", "location") not in alias_pairs
    assert ("Beacon", "organization") in alias_pairs
    assert ("Daniel Loeb", "person") in alias_pairs
    assert ("Real Estate", "organization") in alias_pairs
    assert ("Letter", "location") in alias_pairs


def test_generate_pack_source_supports_pre_resolved_general_bundle(tmp_path: Path) -> None:
    bundle_dir = create_pre_resolved_general_generation_bundle(tmp_path)

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    assert result.pack_id == "general-en"
    assert result.alias_count >= 4
    assert result.rule_count == 1

    pack_dir = Path(result.pack_dir)
    aliases = json.loads((pack_dir / "aliases.json").read_text(encoding="utf-8"))["aliases"]
    alias_pairs = {(item["text"], item["label"]) for item in aliases}

    assert ("Jordan Elliott Vale", "person") in alias_pairs
    assert ("Jordan Vale", "person") in alias_pairs
    assert ("Beacon Group", "organization") in alias_pairs
    assert ("Beacon", "organization") in alias_pairs
    assert ("North Harbor", "location") in alias_pairs
    assert all(item.get("generated") is False for item in aliases)
    assert all("canonical_text" in item for item in aliases)
    assert all("score" in item for item in aliases)
    assert all("alias_score" in item for item in aliases)
    assert all("source_priority" in item for item in aliases)
    assert all("popularity_weight" in item for item in aliases)

    build_metadata = json.loads((pack_dir / "build.json").read_text(encoding="utf-8"))
    assert build_metadata["analysis_backend"] == "pre_resolved_bundle"
    assert build_metadata["matcher"]["algorithm"] == "aho_corasick"
    assert build_metadata["matcher"]["entry_count"] == result.matcher_entry_count
    assert build_metadata["analysis_db_path"] is None
    assert Path(build_metadata["alias_analysis_report_path"]).exists()
    assert not (pack_dir / "analysis.sqlite").exists()

    alias_analysis = json.loads(
        Path(build_metadata["alias_analysis_report_path"]).read_text(encoding="utf-8")
    )
    assert alias_analysis["backend"] == "pre_resolved_bundle"
    assert alias_analysis["retained_alias_count"] == result.alias_count
    assert alias_analysis["candidate_alias_count"] == result.alias_count


def test_generate_pack_source_requires_explicit_source_license_class(tmp_path: Path) -> None:
    bundle_dir = create_finance_generation_bundle(tmp_path)
    bundle_manifest_path = bundle_dir / "bundle.json"
    bundle_manifest = json.loads(bundle_manifest_path.read_text(encoding="utf-8"))
    bundle_manifest["sources"][0]["license"] = "operator-supplied"
    bundle_manifest["sources"][0].pop("license_class", None)
    bundle_manifest_path.write_text(
        json.dumps(bundle_manifest, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must include license_class"):
        generate_pack_source(
            bundle_dir,
            output_dir=tmp_path / "generated-packs",
        )
