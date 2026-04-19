import json
from pathlib import Path

import pytest

from ades.packs.finance_bundle import build_finance_source_bundle
from ades.packs.alias_analysis import build_retained_alias_review_key
from ades.packs.generation import (
    _iter_entity_alias_candidates,
    generate_pack_source,
    refresh_pack_from_analysis_db,
)
from tests.finance_bundle_helpers import create_finance_raw_snapshots
from tests.pack_generation_helpers import (
    create_acronym_general_generation_bundle,
    create_finance_generation_bundle,
    create_general_generation_bundle,
    create_generic_audit_general_generation_bundle,
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
    assert manifest["matcher"]["algorithm"] == "token_trie_v1"
    assert manifest["matcher"]["artifact_path"] == "matcher/token-trie.bin"
    assert manifest["matcher"]["entries_path"] == "matcher/entries.bin"
    assert manifest["matcher"]["entry_count"] == result.matcher_entry_count
    assert Path(result.matcher_artifact_path).exists()
    assert Path(result.matcher_entries_path).exists()
    assert Path(result.matcher_entries_path).with_suffix(".idx").exists()
    assert Path(result.matcher_artifact_path).with_suffix(".stateidx").exists()

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
    assert all(item["source_domain"] == "finance" for item in aliases)
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
    assert build_metadata["matcher"]["algorithm"] == "token_trie_v1"
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


def test_refresh_pack_from_analysis_db_rebuilds_alias_artifacts(tmp_path: Path) -> None:
    bundle_dir = create_finance_generation_bundle(tmp_path)

    original = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    refreshed = refresh_pack_from_analysis_db(
        original.pack_dir,
        output_dir=tmp_path / "refreshed-packs",
        version="0.2.1",
    )

    refreshed_pack_dir = Path(refreshed.pack_dir)
    refreshed_manifest = json.loads(
        (refreshed_pack_dir / "manifest.json").read_text(encoding="utf-8")
    )
    refreshed_build = json.loads(
        (refreshed_pack_dir / "build.json").read_text(encoding="utf-8")
    )
    refreshed_aliases = json.loads(
        (refreshed_pack_dir / "aliases.json").read_text(encoding="utf-8")
    )["aliases"]

    assert refreshed.version == "0.2.1"
    assert refreshed_manifest["version"] == "0.2.1"
    assert refreshed_manifest["matcher"]["algorithm"] == "token_trie_v1"
    assert refreshed_build["analysis_seed_db_path"] == str(
        Path(original.pack_dir) / "analysis.sqlite"
    )
    assert Path(refreshed_build["analysis_db_path"]).exists()
    assert Path(refreshed_build["alias_analysis_report_path"]).exists()
    assert Path(refreshed.matcher_artifact_path).exists()
    assert Path(refreshed.matcher_entries_path).exists()
    assert any(
        item["text"] == "Issuer Alpha Holdings" and item["label"] == "organization"
        for item in refreshed_aliases
    )


def test_generate_pack_source_keeps_finance_shortforms_from_real_bundle(tmp_path: Path) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "raw")
    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        sec_submissions_path=snapshots["sec_submissions"],
        sec_companyfacts_path=snapshots["sec_companyfacts"],
        symbol_directory_path=snapshots["symbol_directory"],
        other_listed_path=snapshots["other_listed"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "finance-source-bundle",
    )

    result = generate_pack_source(
        bundle.bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    aliases = json.loads(
        (Path(result.pack_dir) / "aliases.json").read_text(encoding="utf-8")
    )["aliases"]
    alias_pairs = {(item["text"], item["label"]) for item in aliases}

    assert ("VIX", "market_index") in alias_pairs
    assert ("LSE", "exchange") in alias_pairs
    assert ("Chicago Mercantile Exchange", "exchange") in alias_pairs
    assert ("IAL", "organization") not in alias_pairs


def test_entity_alias_candidates_generate_plain_geopolitical_acronyms() -> None:
    candidates = _iter_entity_alias_candidates(
        "United States",
        {"aliases": ["the US", "U.S."]},
        "location",
        pack_domain="general",
    )

    assert ("US", True) in candidates


def test_entity_alias_candidates_generate_initialism_acronyms_for_structured_orgs() -> None:
    candidates = _iter_entity_alias_candidates(
        "International Energy Agency",
        {"aliases": []},
        "organization",
        pack_domain="general",
    )

    assert ("IEA", True) in candidates


def test_generate_pack_source_drops_structural_org_fragment_aliases(tmp_path: Path) -> None:
    bundle_dir = create_noisy_general_generation_bundle(tmp_path / "bundle")

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    aliases = json.loads(
        (Path(result.pack_dir) / "aliases.json").read_text(encoding="utf-8")
    )["aliases"]
    alias_pairs = {(item["text"], item["label"]) for item in aliases}

    assert ("Harbor China Information", "organization") in alias_pairs
    assert ("Harbor", "organization") not in alias_pairs


def test_generate_pack_source_audits_generic_general_retained_aliases(
    tmp_path: Path,
) -> None:
    bundle_dir = create_generic_audit_general_generation_bundle(tmp_path / "bundle")

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    pack_dir = Path(result.pack_dir)
    aliases = json.loads((pack_dir / "aliases.json").read_text(encoding="utf-8"))["aliases"]
    alias_pairs = {(item["text"], item["label"]) for item in aliases}
    build_metadata = json.loads((pack_dir / "build.json").read_text(encoding="utf-8"))
    alias_analysis = json.loads(
        (pack_dir / "alias-analysis.json").read_text(encoding="utf-8")
    )

    assert ("North Harbor", "location") in alias_pairs
    assert ("Beacon Group", "organization") in alias_pairs
    assert ("March", "location") not in alias_pairs
    assert ("Can", "organization") not in alias_pairs
    assert ("Five", "location") not in alias_pairs
    assert ("Cars", "organization") not in alias_pairs
    assert ("Four Ways", "location") not in alias_pairs
    assert build_metadata["retained_alias_audit_scanned_alias_count"] == result.alias_count + 2
    assert build_metadata["retained_alias_audit_removed_alias_count"] == 2
    assert build_metadata["retained_alias_audit_chunk_size"] == 1000
    assert build_metadata["retained_alias_audit_reason_counts"] == {
        "generic_retained_phrase_alias": 1,
        "generic_retained_single_token_alias": 1,
    }
    assert alias_analysis["retained_alias_audit_removed_alias_count"] == 2
    assert alias_analysis["retained_alias_audit_reason_counts"] == {
        "generic_retained_phrase_alias": 1,
        "generic_retained_single_token_alias": 1,
    }


def test_generate_pack_source_applies_curated_general_exclusion_file(
    tmp_path: Path,
) -> None:
    bundle_dir = create_general_generation_bundle(tmp_path / "bundle")
    exclusion_path = bundle_dir / "general-retained-alias-exclusions.jsonl"
    exclusion_payload = {
        "review_key": build_retained_alias_review_key(
            {
                "text": "Beacon Group",
                "label": "organization",
                "canonical_text": "Beacon Group",
                "entity_id": "organization:beacon-group",
                "source_name": "curated-general",
            }
        ),
        "text": "Beacon Group",
        "label": "organization",
        "canonical_text": "Beacon Group",
        "entity_id": "organization:beacon-group",
        "source_name": "curated-general",
        "decision": "drop",
        "reason_code": "approved_generic_retained_alias",
        "reason": "approved for removal from general-en",
    }
    exclusion_path.write_text(
        json.dumps(exclusion_payload, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    pack_dir = Path(result.pack_dir)
    aliases = json.loads((pack_dir / "aliases.json").read_text(encoding="utf-8"))["aliases"]
    alias_pairs = {(item["text"], item["label"]) for item in aliases}
    build_metadata = json.loads((pack_dir / "build.json").read_text(encoding="utf-8"))
    alias_analysis = json.loads(
        (pack_dir / "alias-analysis.json").read_text(encoding="utf-8")
    )

    assert ("Beacon Group", "organization") not in alias_pairs
    assert ("North Harbor", "location") in alias_pairs
    assert build_metadata["retained_alias_exclusion_removed_alias_count"] == 1
    assert build_metadata["retained_alias_exclusion_source_path"] == str(exclusion_path)
    assert alias_analysis["retained_alias_exclusion_removed_alias_count"] == 1
    assert alias_analysis["retained_alias_exclusion_source_path"] == str(exclusion_path)


def test_generate_pack_source_applies_common_english_word_list_to_all_aliases(
    tmp_path: Path,
) -> None:
    bundle_dir = create_general_generation_bundle(tmp_path / "bundle")
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    records = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    records.extend(
        [
            {
                "entity_id": "organization:exclusive",
                "entity_type": "organization",
                "canonical_text": "Exclusive",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.92,
            },
            {
                "entity_id": "organization:new",
                "entity_type": "organization",
                "canonical_text": "Next Entertainment World",
                "aliases": ["NEW"],
                "source_name": "curated-general",
                "popularity": 0.91,
            },
            {
                "entity_id": "organization:applied-imaging",
                "entity_type": "organization",
                "canonical_text": "Applied Imaging",
                "aliases": ["AI"],
                "source_name": "curated-general",
                "popularity": 0.9,
            },
            {
                "entity_id": "organization:world-bank",
                "entity_type": "organization",
                "canonical_text": "World Bank",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.99,
            },
        ]
    )
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    word_list_path = bundle_dir / "wordfreq-en-top50000.txt"
    word_list_path.write_text("exclusive\nnew\ncare\n", encoding="utf-8")

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    pack_dir = Path(result.pack_dir)
    aliases = json.loads((pack_dir / "aliases.json").read_text(encoding="utf-8"))["aliases"]
    alias_pairs = {(item["text"], item["label"]) for item in aliases}
    build_metadata = json.loads((pack_dir / "build.json").read_text(encoding="utf-8"))
    alias_analysis = json.loads(
        (pack_dir / "alias-analysis.json").read_text(encoding="utf-8")
    )

    assert ("Exclusive", "organization") not in alias_pairs
    assert ("NEW", "organization") not in alias_pairs
    assert ("AI", "organization") not in alias_pairs
    assert ("World Bank", "organization") in alias_pairs
    assert build_metadata["exact_common_english_alias_removed_alias_count"] >= 1
    assert build_metadata["exact_common_english_alias_source_path"] == str(
        word_list_path
    )
    assert alias_analysis["exact_common_english_alias_removed_alias_count"] >= 1
    assert alias_analysis["exact_common_english_alias_source_path"] == str(word_list_path)


def test_generate_pack_source_can_use_callable_general_retained_alias_reviewer(
    tmp_path: Path,
) -> None:
    bundle_dir = create_generic_audit_general_generation_bundle(tmp_path / "bundle")
    calls: list[str] = []
    fail_on_call = False

    def reviewer(alias: dict[str, object]) -> dict[str, str]:
        if fail_on_call:
            raise AssertionError(f"unexpected cache miss for {alias['text']}")
        calls.append(str(alias["text"]))
        if alias["text"] in {"Cars", "Four Ways"}:
            return {
                "decision": "drop",
                "reason_code": "generic_retained_alias_from_ai_review",
                "reason": "too generic for general-en runtime",
            }
        return {
            "decision": "keep",
            "reason_code": "specific_retained_alias",
            "reason": "specific enough to keep",
        }

    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
        general_retained_alias_reviewer=reviewer,
    )

    build_metadata = json.loads(
        (Path(generated.pack_dir) / "build.json").read_text(encoding="utf-8")
    )
    review_cache_path = Path(build_metadata["retained_alias_audit_review_cache_path"])

    assert build_metadata["retained_alias_audit_mode"] == "callable"
    assert build_metadata["retained_alias_audit_invoked_review_count"] == build_metadata[
        "retained_alias_audit_scanned_alias_count"
    ]
    assert build_metadata["retained_alias_audit_cached_review_count"] == 0
    assert review_cache_path.exists()
    assert len(calls) == build_metadata["retained_alias_audit_scanned_alias_count"]

    fail_on_call = True

    regenerated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
        general_retained_alias_reviewer=reviewer,
    )

    refreshed_build_metadata = json.loads(
        (Path(regenerated.pack_dir) / "build.json").read_text(encoding="utf-8")
    )

    assert refreshed_build_metadata["retained_alias_audit_mode"] == "callable"
    assert refreshed_build_metadata["retained_alias_audit_invoked_review_count"] == 0
    assert refreshed_build_metadata["retained_alias_audit_cached_review_count"] == (
        refreshed_build_metadata["retained_alias_audit_scanned_alias_count"]
    )
    assert refreshed_build_metadata["retained_alias_audit_review_cache_path"] == str(
        review_cache_path
    )


def test_entity_alias_candidates_do_not_compact_slash_placeholders() -> None:
    candidates = _iter_entity_alias_candidates(
        "Issuer Alpha Holdings",
        {"aliases": ["N/A"]},
        "organization",
        pack_domain="finance",
    )

    assert ("NA", True) not in candidates


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

    assert ("Alden", "person") not in alias_pairs
    assert ("Third", "organization") not in alias_pairs
    assert ("Vale", "location") not in alias_pairs
    assert ("Beacon", "organization") in alias_pairs
    assert ("Alden Voss", "person") in alias_pairs
    assert ("Harborview", "location") in alias_pairs
    assert ("Harborview", "organization") not in alias_pairs
    assert ("Meridia", "organization") in alias_pairs
    assert ("Europe", "location") in alias_pairs
    assert ("Europe", "organization") not in alias_pairs
    assert ("Real Estate", "organization") in alias_pairs
    assert ("Letter", "location") not in alias_pairs
    assert ("This", "location") not in alias_pairs
    assert ("March", "location") not in alias_pairs
    assert ("Stock", "location") not in alias_pairs


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
    assert all(item["source_domain"] == "general" for item in aliases)
    assert all("score" in item for item in aliases)
    assert all("alias_score" in item for item in aliases)
    assert all("source_priority" in item for item in aliases)
    assert all("popularity_weight" in item for item in aliases)

    build_metadata = json.loads((pack_dir / "build.json").read_text(encoding="utf-8"))
    assert build_metadata["analysis_backend"] == "pre_resolved_bundle"
    assert build_metadata["matcher"]["algorithm"] == "token_trie_v1"
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


def test_generate_pack_source_keeps_generated_org_initialisms(tmp_path: Path) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path)

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    aliases = json.loads(
        (Path(result.pack_dir) / "aliases.json").read_text(encoding="utf-8")
    )["aliases"]
    initialisms = {
        (item["text"], item["label"], item["canonical_text"], item["runtime_tier"])
        for item in aliases
    }

    assert (
        "IEA",
        "organization",
        "International Energy Agency",
        "runtime_exact_acronym_high_precision",
    ) in initialisms


def test_generate_pack_source_prefers_geopolitical_location_acronyms_over_two_letter_org_noise(
    tmp_path: Path,
) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path)

    result = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )

    aliases = json.loads(
        (Path(result.pack_dir) / "aliases.json").read_text(encoding="utf-8")
    )["aliases"]
    alias_entries = {
        (
            item["text"],
            item["label"],
            item["canonical_text"],
            item["runtime_tier"],
        )
        for item in aliases
    }
    alias_text_labels = {(item["text"], item["label"]) for item in aliases}

    assert (
        "US",
        "location",
        "United States",
        "runtime_exact_geopolitical_high_precision",
    ) in alias_entries
    assert (
        "UK",
        "location",
        "United Kingdom",
        "runtime_exact_geopolitical_high_precision",
    ) in alias_entries
    assert (
        "EU",
        "location",
        "European Union",
        "runtime_exact_geopolitical_high_precision",
    ) in alias_entries
    assert ("US", "organization") not in alias_text_labels
    assert ("UK", "organization") not in alias_text_labels
    assert ("EU", "organization") not in alias_text_labels
    assert ("CI", "organization") not in alias_text_labels


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
