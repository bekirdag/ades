import json
from pathlib import Path

import pytest

from ades.packs.generation import generate_pack_source
from tests.pack_generation_helpers import create_finance_generation_bundle


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
    assert result.ambiguous_alias_count == 0
    assert any("sec-companyfacts" in warning for warning in result.warnings)

    pack_dir = Path(result.pack_dir)
    manifest = json.loads((pack_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["pack_id"] == "finance-en"
    assert manifest["rules"] == ["rules.json"]
    assert manifest["labels"] == ["labels.json"]

    labels = json.loads((pack_dir / "labels.json").read_text(encoding="utf-8"))
    assert labels == ["currency_amount", "exchange", "organization", "ticker"]

    aliases = json.loads((pack_dir / "aliases.json").read_text(encoding="utf-8"))["aliases"]
    assert {"text": "Apple", "label": "organization"} in aliases
    assert {"text": "AAPL", "label": "ticker"} in aliases
    assert {"text": "NASDAQ", "label": "exchange"} in aliases
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

    assert result.ambiguous_alias_count == 1
    assert not any(item["text"].casefold() == "nasdaq" for item in aliases)


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
        if row.get("entity_id") == "issuer:nasdaq-inc":
            row["blocked_aliases"] = ["NASDAQ"]
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

    assert result.ambiguous_alias_count == 0
    assert {"text": "NASDAQ", "label": "exchange"} in aliases


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
