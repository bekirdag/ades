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
    assert result.included_entity_count == 3
    assert result.included_rule_count == 1
    assert result.dropped_record_count == 2
    assert result.dropped_alias_count >= 1
    assert result.ambiguous_alias_count == 0

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
