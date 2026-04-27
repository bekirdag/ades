from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "report_missing_seed_coverage.py"
)


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "report_missing_seed_coverage_script",
        SCRIPT_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_bundle(bundle_dir: Path, *, pack_id: str, rows: list[dict[str, object]]) -> None:
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True)
    (bundle_dir / "bundle.json").write_text(
        json.dumps({"pack_id": pack_id}),
        encoding="utf-8",
    )
    entities_path = normalized_dir / "entities.jsonl"
    with entities_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_expand_bundle_dirs_accepts_bundle_and_parent_dirs(tmp_path: Path) -> None:
    module = _load_module()
    bundle_a = tmp_path / "bundle-a"
    bundle_b = tmp_path / "group" / "bundle-b"
    _write_bundle(bundle_a, pack_id="pack-a", rows=[])
    _write_bundle(bundle_b, pack_id="pack-b", rows=[])

    result = module._expand_bundle_dirs([str(bundle_a), str(bundle_b.parent)])

    assert result == [bundle_a.resolve(), bundle_b.resolve()]


def test_bundle_presence_rows_matches_requested_entity_ids(tmp_path: Path) -> None:
    module = _load_module()
    bundle_dir = tmp_path / "bundle-a"
    _write_bundle(
        bundle_dir,
        pack_id="pack-a",
        rows=[
            {
                "entity_id": "wikidata:Q1",
                "canonical_text": "Entity One",
                "entity_type": "organization",
                "source_name": "bundle-a",
            },
            {
                "entity_id": "wikidata:Q2",
                "canonical_text": "Entity Two",
                "entity_type": "person",
                "source_name": "bundle-a",
            },
        ],
    )

    result = module._bundle_presence_rows(
        bundle_dir,
        target_entity_ids={"wikidata:Q1", "wikidata:Q3"},
    )

    assert sorted(result) == ["wikidata:Q1"]
    assert result["wikidata:Q1"]["canonical_text"] == "Entity One"
    assert result["wikidata:Q1"]["entity_type"] == "organization"
