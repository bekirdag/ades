from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path
import sys


def _load_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "audit_saved_unseen_vector_neighbors.py"
    spec = spec_from_file_location("audit_saved_unseen_vector_neighbors", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_load_summary_source_accepts_inline_case_payloads(tmp_path: Path) -> None:
    module = _load_module()
    news_path = tmp_path / "article.txt"
    news_path.write_text("Current unseen article text.", encoding="utf-8")
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "slug": "fresh-case",
                        "title": "Fresh Case",
                        "source": "bbc",
                        "domain_hint": "politics",
                        "country_hint": "uk",
                        "news_path": str(news_path),
                        "plain": {
                            "timing_ms": 12,
                            "entity_count": 1,
                            "entities": [
                                {
                                    "text": "United Kingdom",
                                    "label": "location",
                                    "entity_id": "wikidata:Q145",
                                    "canonical_text": "United Kingdom",
                                }
                            ],
                        },
                        "hinted": {
                            "timing_ms": 9,
                            "entity_count": 2,
                            "entities": [
                                {
                                    "text": "United Kingdom",
                                    "label": "location",
                                    "entity_id": "wikidata:Q145",
                                    "canonical_text": "United Kingdom",
                                },
                                {
                                    "text": "Keir Starmer",
                                    "label": "person",
                                    "entity_id": "wikidata:Q6384808",
                                    "canonical_text": "Keir Starmer",
                                },
                            ],
                            "related_entities": [
                                {
                                    "entity_id": "wikidata:Q11184",
                                    "canonical_text": "Prime Minister of the United Kingdom",
                                    "label": "position",
                                }
                            ],
                            "graph_support": {
                                "requested": True,
                                "applied": True,
                                "warnings": [],
                            },
                        },
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    loaded_summary, cases = module._load_summary_source(summary_path)

    assert loaded_summary.compatible is True
    assert loaded_summary.source_kind == "cases"
    assert loaded_summary.loaded_case_count == 1
    assert cases[0].response_file_path == news_path.resolve()
    assert cases[0].plain.entity_count == 1
    assert cases[0].hinted.related_entity_count == 1
    assert [seed.entity_id for seed in cases[0].seed_entities] == [
        "wikidata:Q145",
        "wikidata:Q6384808",
    ]


def test_load_summary_source_accepts_rerun_response_payloads(tmp_path: Path) -> None:
    module = _load_module()
    response_path = tmp_path / "fresh-case.responses.json"
    response_path.write_text(
        json.dumps(
            {
                "case": {
                    "slug": "fresh-case",
                    "title": "Fresh Case",
                    "source": "bbc",
                    "pack": "general-en",
                    "domain_hint": "politics",
                    "country_hint": "uk",
                },
                "new_plain": {
                    "timing_ms": 12,
                    "entity_count": 1,
                    "entities": [
                        {
                            "text": "United Kingdom",
                            "label": "location",
                            "entity_id": "wikidata:Q145",
                            "canonical_text": "United Kingdom",
                        }
                    ],
                },
                "new_hinted": {
                    "timing_ms": 9,
                    "entity_count": 2,
                    "entities": [
                        {
                            "text": "United Kingdom",
                            "label": "location",
                            "entity_id": "wikidata:Q145",
                            "canonical_text": "United Kingdom",
                        },
                        {
                            "text": "Keir Starmer",
                            "label": "person",
                            "entity_id": "wikidata:Q6384808",
                            "canonical_text": "Keir Starmer",
                        },
                    ],
                    "related_entities": [
                        {
                            "entity_id": "wikidata:Q11184",
                            "canonical_text": "Prime Minister of the United Kingdom",
                            "label": "position",
                        }
                    ],
                    "graph_support": {
                        "requested": True,
                        "applied": True,
                        "warnings": [],
                    },
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "slug": "fresh-case",
                        "response_file_path": str(response_path),
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    loaded_summary, cases = module._load_summary_source(summary_path)

    assert loaded_summary.compatible is True
    assert loaded_summary.loaded_case_count == 1
    assert cases[0].response_file_path == response_path.resolve()
    assert cases[0].pack_id == "general-en"
    assert cases[0].plain.entity_count == 1
    assert cases[0].hinted.related_entity_count == 1
    assert [seed.entity_id for seed in cases[0].seed_entities] == [
        "wikidata:Q145",
        "wikidata:Q6384808",
    ]
