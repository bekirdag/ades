import json
from pathlib import Path

from ades.config import Settings
from ades.service.models import EntityLink, EntityMatch, TagResponse
from ades.vector import graph_builder as graph_builder_module
from ades.vector.context_engine import _apply_surface_conflict_filters, apply_graph_context


def _bundle_dir(root: Path, *, pack_id: str = "general-en") -> Path:
    bundle_dir = root / pack_id
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True)
    (bundle_dir / "bundle.json").write_text(
        json.dumps(
            {
                "pack_id": pack_id,
                "entities_path": "normalized/entities.jsonl",
            }
        ),
        encoding="utf-8",
    )
    (normalized_dir / "entities.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "entity_id": "wikidata:Q1",
                        "canonical_text": "OpenAI",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 100,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q2",
                        "canonical_text": "Microsoft",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 95,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q3",
                        "canonical_text": "the Second",
                        "entity_type": "person",
                        "source_name": "wikidata-general-entities",
                        "popularity": 2,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q4",
                        "canonical_text": "AI Consortium",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 80,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q5",
                        "canonical_text": "Applied AI Guild",
                        "entity_type": "organization",
                        "source_name": "wikidata-general-entities",
                        "popularity": 7,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q6",
                        "canonical_text": "Noise Alpha",
                        "entity_type": "person",
                        "source_name": "wikidata-general-entities",
                        "popularity": 3,
                    }
                ),
                json.dumps(
                    {
                        "entity_id": "wikidata:Q7",
                        "canonical_text": "Noise Beta",
                        "entity_type": "person",
                        "source_name": "wikidata-general-entities",
                        "popularity": 3,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return bundle_dir


def _build_graph_artifact(tmp_path: Path) -> Path:
    bundle_dir = _bundle_dir(tmp_path)
    truthy_lines = [
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q100> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P463> <http://www.wikidata.org/entity/Q300> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q4> .",
        "<http://www.wikidata.org/entity/Q1> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q5> .",
        "<http://www.wikidata.org/entity/Q2> <http://www.wikidata.org/prop/direct/P361> <http://www.wikidata.org/entity/Q5> .",
    ]
    noisy_predicates = [
        "P17",
        "P131",
        "P495",
        "P361",
        "P527",
        "P463",
        "P102",
        "P171",
        "P641",
        "P136",
        "P921",
        "P31",
        "P17",
        "P131",
        "P495",
        "P361",
        "P527",
        "P463",
    ]
    truthy_lines.extend(
        f"<http://www.wikidata.org/entity/Q3> <http://www.wikidata.org/prop/direct/{predicate}> "
        f"<http://www.wikidata.org/entity/Q9{index:02d}> ."
        for index, predicate in enumerate(noisy_predicates, start=1)
    )
    truthy_path = tmp_path / "truthy.nt"
    truthy_path.write_text("\n".join(truthy_lines) + "\n", encoding="utf-8")
    response = graph_builder_module.build_qid_graph_store(
        [bundle_dir],
        truthy_path=truthy_path,
        output_dir=tmp_path / "artifact",
        allowed_predicates=[
            "P31",
            "P463",
            "P17",
            "P131",
            "P495",
            "P361",
            "P527",
            "P102",
            "P171",
            "P641",
            "P136",
            "P921",
        ],
    )
    return Path(response.artifact_path)


def _response_with_three_linked_entities() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="OpenAI",
                label="organization",
                start=0,
                end=6,
                confidence=0.46,
                relevance=0.46,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="OpenAI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Microsoft",
                label="organization",
                start=10,
                end=19,
                confidence=0.44,
                relevance=0.44,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Microsoft",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="the Second",
                label="person",
                start=30,
                end=40,
                confidence=0.24,
                relevance=0.24,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="the Second",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_low_coherence_linked_entities() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="OpenAI",
                label="organization",
                start=0,
                end=6,
                confidence=0.46,
                relevance=0.46,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="OpenAI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Microsoft",
                label="organization",
                start=10,
                end=19,
                confidence=0.44,
                relevance=0.44,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Microsoft",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="the Second",
                label="person",
                start=30,
                end=40,
                confidence=0.24,
                relevance=0.24,
                link=EntityLink(
                    entity_id="wikidata:Q3",
                    canonical_text="the Second",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Noise Alpha",
                label="person",
                start=50,
                end=61,
                confidence=0.41,
                relevance=0.41,
                link=EntityLink(
                    entity_id="wikidata:Q6",
                    canonical_text="Noise Alpha",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Noise Beta",
                label="person",
                start=70,
                end=80,
                confidence=0.4,
                relevance=0.4,
                link=EntityLink(
                    entity_id="wikidata:Q7",
                    canonical_text="Noise Beta",
                    provider="lookup.alias.exact",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def _response_with_lowercase_geonames_noise() -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="general-en",
        pack_version="0.2.0",
        language="en",
        content_type="text/plain",
        entities=[
            EntityMatch(
                text="OpenAI",
                label="organization",
                start=0,
                end=6,
                confidence=0.46,
                relevance=0.46,
                mention_count=2,
                link=EntityLink(
                    entity_id="wikidata:Q1",
                    canonical_text="OpenAI",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="Microsoft",
                label="organization",
                start=10,
                end=19,
                confidence=0.44,
                relevance=0.44,
                link=EntityLink(
                    entity_id="wikidata:Q2",
                    canonical_text="Microsoft",
                    provider="lookup.alias.exact",
                ),
            ),
            EntityMatch(
                text="number two",
                label="location",
                start=30,
                end=40,
                confidence=0.5,
                relevance=0.5,
                mention_count=1,
                link=EntityLink(
                    entity_id="geonames:4955258",
                    canonical_text="Town of Westminster",
                    provider="lookup.alias.geonames",
                ),
            ),
        ],
        topics=[],
        warnings=[],
        timing_ms=5,
    )


def test_apply_graph_context_suppresses_isolated_low_fit_seed(tmp_path: Path) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = _response_with_three_linked_entities()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=True,
        refinement_depth="deep",
    )

    assert [entity.link.entity_id for entity in refined.entities if entity.link is not None] == [
        "wikidata:Q1",
        "wikidata:Q2",
    ]
    assert refined.refinement_applied is True
    assert refined.refinement_strategy == "qid_graph_context_v1"
    assert refined.graph_support is not None
    assert refined.graph_support.applied is True
    assert refined.graph_support.suppressed_entity_ids == ["wikidata:Q3"]
    assert refined.graph_support.suppressed_entity_count == 1
    assert [item.entity_id for item in refined.related_entities] == [
        "wikidata:Q4",
        "wikidata:Q5",
    ]
    assert refined.refinement_debug is not None
    assert any(
        item["entity_id"] == "wikidata:Q3" and item["action"] == "suppress"
        for item in refined.refinement_debug["seed_decisions"]
    )


def test_apply_graph_context_support_only_mode_keeps_entities(tmp_path: Path) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = _response_with_three_linked_entities()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=False,
    )

    assert len(refined.entities) == 3
    assert refined.refinement_applied is False
    assert refined.graph_support is not None
    assert refined.graph_support.applied is True
    assert refined.graph_support.suppressed_entity_count == 0
    assert [item.entity_id for item in refined.related_entities] == [
        "wikidata:Q4",
        "wikidata:Q5",
    ]
    assert refined.refinement_debug is not None
    assert refined.refinement_debug["mode"] == "support_only"


def test_apply_graph_context_support_only_mode_skips_related_entities_for_low_coherence_document(
    tmp_path: Path,
) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = _response_with_low_coherence_linked_entities()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
        graph_context_min_supporting_seeds=2,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=False,
    )

    assert len(refined.entities) == 5
    assert refined.graph_support is not None
    assert refined.graph_support.applied is True
    assert refined.graph_support.coherence_score is not None
    assert refined.graph_support.coherence_score < 0.12
    assert refined.related_entities == []
    assert refined.refinement_debug is not None
    assert refined.refinement_debug["related_debug"]["reason"] == "low_document_coherence"


def test_surface_conflict_filter_suppresses_shorter_conflicting_alias() -> None:
    entities = [
        EntityMatch(
            text="Sir Keir Starmer",
            label="person",
            start=0,
            end=17,
            confidence=0.72,
            relevance=0.72,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q1",
                canonical_text="Keir Starmer",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Sir Keir",
            label="person",
            start=25,
            end=33,
            confidence=0.61,
            relevance=0.61,
            mention_count=7,
            link=EntityLink(
                entity_id="wikidata:Q2",
                canonical_text="David Keir",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Sir Keir Starmer"]
    assert suppressed_ids == ["wikidata:Q2"]
    assert debug["surface_conflict_suppressed"] == ["wikidata:Q2"]


def test_surface_conflict_filter_suppresses_low_value_email_entity() -> None:
    entities = [
        EntityMatch(
            text="life@newsweek.com",
            label="email_address",
            start=0,
            end=17,
            confidence=0.85,
            relevance=0.61,
            mention_count=1,
            link=EntityLink(
                entity_id="ades:rule_regex:general-en:email_address:life-newsweek-com",
                canonical_text="life@newsweek.com",
                provider="rule.regex",
            ),
        ),
        EntityMatch(
            text="Newsweek",
            label="organization",
            start=20,
            end=28,
            confidence=0.72,
            relevance=0.72,
            mention_count=1,
            link=EntityLink(
                entity_id="wikidata:Q18887",
                canonical_text="Newsweek",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Newsweek"]
    assert suppressed_ids == ["ades:rule_regex:general-en:email_address:life-newsweek-com"]
    assert debug["email_address_suppressed"] == [
        "ades:rule_regex:general-en:email_address:life-newsweek-com"
    ]


def test_surface_conflict_filter_suppresses_generic_lowercase_location_phrase() -> None:
    entities = [
        EntityMatch(
            text="high street",
            label="location",
            start=0,
            end=11,
            confidence=0.75,
            relevance=0.64,
            mention_count=1,
            link=EntityLink(
                entity_id="wikidata:Q105716706",
                canonical_text="High Street",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Boots",
            label="organization",
            start=15,
            end=20,
            confidence=0.72,
            relevance=0.72,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q10134",
                canonical_text="Boots",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Boots"]
    assert suppressed_ids == ["wikidata:Q105716706"]
    assert debug["generic_lowercase_location_suppressed"] == ["wikidata:Q105716706"]


def test_surface_conflict_filter_suppresses_numeric_fragment_location() -> None:
    entities = [
        EntityMatch(
            text="43rd",
            label="location",
            start=0,
            end=4,
            confidence=0.71,
            relevance=0.58,
            mention_count=1,
            link=EntityLink(
                entity_id="wikidata:Q2816829",
                canonical_text="43rd",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Alberta",
            label="location",
            start=10,
            end=17,
            confidence=0.8,
            relevance=0.8,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q1951",
                canonical_text="Alberta",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Alberta"]
    assert suppressed_ids == ["wikidata:Q2816829"]
    assert debug["numeric_fragment_suppressed"] == ["wikidata:Q2816829"]


def test_surface_conflict_filter_suppresses_titlecase_common_alias() -> None:
    entities = [
        EntityMatch(
            text="Good luck",
            label="location",
            start=0,
            end=9,
            confidence=0.75,
            relevance=0.54,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "Good luck",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q15221860",
                canonical_text="Good luck",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="AKC",
            label="organization",
            start=20,
            end=23,
            confidence=0.67,
            relevance=0.52,
            mention_count=1,
            link=EntityLink(
                entity_id="ades:heuristic_explicit_definition_acronym:general-en:organization:american-kennel-club",
                canonical_text="American Kennel Club",
                provider="heuristic.explicit_definition_acronym",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["AKC"]
    assert suppressed_ids == ["wikidata:Q15221860"]
    assert debug["titlecase_common_alias_suppressed"] == ["wikidata:Q15221860"]


def test_surface_conflict_filter_suppresses_weak_led_exact_alias() -> None:
    entities = [
        EntityMatch(
            text="For Britain",
            label="organization",
            start=0,
            end=11,
            confidence=0.48,
            relevance=0.48,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "For Britain",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q42530925",
                canonical_text="For Britain",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Britain",
            label="location",
            start=15,
            end=22,
            confidence=0.82,
            relevance=0.82,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q145",
                canonical_text="United Kingdom",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Britain"]
    assert suppressed_ids == ["wikidata:Q42530925"]
    assert debug["weak_led_exact_alias_suppressed"] == ["wikidata:Q42530925"]


def test_surface_conflict_filter_suppresses_late_tail_exact_alias() -> None:
    entities = [
        EntityMatch(
            text="China",
            label="location",
            start=80,
            end=85,
            confidence=0.82,
            relevance=0.82,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q148",
                canonical_text="China",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Evening Standard",
            label="organization",
            start=930,
            end=946,
            confidence=0.45,
            relevance=0.45,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "Evening Standard",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q5419995",
                canonical_text="Evening Standard",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
        document_extent=1000,
    )

    assert [entity.text for entity in filtered] == ["China"]
    assert suppressed_ids == ["wikidata:Q5419995"]
    assert debug["late_tail_exact_alias_suppressed"] == ["wikidata:Q5419995"]


def test_surface_conflict_filter_suppresses_acronym_expansion_fragment_alias() -> None:
    entities = [
        EntityMatch(
            text="Task Force",
            label="organization",
            start=0,
            end=10,
            confidence=0.47,
            relevance=0.47,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "Task Force",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q91945108",
                canonical_text="Task Force",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="JMTF",
            label="organization",
            start=30,
            end=34,
            confidence=0.62,
            relevance=0.62,
            mention_count=1,
            provenance={
                "match_kind": "alias",
                "match_path": "heuristic.explicit_definition_acronym",
                "match_source": "joint military task force",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="ades:heuristic_explicit_definition_acronym:general-en:organization:joint-military-task-force",
                canonical_text="Joint Military Task Force",
                provider="heuristic.explicit_definition_acronym",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["JMTF"]
    assert suppressed_ids == [
        "wikidata:Q91945108",
    ]
    assert debug["acronym_fragment_suppressed"] == ["wikidata:Q91945108"]


def test_surface_conflict_filter_suppresses_article_led_canonical_mismatch_alias() -> None:
    entities = [
        EntityMatch(
            text="the Commonwealth",
            label="organization",
            start=0,
            end=16,
            confidence=0.45,
            relevance=0.45,
            mention_count=2,
            provenance={
                "match_kind": "alias",
                "match_path": "lookup.alias.exact",
                "match_source": "the Commonwealth",
                "source_pack": "general-en",
                "source_domain": "general",
            },
            link=EntityLink(
                entity_id="wikidata:Q7785",
                canonical_text="Commonwealth of Nations",
                provider="lookup.alias.exact",
            ),
        ),
        EntityMatch(
            text="Australia",
            label="location",
            start=20,
            end=29,
            confidence=0.81,
            relevance=0.81,
            mention_count=2,
            link=EntityLink(
                entity_id="wikidata:Q408",
                canonical_text="Australia",
                provider="lookup.alias.exact",
            ),
        ),
    ]

    filtered, suppressed_ids, debug = _apply_surface_conflict_filters(
        entities,
        decision_by_entity_id={},
    )

    assert [entity.text for entity in filtered] == ["Australia"]
    assert suppressed_ids == ["wikidata:Q7785"]
    assert debug["weak_led_exact_alias_suppressed"] == ["wikidata:Q7785"]


def test_apply_graph_context_suppresses_lowercase_geonames_noise(tmp_path: Path) -> None:
    artifact_path = _build_graph_artifact(tmp_path)
    response = _response_with_lowercase_geonames_noise()
    settings = Settings(
        graph_context_enabled=True,
        graph_context_artifact_path=artifact_path,
        graph_context_seed_neighbor_limit=24,
        graph_context_candidate_limit=64,
    )

    refined = apply_graph_context(
        response,
        settings=settings,
        include_related_entities=True,
        include_graph_support=True,
        refine_links=True,
    )

    assert [entity.text for entity in refined.entities] == ["OpenAI", "Microsoft"]
    assert refined.graph_support is not None
    assert "geonames:4955258" in refined.graph_support.suppressed_entity_ids
    assert refined.refinement_debug is not None
    assert refined.refinement_debug["post_filter_debug"]["lowercase_geonames_suppressed"] == [
        "geonames:4955258"
    ]
