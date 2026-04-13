import json
from pathlib import Path

from ades.packs.registry import PackRegistry
from ades.pipeline.hybrid import ProposalSpan
from ades.pipeline.tagger import tag_text
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from tests.pack_registry_helpers import create_pack_source


class _StubProposalProvider:
    def propose(self, text: str, *, allowed_labels: set[str]) -> list[ProposalSpan]:
        assert "organization" in allowed_labels
        start = text.index("Entity Alpha")
        end = start + len("Entity Alpha")
        return [
            ProposalSpan(
                start=start,
                end=end,
                text=text[start:end],
                label="organization",
                confidence=0.82,
                model_name="stub-proposer",
                model_version="test",
            )
        ]


def _install_hybrid_pack(storage_root: Path) -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id="hybrid-en",
        domain="general",
        labels=("organization",),
        aliases=(("Entity Alpha Holdings", "organization"),),
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def test_hybrid_lane_links_proposed_span_to_pack_candidate(tmp_path: Path) -> None:
    pack_id = _install_hybrid_pack(tmp_path)
    text = "Entity Alpha moved."

    deterministic = tag_text(
        text=text,
        pack=pack_id,
        content_type="text/plain",
        storage_root=tmp_path,
        hybrid=False,
    )
    hybrid = tag_text(
        text=text,
        pack=pack_id,
        content_type="text/plain",
        storage_root=tmp_path,
        hybrid=True,
        proposal_provider=_StubProposalProvider(),
    )

    assert deterministic.entities == []
    assert len(hybrid.entities) == 1
    assert hybrid.entities[0].text == "Entity Alpha"
    assert hybrid.entities[0].link is not None
    assert hybrid.entities[0].link.canonical_text == "Entity Alpha Holdings"
    assert hybrid.entities[0].provenance is not None
    assert hybrid.entities[0].provenance.match_kind == "proposal"
    assert hybrid.entities[0].provenance.lane == "proposal_linked"


def test_hybrid_context_prefers_pack_consistent_candidate(tmp_path: Path) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    general_dir = create_pack_source(
        layout.packs_dir,
        pack_id="general-en",
        domain="general",
        labels=("organization",),
    )
    finance_dir = create_pack_source(
        layout.packs_dir,
        pack_id="finance-context-en",
        domain="finance",
        dependencies=("general-en",),
        labels=("organization",),
    )

    (general_dir / "aliases.json").write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "Entity Alpha Group",
                        "label": "organization",
                        "canonical_text": "Entity Alpha Group",
                        "entity_id": "general:entity-alpha",
                        "score": 0.53,
                        "source_priority": 0.53,
                        "popularity_weight": 0.53,
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (finance_dir / "aliases.json").write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "Anchor Finance Holdings",
                        "label": "organization",
                        "canonical_text": "Anchor Finance Holdings",
                        "entity_id": "finance:anchor-finance",
                        "score": 0.8,
                        "source_priority": 0.8,
                        "popularity_weight": 0.8,
                    },
                    {
                        "text": "Entity Alpha Fund",
                        "label": "organization",
                        "canonical_text": "Entity Alpha Fund",
                        "entity_id": "finance:entity-alpha",
                        "score": 0.5,
                        "source_priority": 0.5,
                        "popularity_weight": 0.5,
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    registry = PackRegistry(tmp_path)
    registry.sync_pack_from_disk(general_dir.name)
    registry.sync_pack_from_disk(finance_dir.name)

    no_context = tag_text(
        text="Entity Alpha expanded.",
        pack="finance-context-en",
        content_type="text/plain",
        storage_root=tmp_path,
        hybrid=True,
        proposal_provider=_StubProposalProvider(),
    )
    finance_context = tag_text(
        text="Anchor Finance Holdings met Entity Alpha.",
        pack="finance-context-en",
        content_type="text/plain",
        storage_root=tmp_path,
        hybrid=True,
        proposal_provider=_StubProposalProvider(),
    )

    assert len(no_context.entities) == 1
    assert no_context.entities[0].link is not None
    assert no_context.entities[0].link.canonical_text == "Entity Alpha Group"

    assert len(finance_context.entities) == 2
    linked_entity = next(
        entity
        for entity in finance_context.entities
        if entity.text == "Entity Alpha"
    )
    assert linked_entity.link is not None
    assert linked_entity.link.canonical_text == "Entity Alpha Fund"
    assert linked_entity.provenance is not None
    assert linked_entity.provenance.source_pack == "finance-context-en"
