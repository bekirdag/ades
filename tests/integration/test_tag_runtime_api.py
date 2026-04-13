from pathlib import Path

from ades import pull_pack, tag
from ades.packs.registry import PackRegistry
from ades.pipeline.hybrid import ProposalSpan
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from tests.pack_registry_helpers import create_pack_source


def test_public_api_tag_returns_metrics_pack_version_and_debug(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    response = tag(
        "TICKA traded on EXCHX after USD 12.5 guidance.",
        pack="finance-en",
        storage_root=tmp_path,
        debug=True,
    )

    assert response.schema_version == 2
    assert response.pack_version == "0.1.0"
    assert response.metrics is not None
    assert response.metrics.entity_count >= 3
    assert response.debug is not None


def test_public_api_tag_warns_when_input_is_truncated(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    large_text = ("plain token " * 120_000) + "TICKA traded on EXCHX."

    response = tag(
        large_text,
        pack="finance-en",
        storage_root=tmp_path,
    )

    assert "input_truncated_to_bytes:1000000" in response.warnings


def test_public_api_tag_can_use_hybrid_lane_with_monkeypatched_proposals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id="hybrid-en",
        domain="general",
        labels=("organization",),
        aliases=(("Entity Alpha Holdings", "organization"),),
    )
    PackRegistry(tmp_path).sync_pack_from_disk(pack_dir.name)

    def _proposal_spans(text: str, **_: object) -> list[ProposalSpan]:
        start = text.index("Entity Alpha")
        end = start + len("Entity Alpha")
        return [
            ProposalSpan(
                start=start,
                end=end,
                text=text[start:end],
                label="organization",
                confidence=0.81,
                model_name="patched-proposer",
                model_version="test",
            )
        ]

    monkeypatch.setattr("ades.pipeline.tagger.get_proposal_spans", _proposal_spans)

    response = tag(
        "Entity Alpha moved.",
        pack="hybrid-en",
        storage_root=tmp_path,
        hybrid=True,
    )

    assert len(response.entities) == 1
    assert response.entities[0].provenance is not None
    assert response.entities[0].provenance.match_kind == "proposal"
