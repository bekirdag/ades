from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import tag_text


def test_tagger_uses_lookup_for_multi_token_general_aliases(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("general-en")

    response = tag_text(
        text="Tim Cook spoke about Apple.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Tim Cook", "person") in pairs
    assert ("Apple", "organization") in pairs
