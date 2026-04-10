from pathlib import Path

from ades import build_registry, generate_pack_source, pull_pack, tag
from tests.pack_generation_helpers import create_finance_generation_bundle


def test_public_api_can_generate_build_install_and_tag_generated_pack(
    tmp_path: Path,
) -> None:
    bundle_dir = create_finance_generation_bundle(tmp_path / "bundle")

    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    registry = build_registry(
        [generated.pack_dir],
        output_dir=tmp_path / "registry",
    )

    install_root = tmp_path / "install"
    result = pull_pack(
        "finance-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )
    response = tag(
        "Apple said AAPL traded on NASDAQ after USD 12.5 guidance.",
        pack="finance-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert result.installed == ["finance-en"]
    assert ("Apple", "organization") in entities
    assert ("AAPL", "ticker") in entities
    assert ("NASDAQ", "exchange") in entities
    assert ("USD 12.5", "currency_amount") in entities
