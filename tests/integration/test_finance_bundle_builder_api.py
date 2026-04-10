from pathlib import Path

from ades import (
    build_finance_source_bundle,
    build_registry,
    generate_pack_source,
    pull_pack,
    tag,
)
from tests.finance_bundle_helpers import create_finance_raw_snapshots


def test_public_api_can_build_finance_bundle_then_generate_install_and_tag(
    tmp_path: Path,
) -> None:
    snapshots = create_finance_raw_snapshots(tmp_path / "snapshots")

    bundle = build_finance_source_bundle(
        sec_companies_path=snapshots["sec_companies"],
        symbol_directory_path=snapshots["symbol_directory"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    generated = generate_pack_source(
        bundle.bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    registry = build_registry([generated.pack_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    pull_pack(
        "finance-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )
    response = tag(
        "Apple said AAPL traded on NASDAQ after USD 12.5 guidance and $MSFT rose.",
        pack="finance-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("Apple", "organization") in entities
    assert ("AAPL", "ticker") in entities
    assert ("NASDAQ", "exchange") in entities
    assert ("USD 12.5", "currency_amount") in entities
    assert ("$MSFT", "ticker") in entities
