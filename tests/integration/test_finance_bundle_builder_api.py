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
        sec_submissions_path=snapshots["sec_submissions"],
        sec_companyfacts_path=snapshots["sec_companyfacts"],
        symbol_directory_path=snapshots["symbol_directory"],
        other_listed_path=snapshots["other_listed"],
        finance_people_path=snapshots["finance_people"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    assert Path(bundle.sources_lock_path).exists()
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
        "Jane Doe said Issuer Alpha and TICKA traded on EXCHX after USD 12.5 guidance and $TICKB rose.",
        pack="finance-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("Issuer Alpha", "organization") in entities
    assert ("Jane Doe", "person") in entities
    assert ("TICKA", "ticker") in entities
    assert ("EXCHX", "exchange") in entities
    assert ("USD 12.5", "currency_amount") in entities
    assert ("$TICKB", "ticker") in entities
