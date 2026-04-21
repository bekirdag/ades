from pathlib import Path

from ades import build_registry, tag
from ades.packs.installer import PackInstaller
from tests.pack_registry_helpers import create_finance_registry_sources, create_pack_source


def test_install_falls_back_to_bundled_registry_for_missing_base_dependency_manifests(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fallback_general_dir, fallback_finance_dir = create_finance_registry_sources(
        tmp_path / "fallback-sources",
    )
    fallback_registry = build_registry(
        [fallback_general_dir, fallback_finance_dir],
        output_dir=tmp_path / "fallback-registry",
    )
    monkeypatch.setattr("ades.packs.installer.default_registry_url", lambda: fallback_registry.index_url)

    remote_general_dir, remote_finance_dir = create_finance_registry_sources(
        tmp_path / "remote-sources",
        general_version="0.2.9",
        finance_version="0.2.0",
    )
    remote_finance_ar_dir = create_pack_source(
        tmp_path / "remote-sources",
        pack_id="finance-ar-en",
        domain="finance",
        version="0.2.0",
        tier="domain",
        dependencies=("general-en", "finance-en"),
        labels=("organization", "ticker"),
        aliases=(
            ("Macro Securities", "organization"),
            ("ALUA", "ticker"),
        ),
    )
    remote_registry = build_registry(
        [remote_general_dir, remote_finance_dir, remote_finance_ar_dir],
        output_dir=tmp_path / "remote-registry",
    )
    remote_registry_root = Path(remote_registry.output_dir)
    (remote_registry_root / "packs" / "general-en" / "manifest.json").unlink()
    (remote_registry_root / "packs" / "finance-en" / "manifest.json").unlink()

    install_root = tmp_path / "install"
    installer = PackInstaller(install_root, registry_url=remote_registry.index_url)

    result = installer.install("finance-ar-en")

    assert set(result.installed) == {"finance-ar-en", "finance-en", "general-en"}
    assert result.skipped == ["general-en"]
    general_pack = installer.registry.get_pack("general-en")
    finance_pack = installer.registry.get_pack("finance-en")
    argentina_pack = installer.registry.get_pack("finance-ar-en")
    assert general_pack is not None
    assert finance_pack is not None
    assert argentina_pack is not None
    assert general_pack.version == "0.1.0"
    assert finance_pack.version == "0.1.0"
    assert argentina_pack.version == "0.2.0"

    entities = {
        (entity.text, entity.label)
        for entity in tag(
            "Email team@example.com about ALUA and TICKA via Macro Securities.",
            pack="finance-ar-en",
            storage_root=install_root,
        ).entities
    }
    assert ("team@example.com", "email_address") in entities
    assert ("ALUA", "ticker") in entities
    assert ("TICKA", "ticker") in entities
    assert ("Macro Securities", "organization") in entities
