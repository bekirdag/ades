import json
from pathlib import Path

from ades.packs.matcher_repair import ensure_pack_matcher_artifacts
from ades.packs.manifest import PackManifest
from ades.packs.registry import PackRegistry
from ades.packs.runtime import clear_pack_runtime_cache, load_pack_runtime
from tests.pack_registry_helpers import create_pack_source


def test_ensure_pack_matcher_artifacts_backfills_manifest_and_files(tmp_path: Path) -> None:
    pack_dir = create_pack_source(
        tmp_path,
        pack_id="general-en",
        domain="general",
        aliases=(("Alpha Org", "organization"), ("New York", "location")),
    )
    manifest = PackManifest.load(pack_dir / "manifest.json")

    repaired_manifest, repaired = ensure_pack_matcher_artifacts(pack_dir, manifest)

    assert repaired is True
    assert repaired_manifest.matcher is not None
    assert repaired_manifest.matcher.entry_count == 2
    assert (pack_dir / repaired_manifest.matcher.artifact_path).exists()
    assert (pack_dir / repaired_manifest.matcher.entries_path).exists()
    stored_manifest = json.loads((pack_dir / "manifest.json").read_text(encoding="utf-8"))
    assert stored_manifest["matcher"]["artifact_path"] == "matcher/token-trie.bin"
    assert stored_manifest["matcher"]["entries_path"] == "matcher/entries.bin"


def test_ensure_pack_matcher_artifacts_rebuilds_from_metadata_store_when_manifest_is_stale(
    tmp_path: Path,
) -> None:
    storage_root = tmp_path / "storage"
    pack_dir = create_pack_source(
        storage_root / "packs",
        pack_id="general-en",
        domain="general",
        aliases=(("Alpha Org", "organization"), ("New York", "location")),
    )
    registry = PackRegistry(storage_root)
    assert registry.sync_pack_from_disk("general-en") is True

    # Simulate a stale placeholder-style matcher and alias payload on disk while the DB holds
    # the authoritative alias corpus.
    (pack_dir / "aliases.json").write_text(
        json.dumps({"aliases": [{"text": "Stub Alias", "label": "organization"}]}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    matcher_dir = pack_dir / "matcher"
    matcher_dir.mkdir(exist_ok=True)
    (matcher_dir / "token-trie.bin").write_bytes(b"stale")
    (matcher_dir / "entries.bin").write_bytes(b"stale")
    manifest_payload = json.loads((pack_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest_payload["matcher"] = {
        "schema_version": 1,
        "algorithm": "token_trie_v1",
        "normalization": "normalize_lookup_text:v1",
        "artifact_path": "matcher/token-trie.bin",
        "entries_path": "matcher/entries.bin",
        "entry_count": 1,
        "state_count": 1,
        "max_alias_length": 9,
        "artifact_sha256": "stale",
        "entries_sha256": "stale",
    }
    (pack_dir / "manifest.json").write_text(
        json.dumps(manifest_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    manifest = PackManifest.load(pack_dir / "manifest.json")
    repaired_manifest, repaired = ensure_pack_matcher_artifacts(
        pack_dir,
        manifest,
        metadata_store=registry.store,
    )

    assert repaired is True
    assert repaired_manifest.matcher is not None
    assert repaired_manifest.matcher.entry_count == 2


def test_load_pack_runtime_repairs_missing_matcher_metadata(tmp_path: Path) -> None:
    create_pack_source(
        tmp_path / "packs",
        pack_id="general-en",
        domain="general",
        aliases=(("Alpha Org", "organization"), ("New York", "location")),
        rules=(("email", r"[A-Za-z]+@[A-Za-z]+\\.com"),),
    )
    clear_pack_runtime_cache()
    registry = PackRegistry(tmp_path)

    runtime = load_pack_runtime(tmp_path, "general-en", registry=registry)

    assert runtime is not None
    assert len(runtime.matchers) == 1
    installed_pack = registry.get_pack("general-en")
    assert installed_pack is not None
    assert installed_pack.matcher is not None
    assert installed_pack.matcher.entry_count == 2


def test_load_pack_runtime_uses_existing_matcher_files_without_strict_repair(
    monkeypatch, tmp_path: Path
) -> None:
    pack_dir = create_pack_source(
        tmp_path / "packs",
        pack_id="general-en",
        domain="general",
        aliases=(("Alpha Org", "organization"), ("New York", "location")),
    )
    clear_pack_runtime_cache()
    registry = PackRegistry(tmp_path)
    assert registry.sync_pack_from_disk("general-en") is True

    manifest_payload = json.loads((pack_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest_payload["matcher"]["entry_count"] = 1
    (pack_dir / "manifest.json").write_text(
        json.dumps(manifest_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    def _unexpected_repair(*_args, **_kwargs):
        raise AssertionError("runtime load should not trigger strict matcher repair")

    monkeypatch.setattr("ades.packs.runtime.ensure_pack_matcher_artifacts", _unexpected_repair)

    runtime = load_pack_runtime(tmp_path, "general-en", registry=registry)

    assert runtime is not None
    assert len(runtime.matchers) == 1
