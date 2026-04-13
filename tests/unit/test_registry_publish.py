import hashlib
import io
import json
import tarfile
from pathlib import Path

import zstandard

from ades.packs.publish import build_static_registry
from tests.pack_registry_helpers import create_pack_source


def test_build_static_registry_writes_consumable_manifest_and_artifact(tmp_path: Path) -> None:
    pack_dir = create_pack_source(
        tmp_path / "sources",
        pack_id="general-en",
        domain="general",
        aliases=(("Org Alpha", "organization"),),
        rules=(("url", r"https?://[^\s]+"),),
    )

    result = build_static_registry([pack_dir], output_dir=tmp_path / "registry")

    index_path = Path(result.index_path)
    manifest_path = index_path.parent / "packs" / "general-en" / "manifest.json"
    artifact_path = index_path.parent / "artifacts" / "general-en-0.1.0.tar.zst"
    assert result.index_url == index_path.resolve().as_uri()
    assert result.generated_at.endswith("Z")
    assert result.packs[0].artifact_path == str(artifact_path.resolve())

    index_payload = json.loads(index_path.read_text(encoding="utf-8"))
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifact_sha256 = hashlib.sha256(artifact_path.read_bytes()).hexdigest()

    assert index_payload["packs"]["general-en"]["manifest_url"] == "packs/general-en/manifest.json"
    assert index_payload["packs"]["general-en"]["description"] == "general-en pack for general tagging."
    assert index_payload["packs"]["general-en"]["tags"] == []
    assert manifest_payload["artifacts"][0]["url"] == "../../artifacts/general-en-0.1.0.tar.zst"
    assert manifest_payload["artifacts"][0]["sha256"] == artifact_sha256

    decompressor = zstandard.ZstdDecompressor()
    with decompressor.stream_reader(io.BytesIO(artifact_path.read_bytes())) as reader:
        with tarfile.open(fileobj=reader, mode="r|") as archive:
            names: list[str] = []
            packaged_manifest = None
            for member in archive:
                names.append(member.name)
                if member.name == "manifest.json":
                    packaged_manifest = json.loads(
                        archive.extractfile(member).read().decode("utf-8")
                    )

    assert names == ["aliases.json", "labels.json", "manifest.json", "rules.json"]
    assert packaged_manifest is not None
    assert packaged_manifest["artifacts"][0]["url"] == "../../artifacts/general-en-0.1.0.tar.zst"
    assert "sha256" not in packaged_manifest["artifacts"][0]


def test_build_static_registry_excludes_build_only_analysis_artifacts(tmp_path: Path) -> None:
    pack_dir = create_pack_source(
        tmp_path / "sources",
        pack_id="general-en",
        domain="general",
        aliases=(("Org Alpha", "organization"),),
    )
    (pack_dir / "analysis.sqlite").write_bytes(b"sqlite-build-only")
    (pack_dir / "alias-analysis.json").write_text(
        json.dumps({"summary": "build-only"}, indent=2) + "\n",
        encoding="utf-8",
    )
    (pack_dir / "build.json").write_text(
        json.dumps({"included_entity_count": 1}, indent=2) + "\n",
        encoding="utf-8",
    )

    result = build_static_registry([pack_dir], output_dir=tmp_path / "registry")

    registry_root = Path(result.output_dir)
    published_pack_dir = registry_root / "packs" / "general-en"
    artifact_path = registry_root / "artifacts" / "general-en-0.1.0.tar.zst"

    assert (published_pack_dir / "analysis.sqlite").exists() is False
    assert (published_pack_dir / "alias-analysis.json").exists() is False
    assert (published_pack_dir / "build.json").exists() is True

    decompressor = zstandard.ZstdDecompressor()
    with decompressor.stream_reader(io.BytesIO(artifact_path.read_bytes())) as reader:
        with tarfile.open(fileobj=reader, mode="r|") as archive:
            names = [member.name for member in archive]

    assert "analysis.sqlite" not in names
    assert "alias-analysis.json" not in names
    assert "build.json" in names
