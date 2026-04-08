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
        aliases=(("OpenAI", "organization"),),
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
