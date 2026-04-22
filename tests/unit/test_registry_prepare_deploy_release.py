import hashlib
import io
import json
from pathlib import Path
import tarfile

import pytest
import zstandard

from ades.packs.publish import prepare_registry_deploy_payload
from tests.published_registry_smoke_helpers import (
    create_working_published_registry_dir,
    serve_published_registry_dir,
    write_registry_promotion_spec,
)


def test_prepare_registry_deploy_payload_builds_bundled_registry_when_no_promotion_spec(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "ades.packs.publish.DEFAULT_REGISTRY_PROMOTION_SPEC_PATH",
        tmp_path / "missing-promoted-release.json",
    )

    result = prepare_registry_deploy_payload(
        [],
        output_dir=tmp_path / "deploy-registry",
    )

    assert result.mode == "bundled"
    assert result.promoted_registry_url is None
    assert result.consumer_smoke is None
    assert result.pack_count == 3
    assert [pack.pack_id for pack in result.packs] == [
        "finance-en",
        "general-en",
        "medical-en",
    ]
    assert Path(result.index_path).exists()


def test_prepare_registry_deploy_payload_materializes_promoted_registry(
    tmp_path: Path,
) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)

    with serve_published_registry_dir(registry_dir) as registry_url:
        promotion_spec_path = write_registry_promotion_spec(
            tmp_path,
            registry_url=registry_url,
        )
        result = prepare_registry_deploy_payload(
            [],
            output_dir=tmp_path / "deploy-registry",
            promotion_spec_path=promotion_spec_path,
        )

    assert result.mode == "promoted"
    assert result.promoted_registry_url == registry_url
    assert result.consumer_smoke is not None
    assert result.consumer_smoke.passed is True
    assert result.pack_count == 3
    assert sorted(pack.pack_id for pack in result.packs) == [
        "finance-en",
        "general-en",
        "medical-en",
    ]

    manifest_payload = json.loads(
        (tmp_path / "deploy-registry" / "packs" / "finance-en" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest_payload["artifacts"][0]["url"].startswith("../../artifacts/")
    assert (tmp_path / "deploy-registry" / "artifacts").exists()


def test_prepare_registry_deploy_payload_rejects_tiny_promoted_general_matcher(
    tmp_path: Path,
) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)
    general_manifest_path = registry_dir / "packs" / "general-en" / "manifest.json"
    general_manifest = json.loads(general_manifest_path.read_text(encoding="utf-8"))
    artifact_rel_url = general_manifest["artifacts"][0]["url"]
    artifact_path = (general_manifest_path.parent / artifact_rel_url).resolve()
    general_manifest["matcher"]["entry_count"] = 6
    general_manifest["matcher"]["state_count"] = 10

    corrupted_bytes = _build_tiny_general_pack_artifact_bytes(
        general_manifest,
        source_pack_dir=general_manifest_path.parent,
    )
    artifact_path.write_bytes(corrupted_bytes)
    general_manifest["artifacts"][0]["sha256"] = hashlib.sha256(corrupted_bytes).hexdigest()
    general_manifest_path.write_text(
        json.dumps(general_manifest, indent=2) + "\n",
        encoding="utf-8",
    )

    with serve_published_registry_dir(registry_dir) as registry_url:
        promotion_spec_path = write_registry_promotion_spec(
            tmp_path,
            registry_url=registry_url,
        )
        with pytest.raises(
            ValueError,
            match="implausibly small for deployment",
        ):
            prepare_registry_deploy_payload(
                [],
                output_dir=tmp_path / "deploy-registry",
                promotion_spec_path=promotion_spec_path,
            )


def _build_tiny_general_pack_artifact_bytes(
    manifest_payload: dict[str, object],
    *,
    source_pack_dir: Path,
) -> bytes:
    contents = {
        "manifest.json": (json.dumps(manifest_payload, indent=2) + "\n").encode("utf-8"),
        "aliases.json": (source_pack_dir / "aliases.json").read_bytes(),
        "labels.json": (source_pack_dir / "labels.json").read_bytes(),
        "rules.json": (source_pack_dir / "rules.json").read_bytes(),
        "matcher/token-trie.bin": b"x" * 274,
        "matcher/entries.bin": b"y" * 564,
    }
    compressed = io.BytesIO()
    compressor = zstandard.ZstdCompressor(level=3)
    with compressor.stream_writer(compressed, closefd=False) as compressed_handle:
        with tarfile.open(fileobj=compressed_handle, mode="w|") as archive:
            for name, payload in contents.items():
                tar_info = tarfile.TarInfo(name)
                tar_info.size = len(payload)
                tar_info.uid = 0
                tar_info.gid = 0
                tar_info.uname = ""
                tar_info.gname = ""
                tar_info.mtime = 0
                archive.addfile(tar_info, fileobj=io.BytesIO(payload))
    return compressed.getvalue()
