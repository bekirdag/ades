import json
import subprocess
from pathlib import Path


def create_reviewed_registry_dir(root: Path) -> Path:
    registry_dir = root / "registry"
    (registry_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    (registry_dir / "packs" / "general-en").mkdir(parents=True, exist_ok=True)
    (registry_dir / "index.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-04-10T12:18:49Z",
                "packs": {
                    "general-en": {
                        "version": "0.2.0",
                        "manifest_url": "packs/general-en/manifest.json",
                        "language": "en",
                        "domain": "general",
                        "tier": "base",
                        "description": "General test pack.",
                        "tags": ["general"],
                        "dependencies": [],
                        "min_ades_version": "0.1.0",
                    }
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (registry_dir / "packs" / "general-en" / "manifest.json").write_text(
        json.dumps({"pack_id": "general-en", "version": "0.2.0"}, indent=2) + "\n",
        encoding="utf-8",
    )
    (registry_dir / "artifacts" / "general-en-0.2.0.tar.zst").write_bytes(b"artifact")
    return registry_dir


def install_object_storage_publish_stub(
    monkeypatch,
    *,
    registry_dir: Path,
    bucket: str = "ades-test",
    prefix: str = "generated-pack-releases/test-release",
    endpoint: str = "fsn1.your-objectstorage.com",
):
    resolved_registry_dir = registry_dir.resolve()
    normalized_prefix = prefix.strip("/")
    endpoint_url = f"https://{endpoint}"
    storage_uri = f"s3://{bucket}/{normalized_prefix}/"
    listing = "\n".join(
        [
            f"2026-04-10 15:19:19       1378 {normalized_prefix}/index.json",
            f"2026-04-10 15:19:19        635 {normalized_prefix}/packs/general-en/manifest.json",
            f"2026-04-10 15:19:19       1993 {normalized_prefix}/artifacts/general-en-0.2.0.tar.zst",
        ]
    )
    calls: list[list[str]] = []

    def fake_run(command, *, check, capture_output, text, env):
        calls.append(command)
        assert check is True
        assert capture_output is True
        assert text is True
        assert env["AWS_ACCESS_KEY_ID"] == "test-access"
        assert env["AWS_SECRET_ACCESS_KEY"] == "test-secret"
        assert env["AWS_DEFAULT_REGION"] == "us-east-1"
        assert env["AWS_EC2_METADATA_DISABLED"] == "true"
        if command[:4] == ["aws", "s3", "sync", str(resolved_registry_dir)]:
            assert storage_uri in command
            assert "--endpoint-url" in command
            assert endpoint_url in command
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[:4] == ["aws", "s3", "ls", storage_uri]:
            assert "--recursive" in command
            return subprocess.CompletedProcess(command, 0, stdout=listing, stderr="")
        raise AssertionError(f"Unexpected subprocess command: {command}")

    monkeypatch.setattr("ades.packs.publish.subprocess.run", fake_run)
    monkeypatch.setenv("ADES_PACK_OBJECT_STORAGE_BUCKET", bucket)
    monkeypatch.setenv("ADES_PACK_OBJECT_STORAGE_ENDPOINT", endpoint)
    monkeypatch.setenv("ADES_PACK_OBJECT_STORAGE_ACCESS_KEY_ID", "test-access")
    monkeypatch.setenv("ADES_PACK_OBJECT_STORAGE_SECRET_ACCESS_KEY", "test-secret")
    return calls
