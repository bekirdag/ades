import json
from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_can_write_batch_manifest_and_return_aggregated_warnings(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    alpha_dir = tmp_path / "alpha"
    beta_dir = tmp_path / "beta"
    alpha_dir.mkdir()
    beta_dir.mkdir()
    first_input = alpha_dir / "report.html"
    second_input = beta_dir / "report.html"
    first_input.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    second_input.write_text("<p>EXCHX said Org Beta moved TICKA guidance.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    response = tag_files(
        [first_input, second_input],
        pack="finance-en",
        storage_root=tmp_path,
        content_type="application/json",
        output_dir=output_dir,
        write_manifest=True,
    )

    manifest_path = output_dir.resolve() / "batch.finance-en.ades-manifest.json"
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert response.saved_manifest_path == str(manifest_path)
    assert response.warnings == ["unsupported_content_type:application/json"]
    assert manifest_payload["warnings"] == ["unsupported_content_type:application/json"]
    assert manifest_payload["items"][1]["saved_output_path"] == str(
        output_dir.resolve() / "report.finance-en.ades-2.json"
    )
