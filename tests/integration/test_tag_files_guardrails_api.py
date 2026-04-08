from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_applies_max_input_bytes_guardrail(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    first = tmp_path / "alpha.html"
    second = tmp_path / "beta.html"
    first.write_text("alpha", encoding="utf-8")
    second.write_text("bravo", encoding="utf-8")

    response = tag_files(
        [first, second],
        pack="finance-en",
        storage_root=tmp_path,
        max_input_bytes=5,
    )

    assert response.item_count == 1
    assert response.summary.max_input_bytes == 5
    assert response.summary.processed_count == 1
    assert response.summary.limit_skipped_count == 1
    assert response.summary.processed_input_bytes == len("alpha".encode("utf-8"))
    assert response.skipped[0].reason == "max_input_bytes_limit"
    assert response.items[0].input_size_bytes == len("alpha".encode("utf-8"))
