from pathlib import Path

from ades import lookup_candidates, pull_pack


def test_public_api_lookup_supports_multi_term_cross_field_search(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    response = lookup_candidates(
        "apple organization",
        storage_root=tmp_path,
        limit=10,
    )

    assert response.candidates
    assert response.candidates[0].kind == "alias"
    assert response.candidates[0].value == "Apple"
    assert response.candidates[0].label == "organization"
