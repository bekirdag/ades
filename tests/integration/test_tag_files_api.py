from pathlib import Path

from ades import pull_pack, tag_files


def test_public_api_can_tag_multiple_local_html_files(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    alpha_dir = tmp_path / "alpha"
    beta_dir = tmp_path / "beta"
    alpha_dir.mkdir()
    beta_dir.mkdir()
    first_input = alpha_dir / "report.html"
    second_input = beta_dir / "report.html"
    first_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")
    second_input.write_text("<p>NASDAQ said Apple moved AAPL guidance.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    response = tag_files(
        [first_input, second_input],
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
    )

    assert response.pack == "finance-en"
    assert response.item_count == 2
    pairs = {
        (item.source_path, entity.text, entity.label)
        for item in response.items
        for entity in item.entities
    }
    saved_paths = {item.saved_output_path for item in response.items}

    assert (str(first_input.resolve()), "Apple", "organization") in pairs
    assert (str(first_input.resolve()), "AAPL", "ticker") in pairs
    assert (str(second_input.resolve()), "NASDAQ", "exchange") in pairs
    assert saved_paths == {
        str(output_dir.resolve() / "report.finance-en.ades.json"),
        str(output_dir.resolve() / "report.finance-en.ades-2.json"),
    }
