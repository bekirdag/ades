from pathlib import Path


GUIDELINES_PATH = (
    Path(__file__).resolve().parents[2] / "docs" / "finance_country_library_guidelines.md"
)


def test_country_pack_source_lane_template_covers_required_gates() -> None:
    content = GUIDELINES_PATH.read_text(encoding="utf-8")
    template_start = content.index("## Reusable Country-Pack Source-Lane Template")
    template_end = content.index("## Build Order")
    template = content[template_start:template_end]

    required_phrases = [
        "Source Row Location",
        "source row file path and row number",
        "Parser Hooks",
        "fetch hook",
        "parse hook",
        "validation hook",
        "Validation Gates",
        "Artifact Build Gates",
        "Tests",
        "Smoke",
        "Replay",
        "Coverage Report",
    ]

    for phrase in required_phrases:
        assert phrase in template


def test_country_pack_source_lane_template_names_build_artifacts() -> None:
    content = GUIDELINES_PATH.read_text(encoding="utf-8")
    template_start = content.index("## Reusable Country-Pack Source-Lane Template")
    template_end = content.index("## Build Order")
    template = content[template_start:template_end]

    required_artifacts = [
        "/mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en",
        "/mnt/githubActions/ades_big_data/pack_sources/source_rows/finance-country-en",
        "/mnt/githubActions/ades_big_data/pack_sources/bundles",
        "/mnt/githubActions/ades_big_data/generated_runtime_packs",
        "/mnt/githubActions/ades_big_data/manifests/finance-country-en",
        "artifact ID",
        "artifact hash",
        "generated pack path",
    ]

    for artifact in required_artifacts:
        assert artifact in template
