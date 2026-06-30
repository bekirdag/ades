import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = REPO_ROOT / "docs" / "big_data_workspace_manifest_2026-06-30.json"
WORKSPACE_ROOT = "/mnt/githubActions/ades_big_data"


def _load_manifest() -> dict[str, object]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def test_big_data_workspace_manifest_records_external_storage_contract() -> None:
    payload = _load_manifest()

    assert payload["schema_version"] == 1
    assert payload["workspace_root"] == WORKSPACE_ROOT
    assert WORKSPACE_ROOT in str(payload["storage_policy"])
    assert "not committed to git" in str(payload["git_policy"])

    roots = {
        str(item["name"]): item
        for item in payload["required_roots"]
        if isinstance(item, dict)
    }
    assert {
        "raw_pack_source_snapshots",
        "normalized_pack_bundles",
        "relationship_source_lanes",
        "legacy_country_relationship_reviews",
        "market_graph_artifacts",
        "release_artifacts",
        "vector_and_qid_artifacts",
        "review_feedback_queues",
    } <= set(roots)

    for root in roots.values():
        path = str(root["path"])
        assert path == WORKSPACE_ROOT or path.startswith(f"{WORKSPACE_ROOT}/")
        assert root["manifest_names"]
        assert root["producer_commands"]


def test_big_data_workspace_manifest_is_linked_from_planning_and_build_docs() -> None:
    payload = _load_manifest()
    manifest_file_name = MANIFEST_PATH.name
    linked_docs = [str(path) for path in payload["linked_docs"]]

    assert linked_docs == [
        "docs/news_story_creation_ades_source_lane_inventory_2026-06-30.md",
        "docs/library_pack_publication_workflow.md",
        "docs/v0.1.0_decisions.md",
    ]
    for relative_path in linked_docs:
        doc_path = REPO_ROOT / relative_path
        text = doc_path.read_text(encoding="utf-8")
        assert manifest_file_name in text
        assert WORKSPACE_ROOT in text


def test_big_data_workspace_manifest_names_lane_local_manifest_files() -> None:
    payload = _load_manifest()
    manifest_names = {
        name
        for root in payload["required_roots"]
        if isinstance(root, dict)
        for name in root["manifest_names"]
    }

    assert {
        "sources.fetch.json",
        "bundle.json",
        "sources.lock.json",
        "manifest.json",
        "SOURCE_INVENTORY.md",
        "market_graph_store_manifest.json",
        "qid_graph_store_manifest.json",
        "qid_graph_index_manifest.json",
    } <= manifest_names

    rules = "\n".join(str(rule) for rule in payload["local_manifest_rules"])
    assert "must have a sibling or run-directory manifest" in rules
    assert "must not embed large source payloads" in rules
