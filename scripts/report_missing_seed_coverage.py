#!/usr/bin/env python3
"""Report collection and graph coverage for repeated missing vector seeds."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from ades.config import get_settings
from ades.vector.qdrant import QdrantVectorSearchClient, QdrantVectorSearchError


DEFAULT_ALIASES = (
    "ades-qids-current",
    "ades-qids-business-current",
    "ades-qids-economics-current",
    "ades-qids-finance-current",
    "ades-qids-politics-current",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure where repeated missing seeds are absent or present across "
            "the local graph store and Qdrant aliases."
        )
    )
    parser.add_argument(
        "--summary",
        required=True,
        help="Path to one seed-neighbor audit summary.json file.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="How many top missing seeds to inspect.",
    )
    parser.add_argument(
        "--alias",
        action="append",
        help=(
            "Alias to inspect. Repeat for multiple aliases. Defaults to the "
            "shared alias plus the routed domain aliases."
        ),
    )
    parser.add_argument(
        "--bundle-path",
        action="append",
        help=(
            "Optional bundle directory or parent directory containing bundle "
            "directories. Repeat to inspect raw input coverage alongside graph "
            "and alias coverage."
        ),
    )
    parser.add_argument(
        "--output-dir",
        help=(
            "Optional output directory. Defaults under "
            "docs/planning/live-rss-vector-reviews/."
        ),
    )
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected one JSON object in {path}.")
    return payload


def _safe_slug(value: str) -> str:
    cleaned = "".join(
        character if character.isalnum() or character in {"-", "_"} else "-"
        for character in value.strip().casefold()
    )
    normalized = "-".join(part for part in cleaned.split("-") if part)
    return normalized or "artifact"


def _resolve_output_dir(summary_path: Path, requested: str | None) -> Path:
    if requested:
        return Path(requested).expanduser().resolve()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = _safe_slug(summary_path.parent.name)
    return (
        Path("docs/planning/live-rss-vector-reviews")
        / f"missing-seed-coverage-{slug}-{timestamp}"
    ).resolve()


def _graph_store_lookup(
    graph_store_path: Path,
    *,
    qid: str,
) -> dict[str, Any]:
    connection = sqlite3.connect(str(graph_store_path))
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            SELECT qid, entity_id, canonical_text, entity_type, source_name
            FROM nodes
            WHERE qid = ?
            """,
            (qid,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return {
            "present": False,
            "qid": qid,
            "entity_id": f"wikidata:{qid}",
            "canonical_text": None,
            "entity_type": None,
            "source_name": None,
        }
    return {
        "present": True,
        "qid": str(row["qid"]),
        "entity_id": str(row["entity_id"]) if row["entity_id"] is not None else None,
        "canonical_text": (
            str(row["canonical_text"]) if row["canonical_text"] is not None else None
        ),
        "entity_type": (
            str(row["entity_type"]) if row["entity_type"] is not None else None
        ),
        "source_name": (
            str(row["source_name"]) if row["source_name"] is not None else None
        ),
    }


def _alias_presence(
    client: QdrantVectorSearchClient,
    *,
    alias_name: str,
    entity_id: str,
) -> dict[str, Any]:
    try:
        neighbors = client.query_similar_by_id(
            alias_name,
            point_id=entity_id,
            limit=1,
        )
    except QdrantVectorSearchError as exc:
        if exc.status_code == 404:
            return {
                "alias_name": alias_name,
                "present": False,
                "error": None,
                "nearest_point_id": None,
                "nearest_score": None,
            }
        return {
            "alias_name": alias_name,
            "present": False,
            "error": str(exc),
            "nearest_point_id": None,
            "nearest_score": None,
        }
    nearest = neighbors[0] if neighbors else None
    return {
        "alias_name": alias_name,
        "present": True,
        "error": None,
        "nearest_point_id": nearest.point_id if nearest is not None else None,
        "nearest_score": nearest.score if nearest is not None else None,
    }


def _expand_bundle_dirs(values: list[str] | None) -> list[Path]:
    if not values:
        return []
    resolved_dirs: list[Path] = []
    seen: set[Path] = set()
    for raw_value in values:
        candidate = Path(raw_value).expanduser().resolve()
        if not candidate.exists():
            raise SystemExit(f"Bundle path does not exist: {candidate}")
        if (candidate / "bundle.json").exists():
            if candidate not in seen:
                seen.add(candidate)
                resolved_dirs.append(candidate)
            continue
        child_bundle_dirs = sorted(
            child.resolve()
            for child in candidate.iterdir()
            if child.is_dir() and (child / "bundle.json").exists()
        )
        if not child_bundle_dirs:
            raise SystemExit(
                f"Bundle path is neither a bundle dir nor a parent of bundle dirs: {candidate}"
            )
        for child_bundle_dir in child_bundle_dirs:
            if child_bundle_dir in seen:
                continue
            seen.add(child_bundle_dir)
            resolved_dirs.append(child_bundle_dir)
    return resolved_dirs


def _bundle_presence_rows(
    bundle_dir: Path,
    *,
    target_entity_ids: set[str],
) -> dict[str, dict[str, Any]]:
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    if not entities_path.exists():
        raise SystemExit(f"Bundle entities file does not exist: {entities_path}")
    matched: dict[str, dict[str, Any]] = {}
    remaining = set(target_entity_ids)
    with entities_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            if not remaining:
                break
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            entity_id = payload.get("entity_id")
            if not isinstance(entity_id, str) or entity_id not in remaining:
                continue
            matched[entity_id] = {
                "entity_id": entity_id,
                "canonical_text": payload.get("canonical_text"),
                "entity_type": payload.get("entity_type"),
                "source_name": payload.get("source_name"),
            }
            remaining.remove(entity_id)
    return matched


def _markdown_summary(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Missing Seed Coverage Report")
    lines.append("")
    lines.append(f"Generated at: `{report['generated_at']}`")
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    lines.append(f"- audit summary: `{report['summary_path']}`")
    lines.append(f"- source artifact: `{report['source_artifact_dir']}`")
    lines.append(f"- inspected seeds: `{report['inspected_seed_count']}`")
    lines.append(f"- graph store path: `{report['graph_store_path']}`")
    lines.append("")
    lines.append("## Aggregate")
    lines.append("")
    aggregate = report["aggregate"]
    lines.append(f"- graph-store present: `{aggregate['graph_store_present_count']}`")
    lines.append(
        f"- present in shared alias only check: `{aggregate['shared_alias_present_count']}`"
    )
    lines.append(
        "- present in at least one routed alias: "
        f"`{aggregate['present_in_any_routed_alias_count']}`"
    )
    lines.append(
        f"- absent everywhere checked: `{aggregate['absent_everywhere_checked_count']}`"
    )
    if aggregate["bundle_presence_counts"]:
        lines.append(
            f"- present in at least one inspected raw bundle: `{aggregate['present_in_any_bundle_count']}`"
        )
    lines.append("")
    lines.append("## Alias Presence Counts")
    lines.append("")
    for item in aggregate["alias_presence_counts"]:
        lines.append(f"- `{item['alias_name']}`: `{item['present_count']}` present")
    if aggregate["bundle_presence_counts"]:
        lines.append("")
        lines.append("## Bundle Presence Counts")
        lines.append("")
        for item in aggregate["bundle_presence_counts"]:
            lines.append(
                f"- `{item['pack_id']}` (`{item['bundle_dir']}`): `{item['present_count']}` present"
            )
    lines.append("")
    lines.append("## Seeds")
    lines.append("")
    for seed in report["seeds"]:
        lines.append(
            f"- `{seed['canonical_text']}` ({seed['entity_id']}, {seed['label']}, "
            f"count `{seed['count']}`)"
        )
        lines.append(
            "  graph store: "
            f"`{'present' if seed['graph_store']['present'] else 'missing'}`"
        )
        for alias in seed["aliases"]:
            status = "present" if alias["present"] else "missing"
            extra = ""
            if alias["error"]:
                extra = f", error `{alias['error']}`"
            elif alias["nearest_point_id"]:
                extra = (
                    f", nearest `{alias['nearest_point_id']}` "
                    f"(score `{alias['nearest_score']}`)"
                )
            lines.append(f"  {alias['alias_name']}: `{status}`{extra}")
        for bundle_row in seed.get("bundle_inputs", []):
            bundle_status = "present" if bundle_row["present"] else "missing"
            lines.append(
                "  raw bundle "
                f"`{bundle_row['pack_id']}`: `{bundle_status}`"
            )
    lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    args = _parse_args()
    summary_path = Path(args.summary).expanduser().resolve()
    summary = _load_json(summary_path)
    seed_shape_analysis = summary.get("seed_shape_analysis")
    if not isinstance(seed_shape_analysis, dict):
        raise SystemExit("Summary does not include seed_shape_analysis.")
    top_missing_seeds = seed_shape_analysis.get("top_missing_seeds")
    if not isinstance(top_missing_seeds, list) or not top_missing_seeds:
        raise SystemExit("Summary does not include any top_missing_seeds entries.")

    graph_store_path_value = (
        summary.get("artifact_inventory", {})
        .get("graph_store", {})
        .get("configured_path")
    )
    if not isinstance(graph_store_path_value, str) or not graph_store_path_value.strip():
        settings = get_settings()
        graph_store_path = Path(settings.graph_context_artifact_path).expanduser().resolve()
    else:
        graph_store_path = Path(graph_store_path_value).expanduser().resolve()
    if not graph_store_path.exists():
        raise SystemExit(f"Graph store path does not exist: {graph_store_path}")

    output_dir = _resolve_output_dir(summary_path, args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    aliases = tuple(args.alias) if args.alias else DEFAULT_ALIASES
    bundle_dirs = _expand_bundle_dirs(args.bundle_path)
    settings = get_settings()
    client = QdrantVectorSearchClient(
        settings.vector_search_url,
        api_key=settings.vector_search_api_key,
    )
    try:
        seeds: list[dict[str, Any]] = []
        alias_presence_counts = {alias_name: 0 for alias_name in aliases}
        target_entity_ids = {
            raw_seed["entity_id"]
            for raw_seed in top_missing_seeds[: args.limit]
            if isinstance(raw_seed, dict)
            if isinstance(raw_seed.get("entity_id"), str)
            if raw_seed["entity_id"].startswith("wikidata:")
        }
        bundle_presence_maps: dict[Path, dict[str, dict[str, Any]]] = {}
        bundle_meta_rows: list[dict[str, Any]] = []
        bundle_presence_counts: dict[Path, int] = {}
        for bundle_dir in bundle_dirs:
            bundle_payload = _load_json(bundle_dir / "bundle.json")
            pack_id = bundle_payload.get("pack_id")
            resolved_pack_id = (
                str(pack_id).strip()
                if isinstance(pack_id, str) and str(pack_id).strip()
                else bundle_dir.name
            )
            matched_rows = _bundle_presence_rows(
                bundle_dir,
                target_entity_ids=target_entity_ids,
            )
            bundle_presence_maps[bundle_dir] = matched_rows
            bundle_meta_rows.append(
                {
                    "bundle_dir": str(bundle_dir),
                    "pack_id": resolved_pack_id,
                }
            )
            bundle_presence_counts[bundle_dir] = len(matched_rows)
        graph_store_present_count = 0
        shared_alias_present_count = 0
        present_in_any_routed_alias_count = 0
        absent_everywhere_checked_count = 0
        present_in_any_bundle_count = 0

        for raw_seed in top_missing_seeds[: args.limit]:
            if not isinstance(raw_seed, dict):
                continue
            entity_id = raw_seed.get("entity_id")
            canonical_text = raw_seed.get("canonical_text")
            label = raw_seed.get("label")
            count = raw_seed.get("count")
            if not isinstance(entity_id, str) or not entity_id.startswith("wikidata:"):
                continue
            qid = entity_id.removeprefix("wikidata:")
            graph_store = _graph_store_lookup(graph_store_path, qid=qid)
            if graph_store["present"]:
                graph_store_present_count += 1

            alias_rows = [
                _alias_presence(client, alias_name=alias_name, entity_id=entity_id)
                for alias_name in aliases
            ]
            for alias_row in alias_rows:
                if alias_row["present"]:
                    alias_presence_counts[alias_row["alias_name"]] += 1
            shared_present = any(
                alias_row["present"]
                for alias_row in alias_rows
                if alias_row["alias_name"] == "ades-qids-current"
            )
            routed_present = any(
                alias_row["present"]
                for alias_row in alias_rows
                if alias_row["alias_name"] != "ades-qids-current"
            )
            if shared_present:
                shared_alias_present_count += 1
            if routed_present:
                present_in_any_routed_alias_count += 1
            if not shared_present and not routed_present:
                absent_everywhere_checked_count += 1
            bundle_rows = []
            bundle_present = False
            for bundle_meta in bundle_meta_rows:
                bundle_dir = Path(bundle_meta["bundle_dir"])
                matched_row = bundle_presence_maps[bundle_dir].get(entity_id)
                present = matched_row is not None
                if present:
                    bundle_present = True
                bundle_rows.append(
                    {
                        "bundle_dir": bundle_meta["bundle_dir"],
                        "pack_id": bundle_meta["pack_id"],
                        "present": present,
                        "canonical_text": (
                            matched_row.get("canonical_text") if matched_row else None
                        ),
                        "entity_type": (
                            matched_row.get("entity_type") if matched_row else None
                        ),
                    }
                )
            if bundle_present:
                present_in_any_bundle_count += 1

            seeds.append(
                {
                    "entity_id": entity_id,
                    "canonical_text": canonical_text,
                    "label": label,
                    "count": int(count) if isinstance(count, (int, float)) else 0,
                    "graph_store": graph_store,
                    "aliases": alias_rows,
                    "bundle_inputs": bundle_rows,
                }
            )
    finally:
        client.close()

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary_path": str(summary_path),
        "source_artifact_dir": str(summary_path.parent),
        "graph_store_path": str(graph_store_path),
        "inspected_seed_count": len(seeds),
        "aggregate": {
            "graph_store_present_count": graph_store_present_count,
            "shared_alias_present_count": shared_alias_present_count,
            "present_in_any_routed_alias_count": present_in_any_routed_alias_count,
            "absent_everywhere_checked_count": absent_everywhere_checked_count,
            "present_in_any_bundle_count": present_in_any_bundle_count,
            "alias_presence_counts": [
                {"alias_name": alias_name, "present_count": present_count}
                for alias_name, present_count in alias_presence_counts.items()
            ],
            "bundle_presence_counts": [
                {
                    "bundle_dir": bundle_meta["bundle_dir"],
                    "pack_id": bundle_meta["pack_id"],
                    "present_count": bundle_presence_counts[Path(bundle_meta["bundle_dir"])],
                }
                for bundle_meta in bundle_meta_rows
            ],
        },
        "seeds": seeds,
    }

    summary_output_path = output_dir / "summary.json"
    index_output_path = output_dir / "index.md"
    summary_output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    index_output_path.write_text(_markdown_summary(report), encoding="utf-8")

    print(str(summary_output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
