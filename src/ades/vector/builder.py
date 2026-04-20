"""Offline builder for the hosted QID graph vector index."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any, Iterable, Iterator

from ..service.models import VectorIndexBuildResponse
from .qdrant import QdrantVectorSearchClient

DEFAULT_QID_GRAPH_DIMENSIONS = 384
DEFAULT_QID_GRAPH_ALLOWED_PREDICATES = (
    "P31",
    "P279",
    "P361",
    "P527",
    "P131",
    "P17",
    "P495",
    "P463",
    "P102",
    "P171",
    "P641",
    "P136",
    "P921",
)
_BUILDER_VERSION = "qid-graph-v1"
_TRUTHY_ENTITY_TRIPLE_RE = re.compile(
    r"^<http://www\.wikidata\.org/entity/(?P<subject>Q\d+)>\s+"
    r"<http://www\.wikidata\.org/prop/direct/(?P<predicate>P\d+)>\s+"
    r"<http://www\.wikidata\.org/entity/(?P<object>Q\d+)>\s+\.\s*$"
)


@dataclass
class _TargetEntity:
    entity_id: str
    qid: str
    canonical_text: str
    entity_type: str | None = None
    source_name: str | None = None
    packs: set[str] = field(default_factory=set)
    sparse_vector: dict[int, float] = field(default_factory=dict)


def _feature_bucket(feature: str, *, dimensions: int) -> tuple[int, float]:
    digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=16).digest()
    index = int.from_bytes(digest[:8], byteorder="big", signed=False) % dimensions
    sign = 1.0 if digest[8] % 2 == 0 else -1.0
    return index, sign


def _add_feature(
    sparse_vector: dict[int, float],
    feature: str,
    *,
    dimensions: int,
    weight: float = 1.0,
) -> None:
    index, sign = _feature_bucket(feature, dimensions=dimensions)
    sparse_vector[index] = sparse_vector.get(index, 0.0) + (sign * weight)


def _iter_text_lines(path: Path) -> Iterator[str]:
    if path.suffix.casefold() == ".gz":
        pigz_path = shutil.which("pigz")
        if pigz_path is not None:
            process = subprocess.Popen(
                [
                    pigz_path,
                    "-dc",
                    str(path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if process.stdout is None or process.stderr is None:
                process.kill()
                raise RuntimeError(f"Unable to stream gzip file with pigz: {path}")
            try:
                yield from process.stdout
            finally:
                process.stdout.close()
                stderr_output = process.stderr.read()
                process.stderr.close()
                return_code = process.wait()
                if return_code != 0:
                    raise RuntimeError(
                        f"pigz failed while streaming {path}: {stderr_output.strip() or return_code}"
                    )
            return
        rapidgzip_path = shutil.which("rapidgzip")
        if rapidgzip_path is not None:
            process = subprocess.Popen(
                [
                    rapidgzip_path,
                    "-d",
                    "-c",
                    "-P",
                    "0",
                    "--no-verify",
                    str(path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if process.stdout is None or process.stderr is None:
                process.kill()
                raise RuntimeError(f"Unable to stream gzip file with rapidgzip: {path}")
            try:
                yield from process.stdout
            finally:
                process.stdout.close()
                stderr_output = process.stderr.read()
                process.stderr.close()
                return_code = process.wait()
                if return_code != 0:
                    raise RuntimeError(
                        f"rapidgzip failed while streaming {path}: {stderr_output.strip() or return_code}"
                    )
            return
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
            yield from handle
        return
    with path.open("r", encoding="utf-8") as handle:
        yield from handle


def _load_bundle_manifest(bundle_dir: Path) -> dict[str, Any]:
    bundle_path = bundle_dir / "bundle.json"
    if not bundle_path.exists():
        raise FileNotFoundError(f"Bundle manifest not found: {bundle_path}")
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Bundle manifest is not a JSON object: {bundle_path}")
    return payload


def _load_targets(
    bundle_dirs: list[Path],
    *,
    dimensions: int,
) -> tuple[dict[str, _TargetEntity], list[str], list[str]]:
    targets: dict[str, _TargetEntity] = {}
    warnings: list[str] = []
    pack_ids: list[str] = []
    for bundle_dir in bundle_dirs:
        manifest = _load_bundle_manifest(bundle_dir)
        pack_id = str(manifest.get("pack_id") or bundle_dir.name).strip()
        if pack_id:
            pack_ids.append(pack_id)
        entities_rel_path = str(manifest.get("entities_path") or "normalized/entities.jsonl")
        entities_path = (bundle_dir / entities_rel_path).resolve()
        if not entities_path.exists():
            raise FileNotFoundError(f"Bundle entities file not found: {entities_path}")
        skipped_non_wikidata = 0
        with entities_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                payload = json.loads(line)
                if not isinstance(payload, dict):
                    continue
                entity_id = str(payload.get("entity_id") or "").strip()
                if not entity_id.startswith("wikidata:Q"):
                    skipped_non_wikidata += 1
                    continue
                qid = entity_id.partition(":")[2]
                entry = targets.get(qid)
                if entry is None:
                    entry = _TargetEntity(
                        entity_id=entity_id,
                        qid=qid,
                        canonical_text=str(payload.get("canonical_text") or qid).strip() or qid,
                        entity_type=(
                            str(payload.get("entity_type")).strip()
                            if payload.get("entity_type") is not None
                            else None
                        ),
                        source_name=(
                            str(payload.get("source_name")).strip()
                            if payload.get("source_name") is not None
                            else None
                        ),
                    )
                    if entry.entity_type:
                        _add_feature(
                            entry.sparse_vector,
                            f"meta:type:{entry.entity_type}",
                            dimensions=dimensions,
                            weight=0.5,
                        )
                    if entry.source_name:
                        _add_feature(
                            entry.sparse_vector,
                            f"meta:source:{entry.source_name}",
                            dimensions=dimensions,
                            weight=0.25,
                        )
                    targets[qid] = entry
                entry.packs.add(pack_id)
        if skipped_non_wikidata:
            warnings.append(f"skipped_non_wikidata:{pack_id}:{skipped_non_wikidata}")
    return targets, sorted(set(pack_ids)), warnings


def _ingest_truthy_graph(
    targets: dict[str, _TargetEntity],
    *,
    truthy_path: Path,
    dimensions: int,
    allowed_predicates: set[str],
) -> tuple[int, int]:
    processed_line_count = 0
    matched_statement_count = 0
    for line in _iter_text_lines(truthy_path):
        processed_line_count += 1
        match = _TRUTHY_ENTITY_TRIPLE_RE.match(line)
        if match is None:
            continue
        predicate = match.group("predicate")
        if predicate not in allowed_predicates:
            continue
        subject = match.group("subject")
        obj = match.group("object")
        touched = False
        subject_entry = targets.get(subject)
        if subject_entry is not None:
            _add_feature(
                subject_entry.sparse_vector,
                f"out:{predicate}:{obj}",
                dimensions=dimensions,
            )
            touched = True
        object_entry = targets.get(obj)
        if object_entry is not None:
            _add_feature(
                object_entry.sparse_vector,
                f"in:{predicate}:{subject}",
                dimensions=dimensions,
            )
            touched = True
        if touched:
            matched_statement_count += 1
    return processed_line_count, matched_statement_count


def _normalized_sparse_items(
    sparse_vector: dict[int, float],
) -> list[list[int | float]]:
    if not sparse_vector:
        return [[0, 1.0]]
    norm = math.sqrt(sum(value * value for value in sparse_vector.values()))
    if norm == 0.0:
        return [[0, 1.0]]
    return [
        [index, round(value / norm, 6)]
        for index, value in sorted(sparse_vector.items())
        if value != 0.0
    ]


def _write_sparse_artifact(
    targets: dict[str, _TargetEntity],
    *,
    artifact_path: Path,
) -> int:
    point_count = 0
    with gzip.open(artifact_path, "wt", encoding="utf-8") as handle:
        for qid in sorted(targets):
            entry = targets[qid]
            record = {
                "id": entry.entity_id,
                "vector": _normalized_sparse_items(entry.sparse_vector),
                "payload": {
                    "entity_id": entry.entity_id,
                    "canonical_text": entry.canonical_text,
                    "entity_type": entry.entity_type,
                    "source_name": entry.source_name,
                    "packs": sorted(entry.packs),
                },
            }
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")
            point_count += 1
    return point_count


def _iter_dense_points_from_artifact(
    artifact_path: Path,
    *,
    dimensions: int,
) -> Iterator[dict[str, Any]]:
    with gzip.open(artifact_path, "rt", encoding="utf-8") as handle:
        for line in handle:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                continue
            vector = [0.0] * dimensions
            sparse_values = payload.get("vector")
            if isinstance(sparse_values, list):
                for item in sparse_values:
                    if (
                        isinstance(item, list)
                        and len(item) == 2
                        and isinstance(item[0], (int, float))
                        and isinstance(item[1], (int, float))
                    ):
                        index = int(item[0])
                        if 0 <= index < dimensions:
                            vector[index] = float(item[1])
            yield {
                "id": payload.get("id"),
                "payload": payload.get("payload"),
                "vector": vector,
            }


def _default_collection_name(pack_ids: Iterable[str]) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    pack_suffix = "-".join(sorted(set(pack_ids))) or "qid-graph"
    return f"ades-{pack_suffix}-{stamp}"


def build_qid_graph_index(
    bundle_dirs: Iterable[str | Path],
    *,
    truthy_path: str | Path,
    output_dir: str | Path,
    dimensions: int = DEFAULT_QID_GRAPH_DIMENSIONS,
    allowed_predicates: Iterable[str] | None = None,
    qdrant_url: str | None = None,
    qdrant_api_key: str | None = None,
    collection_name: str | None = None,
    publish_alias: str | None = None,
) -> VectorIndexBuildResponse:
    """Build one hosted QID graph artifact and optionally publish it to Qdrant."""

    resolved_bundle_dirs = [Path(path).expanduser().resolve() for path in bundle_dirs]
    if not resolved_bundle_dirs:
        raise ValueError("At least one bundle directory is required.")
    if dimensions <= 0:
        raise ValueError("dimensions must be positive.")
    resolved_truthy_path = Path(truthy_path).expanduser().resolve()
    if not resolved_truthy_path.exists():
        raise FileNotFoundError(f"Truthy RDF dump not found: {resolved_truthy_path}")
    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    predicate_values = {
        str(predicate).strip().upper()
        for predicate in (
            allowed_predicates if allowed_predicates is not None else DEFAULT_QID_GRAPH_ALLOWED_PREDICATES
        )
        if str(predicate).strip()
    }
    if not predicate_values:
        raise ValueError("At least one allowed predicate is required.")

    targets, pack_ids, warnings = _load_targets(
        resolved_bundle_dirs,
        dimensions=dimensions,
    )
    if not targets:
        raise ValueError("No Wikidata-backed QIDs were found in the provided bundles.")

    processed_line_count, matched_statement_count = _ingest_truthy_graph(
        targets,
        truthy_path=resolved_truthy_path,
        dimensions=dimensions,
        allowed_predicates=predicate_values,
    )

    artifact_path = resolved_output_dir / "qid_graph_points.jsonl.gz"
    point_count = _write_sparse_artifact(targets, artifact_path=artifact_path)
    manifest_path = resolved_output_dir / "qid_graph_index_manifest.json"

    response = VectorIndexBuildResponse(
        output_dir=str(resolved_output_dir),
        manifest_path=str(manifest_path),
        artifact_path=str(artifact_path),
        collection_name=None,
        alias_name=None,
        qdrant_url=None,
        published=False,
        dimensions=dimensions,
        point_count=point_count,
        target_entity_count=len(targets),
        bundle_count=len(resolved_bundle_dirs),
        bundle_dirs=[str(path) for path in resolved_bundle_dirs],
        pack_ids=pack_ids,
        truthy_path=str(resolved_truthy_path),
        processed_line_count=processed_line_count,
        matched_statement_count=matched_statement_count,
        allowed_predicates=sorted(predicate_values),
        warnings=warnings,
    )

    if qdrant_url:
        resolved_collection_name = collection_name or _default_collection_name(pack_ids)
        with QdrantVectorSearchClient(qdrant_url, api_key=qdrant_api_key) as client:
            client.ensure_collection(
                resolved_collection_name,
                dimensions=dimensions,
            )
            client.upsert_points(
                resolved_collection_name,
                _iter_dense_points_from_artifact(
                    artifact_path,
                    dimensions=dimensions,
                ),
            )
            if publish_alias:
                client.set_alias(publish_alias, resolved_collection_name)
        response = response.model_copy(
            update={
                "collection_name": resolved_collection_name,
                "alias_name": publish_alias,
                "qdrant_url": qdrant_url,
                "published": True,
            }
        )

    manifest_path.write_text(
        json.dumps(
            {
                **response.model_dump(mode="json"),
                "builder_version": _BUILDER_VERSION,
                "built_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return response
