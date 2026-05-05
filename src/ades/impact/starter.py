"""Packaged starter data for market impact graph builds."""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from importlib import resources
from pathlib import Path
from typing import Iterator

from ..service.models import MarketGraphStoreBuildResponse
from .graph_builder import build_market_graph_store

STARTER_RESOURCE_PACKAGE = "ades.resources.impact.phase1_starter"
STARTER_ARTIFACT_VERSION = "2026-05-05T00:00:00Z"


@contextmanager
def starter_golden_set_path() -> Iterator[Path]:
    """Yield a filesystem path to the packaged starter golden set."""

    resource = resources.files(STARTER_RESOURCE_PACKAGE) / "golden_set.json"
    with resources.as_file(resource) as path:
        yield Path(path)


@contextmanager
def starter_source_paths() -> Iterator[tuple[Path, Path]]:
    """Yield filesystem paths for the packaged starter node and edge TSVs."""

    package_files = resources.files(STARTER_RESOURCE_PACKAGE)
    with ExitStack() as stack:
        node_path = stack.enter_context(resources.as_file(package_files / "impact_nodes.tsv"))
        edge_path = stack.enter_context(resources.as_file(package_files / "impact_edges.tsv"))
        yield Path(node_path), Path(edge_path)


def build_starter_market_graph_store(
    *,
    output_dir: str | Path,
    artifact_version: str = STARTER_ARTIFACT_VERSION,
) -> MarketGraphStoreBuildResponse:
    """Build the reviewed starter market graph artifact."""

    with starter_source_paths() as (node_path, edge_path):
        return build_market_graph_store(
            node_tsv_paths=[node_path],
            edge_tsv_paths=[edge_path],
            output_dir=output_dir,
            artifact_version=artifact_version,
        )
