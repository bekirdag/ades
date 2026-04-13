from __future__ import annotations

from contextlib import contextmanager
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading

from ades import (
    build_finance_source_bundle,
    build_general_source_bundle,
    build_medical_source_bundle,
    refresh_generated_packs,
)
from tests.finance_bundle_helpers import create_finance_raw_snapshots
from tests.general_bundle_helpers import create_general_raw_snapshots
from tests.medical_bundle_helpers import create_medical_raw_snapshots


class _QuietSimpleHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:  # pragma: no cover - noise only
        return


def create_working_published_registry_dir(root: Path) -> Path:
    finance_snapshots = create_finance_raw_snapshots(root / "finance-snapshots")
    general_snapshots = create_general_raw_snapshots(root / "general-snapshots")
    medical_snapshots = create_medical_raw_snapshots(root / "medical-snapshots")

    finance_bundle = build_finance_source_bundle(
        sec_companies_path=finance_snapshots["sec_companies"],
        symbol_directory_path=finance_snapshots["symbol_directory"],
        curated_entities_path=finance_snapshots["curated_entities"],
        output_dir=root / "bundles",
    )
    general_bundle = build_general_source_bundle(
        wikidata_entities_path=general_snapshots["wikidata_entities"],
        geonames_places_path=general_snapshots["geonames_places"],
        curated_entities_path=general_snapshots["curated_entities"],
        output_dir=root / "bundles",
    )
    medical_bundle = build_medical_source_bundle(
        disease_ontology_path=medical_snapshots["disease_ontology"],
        hgnc_genes_path=medical_snapshots["hgnc_genes"],
        uniprot_proteins_path=medical_snapshots["uniprot_proteins"],
        clinical_trials_path=medical_snapshots["clinical_trials"],
        curated_entities_path=medical_snapshots["curated_entities"],
        output_dir=root / "bundles",
    )
    refresh = refresh_generated_packs(
        [finance_bundle.bundle_dir, general_bundle.bundle_dir, medical_bundle.bundle_dir],
        output_dir=root / "refresh-output",
        materialize_registry=True,
    )
    if not refresh.passed or refresh.registry is None:
        raise AssertionError("Expected generated registry refresh fixture to pass.")
    return Path(refresh.registry.output_dir)


def write_registry_promotion_spec(
    root: Path,
    *,
    registry_url: str,
    smoke_pack_ids: tuple[str, ...] = ("finance-en", "medical-en"),
) -> Path:
    spec_path = root / "promoted-release.json"
    spec_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "registry_url": registry_url,
                "smoke_pack_ids": list(smoke_pack_ids),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return spec_path


@contextmanager
def serve_published_registry_dir(registry_dir: Path):
    handler = partial(_QuietSimpleHTTPRequestHandler, directory=str(registry_dir))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/index.json"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
