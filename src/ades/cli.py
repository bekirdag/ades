"""Command line interface for ades."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from .api import activate_pack as api_activate_pack
from .api import build_finance_source_bundle as api_build_finance_source_bundle
from .api import build_general_source_bundle as api_build_general_source_bundle
from .api import build_medical_source_bundle as api_build_medical_source_bundle
from .api import build_registry as api_build_registry
from .api import deactivate_pack as api_deactivate_pack
from .api import fetch_finance_source_snapshot as api_fetch_finance_source_snapshot
from .api import fetch_general_source_snapshot as api_fetch_general_source_snapshot
from .api import fetch_medical_source_snapshot as api_fetch_medical_source_snapshot
from .api import generate_pack_source as api_generate_pack_source
from .api import lookup_candidates as api_lookup_candidates
from .api import list_available_packs as api_list_available_packs
from .api import list_packs as api_list_packs
from .api import publish_release as api_publish_release
from .api import pull_pack as api_pull_pack
from .api import refresh_generated_packs as api_refresh_generated_packs
from .api import report_generated_pack as api_report_generated_pack
from .api import remove_pack as api_remove_pack
from .api import release_versions as api_release_versions
from .api import status as api_status
from .api import sync_release_version as api_sync_release_version
from .api import tag as api_tag
from .api import tag_file as api_tag_file
from .api import tag_files as api_tag_files
from .api import validate_general_pack_quality as api_validate_general_pack_quality
from .api import validate_finance_pack_quality as api_validate_finance_pack_quality
from .api import validate_medical_pack_quality as api_validate_medical_pack_quality
from .api import validate_release as api_validate_release
from .api import verify_release as api_verify_release
from .api import write_release_manifest as api_write_release_manifest
from .config import InvalidConfigurationError, get_settings
from .packs.installer import PackInstaller
from .storage import UnsupportedRuntimeConfigurationError
from .storage.paths import build_storage_layout, ensure_storage_layout


app = typer.Typer(help="ades local semantic enrichment CLI", no_args_is_help=True)
list_app = typer.Typer(help="List available or installed ades resources.")
packs_app = typer.Typer(help="Inspect installed ades packs.")
registry_app = typer.Typer(help="Build static pack registries for external distribution.")
release_app = typer.Typer(help="Build and verify local release artifacts.")
app.add_typer(list_app, name="list")
app.add_typer(packs_app, name="packs")
app.add_typer(registry_app, name="registry")
app.add_typer(release_app, name="release")


def _echo_json(payload: object) -> None:
    """Render one payload as stable JSON for the CLI."""

    typer.echo(json.dumps(payload, indent=2))


def _exit_with_cli_error(exc: Exception) -> None:
    """Render one operator-facing CLI error and stop the command."""

    typer.echo(str(exc), err=True)
    raise typer.Exit(code=1) from exc


def _exit_with_configuration_error(exc: Exception) -> None:
    """Render one configuration/runtime error and stop the CLI command."""

    _exit_with_cli_error(exc)


def _installer(*, registry_url: str | None = None) -> PackInstaller:
    settings = get_settings()
    return PackInstaller(
        settings.storage_root,
        registry_url=registry_url or settings.registry_url,
        runtime_target=settings.runtime_target,
        metadata_backend=settings.metadata_backend,
        database_url=settings.database_url,
    )


def _echo_pack_listing(*, mode: str, packs: list[dict[str, object]], registry_url: str | None = None) -> None:
    """Render one installed or available pack listing."""

    payload: dict[str, object] = {
        "mode": mode,
        "pack_ids": [str(pack["pack_id"]) for pack in packs],
        "packs": packs,
    }
    if registry_url is not None:
        payload["registry_url"] = registry_url
    _echo_json(payload)


def _render_pack_listing(
    *,
    available: bool,
    active_only: bool,
    registry_url: str | None,
) -> None:
    """Render either an installed-pack or available-pack listing."""

    if available:
        if active_only:
            raise typer.BadParameter("--active-only cannot be used with available pack listings.")
        try:
            packs = [
                pack.model_dump(mode="json")
                for pack in api_list_available_packs(registry_url=registry_url)
            ]
            effective_registry_url = registry_url or get_settings().registry_url
        except FileNotFoundError as exc:
            _exit_with_configuration_error(exc)
        except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
            _exit_with_configuration_error(exc)
        _echo_pack_listing(
            mode="available",
            packs=packs,
            registry_url=effective_registry_url,
        )
        return

    if registry_url is not None:
        raise typer.BadParameter("--registry-url can only be used with available pack listings.")
    try:
        packs = [
            pack.model_dump(mode="json")
            for pack in api_list_packs(active_only=active_only)
        ]
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
        _exit_with_configuration_error(exc)
    _echo_pack_listing(mode="installed", packs=packs)


@app.command()
def status() -> None:
    """Show the local ades runtime status."""

    try:
        payload = api_status()
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
        _exit_with_configuration_error(exc)
    _echo_json(payload.model_dump(mode="json"))


@packs_app.command("list")
def packs_list(
    available: bool = typer.Option(False, "--available", help="List registry packs instead."),
    active_only: bool = typer.Option(False, "--active-only", help="List only active installed packs."),
    registry_url: str | None = typer.Option(
        None,
        "--registry-url",
        help="Override the registry URL or file path when listing --available packs.",
    ),
) -> None:
    """List installed packs or available registry packs."""

    _render_pack_listing(
        available=available,
        active_only=active_only,
        registry_url=registry_url,
    )


@list_app.command("packs")
def list_packs_alias(
    installed: bool = typer.Option(
        False,
        "--installed",
        help="List installed packs instead of available registry packs.",
    ),
    active_only: bool = typer.Option(
        False,
        "--active-only",
        help="List only active installed packs when using --installed.",
    ),
    registry_url: str | None = typer.Option(
        None,
        "--registry-url",
        help="Override the registry URL or file path for available-pack listings.",
    ),
) -> None:
    """List packs with available registry packs as the default view."""

    _render_pack_listing(
        available=not installed,
        active_only=active_only,
        registry_url=registry_url,
    )


@packs_app.command("activate")
def packs_activate(pack: str) -> None:
    """Activate an installed pack."""

    try:
        result = api_activate_pack(pack)
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
        _exit_with_configuration_error(exc)
    if result is None:
        raise typer.Exit(code=1)
    _echo_json(result.model_dump(mode="json"))


@packs_app.command("deactivate")
def packs_deactivate(pack: str) -> None:
    """Deactivate an installed pack."""

    try:
        result = api_deactivate_pack(pack)
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
        _exit_with_configuration_error(exc)
    if result is None:
        raise typer.Exit(code=1)
    _echo_json(result.model_dump(mode="json"))


@packs_app.command("remove")
def packs_remove(pack: str) -> None:
    """Remove an installed pack."""

    try:
        result = api_remove_pack(pack)
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
        _exit_with_configuration_error(exc)
    if result is None:
        raise typer.Exit(code=1)
    _echo_json(result.model_dump(mode="json"))


@packs_app.command("lookup")
def packs_lookup(
    query: str,
    pack: str = typer.Option(None, help="Restrict lookup to a single pack."),
    exact_alias: bool = typer.Option(False, "--exact-alias", help="Match alias text exactly."),
    active_only: bool = typer.Option(True, "--active-only/--include-inactive", help="Search only active packs by default."),
    limit: int = typer.Option(20, min=1, max=100, help="Maximum number of candidates to return."),
) -> None:
    """Search deterministic alias and rule metadata from the local SQLite store."""

    try:
        response = api_lookup_candidates(
            query,
            pack_id=pack,
            exact_alias=exact_alias,
            active_only=active_only,
            limit=limit,
        )
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
        _exit_with_configuration_error(exc)
    _echo_json(response.model_dump(mode="json"))


@app.command()
def pull(
    pack: str,
    registry_url: str | None = typer.Option(
        None,
        "--registry-url",
        help="Override the registry URL or file path for this pull.",
    ),
) -> None:
    """Install a pack and any required dependencies."""

    try:
        result = api_pull_pack(pack, registry_url=registry_url)
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except UnsupportedRuntimeConfigurationError as exc:
        _exit_with_configuration_error(exc)
    except InvalidConfigurationError as exc:
        _exit_with_configuration_error(exc)
    _echo_json(
        {
            "requested_pack": result.requested_pack,
            "registry_url": result.registry_url,
            "installed": result.installed,
            "skipped": result.skipped,
        }
    )


@registry_app.command("build")
def registry_build(
    pack_dirs: list[Path] = typer.Argument(..., help="Local pack directories to publish."),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where the static registry should be written.",
    ),
) -> None:
    """Build a static file-based registry from local pack directories."""

    try:
        response = api_build_registry(pack_dirs, output_dir=output_dir)
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("generate-pack")
def registry_generate_pack(
    bundle_dir: Path = typer.Argument(..., help="Normalized bundle directory with bundle.json."),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where the generated pack should be written.",
    ),
    version: str | None = typer.Option(
        None,
        "--version",
        help="Override the generated pack version instead of using bundle.json.",
    ),
    include_build_metadata: bool = typer.Option(
        True,
        "--build-metadata/--no-build-metadata",
        help="Write sources.json and build.json sidecars for generation provenance.",
    ),
    include_build_only: bool = typer.Option(
        False,
        "--include-build-only",
        help="Include records marked build_only in the generated runtime pack.",
    ),
) -> None:
    """Generate one runtime-compatible pack directory from a normalized source bundle."""

    try:
        response = api_generate_pack_source(
            bundle_dir,
            output_dir=output_dir,
            version=version,
            include_build_metadata=include_build_metadata,
            include_build_only=include_build_only,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("report-pack")
def registry_report_pack(
    bundle_dir: Path = typer.Argument(..., help="Normalized bundle directory with bundle.json."),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where the generated pack should be written for reporting.",
    ),
    version: str | None = typer.Option(
        None,
        "--version",
        help="Override the generated pack version instead of using bundle.json.",
    ),
    include_build_metadata: bool = typer.Option(
        True,
        "--build-metadata/--no-build-metadata",
        help="Write sources.json and build.json sidecars for generation provenance.",
    ),
    include_build_only: bool = typer.Option(
        False,
        "--include-build-only",
        help="Include records marked build_only in the generated runtime pack.",
    ),
) -> None:
    """Generate one pack directory and report stable output statistics."""

    try:
        response = api_report_generated_pack(
            bundle_dir,
            output_dir=output_dir,
            version=version,
            include_build_metadata=include_build_metadata,
            include_build_only=include_build_only,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("refresh-generated-packs")
def registry_refresh_generated_packs(
    bundle_dirs: list[Path] = typer.Argument(
        ...,
        help="One or more normalized bundle directories with bundle.json.",
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where report, quality, and registry refresh output should be written.",
    ),
    general_bundle_dir: Path | None = typer.Option(
        None,
        "--general-bundle-dir",
        help="Optional general-en bundle directory for medical pack refresh when it is not in the positional bundle list.",
    ),
    min_expected_recall: float = typer.Option(
        1.0,
        "--min-expected-recall",
        min=0.0,
        max=1.0,
        help="Minimum acceptable recall across expected fixture cases.",
    ),
    max_unexpected_hits: int = typer.Option(
        0,
        "--max-unexpected-hits",
        min=0,
        help="Maximum unexpected tagged entities allowed across fixture cases.",
    ),
    max_ambiguous_aliases: int | None = typer.Option(
        None,
        "--max-ambiguous-aliases",
        min=0,
        help="Maximum ambiguous alias keys allowed after generation. If omitted, ades uses the pack-specific default quality budget.",
    ),
    max_dropped_alias_ratio: float = typer.Option(
        0.5,
        "--max-dropped-alias-ratio",
        min=0.0,
        max=1.0,
        help="Maximum acceptable dropped-alias ratio after normalization and pruning.",
    ),
) -> None:
    """Refresh one or more generated packs into a quality-gated registry release."""

    try:
        response = api_refresh_generated_packs(
            bundle_dirs,
            output_dir=output_dir,
            general_bundle_dir=general_bundle_dir,
            min_expected_recall=min_expected_recall,
            max_unexpected_hits=max_unexpected_hits,
            max_ambiguous_aliases=max_ambiguous_aliases,
            max_dropped_alias_ratio=max_dropped_alias_ratio,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))
    if not response.passed:
        raise typer.Exit(code=1)


@registry_app.command("validate-finance-quality")
def registry_validate_finance_quality(
    bundle_dir: Path = typer.Argument(..., help="Normalized finance bundle directory with bundle.json."),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where generated pack, registry, and install validation output should be written.",
    ),
    version: str | None = typer.Option(
        None,
        "--version",
        help="Override the generated pack version instead of using bundle.json.",
    ),
    min_expected_recall: float = typer.Option(
        1.0,
        "--min-expected-recall",
        min=0.0,
        max=1.0,
        help="Minimum acceptable recall across expected finance fixtures.",
    ),
    max_unexpected_hits: int = typer.Option(
        0,
        "--max-unexpected-hits",
        min=0,
        help="Maximum unexpected tagged entities allowed across fixture cases.",
    ),
    max_ambiguous_aliases: int = typer.Option(
        300,
        "--max-ambiguous-aliases",
        min=0,
        help="Maximum ambiguous alias keys allowed after generation.",
    ),
    max_dropped_alias_ratio: float = typer.Option(
        0.5,
        "--max-dropped-alias-ratio",
        min=0.0,
        max=1.0,
        help="Maximum acceptable dropped-alias ratio after normalization and pruning.",
    ),
) -> None:
    """Build, install, and evaluate one generated `finance-en` pack bundle."""

    try:
        response = api_validate_finance_pack_quality(
            bundle_dir,
            output_dir=output_dir,
            version=version,
            min_expected_recall=min_expected_recall,
            max_unexpected_hits=max_unexpected_hits,
            max_ambiguous_aliases=max_ambiguous_aliases,
            max_dropped_alias_ratio=max_dropped_alias_ratio,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))
    if not response.passed:
        raise typer.Exit(code=1)


@registry_app.command("validate-general-quality")
def registry_validate_general_quality(
    bundle_dir: Path = typer.Argument(..., help="Normalized general bundle directory with bundle.json."),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where generated pack, registry, and install validation output should be written.",
    ),
    version: str | None = typer.Option(
        None,
        "--version",
        help="Override the generated pack version instead of using bundle.json.",
    ),
    min_expected_recall: float = typer.Option(
        1.0,
        "--min-expected-recall",
        min=0.0,
        max=1.0,
        help="Minimum acceptable recall across expected general fixtures.",
    ),
    max_unexpected_hits: int = typer.Option(
        0,
        "--max-unexpected-hits",
        min=0,
        help="Maximum unexpected tagged entities allowed across fixture cases.",
    ),
    max_ambiguous_aliases: int = typer.Option(
        25,
        "--max-ambiguous-aliases",
        min=0,
        help="Maximum ambiguous alias keys allowed after generation.",
    ),
    max_dropped_alias_ratio: float = typer.Option(
        0.5,
        "--max-dropped-alias-ratio",
        min=0.0,
        max=1.0,
        help="Maximum acceptable dropped-alias ratio after normalization and pruning.",
    ),
) -> None:
    """Build, install, and evaluate one generated `general-en` pack bundle."""

    try:
        response = api_validate_general_pack_quality(
            bundle_dir,
            output_dir=output_dir,
            version=version,
            min_expected_recall=min_expected_recall,
            max_unexpected_hits=max_unexpected_hits,
            max_ambiguous_aliases=max_ambiguous_aliases,
            max_dropped_alias_ratio=max_dropped_alias_ratio,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))
    if not response.passed:
        raise typer.Exit(code=1)


@registry_app.command("validate-medical-quality")
def registry_validate_medical_quality(
    bundle_dir: Path = typer.Argument(..., help="Normalized medical bundle directory with bundle.json."),
    general_bundle_dir: Path = typer.Option(
        ...,
        "--general-bundle-dir",
        help="Normalized general bundle directory used to satisfy the medical pack dependency.",
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where generated packs, registry, and install validation output should be written.",
    ),
    version: str | None = typer.Option(
        None,
        "--version",
        help="Override the generated medical pack version instead of using bundle.json.",
    ),
    min_expected_recall: float = typer.Option(
        1.0,
        "--min-expected-recall",
        min=0.0,
        max=1.0,
        help="Minimum acceptable recall across expected medical fixtures.",
    ),
    max_unexpected_hits: int = typer.Option(
        0,
        "--max-unexpected-hits",
        min=0,
        help="Maximum unexpected tagged entities allowed across fixture cases.",
    ),
    max_ambiguous_aliases: int = typer.Option(
        25,
        "--max-ambiguous-aliases",
        min=0,
        help="Maximum ambiguous alias keys allowed after generation.",
    ),
    max_dropped_alias_ratio: float = typer.Option(
        0.5,
        "--max-dropped-alias-ratio",
        min=0.0,
        max=1.0,
        help="Maximum acceptable dropped-alias ratio after normalization and pruning.",
    ),
) -> None:
    """Build, install, and evaluate one generated `medical-en` pack bundle."""

    try:
        response = api_validate_medical_pack_quality(
            bundle_dir,
            general_bundle_dir=general_bundle_dir,
            output_dir=output_dir,
            version=version,
            min_expected_recall=min_expected_recall,
            max_unexpected_hits=max_unexpected_hits,
            max_ambiguous_aliases=max_ambiguous_aliases,
            max_dropped_alias_ratio=max_dropped_alias_ratio,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))
    if not response.passed:
        raise typer.Exit(code=1)


@registry_app.command("fetch-finance-sources")
def registry_fetch_finance_sources(
    output_dir: Path = typer.Option(
        Path("/mnt/githubActions/ades_big_data/pack_sources/raw/finance-en"),
        "--output-dir",
        help="Directory where immutable finance source snapshots should be written.",
    ),
    snapshot: str | None = typer.Option(
        None,
        "--snapshot",
        help="Snapshot date in YYYY-MM-DD format. Defaults to today.",
    ),
    sec_url: str = typer.Option(
        "https://www.sec.gov/files/company_tickers.json",
        "--sec-url",
        help="URL or file path for the SEC company_tickers snapshot.",
    ),
    symbol_directory_url: str = typer.Option(
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt",
        "--symbol-directory-url",
        help="URL or file path for the Nasdaq symbol-directory snapshot.",
    ),
    user_agent: str = typer.Option(
        "ades/0.1.0 (ops@adestool.com)",
        "--user-agent",
        help="User-Agent header for HTTP source fetches.",
    ),
) -> None:
    """Download one real finance source snapshot set under the big-data root."""

    try:
        response = api_fetch_finance_source_snapshot(
            output_dir=output_dir,
            snapshot=snapshot,
            sec_companies_url=sec_url,
            symbol_directory_url=symbol_directory_url,
            user_agent=user_agent,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("build-finance-bundle")
def registry_build_finance_bundle(
    sec_company_tickers: Path = typer.Option(
        ...,
        "--sec-company-tickers",
        help="Path to the SEC company_tickers snapshot JSON file.",
    ),
    symbol_directory: Path = typer.Option(
        ...,
        "--symbol-directory",
        help="Path to the symbol-directory delimited file.",
    ),
    curated_entities: Path = typer.Option(
        ...,
        "--curated-entities",
        help="Path to the curated finance entities JSON or JSONL file.",
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where the normalized finance bundle should be written.",
    ),
    version: str = typer.Option(
        "0.2.0",
        "--version",
        help="Pack version to record in the generated bundle manifest.",
    ),
) -> None:
    """Build one normalized finance source bundle from raw snapshot files."""

    try:
        response = api_build_finance_source_bundle(
            sec_companies_path=sec_company_tickers,
            symbol_directory_path=symbol_directory,
            curated_entities_path=curated_entities,
            output_dir=output_dir,
            version=version,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("build-general-bundle")
def registry_build_general_bundle(
    wikidata_entities: Path = typer.Option(
        ...,
        "--wikidata-entities",
        help="JSON or JSONL snapshot of operator-filtered Wikidata-style general entities.",
    ),
    geonames_places: Path = typer.Option(
        ...,
        "--geonames-places",
        help="Delimited GeoNames-style place snapshot file.",
    ),
    curated_entities: Path = typer.Option(
        ...,
        "--curated-entities",
        help="JSON or JSONL snapshot of curated general entities.",
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where the normalized bundle should be written.",
    ),
    version: str = typer.Option(
        "0.2.0",
        "--version",
        help="Version recorded in the generated bundle manifest.",
    ),
) -> None:
    """Build one normalized `general-en` source bundle from raw snapshot files."""

    try:
        response = api_build_general_source_bundle(
            wikidata_entities_path=wikidata_entities,
            geonames_places_path=geonames_places,
            curated_entities_path=curated_entities,
            output_dir=output_dir,
            version=version,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("fetch-general-sources")
def registry_fetch_general_sources(
    output_dir: Path = typer.Option(
        Path("/mnt/githubActions/ades_big_data/pack_sources/raw/general-en"),
        "--output-dir",
        help="Directory where immutable general source snapshots should be written.",
    ),
    snapshot: str | None = typer.Option(
        None,
        "--snapshot",
        help="Snapshot date in YYYY-MM-DD format. Defaults to today.",
    ),
    wikidata_url: str = typer.Option(
        "https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q265852|Q2283|Q95|Q3884|Q380&languages=en&props=labels|aliases&format=json",
        "--wikidata-url",
        help="URL or file path for the bounded Wikidata general-entity snapshot.",
    ),
    geonames_url: str = typer.Option(
        "https://download.geonames.org/export/dump/cities15000.zip",
        "--geonames-url",
        help="URL or file path for the GeoNames cities15000 snapshot zip.",
    ),
    user_agent: str = typer.Option(
        "ades/0.1.0 (ops@adestool.com)",
        "--user-agent",
        help="User-Agent header for HTTP source fetches.",
    ),
) -> None:
    """Download one real general source snapshot set under the big-data root."""

    try:
        response = api_fetch_general_source_snapshot(
            output_dir=output_dir,
            snapshot=snapshot,
            wikidata_url=wikidata_url,
            geonames_places_url=geonames_url,
            user_agent=user_agent,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("fetch-medical-sources")
def registry_fetch_medical_sources(
    output_dir: Path = typer.Option(
        Path("/mnt/githubActions/ades_big_data/pack_sources/raw/medical-en"),
        "--output-dir",
        help="Directory where immutable medical source snapshots should be written.",
    ),
    snapshot: str | None = typer.Option(
        None,
        "--snapshot",
        help="Snapshot date in YYYY-MM-DD format. Defaults to today.",
    ),
    disease_ontology_url: str = typer.Option(
        "https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/main/src/ontology/doid.obo",
        "--disease-ontology-url",
        help="URL or file path for the Disease Ontology source file.",
    ),
    hgnc_genes_url: str = typer.Option(
        "https://storage.googleapis.com/public-download-files/hgnc/json/json/hgnc_complete_set.json",
        "--hgnc-genes-url",
        help="URL or file path for the HGNC complete-set source JSON.",
    ),
    uniprot_proteins_url: str = typer.Option(
        "https://rest.uniprot.org/uniprotkb/search?query=%28reviewed%3Atrue%20AND%20organism_id%3A9606%29&format=json&fields=accession%2Cprotein_name%2Cgene_names&size=500",
        "--uniprot-proteins-url",
        help="URL or file path for the UniProt reviewed-protein source JSON.",
    ),
    clinical_trials_url: str = typer.Option(
        "https://clinicaltrials.gov/api/v2/studies?query.cond=diabetes&pageSize=200&format=json",
        "--clinical-trials-url",
        help="URL or file path for the ClinicalTrials.gov study source JSON.",
    ),
    user_agent: str = typer.Option(
        "ades/0.1.0 (ops@adestool.com)",
        "--user-agent",
        help="User-Agent header for HTTP source fetches.",
    ),
    uniprot_max_records: int = typer.Option(
        2000,
        "--uniprot-max-records",
        min=1,
        help="Maximum number of UniProt protein records to keep in one snapshot.",
    ),
    clinical_trials_max_records: int = typer.Option(
        500,
        "--clinical-trials-max-records",
        min=1,
        help="Maximum number of ClinicalTrials.gov studies to keep in one snapshot.",
    ),
) -> None:
    """Download one real medical source snapshot set under the big-data root."""

    try:
        response = api_fetch_medical_source_snapshot(
            output_dir=output_dir,
            snapshot=snapshot,
            disease_ontology_url=disease_ontology_url,
            hgnc_genes_url=hgnc_genes_url,
            uniprot_proteins_url=uniprot_proteins_url,
            clinical_trials_url=clinical_trials_url,
            user_agent=user_agent,
            uniprot_max_records=uniprot_max_records,
            clinical_trials_max_records=clinical_trials_max_records,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("build-medical-bundle")
def registry_build_medical_bundle(
    disease_ontology: Path = typer.Option(
        ...,
        "--disease-ontology",
        help="JSON or JSONL snapshot of disease ontology terms.",
    ),
    hgnc_genes: Path = typer.Option(
        ...,
        "--hgnc-genes",
        help="JSON or JSONL snapshot of HGNC gene symbols and aliases.",
    ),
    uniprot_proteins: Path = typer.Option(
        ...,
        "--uniprot-proteins",
        help="JSON or JSONL snapshot of UniProt reviewed protein names.",
    ),
    clinical_trials: Path = typer.Option(
        ...,
        "--clinical-trials",
        help="JSON or JSONL snapshot of ClinicalTrials.gov study records.",
    ),
    curated_entities: Path = typer.Option(
        ...,
        "--curated-entities",
        help="JSON or JSONL snapshot of curated medical entities.",
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where the normalized medical bundle should be written.",
    ),
    version: str = typer.Option(
        "0.2.0",
        "--version",
        help="Version recorded in the generated bundle manifest.",
    ),
) -> None:
    """Build one normalized `medical-en` source bundle from raw snapshot files."""

    try:
        response = api_build_medical_source_bundle(
            disease_ontology_path=disease_ontology,
            hgnc_genes_path=hgnc_genes,
            uniprot_proteins_path=uniprot_proteins,
            clinical_trials_path=clinical_trials,
            curated_entities_path=curated_entities,
            output_dir=output_dir,
            version=version,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@release_app.command("verify")
def release_verify(
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where verified Python and npm artifacts should be written.",
    ),
    no_clean: bool = typer.Option(
        False,
        "--no-clean",
        help="Keep any existing release artifacts under the output directory.",
    ),
    smoke_install: bool = typer.Option(
        True,
        "--smoke-install/--no-smoke-install",
        help="Run clean-environment install smoke checks for the built wheel and npm tarball.",
    ),
) -> None:
    """Build and verify the current local Python and npm release artifacts."""

    try:
        response = api_verify_release(
            output_dir=output_dir,
            clean=not no_clean,
            smoke_install=smoke_install,
        )
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@release_app.command("validate")
def release_validate(
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where test-validated release artifacts should be written.",
    ),
    manifest_output: Path | None = typer.Option(
        None,
        "--manifest-output",
        help="Write the release manifest JSON to this explicit file path.",
    ),
    version: str | None = typer.Option(
        None,
        "--version",
        help="Optionally synchronize release versions before generating the manifest.",
    ),
    test_command: list[str] = typer.Option(
        None,
        "--test-command",
        help="Override the test command by repeating this option for each argument.",
    ),
    no_clean: bool = typer.Option(
        False,
        "--no-clean",
        help="Keep any existing release artifacts under the output directory.",
    ),
    smoke_install: bool = typer.Option(
        True,
        "--smoke-install/--no-smoke-install",
        help="Run clean-environment install smoke checks for the built wheel and npm tarball.",
    ),
) -> None:
    """Run tests, then build and persist one coordinated local release manifest."""

    try:
        response = api_validate_release(
            output_dir=output_dir,
            manifest_path=manifest_output,
            version=version,
            clean=not no_clean,
            smoke_install=smoke_install,
            tests_command=test_command or None,
        )
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))
    if not response.overall_success:
        raise typer.Exit(code=1)


@release_app.command("publish")
def release_publish(
    manifest_path: Path = typer.Option(
        ...,
        "--manifest-path",
        help="Validated release manifest created by `ades release validate`.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show coordinated publish commands without executing them.",
    ),
) -> None:
    """Publish one validated release manifest to Python and npm registries."""

    try:
        response = api_publish_release(
            manifest_path=manifest_path,
            dry_run=dry_run,
        )
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))
    if not response.overall_success:
        raise typer.Exit(code=1)


@release_app.command("versions")
def release_versions() -> None:
    """Show the current coordinated release version state."""

    try:
        response = api_release_versions()
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@release_app.command("sync-version")
def release_sync_version(version: str = typer.Argument(..., help="Target release version.")) -> None:
    """Synchronize Python and npm release versions to one target."""

    try:
        response = api_sync_release_version(version)
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@release_app.command("manifest")
def release_manifest(
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where release artifacts and the manifest should be written.",
    ),
    manifest_output: Path | None = typer.Option(
        None,
        "--manifest-output",
        help="Write the release manifest JSON to this explicit file path.",
    ),
    version: str | None = typer.Option(
        None,
        "--version",
        help="Optionally synchronize release versions before generating the manifest.",
    ),
    no_clean: bool = typer.Option(
        False,
        "--no-clean",
        help="Keep any existing release artifacts under the output directory.",
    ),
    smoke_install: bool = typer.Option(
        True,
        "--smoke-install/--no-smoke-install",
        help="Run clean-environment install smoke checks for the built wheel and npm tarball.",
    ),
) -> None:
    """Build artifacts and persist one coordinated release manifest."""

    try:
        response = api_write_release_manifest(
            output_dir=output_dir,
            manifest_path=manifest_output,
            version=version,
            clean=not no_clean,
            smoke_install=smoke_install,
        )
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@app.command()
def tag(
    text: str | None = typer.Argument(None, help="Inline text to tag."),
    file: Path | None = typer.Option(None, "--file", help="Tag a local file instead of inline text."),
    pack: str | None = typer.Option(None, help="Pack id, for example finance-en."),
    content_type: str | None = typer.Option(None, help="Override the input content type."),
    output: Path | None = typer.Option(None, "--output", help="Write JSON output to this file path."),
    output_dir: Path | None = typer.Option(
        None,
        "--output-dir",
        help="Write JSON output to a generated pack-aware filename in this directory.",
    ),
    compact_output: bool = typer.Option(
        False,
        "--compact-output",
        help="Persist compact JSON instead of pretty-printed JSON.",
    ),
) -> None:
    """Tag inline text or a local file through the local pipeline."""

    if text is None and file is None:
        raise typer.BadParameter("Provide inline text or use --file.")
    if text is not None and file is not None:
        raise typer.BadParameter("Use inline text or --file, not both.")
    if output is not None and output_dir is not None:
        raise typer.BadParameter("Use --output or --output-dir, not both.")

    if file is not None:
        response = api_tag_file(
            file,
            pack=pack,
            content_type=content_type,
            output_path=output,
            output_dir=output_dir,
            pretty_output=not compact_output,
        )
    else:
        response = api_tag(
            text,
            pack=pack,
            content_type=content_type or "text/plain",
            output_path=output,
            output_dir=output_dir,
            pretty_output=not compact_output,
        )
    _echo_json(response.model_dump(mode="json"))


@app.command("tag-files")
def tag_files(
    files: list[Path] = typer.Argument(None, help="Local file paths to tag."),
    pack: str | None = typer.Option(None, help="Pack id, for example finance-en."),
    content_type: str | None = typer.Option(None, help="Override the input content type."),
    directories: list[Path] = typer.Option(
        None,
        "--directory",
        help="Directory to scan for local files. Can be provided multiple times.",
    ),
    glob_patterns: list[str] = typer.Option(
        None,
        "--glob",
        help="Glob pattern to expand into local file paths. Can be provided multiple times.",
    ),
    manifest_input: Path | None = typer.Option(
        None,
        "--manifest-input",
        help="Replay or resume a corpus run from this saved batch manifest artifact.",
    ),
    manifest_mode: str = typer.Option(
        "resume",
        "--manifest-mode",
        help="Manifest replay mode: resume, processed, or all.",
    ),
    skip_unchanged: bool = typer.Option(
        False,
        "--skip-unchanged",
        help="When used with --manifest-input and explicit sources, skip files whose source fingerprint matches the saved manifest.",
    ),
    reuse_unchanged_outputs: bool = typer.Option(
        False,
        "--reuse-unchanged-outputs",
        help="When skipping unchanged files, carry forward their prior saved output metadata from the manifest.",
    ),
    repair_missing_reused_outputs: bool = typer.Option(
        False,
        "--repair-missing-reused-outputs",
        help="When reusing unchanged outputs, regenerate missing JSON artifacts instead of only warning.",
    ),
    include_patterns: list[str] = typer.Option(
        None,
        "--include",
        help="Include only discovered files whose basename or full path matches this glob. Can be provided multiple times.",
    ),
    exclude_patterns: list[str] = typer.Option(
        None,
        "--exclude",
        help="Exclude discovered files whose basename or full path matches this glob. Can be provided multiple times.",
    ),
    max_files: int | None = typer.Option(
        None,
        "--max-files",
        min=0,
        help="Maximum number of filtered files to process before the remaining inputs are skipped.",
    ),
    max_input_bytes: int | None = typer.Option(
        None,
        "--max-bytes",
        min=0,
        help="Maximum cumulative input bytes to process before the remaining inputs are skipped.",
    ),
    non_recursive: bool = typer.Option(
        False,
        "--non-recursive",
        help="Scan provided directories without descending into subdirectories.",
    ),
    output_dir: Path | None = typer.Option(
        None,
        "--output-dir",
        help="Write JSON outputs to generated pack-aware filenames in this directory.",
    ),
    write_manifest: bool = typer.Option(
        False,
        "--write-manifest",
        help="Write a stable run manifest JSON file for this batch alongside per-file outputs.",
    ),
    manifest_output: Path | None = typer.Option(
        None,
        "--manifest-output",
        help="Write the batch run manifest JSON to this explicit file path.",
    ),
    compact_output: bool = typer.Option(
        False,
        "--compact-output",
        help="Persist compact JSON instead of pretty-printed JSON.",
    ),
) -> None:
    """Tag multiple local files through the local pipeline."""

    if not files and not directories and not glob_patterns and manifest_input is None:
        raise typer.BadParameter("Provide file paths, --directory, --glob, or --manifest-input.")
    if skip_unchanged and manifest_input is None:
        raise typer.BadParameter("Use --manifest-input when enabling --skip-unchanged.")
    if reuse_unchanged_outputs and manifest_input is None:
        raise typer.BadParameter("Use --manifest-input when enabling --reuse-unchanged-outputs.")
    if reuse_unchanged_outputs and not skip_unchanged:
        raise typer.BadParameter("Use --skip-unchanged when enabling --reuse-unchanged-outputs.")
    if repair_missing_reused_outputs and manifest_input is None:
        raise typer.BadParameter("Use --manifest-input when enabling --repair-missing-reused-outputs.")
    if repair_missing_reused_outputs and not reuse_unchanged_outputs:
        raise typer.BadParameter(
            "Use --reuse-unchanged-outputs when enabling --repair-missing-reused-outputs."
        )
    if (write_manifest or manifest_output is not None) and output_dir is None:
        raise typer.BadParameter("Use --output-dir when writing a batch manifest.")
    response = api_tag_files(
        files or [],
        pack=pack,
        content_type=content_type,
        output_dir=output_dir,
        pretty_output=not compact_output,
        directories=directories or [],
        glob_patterns=glob_patterns or [],
        manifest_input_path=manifest_input,
        manifest_replay_mode=manifest_mode,
        skip_unchanged=skip_unchanged,
        reuse_unchanged_outputs=reuse_unchanged_outputs,
        repair_missing_reused_outputs=repair_missing_reused_outputs,
        recursive=not non_recursive,
        include_patterns=include_patterns or [],
        exclude_patterns=exclude_patterns or [],
        max_files=max_files,
        max_input_bytes=max_input_bytes,
        write_manifest=write_manifest,
        manifest_output_path=manifest_output,
    )
    _echo_json(response.model_dump(mode="json"))


@app.command()
def serve(
    host: str = typer.Option(None, help="Override the bind host."),
    port: int = typer.Option(None, help="Override the bind port."),
    reload: bool = typer.Option(False, help="Enable auto-reload for development."),
) -> None:
    """Run the configured ades FastAPI service."""

    import uvicorn

    settings = get_settings()
    layout = ensure_storage_layout(build_storage_layout(settings.storage_root))
    typer.echo(f"Using storage root: {layout.storage_root}")
    uvicorn.run(
        "ades.api:create_service_app",
        host=host or settings.host,
        port=port or settings.port,
        factory=True,
        reload=reload,
    )


def main() -> None:
    """Entrypoint used by the console script."""

    app()
