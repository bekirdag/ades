"""Command line interface for ades."""

from __future__ import annotations

from contextlib import ExitStack
import json
from pathlib import Path

import click
import httpx
import typer
from typer.main import get_command

from .api import activate_pack as api_activate_pack
from .api import benchmark_runtime as api_benchmark_runtime
from .api import benchmark_matcher_backends as api_benchmark_matcher_backends
from .api import build_finance_source_bundle as api_build_finance_source_bundle
from .api import build_finance_country_source_bundles as api_build_finance_country_source_bundles
from .api import build_general_source_bundle as api_build_general_source_bundle
from .api import build_medical_source_bundle as api_build_medical_source_bundle
from .api import build_registry as api_build_registry
from .api import compare_extraction_quality_reports as api_compare_extraction_quality_reports
from .api import deactivate_pack as api_deactivate_pack
from .api import diff_pack_versions as api_diff_pack_versions
from .api import evaluate_extraction_quality as api_evaluate_extraction_quality
from .api import evaluate_live_news_feedback as api_evaluate_live_news_feedback
from .api import evaluate_extraction_release_thresholds as api_evaluate_extraction_release_thresholds
from .api import fetch_finance_source_snapshot as api_fetch_finance_source_snapshot
from .api import fetch_finance_country_source_snapshots as api_fetch_finance_country_source_snapshots
from .api import fetch_general_source_snapshot as api_fetch_general_source_snapshot
from .api import fetch_medical_source_snapshot as api_fetch_medical_source_snapshot
from .api import generate_pack_source as api_generate_pack_source
from .api import get_pack_health as api_get_pack_health
from .api import lookup_candidates as api_lookup_candidates
from .api import list_available_packs as api_list_available_packs
from .api import list_packs as api_list_packs
from .api import prepare_registry_deploy_release as api_prepare_registry_deploy_release
from .api import publish_generated_registry_release as api_publish_generated_registry_release
from .api import publish_release as api_publish_release
from .api import pull_pack as api_pull_pack
from .api import refresh_generated_packs as api_refresh_generated_packs
from .api import report_generated_pack as api_report_generated_pack
from .api import remove_pack as api_remove_pack
from .api import release_versions as api_release_versions
from .api import status as api_status
from .api import smoke_test_published_generated_registry as api_smoke_test_published_generated_registry
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
from .packs.installer import InstallResult, PackInstaller
from .packs.quality_defaults import DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES
from .service.client import (
    LocalServiceRequestError,
    LocalServiceUnavailableError,
    should_use_local_service,
    tag_file_via_local_service,
    tag_files_via_local_service,
    tag_via_local_service,
)
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


def _format_cli_cell(
    value: object,
    *,
    empty: str = "-",
    max_width: int | None = None,
) -> str:
    """Render one CLI cell value as compact human-readable text."""

    if value is None:
        text = empty
    elif isinstance(value, bool):
        text = "yes" if value else "no"
    elif isinstance(value, list):
        text = ", ".join(str(item) for item in value) if value else empty
    else:
        text = str(value).strip() or empty
    if max_width is not None and len(text) > max_width:
        if max_width <= 3:
            return text[:max_width]
        return f"{text[: max_width - 3]}..."
    return text


def _render_text_table(headers: list[str], rows: list[list[str]]) -> str:
    """Render one plain-text ASCII table."""

    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def render_row(row: list[str]) -> str:
        return "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row)).rstrip()

    divider = ["-" * width for width in widths]
    lines = [render_row(headers), render_row(divider)]
    lines.extend(render_row(row) for row in rows)
    return "\n".join(lines)


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


def _echo_pack_listing_table(
    *,
    mode: str,
    packs: list[dict[str, object]],
    registry_url: str | None = None,
    active_only: bool = False,
) -> None:
    """Render one installed or available pack listing as a human-readable table."""

    if mode == "available":
        typer.echo(f"Available packs ({len(packs)})")
        if registry_url is not None:
            typer.echo(f"Registry: {registry_url}")
        if not packs:
            typer.echo("No packs are available from the configured registry.")
            return
        headers = [
            "PACK ID",
            "VERSION",
            "DOMAIN",
            "TIER",
            "LANG",
            "DEPENDENCIES",
            "DESCRIPTION",
        ]
        rows = [
            [
                _format_cli_cell(pack.get("pack_id")),
                _format_cli_cell(pack.get("version")),
                _format_cli_cell(pack.get("domain")),
                _format_cli_cell(pack.get("tier")),
                _format_cli_cell(pack.get("language")),
                _format_cli_cell(pack.get("dependencies"), max_width=24),
                _format_cli_cell(pack.get("description"), max_width=52),
            ]
            for pack in packs
        ]
        typer.echo(_render_text_table(headers, rows))
        return

    title = "Installed packs"
    if active_only:
        title += " (active only)"
    title += f" ({len(packs)})"
    typer.echo(title)
    if not packs:
        typer.echo("No packs are installed yet. Run `ades pull general-en` or `ades pull <pack-id>` to add one.")
        return
    headers = [
        "PACK ID",
        "VERSION",
        "DOMAIN",
        "TIER",
        "LANG",
        "ACTIVE",
        "DESCRIPTION",
    ]
    rows = [
        [
            _format_cli_cell(pack.get("pack_id")),
            _format_cli_cell(pack.get("version")),
            _format_cli_cell(pack.get("domain")),
            _format_cli_cell(pack.get("tier")),
            _format_cli_cell(pack.get("language")),
            _format_cli_cell(pack.get("active")),
            _format_cli_cell(pack.get("description"), max_width=52),
        ]
        for pack in packs
    ]
    typer.echo(_render_text_table(headers, rows))


def _echo_pull_summary(result: InstallResult) -> None:
    """Render one human-readable pack pull summary."""

    typer.echo("Pull complete")
    typer.echo(f"Requested pack: {result.requested_pack}")
    typer.echo(f"Registry: {result.registry_url}")
    typer.echo("")
    typer.echo(f"Installed ({len(result.installed)}):")
    if result.installed:
        for pack_id in result.installed:
            typer.echo(f"  {pack_id}")
    else:
        typer.echo("  none")
    typer.echo(f"Skipped ({len(result.skipped)}):")
    if result.skipped:
        for pack_id in result.skipped:
            typer.echo(f"  {pack_id}")
    else:
        typer.echo("  none")


def _echo_pack_health_summary(payload: dict[str, object]) -> None:
    """Render one pack-health response as compact operator-facing tables."""

    summary_rows = [
        ["PACK", _format_cli_cell(payload.get("pack_id"))],
        ["WINDOW", _format_cli_cell(payload.get("requested_window"))],
        ["OBSERVATIONS", _format_cli_cell(payload.get("observation_count"))],
        ["LATEST VERSION", _format_cli_cell(payload.get("latest_pack_version"))],
        ["LATEST OBSERVED", _format_cli_cell(payload.get("latest_observed_at"))],
        ["AVG ENTITIES", _format_cli_cell(f"{float(payload.get('average_entity_count', 0.0)):.2f}")],
        [
            "AVG DENSITY",
            _format_cli_cell(
                f"{float(payload.get('average_entities_per_100_tokens', 0.0)):.2f}"
            ),
        ],
        ["ZERO ENTITY RATE", _format_cli_cell(f"{float(payload.get('zero_entity_rate', 0.0)):.2%}")],
        ["WARNING RATE", _format_cli_cell(f"{float(payload.get('warning_rate', 0.0)):.2%}")],
        [
            "LOW DENSITY RATE",
            _format_cli_cell(
                f"{float(payload.get('low_density_warning_rate', 0.0)):.2%}"
            ),
        ],
        ["P95 LATENCY MS", _format_cli_cell(payload.get("p95_timing_ms"))],
    ]
    typer.echo(_render_text_table(["Metric", "Value"], summary_rows))

    per_label = payload.get("per_label_counts")
    if isinstance(per_label, dict) and per_label:
        label_rows = [
            [_format_cli_cell(label), _format_cli_cell(count)]
            for label, count in sorted(
                per_label.items(),
                key=lambda item: (-int(item[1]), str(item[0]).casefold(), str(item[0])),
            )
        ]
        typer.echo("")
        typer.echo(_render_text_table(["Label", "Count"], label_rows))

    per_lane = payload.get("per_lane_counts")
    if isinstance(per_lane, dict) and per_lane:
        lane_rows = [
            [_format_cli_cell(lane), _format_cli_cell(count)]
            for lane, count in sorted(
                per_lane.items(),
                key=lambda item: (-int(item[1]), str(item[0]).casefold(), str(item[0])),
            )
        ]
        typer.echo("")
        typer.echo(_render_text_table(["Lane", "Count"], lane_rows))


def _render_pack_listing(
    *,
    available: bool,
    active_only: bool,
    registry_url: str | None,
    json_output: bool,
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
        if json_output:
            _echo_pack_listing(
                mode="available",
                packs=packs,
                registry_url=effective_registry_url,
            )
        else:
            _echo_pack_listing_table(
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
    if json_output:
        _echo_pack_listing(mode="installed", packs=packs)
    else:
        _echo_pack_listing_table(mode="installed", packs=packs, active_only=active_only)


def _echo_help(command_path: list[str] | None = None) -> None:
    """Render help for the root CLI or one nested command path."""

    path = [segment.strip() for segment in (command_path or []) if segment.strip()]
    info_parts = ["ades"]
    command: click.Command = get_command(app)
    with ExitStack() as stack:
        context: click.Context = stack.enter_context(click.Context(command, info_name="ades"))
        for segment in path:
            if not isinstance(command, click.Group):
                joined = " ".join(info_parts)
                _exit_with_cli_error(
                    ValueError(f"Command `{joined}` does not have subcommands. Run `ades help` for the root command list.")
                )
            next_command = command.get_command(context, segment)
            if next_command is None:
                joined = " ".join(["ades", *path])
                _exit_with_cli_error(ValueError(f"Unknown command path `{joined}`. Run `ades help` for the available commands."))
            command = next_command
            info_parts.append(segment)
            context = stack.enter_context(click.Context(command, info_name=segment, parent=context))
        typer.echo(command.get_help(context))


def _resolve_tag_pack_or_exit(
    pack: str | None,
    *,
    manifest_input: Path | None = None,
) -> str | None:
    """Resolve one tag pack id and stop the CLI with user-facing guidance when unavailable."""

    try:
        settings = get_settings()
        installed_packs = api_list_packs()
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
        _exit_with_configuration_error(exc)

    resolved_pack = pack
    if resolved_pack is None and manifest_input is not None:
        try:
            manifest_payload = json.loads(manifest_input.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        manifest_pack = manifest_payload.get("pack") if isinstance(manifest_payload, dict) else None
        if isinstance(manifest_pack, str) and manifest_pack.strip():
            resolved_pack = manifest_pack.strip()
    if resolved_pack is None:
        resolved_pack = settings.default_pack
    if not installed_packs:
        _exit_with_cli_error(
            ValueError(
                "No packs are installed yet. Run `ades pull general-en` or `ades pull <pack-id>` before using `ades tag`."
            )
        )

    installed_by_id = {summary.pack_id: summary for summary in installed_packs}
    selected_pack = installed_by_id.get(resolved_pack)
    if selected_pack is None:
        _exit_with_cli_error(
            ValueError(
                f"Pack `{resolved_pack}` is not installed. Run `ades pull {resolved_pack}` first or choose an installed pack with `--pack`."
            )
        )
    if not selected_pack.active:
        _exit_with_cli_error(
            ValueError(
                f"Pack `{resolved_pack}` is installed but inactive. Run `ades packs activate {resolved_pack}` first."
            )
        )
    return resolved_pack


def _tag_text_response(
    text: str,
    *,
    pack: str,
    content_type: str,
    output_path: Path | None,
    output_dir: Path | None,
    pretty_output: bool,
):
    """Run one inline tag request through the preferred runtime path."""

    settings = get_settings()
    if should_use_local_service(settings):
        return tag_via_local_service(
            text,
            pack=pack,
            content_type=content_type,
            output_path=output_path,
            output_dir=output_dir,
            pretty_output=pretty_output,
            settings=settings,
        )
    return api_tag(
        text,
        pack=pack,
        content_type=content_type,
        output_path=output_path,
        output_dir=output_dir,
        pretty_output=pretty_output,
    )


def _tag_file_response(
    path: Path,
    *,
    pack: str,
    content_type: str | None,
    output_path: Path | None,
    output_dir: Path | None,
    pretty_output: bool,
):
    """Run one file tag request through the preferred runtime path."""

    settings = get_settings()
    if should_use_local_service(settings):
        return tag_file_via_local_service(
            path,
            pack=pack,
            content_type=content_type,
            output_path=output_path,
            output_dir=output_dir,
            pretty_output=pretty_output,
            settings=settings,
        )
    return api_tag_file(
        path,
        pack=pack,
        content_type=content_type,
        output_path=output_path,
        output_dir=output_dir,
        pretty_output=pretty_output,
    )


def _tag_files_response(
    files: list[Path],
    *,
    pack: str | None,
    content_type: str | None,
    output_dir: Path | None,
    pretty_output: bool,
    directories: list[Path],
    glob_patterns: list[str],
    manifest_input: Path | None,
    manifest_mode: str,
    skip_unchanged: bool,
    reuse_unchanged_outputs: bool,
    repair_missing_reused_outputs: bool,
    recursive: bool,
    include_patterns: list[str],
    exclude_patterns: list[str],
    max_files: int | None,
    max_input_bytes: int | None,
    write_manifest: bool,
    manifest_output: Path | None,
):
    """Run one batch tag request through the preferred runtime path."""

    settings = get_settings()
    if should_use_local_service(settings):
        return tag_files_via_local_service(
            files,
            pack=pack,
            content_type=content_type,
            output_dir=output_dir,
            pretty_output=pretty_output,
            settings=settings,
            directories=directories,
            glob_patterns=glob_patterns,
            manifest_input_path=manifest_input,
            manifest_replay_mode=manifest_mode,
            skip_unchanged=skip_unchanged,
            reuse_unchanged_outputs=reuse_unchanged_outputs,
            repair_missing_reused_outputs=repair_missing_reused_outputs,
            recursive=recursive,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            max_files=max_files,
            max_input_bytes=max_input_bytes,
            write_manifest=write_manifest,
            manifest_output_path=manifest_output,
        )
    return api_tag_files(
        files,
        pack=pack,
        content_type=content_type,
        output_dir=output_dir,
        pretty_output=pretty_output,
        directories=directories,
        glob_patterns=glob_patterns,
        manifest_input_path=manifest_input,
        manifest_replay_mode=manifest_mode,
        skip_unchanged=skip_unchanged,
        reuse_unchanged_outputs=reuse_unchanged_outputs,
        repair_missing_reused_outputs=repair_missing_reused_outputs,
        recursive=recursive,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
        max_files=max_files,
        max_input_bytes=max_input_bytes,
        write_manifest=write_manifest,
        manifest_output_path=manifest_output,
    )


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


@app.command("help")
def help_command(
    command_path: list[str] | None = typer.Argument(
        None,
        metavar="[COMMAND] [SUBCOMMAND]...",
        help="Optional command path to inspect, for example `ades help list packs`.",
    ),
) -> None:
    """Show help for the CLI or one nested command."""

    _echo_help(command_path)


@packs_app.command("list")
def packs_list(
    available: bool = typer.Option(False, "--available", help="List registry packs instead."),
    active_only: bool = typer.Option(False, "--active-only", help="List only active installed packs."),
    json_output: bool = typer.Option(False, "--json", help="Print JSON instead of a human-readable table."),
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
        json_output=json_output,
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
    json_output: bool = typer.Option(False, "--json", help="Print JSON instead of a human-readable table."),
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
        json_output=json_output,
    )


@list_app.command("installed")
def list_installed_alias(
    active_only: bool = typer.Option(False, "--active-only", help="List only active installed packs."),
    json_output: bool = typer.Option(False, "--json", help="Print JSON instead of a human-readable table."),
) -> None:
    """List installed packs with a human-readable table by default."""

    _render_pack_listing(
        available=False,
        active_only=active_only,
        registry_url=None,
        json_output=json_output,
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
    fuzzy: bool = typer.Option(
        False,
        "--fuzzy",
        help="Use fuzzy operator lookup when the backend supports it.",
    ),
    active_only: bool = typer.Option(True, "--active-only/--include-inactive", help="Search only active packs by default."),
    limit: int = typer.Option(20, min=1, max=100, help="Maximum number of candidates to return."),
) -> None:
    """Search deterministic alias and rule metadata from the configured local store."""

    try:
        response = api_lookup_candidates(
            query,
            pack_id=pack,
            exact_alias=exact_alias,
            fuzzy=fuzzy,
            active_only=active_only,
            limit=limit,
        )
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
        _exit_with_configuration_error(exc)
    _echo_json(response.model_dump(mode="json"))


@packs_app.command("health")
def packs_health(
    pack: str,
    limit: int = typer.Option(
        100,
        "--limit",
        min=1,
        max=1000,
        help="Number of recent observations to summarize.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Print JSON instead of a human-readable summary.",
    ),
) -> None:
    """Summarize recent extraction health for one installed pack."""

    try:
        response = api_get_pack_health(pack, limit=limit)
    except FileNotFoundError as exc:
        _exit_with_configuration_error(exc)
    except (UnsupportedRuntimeConfigurationError, ValueError) as exc:
        _exit_with_configuration_error(exc)
    payload = response.model_dump(mode="json")
    if json_output:
        _echo_json(payload)
        return
    _echo_pack_health_summary(payload)


@app.command()
def pull(
    pack: str,
    json_output: bool = typer.Option(False, "--json", help="Print JSON instead of a human-readable summary."),
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
    if json_output:
        _echo_json(
            {
                "requested_pack": result.requested_pack,
                "registry_url": result.registry_url,
                "installed": result.installed,
                "skipped": result.skipped,
            }
        )
    else:
        _echo_pull_summary(result)


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
    materialize_registry: bool = typer.Option(
        False,
        "--materialize-registry/--candidate-only",
        help="Materialize a static registry from passing candidate bundles instead of stopping after candidate reports and quality output.",
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
            materialize_registry=materialize_registry,
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


@registry_app.command("publish-generated-release")
def registry_publish_generated_release(
    registry_dir: Path = typer.Argument(
        ...,
        help="Reviewed registry directory containing index.json, packs/, and artifacts/.",
    ),
    prefix: str = typer.Option(
        ...,
        "--prefix",
        help="Immutable object-storage prefix where the registry should be uploaded.",
    ),
    bucket: str | None = typer.Option(
        None,
        "--bucket",
        help="Optional object-storage bucket override. Defaults to ADES_PACK_OBJECT_STORAGE_BUCKET.",
    ),
    endpoint: str | None = typer.Option(
        None,
        "--endpoint",
        help="Optional object-storage endpoint override. Defaults to ADES_PACK_OBJECT_STORAGE_ENDPOINT.",
    ),
    region: str = typer.Option(
        "us-east-1",
        "--region",
        help="AWS-compatible region passed to the underlying CLI environment.",
    ),
    delete: bool = typer.Option(
        True,
        "--delete/--no-delete",
        help="Delete objects missing from the local registry during sync.",
    ),
) -> None:
    """Publish one reviewed generated registry release to object storage."""

    try:
        response = api_publish_generated_registry_release(
            registry_dir,
            prefix=prefix,
            bucket=bucket,
            endpoint=endpoint,
            region=region,
            delete=delete,
        )
    except (FileNotFoundError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("prepare-deploy-release")
def registry_prepare_deploy_release(
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        help="Directory where the deploy-owned registry payload should be written.",
    ),
    pack_dirs: list[Path] | None = typer.Option(
        None,
        "--pack-dir",
        help=(
            "Optional local pack directory override. Repeatable. "
            "Defaults to the bundled starter packs when no promotion spec is active."
        ),
    ),
    promotion_spec: Path | None = typer.Option(
        None,
        "--promotion-spec",
        help=(
            "Optional approved promotion spec JSON. Defaults to "
            "src/ades/resources/registry/promoted-release.json when present."
        ),
    ),
) -> None:
    """Prepare the deploy-owned registry payload from bundled packs or a promoted release."""

    try:
        response = api_prepare_registry_deploy_release(
            output_dir=output_dir,
            pack_dirs=pack_dirs or (),
            promotion_spec_path=promotion_spec,
        )
    except (FileNotFoundError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("smoke-published-release")
def registry_smoke_published_release(
    registry_url: str = typer.Argument(
        ...,
        help="Published registry index URL to smoke through clean pull/install/tag checks.",
    ),
    pack_ids: list[str] = typer.Option(
        None,
        "--pack-id",
        help=(
            "Optional pack id to smoke. Repeatable. "
            "Defaults to the supported smoke packs present in the published registry."
        ),
    ),
) -> None:
    """Run clean consumer smoke against one published generated registry URL."""

    try:
        response = api_smoke_test_published_generated_registry(
            registry_url,
            pack_ids=pack_ids,
        )
    except (FileNotFoundError, ValueError) as exc:
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
    fixture_profile: str = typer.Option(
        "benchmark",
        "--fixture-profile",
        help="Finance quality suite to run: benchmark or smoke.",
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
            fixture_profile=fixture_profile,
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
    fixture_profile: str = typer.Option(
        "benchmark",
        "--fixture-profile",
        help="General quality suite to run: benchmark or smoke.",
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
        DEFAULT_GENERAL_MAX_AMBIGUOUS_ALIASES,
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
            fixture_profile=fixture_profile,
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
    fixture_profile: str = typer.Option(
        "benchmark",
        "--fixture-profile",
        help="Medical quality suite to run: benchmark or smoke.",
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
            fixture_profile=fixture_profile,
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


@registry_app.command("evaluate-extraction-quality")
def registry_evaluate_extraction_quality(
    pack_id: str = typer.Argument(..., help="Installed pack id to evaluate."),
    golden_set_path: Path | None = typer.Option(
        None,
        "--golden-set-path",
        help="Optional explicit golden set JSON path. Defaults to the /mnt quality root.",
    ),
    profile: str = typer.Option(
        "default",
        "--profile",
        help="Golden set profile to use when --golden-set-path is omitted.",
    ),
    hybrid: bool = typer.Option(
        False,
        "--hybrid/--deterministic",
        help="Evaluate hybrid mode instead of deterministic mode.",
    ),
    write_report: bool = typer.Option(
        True,
        "--write-report/--no-write-report",
        help="Persist the evaluated report to disk.",
    ),
    report_path: Path | None = typer.Option(
        None,
        "--report-path",
        help="Optional explicit JSON path for the persisted quality report.",
    ),
) -> None:
    """Evaluate one installed pack against one golden set."""

    try:
        response = api_evaluate_extraction_quality(
            pack_id,
            golden_set_path=golden_set_path,
            profile=profile,
            hybrid=hybrid,
            write_report=write_report,
            report_path=report_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("evaluate-live-news")
def registry_evaluate_live_news(
    pack_id: str = typer.Argument(..., help="Installed pack id to evaluate."),
    article_limit: int = typer.Option(
        10,
        "--article-limit",
        min=1,
        max=100,
        help="Maximum number of live articles to evaluate across the configured RSS feeds.",
    ),
    per_feed_limit: int = typer.Option(
        10,
        "--per-feed-limit",
        min=1,
        max=50,
        help="Maximum number of RSS items to inspect per feed before collecting article samples.",
    ),
    write_report: bool = typer.Option(
        True,
        "--write-report/--no-write-report",
        help="Persist the live-news feedback report to disk.",
    ),
    report_path: Path | None = typer.Option(
        None,
        "--report-path",
        help="Optional explicit JSON path for the persisted live-news feedback report.",
    ),
) -> None:
    """Fetch live RSS items, tag article text, and summarize generic issue classes."""

    try:
        response = api_evaluate_live_news_feedback(
            pack_id,
            article_limit=article_limit,
            per_feed_limit=per_feed_limit,
            write_report=write_report,
            report_path=report_path,
        )
    except (FileNotFoundError, ValueError, httpx.HTTPError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("benchmark-runtime")
def registry_benchmark_runtime(
    pack_id: str = typer.Argument(..., help="Installed pack id to benchmark."),
    golden_set_path: Path | None = typer.Option(
        None,
        "--golden-set-path",
        help="Optional explicit golden set JSON path. Defaults to the /mnt quality root.",
    ),
    profile: str = typer.Option(
        "default",
        "--profile",
        help="Golden set profile to use when --golden-set-path is omitted.",
    ),
    hybrid: bool = typer.Option(
        False,
        "--hybrid/--deterministic",
        help="Benchmark hybrid mode instead of deterministic mode.",
    ),
    warm_runs: int = typer.Option(
        3,
        "--warm-runs",
        min=1,
        max=25,
        help="Number of warm tagging passes to benchmark per golden document.",
    ),
    lookup_limit: int = typer.Option(
        20,
        "--lookup-limit",
        min=1,
        max=100,
        help="Candidate limit for operator lookup benchmark queries.",
    ),
    write_report: bool = typer.Option(
        True,
        "--write-report/--no-write-report",
        help="Persist the runtime benchmark report to disk.",
    ),
    report_path: Path | None = typer.Option(
        None,
        "--report-path",
        help="Optional explicit JSON path for the persisted benchmark report.",
    ),
) -> None:
    """Benchmark matcher load, warm tagging, operator lookup, and disk usage."""

    try:
        response = api_benchmark_runtime(
            pack_id,
            golden_set_path=golden_set_path,
            profile=profile,
            hybrid=hybrid,
            warm_runs=warm_runs,
            lookup_limit=lookup_limit,
            write_report=write_report,
            report_path=report_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("benchmark-matcher-backends")
def registry_benchmark_matcher_backends(
    pack_id: str = typer.Argument(..., help="Installed pack id to benchmark."),
    golden_set_path: Path | None = typer.Option(
        None,
        "--golden-set-path",
        help="Optional explicit golden set JSON path. Defaults to the /mnt quality root.",
    ),
    profile: str = typer.Option(
        "default",
        "--profile",
        help="Golden set profile to use when --golden-set-path is omitted.",
    ),
    alias_limit: int = typer.Option(
        10000,
        "--alias-limit",
        min=1,
        max=500000,
        help="Maximum retained aliases in the bounded benchmark slice.",
    ),
    scan_limit: int = typer.Option(
        50000,
        "--scan-limit",
        min=1,
        max=1000000,
        help="Maximum raw aliases to scan before slice selection stops.",
    ),
    min_alias_score: float = typer.Option(
        0.8,
        "--min-alias-score",
        min=0.0,
        max=1.0,
        help="Minimum alias score for the bounded exact-tier slice.",
    ),
    exact_tier_min_token_count: int = typer.Option(
        2,
        "--exact-tier-min-token-count",
        min=1,
        max=16,
        help="Minimum normalized token count for aliases retained in the benchmark slice.",
    ),
    query_runs: int = typer.Option(
        3,
        "--query-runs",
        min=1,
        max=25,
        help="Number of repeated query passes per golden document for each candidate backend.",
    ),
    write_report: bool = typer.Option(
        True,
        "--write-report/--no-write-report",
        help="Persist the matcher backend benchmark report to disk.",
    ),
    report_path: Path | None = typer.Option(
        None,
        "--report-path",
        help="Optional explicit JSON path for the persisted matcher backend report.",
    ),
    output_root: Path | None = typer.Option(
        None,
        "--output-root",
        help="Optional explicit working directory for benchmark artifacts.",
    ),
) -> None:
    """Benchmark candidate matcher backends on a bounded installed-pack alias slice."""

    try:
        response = api_benchmark_matcher_backends(
            pack_id,
            golden_set_path=golden_set_path,
            profile=profile,
            alias_limit=alias_limit,
            scan_limit=scan_limit,
            min_alias_score=min_alias_score,
            exact_tier_min_token_count=exact_tier_min_token_count,
            query_runs=query_runs,
            write_report=write_report,
            report_path=report_path,
            output_root=output_root,
        )
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("compare-extraction-quality")
def registry_compare_extraction_quality(
    baseline_report_path: Path = typer.Option(
        ...,
        "--baseline-report-path",
        help="Stored baseline extraction-quality report JSON.",
    ),
    candidate_report_path: Path = typer.Option(
        ...,
        "--candidate-report-path",
        help="Stored candidate extraction-quality report JSON.",
    ),
) -> None:
    """Compare two stored extraction-quality reports."""

    try:
        response = api_compare_extraction_quality_reports(
            baseline_report_path=baseline_report_path,
            candidate_report_path=candidate_report_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("diff-pack")
def registry_diff_pack(
    old_pack_dir: Path = typer.Argument(..., help="Earlier runtime pack directory."),
    new_pack_dir: Path = typer.Argument(..., help="Candidate runtime pack directory."),
) -> None:
    """Return the semantic diff between two runtime pack directories."""

    try:
        response = api_diff_pack_versions(
            old_pack_dir,
            new_pack_dir,
        )
    except (FileNotFoundError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("evaluate-release-thresholds")
def registry_evaluate_release_thresholds(
    report_path: Path = typer.Option(
        ...,
        "--report-path",
        help="Stored extraction-quality report JSON to evaluate.",
    ),
    mode: str = typer.Option(
        "deterministic",
        "--mode",
        help="Release threshold mode: deterministic or hybrid.",
    ),
    baseline_report_path: Path | None = typer.Option(
        None,
        "--baseline-report-path",
        help="Optional baseline quality report for per-label or hybrid deltas.",
    ),
    min_recall: float | None = typer.Option(
        None,
        "--min-recall",
        min=0.0,
        max=1.0,
        help="Minimum acceptable recall. Uses the locked mode default when omitted.",
    ),
    min_precision: float | None = typer.Option(
        None,
        "--min-precision",
        min=0.0,
        max=1.0,
        help="Minimum acceptable precision. Uses the locked mode default when omitted.",
    ),
    max_label_recall_drop: float | None = typer.Option(
        None,
        "--max-label-recall-drop",
        min=0.0,
        max=1.0,
        help="Maximum allowed negative per-label recall delta. Uses the locked mode default when omitted.",
    ),
    min_recall_lift: float | None = typer.Option(
        None,
        "--min-recall-lift",
        min=0.0,
        max=1.0,
        help="Minimum recall lift required for hybrid mode. Uses the locked mode default when omitted.",
    ),
    max_precision_drop: float | None = typer.Option(
        None,
        "--max-precision-drop",
        min=0.0,
        max=1.0,
        help="Maximum precision drop allowed for hybrid mode. Uses the locked mode default when omitted.",
    ),
    max_p95_latency_ms: int | None = typer.Option(
        None,
        "--max-p95-latency-ms",
        min=0,
        help="Maximum allowed p95 latency in milliseconds. Uses the locked mode default when omitted.",
    ),
    max_model_artifact_bytes: int | None = typer.Option(
        None,
        "--max-model-artifact-bytes",
        min=0,
        help="Maximum allowed model artifact size in bytes for hybrid mode. Uses the locked mode default when omitted.",
    ),
    max_peak_memory_mb: int | None = typer.Option(
        None,
        "--max-peak-memory-mb",
        min=0,
        help="Maximum allowed model peak memory in MB for hybrid mode. Uses the locked mode default when omitted.",
    ),
    model_artifact_path: Path | None = typer.Option(
        None,
        "--model-artifact-path",
        help="Optional model artifact path used to measure hybrid artifact size.",
    ),
    peak_memory_mb: int | None = typer.Option(
        None,
        "--peak-memory-mb",
        min=0,
        help="Observed hybrid peak memory in MB.",
    ),
) -> None:
    """Evaluate one stored extraction-quality report against release thresholds."""

    try:
        response = api_evaluate_extraction_release_thresholds(
            report_path=report_path,
            mode=mode,
            baseline_report_path=baseline_report_path,
            min_recall=min_recall,
            min_precision=min_precision,
            max_label_recall_drop=max_label_recall_drop,
            min_recall_lift=min_recall_lift,
            max_precision_drop=max_precision_drop,
            max_p95_latency_ms=max_p95_latency_ms,
            max_model_artifact_bytes=max_model_artifact_bytes,
            max_peak_memory_mb=max_peak_memory_mb,
            model_artifact_path=model_artifact_path,
            peak_memory_mb=peak_memory_mb,
        )
    except (FileNotFoundError, ValueError) as exc:
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
    sec_submissions_url: str = typer.Option(
        "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
        "--sec-submissions-url",
        help="URL or file path for the SEC submissions bulk snapshot.",
    ),
    sec_companyfacts_url: str = typer.Option(
        "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        "--sec-companyfacts-url",
        help="URL or file path for the SEC companyfacts bulk snapshot.",
    ),
    symbol_directory_url: str = typer.Option(
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt",
        "--symbol-directory-url",
        help="URL or file path for the Nasdaq symbol-directory snapshot.",
    ),
    other_listed_url: str = typer.Option(
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
        "--other-listed-url",
        help="URL or file path for the Nasdaq other-listed snapshot.",
    ),
    finance_people_url: str | None = typer.Option(
        None,
        "--finance-people-url",
        help="Optional URL or file path for a finance people snapshot JSON or JSONL file.",
    ),
    derive_finance_people_from_sec: bool = typer.Option(
        False,
        "--derive-finance-people-from-sec/--no-derive-finance-people-from-sec",
        help="Derive finance people from recent SEC proxy filings when no explicit finance people snapshot is provided.",
    ),
    finance_people_archive_base_url: str = typer.Option(
        "https://www.sec.gov/Archives/edgar/data",
        "--finance-people-archive-base-url",
        help="Base archive URL used when deriving finance people from SEC proxy filings.",
    ),
    finance_people_max_companies: int | None = typer.Option(
        None,
        "--finance-people-max-companies",
        min=1,
        help="Optional limit on companies scanned when deriving finance people from SEC proxy filings.",
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
            sec_submissions_url=sec_submissions_url,
            sec_companyfacts_url=sec_companyfacts_url,
            symbol_directory_url=symbol_directory_url,
            other_listed_url=other_listed_url,
            finance_people_url=finance_people_url,
            derive_finance_people_from_sec=derive_finance_people_from_sec,
            finance_people_archive_base_url=finance_people_archive_base_url,
            finance_people_max_companies=finance_people_max_companies,
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
    sec_submissions: Path | None = typer.Option(
        None,
        "--sec-submissions",
        help="Optional path to the SEC submissions bulk snapshot ZIP file.",
    ),
    sec_companyfacts: Path | None = typer.Option(
        None,
        "--sec-companyfacts",
        help="Optional path to the SEC companyfacts bulk snapshot ZIP file.",
    ),
    symbol_directory: Path = typer.Option(
        ...,
        "--symbol-directory",
        help="Path to the symbol-directory delimited file.",
    ),
    other_listed: Path | None = typer.Option(
        None,
        "--other-listed",
        help="Optional path to the other-listed delimited file.",
    ),
    finance_people: Path | None = typer.Option(
        None,
        "--finance-people",
        help="Optional path to a finance people snapshot JSON or JSONL file.",
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
            sec_submissions_path=sec_submissions,
            sec_companyfacts_path=sec_companyfacts,
            symbol_directory_path=symbol_directory,
            other_listed_path=other_listed,
            finance_people_path=finance_people,
            curated_entities_path=curated_entities,
            output_dir=output_dir,
            version=version,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("fetch-finance-country-sources")
def registry_fetch_finance_country_sources(
    output_dir: Path = typer.Option(
        Path("/mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en"),
        "--output-dir",
        help="Directory where immutable country-finance source snapshots should be written.",
    ),
    snapshot: str | None = typer.Option(
        None,
        "--snapshot",
        help="Snapshot date in YYYY-MM-DD format. Defaults to today.",
    ),
    country_code: list[str] = typer.Option(
        None,
        "--country-code",
        help="Restrict the fetch to specific two-letter country codes. Repeat for multiple countries.",
    ),
    user_agent: str = typer.Option(
        "ades/0.1.0 (ops@adestool.com)",
        "--user-agent",
        help="User-Agent header for HTTP source fetches.",
    ),
) -> None:
    """Download official source landing pages for country-scoped finance packs."""

    try:
        response = api_fetch_finance_country_source_snapshots(
            output_dir=output_dir,
            snapshot=snapshot,
            country_codes=country_code or None,
            user_agent=user_agent,
        )
    except (FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
        _exit_with_cli_error(exc)
    _echo_json(response.model_dump(mode="json"))


@registry_app.command("build-finance-country-bundles")
def registry_build_finance_country_bundles(
    snapshot_dir: Path = typer.Option(
        ...,
        "--snapshot-dir",
        help="Directory containing one downloaded country-finance snapshot set.",
    ),
    output_dir: Path = typer.Option(
        Path("/mnt/githubActions/ades_big_data/pack_sources/bundles/finance-country-en"),
        "--output-dir",
        help="Directory where normalized country-finance bundles should be written.",
    ),
    country_code: list[str] = typer.Option(
        None,
        "--country-code",
        help="Restrict the bundle build to specific two-letter country codes. Repeat for multiple countries.",
    ),
    version: str = typer.Option(
        "0.2.0",
        "--version",
        help="Pack version to record in the generated bundle manifests.",
    ),
) -> None:
    """Build country-scoped finance bundles from downloaded source snapshots."""

    try:
        response = api_build_finance_country_source_bundles(
            snapshot_dir=snapshot_dir,
            output_dir=output_dir,
            country_codes=country_code or None,
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
    wikidata_url: str | None = typer.Option(
        None,
        "--wikidata-url",
        help="Optional URL or file path for a prefiltered bounded Wikidata entity snapshot. If omitted, the two-pass truthy-plus-JSON dump flow is used.",
    ),
    wikidata_truthy_url: str | None = typer.Option(
        "https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.gz",
        "--wikidata-truthy-url",
        help="URL or file path for the Wikidata truthy RDF dump used for type gating.",
    ),
    wikidata_entities_url: str | None = typer.Option(
        "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz",
        "--wikidata-entities-url",
        help="URL or file path for the Wikidata JSON dump used to hydrate the selected QIDs.",
    ),
    geonames_url: str = typer.Option(
        "https://download.geonames.org/export/dump/allCountries.zip",
        "--geonames-url",
        help="URL or file path for the GeoNames allCountries snapshot zip.",
    ),
    geonames_alternate_names_url: str | None = typer.Option(
        "https://download.geonames.org/export/dump/alternateNamesV2.zip",
        "--geonames-alternate-names-url",
        help="Optional URL or file path for the GeoNames alternateNamesV2 snapshot zip.",
    ),
    geonames_modifications_url: str | None = typer.Option(
        None,
        "--geonames-modifications-url",
        help="Optional URL or file path for a GeoNames modifications delta file.",
    ),
    geonames_deletes_url: str | None = typer.Option(
        None,
        "--geonames-deletes-url",
        help="Optional URL or file path for a GeoNames deletes delta file.",
    ),
    geonames_alternate_modifications_url: str | None = typer.Option(
        None,
        "--geonames-alternate-modifications-url",
        help="Optional URL or file path for a GeoNames alternate-names modifications delta file.",
    ),
    geonames_alternate_deletes_url: str | None = typer.Option(
        None,
        "--geonames-alternate-deletes-url",
        help="Optional URL or file path for a GeoNames alternate-names deletes delta file.",
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
            wikidata_truthy_url=wikidata_truthy_url,
            wikidata_entities_url=wikidata_entities_url,
            geonames_places_url=geonames_url,
            geonames_alternate_names_url=geonames_alternate_names_url,
            geonames_modifications_url=geonames_modifications_url,
            geonames_deletes_url=geonames_deletes_url,
            geonames_alternate_modifications_url=geonames_alternate_modifications_url,
            geonames_alternate_deletes_url=geonames_alternate_deletes_url,
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
        "https://clinicaltrials.gov/api/v2/studies?format=json",
        "--clinical-trials-url",
        help="URL or file path for the ClinicalTrials.gov study source JSON.",
    ),
    orange_book_url: str = typer.Option(
        "https://www.fda.gov/media/76860/download?attachment",
        "--orange-book-url",
        help="URL or file path for the Orange Book products download.",
    ),
    user_agent: str = typer.Option(
        "ades/0.1.0 (ops@adestool.com)",
        "--user-agent",
        help="User-Agent header for HTTP source fetches.",
    ),
    uniprot_max_records: int = typer.Option(
        0,
        "--uniprot-max-records",
        min=0,
        help="Maximum number of UniProt protein records to keep in one snapshot. Use 0 for no cap.",
    ),
    clinical_trials_max_records: int = typer.Option(
        0,
        "--clinical-trials-max-records",
        min=0,
        help="Maximum number of ClinicalTrials.gov studies to keep in one snapshot. Use 0 for no cap.",
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
            orange_book_url=orange_book_url,
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
    orange_book_products: Path | None = typer.Option(
        None,
        "--orange-book-products",
        help="Optional JSON or JSONL snapshot of Orange Book drug products.",
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
            orange_book_products_path=orange_book_products,
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
    resolved_pack = _resolve_tag_pack_or_exit(pack)
    try:
        if file is not None:
            response = _tag_file_response(
                file,
                pack=resolved_pack,
                content_type=content_type,
                output_path=output,
                output_dir=output_dir,
                pretty_output=not compact_output,
            )
        else:
            response = _tag_text_response(
                text,
                pack=resolved_pack,
                content_type=content_type or "text/plain",
                output_path=output,
                output_dir=output_dir,
                pretty_output=not compact_output,
            )
    except FileNotFoundError as exc:
        _exit_with_cli_error(exc)
    except (
        InvalidConfigurationError,
        LocalServiceUnavailableError,
        LocalServiceRequestError,
        UnsupportedRuntimeConfigurationError,
        ValueError,
    ) as exc:
        _exit_with_cli_error(exc)
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
    resolved_pack = _resolve_tag_pack_or_exit(pack, manifest_input=manifest_input)
    try:
        response = _tag_files_response(
            files or [],
            pack=resolved_pack,
            content_type=content_type,
            output_dir=output_dir,
            pretty_output=not compact_output,
            directories=directories or [],
            glob_patterns=glob_patterns or [],
            manifest_input=manifest_input,
            manifest_mode=manifest_mode,
            skip_unchanged=skip_unchanged,
            reuse_unchanged_outputs=reuse_unchanged_outputs,
            repair_missing_reused_outputs=repair_missing_reused_outputs,
            recursive=not non_recursive,
            include_patterns=include_patterns or [],
            exclude_patterns=exclude_patterns or [],
            max_files=max_files,
            max_input_bytes=max_input_bytes,
            write_manifest=write_manifest,
            manifest_output=manifest_output,
        )
    except FileNotFoundError as exc:
        _exit_with_cli_error(exc)
    except (
        InvalidConfigurationError,
        LocalServiceUnavailableError,
        LocalServiceRequestError,
        UnsupportedRuntimeConfigurationError,
        ValueError,
    ) as exc:
        _exit_with_cli_error(exc)
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
