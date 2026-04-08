"""Command line interface for ades."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from .api import activate_pack as api_activate_pack
from .api import deactivate_pack as api_deactivate_pack
from .api import lookup_candidates as api_lookup_candidates
from .api import list_packs as api_list_packs
from .api import pull_pack as api_pull_pack
from .api import status as api_status
from .api import tag as api_tag
from .api import tag_file as api_tag_file
from .api import tag_files as api_tag_files
from .config import get_settings
from .packs.installer import PackInstaller
from .storage.paths import build_storage_layout, ensure_storage_layout


app = typer.Typer(help="ades local semantic enrichment CLI", no_args_is_help=True)
packs_app = typer.Typer(help="Inspect installed ades packs.")
app.add_typer(packs_app, name="packs")


def _installer() -> PackInstaller:
    settings = get_settings()
    return PackInstaller(settings.storage_root, registry_url=settings.registry_url)


@app.command()
def status() -> None:
    """Show the local ades runtime status."""

    payload = api_status()
    typer.echo(json.dumps(payload.model_dump(), indent=2))


@packs_app.command("list")
def packs_list(
    available: bool = typer.Option(False, "--available", help="List registry packs instead."),
    active_only: bool = typer.Option(False, "--active-only", help="List only active installed packs."),
) -> None:
    """List installed packs or available registry packs."""

    if available:
        if active_only:
            raise typer.BadParameter("--active-only cannot be used with --available.")
        packs = _installer().available_packs()
    else:
        packs = [pack.pack_id for pack in api_list_packs(active_only=active_only)]
    typer.echo(json.dumps({"packs": packs}, indent=2))


@packs_app.command("activate")
def packs_activate(pack: str) -> None:
    """Activate an installed pack."""

    result = api_activate_pack(pack)
    if result is None:
        raise typer.Exit(code=1)
    typer.echo(json.dumps(result.model_dump(), indent=2))


@packs_app.command("deactivate")
def packs_deactivate(pack: str) -> None:
    """Deactivate an installed pack."""

    result = api_deactivate_pack(pack)
    if result is None:
        raise typer.Exit(code=1)
    typer.echo(json.dumps(result.model_dump(), indent=2))


@packs_app.command("lookup")
def packs_lookup(
    query: str,
    pack: str = typer.Option(None, help="Restrict lookup to a single pack."),
    exact_alias: bool = typer.Option(False, "--exact-alias", help="Match alias text exactly."),
    active_only: bool = typer.Option(True, "--active-only/--include-inactive", help="Search only active packs by default."),
    limit: int = typer.Option(20, min=1, max=100, help="Maximum number of candidates to return."),
) -> None:
    """Search deterministic alias and rule metadata from the local SQLite store."""

    response = api_lookup_candidates(
        query,
        pack_id=pack,
        exact_alias=exact_alias,
        active_only=active_only,
        limit=limit,
    )
    typer.echo(json.dumps(response.model_dump(), indent=2))


@app.command()
def pull(pack: str) -> None:
    """Install a pack and any required dependencies."""

    result = api_pull_pack(pack)
    typer.echo(
        json.dumps(
            {
                "requested_pack": result.requested_pack,
                "registry_url": result.registry_url,
                "installed": result.installed,
                "skipped": result.skipped,
            },
            indent=2,
        )
    )


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
    typer.echo(json.dumps(response.model_dump(), indent=2))


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
        recursive=not non_recursive,
        include_patterns=include_patterns or [],
        exclude_patterns=exclude_patterns or [],
        max_files=max_files,
        max_input_bytes=max_input_bytes,
        write_manifest=write_manifest,
        manifest_output_path=manifest_output,
    )
    typer.echo(json.dumps(response.model_dump(), indent=2))


@app.command()
def serve(
    host: str = typer.Option(None, help="Override the bind host."),
    port: int = typer.Option(None, help="Override the bind port."),
    reload: bool = typer.Option(False, help="Enable auto-reload for development."),
) -> None:
    """Run the local FastAPI service."""

    import uvicorn

    settings = get_settings()
    layout = ensure_storage_layout(build_storage_layout(settings.storage_root))
    typer.echo(f"Using storage root: {layout.storage_root}")
    uvicorn.run(
        "ades.service.app:create_app",
        host=host or settings.host,
        port=port or settings.port,
        factory=True,
        reload=reload,
    )


def main() -> None:
    """Entrypoint used by the console script."""

    app()
