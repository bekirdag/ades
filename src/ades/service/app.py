"""Local FastAPI application for ades."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from ..api import (
    activate_pack,
    deactivate_pack,
    get_pack,
    list_packs,
    lookup_candidates,
    status,
    tag,
    tag_file,
    tag_files,
)
from ..version import __version__
from .models import (
    BatchFileTagRequest,
    BatchTagResponse,
    FileTagRequest,
    LookupResponse,
    PackSummary,
    StatusResponse,
    TagRequest,
    TagResponse,
)


def create_app(*, storage_root: str | Path | None = None) -> FastAPI:
    """Create a FastAPI application bound to a storage root."""

    app = FastAPI(title="ades", version=__version__)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        """Simple health endpoint."""

        return {"status": "ok", "version": __version__}

    @app.get("/v0/status", response_model=StatusResponse)
    def runtime_status() -> StatusResponse:
        """Report local runtime status."""

        return status(storage_root=storage_root)

    @app.get("/v0/packs", response_model=list[PackSummary])
    def runtime_list_packs(active_only: bool = Query(False)) -> list[PackSummary]:
        """List locally installed packs."""

        return list_packs(storage_root=storage_root, active_only=active_only)

    @app.get("/v0/packs/{pack_id}", response_model=PackSummary)
    def runtime_get_pack(pack_id: str) -> PackSummary:
        """Get a single installed pack."""

        pack = get_pack(pack_id, storage_root=storage_root)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        return pack

    @app.post("/v0/packs/{pack_id}/activate", response_model=PackSummary)
    def runtime_activate_pack(pack_id: str) -> PackSummary:
        """Activate a single installed pack."""

        pack = activate_pack(pack_id, storage_root=storage_root)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        return pack

    @app.post("/v0/packs/{pack_id}/deactivate", response_model=PackSummary)
    def runtime_deactivate_pack(pack_id: str) -> PackSummary:
        """Deactivate a single installed pack."""

        pack = deactivate_pack(pack_id, storage_root=storage_root)
        if pack is None:
            raise HTTPException(status_code=404, detail=f"Pack not found: {pack_id}")
        return pack

    @app.post("/v0/tag", response_model=TagResponse)
    def runtime_tag(request: TagRequest) -> TagResponse:
        """Tag text through the local in-process pipeline."""

        try:
            return tag(
                request.text,
                pack=request.pack,
                content_type=request.content_type,
                output_path=request.output.path if request.output else None,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/tag/file", response_model=TagResponse)
    def runtime_tag_file(request: FileTagRequest) -> TagResponse:
        """Tag a local file through the in-process pipeline."""

        try:
            return tag_file(
                request.path,
                pack=request.pack,
                content_type=request.content_type,
                output_path=request.output.path if request.output else None,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v0/tag/files", response_model=BatchTagResponse)
    def runtime_tag_files(request: BatchFileTagRequest) -> BatchTagResponse:
        """Tag multiple local files through the in-process pipeline."""

        if request.output and request.output.path is not None:
            raise HTTPException(
                status_code=400,
                detail="Batch file tagging supports output.directory only.",
            )
        if (
            request.output
            and (request.output.write_manifest or request.output.manifest_path is not None)
            and request.output.directory is None
        ):
            raise HTTPException(
                status_code=400,
                detail="Batch manifest export requires output.directory.",
            )
        try:
            return tag_files(
                request.paths,
                pack=request.pack,
                content_type=request.content_type,
                output_dir=request.output.directory if request.output else None,
                pretty_output=request.output.pretty if request.output else True,
                storage_root=storage_root,
                directories=request.directories,
                glob_patterns=request.glob_patterns,
                manifest_input_path=request.manifest_input_path,
                manifest_replay_mode=request.manifest_replay_mode,
                recursive=request.recursive,
                include_patterns=request.include_patterns,
                exclude_patterns=request.exclude_patterns,
                max_files=request.max_files,
                max_input_bytes=request.max_input_bytes,
                write_manifest=request.output.write_manifest if request.output else False,
                manifest_output_path=request.output.manifest_path if request.output else None,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IsADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotADirectoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v0/lookup", response_model=LookupResponse)
    def runtime_lookup(
        q: str = Query(..., min_length=1),
        pack_id: str | None = Query(None),
        exact_alias: bool = Query(False),
        active_only: bool = Query(True),
        limit: int = Query(20, ge=1, le=100),
    ) -> LookupResponse:
        """Search deterministic alias and rule metadata candidates."""

        return lookup_candidates(
            q,
            storage_root=storage_root,
            pack_id=pack_id,
            exact_alias=exact_alias,
            active_only=active_only,
            limit=limit,
        )

    return app


app = create_app()
