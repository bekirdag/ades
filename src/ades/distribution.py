"""Distribution, installer, and packaging metadata helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from typing import Any

from .version import __version__


NPM_PACKAGE_NAME = "@bekirdag/ades"
NPM_COMMAND_NAME = "ades"
PYTHON_DISTRIBUTION_NAME = "ades-tool"
NPM_RUNTIME_DIR_ENV = "ADES_NPM_RUNTIME_DIR"
NPM_PYTHON_BIN_ENV = "ADES_PYTHON_BIN"
NPM_PYTHON_PACKAGE_SPEC_ENV = "ADES_PYTHON_PACKAGE_SPEC"
NPM_PIP_INSTALL_ARGS_ENV = "ADES_NPM_PIP_INSTALL_ARGS"


def resolve_project_root() -> Path:
    """Return the ades source project root when available."""

    return Path(__file__).resolve().parents[2]


def build_npm_python_package_spec() -> str:
    """Return the default Python package spec used by the npm wrapper."""

    return f"{PYTHON_DISTRIBUTION_NAME}=={__version__}"


def resolve_npm_package_dir() -> Path:
    """Return the source checkout npm wrapper directory."""

    return resolve_project_root() / "npm" / "ades"


def load_npm_package_json(*, package_dir: str | Path | None = None) -> dict[str, Any]:
    """Load the npm wrapper package.json payload from the source checkout."""

    root = Path(package_dir).expanduser() if package_dir is not None else resolve_npm_package_dir()
    package_json_path = root / "package.json"
    return json.loads(package_json_path.read_text(encoding="utf-8"))


def resolve_npm_runtime_dir(*, env: dict[str, str] | None = None) -> Path:
    """Return the default runtime directory used by the npm wrapper."""

    env_map = env or os.environ
    override = env_map.get(NPM_RUNTIME_DIR_ENV)
    if override:
        return Path(override).expanduser()

    if sys.platform == "win32":
        local_app_data = env_map.get("LOCALAPPDATA")
        if local_app_data:
            base_dir = Path(local_app_data)
        else:
            home = Path(env_map.get("USERPROFILE", Path.home()))
            base_dir = home / "AppData" / "Local"
    elif sys.platform == "darwin":
        base_dir = Path(env_map.get("HOME", str(Path.home()))).expanduser() / "Library" / "Application Support"
    else:
        xdg_data_home = env_map.get("XDG_DATA_HOME")
        if xdg_data_home:
            base_dir = Path(xdg_data_home).expanduser()
        else:
            base_dir = Path(env_map.get("HOME", str(Path.home()))).expanduser() / ".local" / "share"

    return base_dir / "ades" / "npm-runtime" / __version__
