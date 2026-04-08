import json
from pathlib import Path

from ades.api import npm_installer_info
from ades.distribution import (
    NPM_COMMAND_NAME,
    NPM_PACKAGE_NAME,
    build_npm_python_package_spec,
    resolve_npm_runtime_dir,
)
from ades.version import __version__
from tests.npm_wrapper_helpers import NPM_PACKAGE_JSON


def test_npm_installer_info_matches_python_version_and_defaults() -> None:
    info = npm_installer_info()

    assert info.npm_package == NPM_PACKAGE_NAME
    assert info.command == NPM_COMMAND_NAME
    assert info.wrapper_version == __version__
    assert info.python_package_spec == build_npm_python_package_spec()
    assert Path(info.runtime_dir) == resolve_npm_runtime_dir().resolve()


def test_npm_package_json_version_matches_python_package_version() -> None:
    package_payload = json.loads(NPM_PACKAGE_JSON.read_text(encoding="utf-8"))

    assert package_payload["name"] == NPM_PACKAGE_NAME
    assert package_payload["version"] == __version__
    assert package_payload["bin"]["ades"] == "bin/ades.cjs"
