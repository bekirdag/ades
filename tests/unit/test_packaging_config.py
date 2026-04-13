from __future__ import annotations

from pathlib import Path
import tomllib


def test_hatch_build_includes_full_python_package() -> None:
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    payload = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))

    targets = payload["tool"]["hatch"]["build"]["targets"]
    wheel_packages = targets["wheel"]["packages"]
    sdist_include = targets["sdist"]["include"]

    assert "src/ades" in wheel_packages
    assert "/src/ades" in sdist_include
    assert "/src/ades/resources" not in sdist_include
