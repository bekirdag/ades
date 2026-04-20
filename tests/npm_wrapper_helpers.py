import json
from pathlib import Path
import textwrap


REPO_ROOT = Path(__file__).resolve().parents[1]
NPM_WRAPPER_BIN = REPO_ROOT / "npm" / "ades" / "bin" / "ades.cjs"
NPM_PACKAGE_JSON = REPO_ROOT / "npm" / "ades" / "package.json"


def create_fake_python_environment(root: Path) -> tuple[Path, Path]:
    log_path = root / "fake-python-log.jsonl"
    python_path = root / "fake-python"
    python_path.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import json
            import os
            from pathlib import Path
            import shutil
            import sys

            log_path = Path(os.environ["ADES_NPM_TEST_LOG"])
            log_path.parent.mkdir(parents=True, exist_ok=True)

            def append_log() -> None:
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps({"argv": sys.argv[1:], "executable": sys.argv[0]}) + "\\n")

            append_log()

            if sys.argv[1:] == ["--version"]:
                print("Python 3.11.0")
                raise SystemExit(0)

            if sys.argv[1:3] == ["-m", "venv"]:
                runtime_dir = Path(sys.argv[3]).resolve()
                bin_dir = runtime_dir / "bin"
                bin_dir.mkdir(parents=True, exist_ok=True)
                target_python = bin_dir / "python"
                shutil.copy2(Path(sys.argv[0]).resolve(), target_python)
                target_python.chmod(0o755)
                raise SystemExit(0)

            if sys.argv[1:4] == ["-m", "pip", "install"]:
                bin_dir = Path(sys.argv[0]).resolve().parent
                ades_path = bin_dir / "ades"
                ades_path.write_text(
                    "#!/usr/bin/env python3\\n"
                    "import json\\n"
                    "import sys\\n"
                    "print(json.dumps({\\"argv\\": sys.argv[1:], \\"bootstrap\\": \\"ok\\"}))\\n",
                    encoding="utf-8",
                )
                ades_path.chmod(0o755)
                raise SystemExit(0)

            raise SystemExit(f"Unexpected fake python invocation: {sys.argv}")
            """
        ),
        encoding="utf-8",
    )
    python_path.chmod(0o755)
    return python_path, log_path


def read_json_lines(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
