# ades-cli

`ades-cli` is the npm wrapper for the Python-based `ades` runtime.

On first execution it bootstraps a user-local Python virtual environment, installs the matching `ades` Python package, and then delegates all CLI arguments to the Python `ades` command.

Environment overrides:

- `ADES_PYTHON_BIN`: explicit Python executable to use for bootstrap
- `ADES_PYTHON_PACKAGE_SPEC`: explicit pip install target such as `ades==0.1.0` or a local path
- `ADES_NPM_RUNTIME_DIR`: explicit runtime directory for the wrapper-managed virtual environment
- `ADES_NPM_PIP_INSTALL_ARGS`: extra arguments appended to `pip install`
