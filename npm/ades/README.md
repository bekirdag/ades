# @bekirdag/ades

`@bekirdag/ades` is the npm wrapper for the Python-based `ades` runtime.

Install it globally, then use the normal `ades` command:

```bash
npm install -g @bekirdag/ades
ades --version
ades pull general-en
ades pull finance-en
ades tag "NVIDIA reported earnings in Germany." --pack finance-en
```

On first execution the wrapper bootstraps a user-local Python environment and installs the matching Python package.

`timing_ms` and the other response-time fields in `ades` responses are in milliseconds.

Detailed usage:

- https://github.com/bekirdag/ades/blob/main/docs/usage.md

Environment overrides:

- `ADES_PYTHON_BIN`
- `ADES_PYTHON_PACKAGE_SPEC`
- `ADES_NPM_RUNTIME_DIR`
- `ADES_NPM_PIP_INSTALL_ARGS`
