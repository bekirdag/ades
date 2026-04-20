# ades

`ades` is a local-first semantic enrichment tool.

It installs small domain libraries, tags text or files locally, and can also run as a local HTTP service.

## Install

```bash
pip install ades-tool
# or
npm install -g @bekirdag/ades
```

The npm package is a thin wrapper around the Python runtime.

## Pull Libraries

Pull only the libraries you need:

```bash
ades pull general-en
ades pull finance-en
ades pull medical-en
ades pull finance-de-en
```

Current examples:

- `general-en`: shared English baseline
- `finance-en`: English finance entities
- `medical-en`: English medical entities
- `finance-de-en`: Germany public-market finance entities

Dependencies are pulled automatically.

## Tag Text

```bash
ades tag "NVIDIA reported earnings in Germany." --pack finance-en
```

Example response:

```json
{
  "pack": "finance-en",
  "entities": [],
  "topics": [],
  "warnings": [],
  "timing_ms": 12
}
```

`timing_ms` and the other response-time fields are in milliseconds.

## Run the Local Service

```bash
ades serve
```

Health check:

```text
GET /healthz
```

## More

- Detailed usage: [docs/usage.md](docs/usage.md)
- Production deployment: [docs/production_deployment.md](docs/production_deployment.md)
