# ades Usage

## Install

```bash
pip install ades-tool
# or
npm install -g @bekirdag/ades
```

## Version and Help

```bash
ades --version
ades -V
ades help
```

## Pull Libraries

```bash
ades pull general-en
ades pull finance-en
ades pull medical-en
ades pull finance-de-en
```

List available libraries:

```bash
ades list
```

List installed libraries:

```bash
ades packs
```

## Tag Text

```bash
ades tag "NVIDIA reported earnings in Germany." --pack finance-en
```

## Tag a File

```bash
ades tag --file ./report.txt --pack finance-en
```

## Tag Multiple Files

```bash
ades tag-files ./a.txt ./b.txt --pack finance-en
```

## Run the Local Service

```bash
ades serve
```

Useful endpoints:

- `GET /healthz`
- `GET /v0/status`
- `POST /v0/tag`

## Notes

- Pull only the libraries you need.
- Dependency libraries are installed automatically.
- `timing_ms` and the other response-time fields are in milliseconds.
