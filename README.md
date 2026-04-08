# ades

`ades` is a local-first semantic enrichment tool for AI systems. It is being designed as both a Python library and a localhost service so tools such as Docdex, mcoda, quantitative trading workflows, LLM refinement pipelines, Codali, and domain research systems can share the same entity extraction and tagging runtime.

The core idea is simple: users install the runtime, pull the domain packs they need, and run everything locally. Example packs include `finance-en` and `medical-en`. Large downloaded data for this project must live under `/mnt/githubActions/ades_big_data`, not inside this repository.

## Planned Usage

```bash
pip install ades
npm install -g ades

ades pull finance-en
ades pull medical-en
ades serve
ades tag "Apple CEO Tim Cook announced quarterly earnings."
```

## Planned Modes

- Library mode: import `ades` directly from Python for in-process enrichment.
- CLI mode: pull packs, tag text, inspect installed packs, and run the local service.
- Local service mode: run `ades serve` and expose a localhost API that multiple tools or agents can share.

## Initial Product Direction

- Python-first implementation
- Tiered downloadable packs from smallest to biggest
- Local pack and model storage under `/mnt/githubActions/ades_big_data`
- Reusable structured enrichment for entity extraction, linking, topic tagging, and related AI workflows
- Packaging for both PyPI and npm for easy installation

## Current Repo Status

This repository currently contains the architecture notes, project guidance, and the implementation plan for the first version of `ades`. The next step is to scaffold the Python package, CLI, local service, and pack manager around the plan in [`docs/implementation_plan.md`](/home/wodo/apps/ades/docs/implementation_plan.md).
