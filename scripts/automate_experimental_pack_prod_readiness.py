#!/usr/bin/env python3
"""Run the experimental ADES vector-pack production-readiness queue."""

from __future__ import annotations

import argparse
from collections import Counter
import dataclasses
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import time
from typing import Any, Iterable


DEFAULT_AGENT = "sukunahikona-codex-55-xhigh"
DEFAULT_OUTPUT_ROOT = Path("/mnt/githubActions/ades_big_data/experimental_pack_prod_readiness")
DEFAULT_COMMAND_TEMPLATE = "mcoda agent-run {agent} --prompt-file {prompt_file}"
DEFAULT_USAGE_LIMIT_COMMAND_TEMPLATE = "mcoda agent limits --agent {agent} --json"
DEFAULT_VERSION = "0.2.0"

STATE_SCHEMA_VERSION = 1
PENDING_STATUS = "pending"
RUNNING_STATUS = "running"
COMPLETE_STATUS = "complete"
FAILED_STATUS = "failed"
VALIDATION_FAILED_STATUS = "validation_failed"
USAGE_LIMIT_STATUS = "usage_limited"
AGENT_UNAVAILABLE_STATUS = "agent_unavailable"
INTERRUPTED_STATUS = "interrupted"

RETRYABLE_STATUSES = {
    FAILED_STATUS,
    VALIDATION_FAILED_STATUS,
    USAGE_LIMIT_STATUS,
    AGENT_UNAVAILABLE_STATUS,
    INTERRUPTED_STATUS,
}

DEFAULT_STAGES = ("source-plan", "implement", "validate", "promote")
SOURCE_ELIGIBLE_TIERS = (
    "official",
    "issuer_disclosed",
    "exchange",
    "regulator",
    "government",
    "licensed",
    "industry_association",
)
PROPOSAL_ONLY_TIERS = (
    "wikidata_bridge",
    "reviewed_proposal",
    "local_pack_metadata",
    "test_fixture",
    "unknown",
)

EXCLUDED_SCAN_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "storage",
    "venv",
}


@dataclasses.dataclass(frozen=True)
class ExperimentalPackSpec:
    pack_id: str
    domain: str
    title: str
    purpose: str
    source_targets: tuple[str, ...]
    known_gap: str


@dataclasses.dataclass(frozen=True)
class QueueTask:
    task_id: str
    stage: str
    pack: ExperimentalPackSpec
    prompt: str

    @property
    def prompt_hash(self) -> str:
        return hashlib.sha256(self.prompt.encode("utf-8")).hexdigest()


@dataclasses.dataclass(frozen=True)
class RunnerLockInfo:
    pid: int | None
    created_at: str | None


PACK_SPECS = (
    ExperimentalPackSpec(
        pack_id="business-vector-en",
        domain="business",
        title="Business Vector",
        purpose=(
            "Business-event and operating-company concept/entity routing for BDYA market "
            "news digestion."
        ),
        source_targets=(
            "company filing taxonomies and issuer-disclosed event vocabularies",
            "exchange/regulator disclosure categories",
            "industry association glossaries for supply-chain and corporate actions",
        ),
        known_gap=(
            "The current builder is a curated BDYA overlay. Production promotion needs "
            "source-backed entity/rule provenance and broader false-positive evaluation."
        ),
    ),
    ExperimentalPackSpec(
        pack_id="economics-vector-en",
        domain="economics",
        title="Economics Vector",
        purpose=(
            "Macroeconomic indicators, policy events, trade concepts, labor data, and "
            "growth/inflation routing for market news."
        ),
        source_targets=(
            "official statistics agencies and central-bank glossaries",
            "IMF, World Bank, OECD, FRED, and government data dictionaries",
            "trade authority and treasury/fiscal-policy source vocabularies",
        ),
        known_gap=(
            "The current pack is curated and broad. Production promotion needs official "
            "source coverage for each indicator/policy concept plus shadow latency/noise "
            "evidence."
        ),
    ),
    ExperimentalPackSpec(
        pack_id="politics-vector-en",
        domain="politics",
        title="Politics Vector",
        purpose=(
            "Sanctions, elections, government, conflict, trade policy, and geopolitical "
            "risk routing for market news."
        ),
        source_targets=(
            "government sanctions and export-control lists",
            "official election/government/ministry sources",
            "recognized treaty, security, and intergovernmental organization sources",
        ),
        known_gap=(
            "The current pack is a curated news overlay. Production promotion needs strict "
            "source-tier evidence because political/geopolitical aliases are high-noise."
        ),
    ),
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-") or "task"


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def warn(message: str) -> None:
    print(f"warning: {message}", file=sys.stderr)


def write_text_atomic(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(text, encoding=encoding)
    os.replace(tmp, path)


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def resolve_under_repo(repo_root: Path, value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a resumable mcoda queue that gets the experimental ADES vector packs "
            "ready for production promotion."
        )
    )
    parser.add_argument("--repo-root", type=Path, default=default_repo_root())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--state-dir",
        default="",
        help="Queue state directory. Defaults to <output-root>/<run-id>/queue.",
    )
    parser.add_argument("--progress", default="", help="Defaults to <state-dir>/progress.md.")
    parser.add_argument("--run-id", default=dt.date.today().isoformat())
    parser.add_argument("--version", default=DEFAULT_VERSION)
    parser.add_argument(
        "--pack",
        action="append",
        default=[],
        help="Run only this pack id. Repeatable. Defaults to all experimental packs.",
    )
    parser.add_argument(
        "--stages",
        default=",".join(DEFAULT_STAGES),
        help="Comma-separated stages: source-plan,implement,validate,promote.",
    )
    parser.add_argument("--list-packs", action="store_true")
    parser.add_argument("--list-tasks", action="store_true")
    parser.add_argument("--show-prompts", action="store_true")
    parser.add_argument("--bootstrap-only", action="store_true")
    parser.add_argument(
        "--skip-bootstrap",
        action="store_true",
        help="Do not build current bundle/generated-pack reports before queueing.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-runs", type=int, default=0)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--agent", default=DEFAULT_AGENT)
    parser.add_argument("--command-template", default=DEFAULT_COMMAND_TEMPLATE)
    parser.add_argument("--usage-limit-command-template", default=DEFAULT_USAGE_LIMIT_COMMAND_TEMPLATE)
    parser.add_argument("--usage-limit-timeout-seconds", type=int, default=60)
    parser.add_argument("--skip-usage-limit-check", action="store_true")
    parser.add_argument("--skip-agent-health-check", action="store_true")
    parser.add_argument("--usage-limit-retry-delay-seconds", type=int, default=300)
    parser.add_argument("--usage-limit-max-retries", type=int, default=0, help="0 means unlimited.")
    parser.add_argument("--agent-unavailable-retry-delay-seconds", type=int, default=300)
    parser.add_argument("--agent-unavailable-max-retries", type=int, default=0, help="0 means unlimited.")
    parser.add_argument("--task-failure-retry-delay-seconds", type=int, default=120)
    parser.add_argument("--task-failure-max-retries", type=int, default=1)
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--ignore-lock", action="store_true")
    parser.add_argument("--lock-retry-delay-seconds", type=int, default=30)
    parser.add_argument("--lock-max-wait-seconds", type=int, default=0)
    parser.add_argument("--lock-stale-seconds", type=int, default=7200)
    parser.add_argument("--kill-stale-lock-process", action="store_true")
    parser.add_argument(
        "--allow-publish",
        action="store_true",
        help="Allow delegated promotion tasks to publish/upload registry artifacts.",
    )
    parser.add_argument(
        "--allow-git-push",
        action="store_true",
        help="Allow delegated promotion tasks to commit/push changes after validation.",
    )
    parser.add_argument(
        "--allow-prod-server-deploy",
        action="store_true",
        help="Allow delegated promotion tasks to deploy to the adestool production server.",
    )
    return parser.parse_args(argv)


def selected_packs(args: argparse.Namespace) -> list[ExperimentalPackSpec]:
    if not args.pack:
        return list(PACK_SPECS)
    by_id = {spec.pack_id: spec for spec in PACK_SPECS}
    selected: list[ExperimentalPackSpec] = []
    unknown: list[str] = []
    for raw_pack in args.pack:
        pack_id = raw_pack.strip()
        spec = by_id.get(pack_id)
        if spec is None:
            unknown.append(pack_id)
        else:
            selected.append(spec)
    if unknown:
        raise RuntimeError(f"Unknown experimental pack id(s): {', '.join(unknown)}")
    return selected


def parse_stages(args: argparse.Namespace) -> list[str]:
    allowed = set(DEFAULT_STAGES)
    stages = [slugify(value) for value in args.stages.split(",") if value.strip()]
    unknown = [stage for stage in stages if stage not in allowed]
    if unknown:
        raise RuntimeError(f"Unsupported stage(s): {', '.join(unknown)}")
    return stages or list(DEFAULT_STAGES)


def ensure_repo_importable(repo_root: Path) -> None:
    src_path = repo_root / "src"
    if src_path.is_dir() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))


def count_jsonl_records(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object at {path}")
    return payload


def bootstrap_current_pack_reports(
    *,
    repo_root: Path,
    run_root: Path,
    version: str,
    packs: list[ExperimentalPackSpec],
) -> dict[str, dict[str, Any]]:
    ensure_repo_importable(repo_root)
    from ades.packs.bdya_phase6 import build_bdya_domain_source_bundles
    from ades.packs.generation import generate_pack_source

    bundle_root = run_root / "current" / "bundles"
    generated_root = run_root / "current" / "generated"
    report_root = run_root / "current" / "reports"
    wanted = {pack.pack_id for pack in packs}
    bundle_results = build_bdya_domain_source_bundles(output_dir=bundle_root, version=version)
    bundles_by_pack = {result.pack_id: result for result in bundle_results}
    reports: dict[str, dict[str, Any]] = {}
    for pack in packs:
        bundle = bundles_by_pack.get(pack.pack_id)
        if bundle is None:
            raise RuntimeError(f"Builder did not produce expected pack: {pack.pack_id}")
        generated = generate_pack_source(
            bundle.bundle_dir,
            output_dir=generated_root,
            version=version,
        )
        bundle_dir = Path(bundle.bundle_dir)
        manifest = read_json(bundle_dir / "bundle.json")
        sources_lock = read_json(bundle_dir / "sources.lock.json")
        source_entities_path = bundle_dir / str(manifest["entities_path"])
        source_rules_path = bundle_dir / str(manifest["rules_path"])
        sources = sources_lock.get("sources", [])
        source_names = [
            str(item.get("name", ""))
            for item in sources
            if isinstance(item, dict) and item.get("name")
        ]
        report = {
            "pack_id": pack.pack_id,
            "domain": pack.domain,
            "version": version,
            "bundle_dir": str(bundle_dir),
            "generated_pack_dir": str(generated.pack_dir),
            "bundle_manifest_path": str(bundle.bundle_manifest_path),
            "sources_lock_path": str(bundle_dir / "sources.lock.json"),
            "source_entities_path": str(source_entities_path),
            "source_rules_path": str(source_rules_path),
            "manifest_tier": manifest.get("tier"),
            "source_count": len(sources) if isinstance(sources, list) else 0,
            "source_names": source_names,
            "source_license_classes": generated.source_license_classes,
            "publishable_sources_only": generated.publishable_sources_only,
            "publishable_source_count": generated.publishable_source_count,
            "restricted_source_count": generated.restricted_source_count,
            "input_entity_count": count_jsonl_records(source_entities_path),
            "input_rule_count": count_jsonl_records(source_rules_path),
            "label_count": generated.label_count,
            "alias_count": generated.alias_count,
            "ambiguous_alias_count": generated.ambiguous_alias_count,
            "dropped_alias_count": generated.dropped_alias_count,
            "included_entity_count": generated.included_entity_count,
            "included_rule_count": generated.included_rule_count,
            "matcher_entry_count": generated.matcher_entry_count,
            "matcher_state_count": generated.matcher_state_count,
            "warnings": generated.warnings,
            "known_gap": pack.known_gap,
        }
        write_json_atomic(report_root / f"{pack.pack_id}.json", report)
        reports[pack.pack_id] = report
    missing = sorted(wanted - set(reports))
    if missing:
        raise RuntimeError(f"Missing bootstrap reports for: {', '.join(missing)}")
    write_json_atomic(
        report_root / "summary.json",
        {
            "generated_at": utc_now(),
            "repo_root": str(repo_root),
            "version": version,
            "packs": reports,
        },
    )
    return reports


def write_readiness_plan(
    *,
    plan_path: Path,
    repo_root: Path,
    run_root: Path,
    packs: list[ExperimentalPackSpec],
    reports: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> None:
    lines = [
        "# Experimental ADES Vector Pack Production Readiness Plan",
        "",
        f"- Generated: {utc_now()}",
        f"- Repo: `{repo_root}`",
        f"- Run root: `{run_root}`",
        f"- Mcoda agent: `{args.agent}`",
        f"- Packs: {', '.join(pack.pack_id for pack in packs)}",
        "",
        "## Promotion Criteria",
        "",
        "1. Replace curated-only provenance with source-backed normalized bundles.",
        "2. Keep only source tiers eligible for production promotion: "
        f"{', '.join(SOURCE_ELIGIBLE_TIERS)}.",
        "3. Treat proposal-only tiers as non-publishable evidence: "
        f"{', '.join(PROPOSAL_ONLY_TIERS)}.",
        "4. Add pack-specific golden sets and negative corpora for exact and vector lanes.",
        "5. Pass local generation, `refresh-generated-packs`, vector-quality thresholds, "
        "saved-audit release thresholds, and BDYA shadow evaluation.",
        "6. Materialize and smoke the generated registry before any npm, pip, or adestool "
        "server deployment.",
        "7. Keep git clean by committing only reviewed source/test/docs changes and pushing "
        "only when `--allow-git-push` is set.",
        "",
        "## Pack Work",
        "",
    ]
    for pack in packs:
        report = reports.get(pack.pack_id, {})
        lines.extend(
            [
                f"### {pack.pack_id}",
                "",
                f"- Current bundle tier: `{report.get('manifest_tier', 'unknown')}`",
                f"- Current source names: {', '.join(report.get('source_names', []) or ['unknown'])}",
                f"- Current license classes: `{report.get('source_license_classes', {})}`",
                f"- Current entity/rule counts: "
                f"{report.get('input_entity_count', 'unknown')} entities, "
                f"{report.get('input_rule_count', 'unknown')} rules",
                f"- Production source targets: {', '.join(pack.source_targets)}",
                f"- Known gap: {pack.known_gap}",
                "",
            ]
        )
    lines.extend(
        [
            "## Queue Stages",
            "",
            "- `source-plan`: produce a source map, gap list, and acceptance checklist.",
            "- `implement`: make the source-backed bundle/test changes.",
            "- `validate`: run local gates and write validation evidence.",
            "- `promote`: prepare registry/server/npm/pip release artifacts; publish only when "
            "explicitly allowed.",
            "",
        ]
    )
    write_text_atomic(plan_path, "\n".join(lines))


def pack_artifact_context(
    pack: ExperimentalPackSpec,
    reports: dict[str, dict[str, Any]],
    run_root: Path,
) -> str:
    report = reports.get(pack.pack_id)
    payload = {
        "run_root": str(run_root),
        "pack": dataclasses.asdict(pack),
        "current_report": report
        or {
            "skipped": True,
            "reason": "bootstrap was skipped; inspect current bundle and registry state directly",
        },
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def prompt_header(
    *,
    repo_root: Path,
    run_root: Path,
    plan_path: Path,
    pack: ExperimentalPackSpec,
    reports: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> str:
    return f"""You are getting one experimental ADES vector pack production-ready.

Repository: {repo_root}
Run root: {run_root}
Readiness plan: {plan_path}
Pack: {pack.pack_id} ({pack.title})
Domain: {pack.domain}
Current purpose: {pack.purpose}

Current bootstrap context:
{pack_artifact_context(pack, reports, run_root)}

Hard requirements:
- Use existing ADES package-generation patterns and keep changes tightly scoped.
- Do not fabricate production provenance. Any source-backed entity/rule must trace to an
  eligible source tier: {", ".join(SOURCE_ELIGIBLE_TIERS)}.
- Treat {", ".join(PROPOSAL_ONLY_TIERS)} as proposal-only, shadow-only, or build-only evidence.
- The current curated BDYA overlay is useful seed material, but it is not enough by itself
  to justify production promotion.
- Add or update focused tests when changing builder behavior, quality gates, registry
  metadata, or evaluation data.
- Write audit artifacts under {run_root}; repo changes should be source, tests, or docs only.
- Preserve unrelated user changes. Inspect git status before edits and do not revert
  work you did not make.
- Do not commit, push, publish to npm/pip, upload a generated registry, or deploy to the
  adestool server unless the launch flags below explicitly allow it.

Launch flags:
- allow_publish={args.allow_publish}
- allow_git_push={args.allow_git_push}
- allow_prod_server_deploy={args.allow_prod_server_deploy}

Important repo paths and commands:
- Source builder: src/ades/packs/bdya_phase6.py
- Generation API: src/ades/packs/generation.py
- Source governance: src/ades/impact/source_catalog.py
- Existing tests: tests/unit/test_bdya_phase6_packs.py
- Refresh CLI: ades registry refresh-generated-packs <bundle-dir...> --output-dir <dir>
- Vector quality CLI: ades registry evaluate-vector-quality {pack.pack_id}
- Published smoke CLI: ades registry smoke-published-release <registry-url>
"""


def source_plan_prompt(
    repo_root: Path,
    run_root: Path,
    plan_path: Path,
    pack: ExperimentalPackSpec,
    reports: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> str:
    return f"""{prompt_header(repo_root=repo_root, run_root=run_root, plan_path=plan_path, pack=pack, reports=reports, args=args)}
Stage: source-plan

Goal:
Create a concrete production-readiness source plan for `{pack.pack_id}`.

Required outputs:
1. `{run_root}/source_plans/{pack.pack_id}.md`
2. `{run_root}/source_plans/{pack.pack_id}.json`

The source plan must:
- Inventory the current entities/rules and classify which are already source-backed,
  which need source replacement, which should remain shadow-only, and which should be cut.
- Identify concrete production-eligible source families for the pack:
{json.dumps(pack.source_targets, indent=2)}
- Define record-level provenance fields that the normalized bundle should carry.
- Define the golden-set shape: exact-match positives, vector-neighbor positives,
  suppression/negative cases, and easy cases that must be 100% pass.
- Define shadow-evaluation metrics for BDYA news digestion: entity lift, passive entity
  lift, terminal candidates, impact paths, warnings/errors, p50/p95/max latency, and
  downstream usefulness.
- End with exact next implementation steps and commands.

Do not publish or deploy anything in this stage.
"""


def implement_prompt(
    repo_root: Path,
    run_root: Path,
    plan_path: Path,
    pack: ExperimentalPackSpec,
    reports: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> str:
    return f"""{prompt_header(repo_root=repo_root, run_root=run_root, plan_path=plan_path, pack=pack, reports=reports, args=args)}
Stage: implement

Goal:
Make `{pack.pack_id}` production-ready in the smallest defensible repo surface.

Required work:
- Read `{run_root}/source_plans/{pack.pack_id}.md` if present; otherwise create the source
  plan first.
- Implement source-backed bundle generation or metadata changes needed for this pack.
- Add/update tests that prove source governance, entity/rule coverage, negative text
  behavior, and generated-pack compatibility.
- Add/update golden-set or audit fixtures needed by `ades registry evaluate-vector-quality`
  when the repo has an established location for them.
- Keep experimental curated entries as build-only/shadow-only if they lack production
  source evidence.
- Only change public registry tier/status after the local production gates have evidence.

Acceptance checks to run when feasible:
- python -m pytest tests/unit/test_bdya_phase6_packs.py
- python -m pytest tests/unit/test_pack_generation.py
- ades registry refresh-generated-packs <bundle-dir...> --output-dir {run_root}/refresh/{pack.pack_id}

Write implementation notes to `{run_root}/implementation/{pack.pack_id}.md`.
Do not publish, push, or deploy in this stage.
"""


def validate_prompt(
    repo_root: Path,
    run_root: Path,
    plan_path: Path,
    pack: ExperimentalPackSpec,
    reports: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> str:
    return f"""{prompt_header(repo_root=repo_root, run_root=run_root, plan_path=plan_path, pack=pack, reports=reports, args=args)}
Stage: validate

Goal:
Run the evidence gates that decide whether `{pack.pack_id}` is production-ready.

Required validation evidence:
1. Regenerate the source bundle and runtime pack.
2. Run focused unit/component tests for BDYA domain packs and generated pack refresh.
3. Run `ades registry refresh-generated-packs` with materialized registry output when
   source governance passes.
4. Run `ades registry evaluate-vector-quality {pack.pack_id}` against the relevant golden
   set/profile if available; otherwise create an explicit blocker naming the missing
   golden set path.
5. Run or prepare saved-vector-audit release validation using
   `scripts/evaluate_saved_vector_audit_release.py` when a saved audit summary exists.
6. Run or prepare BDYA shadow evaluation using the five-pack production pattern:
   general-en, business-vector-en, economics-vector-en, politics-vector-en, finance-en,
   plus up to six country finance packs.

Write `{run_root}/validation/{pack.pack_id}.md` with:
- commands run,
- pass/fail result,
- exact report paths,
- remaining blockers,
- whether the pack can move from experimental to domain/production tier.

Do not publish, push, or deploy in this stage.
"""


def promote_prompt(
    repo_root: Path,
    run_root: Path,
    plan_path: Path,
    pack: ExperimentalPackSpec,
    reports: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> str:
    return f"""{prompt_header(repo_root=repo_root, run_root=run_root, plan_path=plan_path, pack=pack, reports=reports, args=args)}
Stage: promote

Goal:
Prepare `{pack.pack_id}` for production publication after validation evidence exists.

Required work:
- Read `{run_root}/validation/{pack.pack_id}.md` and do not promote if validation failed
  or is missing.
- Prepare the generated registry release using existing ADES release commands.
- If public registry publication is needed, run it only when allow_publish=True.
- If npm or pip package publication is needed because code/package artifacts changed,
  prepare exact commands and publish only when allow_publish=True.
- If adestool production server deployment is needed, deploy only when
  allow_prod_server_deploy=True.
- Commit and push only when allow_git_push=True and the repo is clean except intended
  changes.
- Always run or document `ades registry smoke-published-release <registry-url>` after a
  published registry is available.

Write `{run_root}/promotion/{pack.pack_id}.md` with the final promotion decision, release
commands, deployed URLs or blocker reasons, package versions, and git commit/push status.
"""


PROMPT_BUILDERS = {
    "source-plan": source_plan_prompt,
    "implement": implement_prompt,
    "validate": validate_prompt,
    "promote": promote_prompt,
}


def build_tasks(
    *,
    repo_root: Path,
    run_root: Path,
    plan_path: Path,
    packs: list[ExperimentalPackSpec],
    reports: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> list[QueueTask]:
    tasks: list[QueueTask] = []
    for pack in packs:
        for stage in parse_stages(args):
            prompt = PROMPT_BUILDERS[stage](repo_root, run_root, plan_path, pack, reports, args)
            tasks.append(
                QueueTask(
                    task_id=f"{pack.pack_id}-{stage}",
                    stage=stage,
                    pack=pack,
                    prompt=prompt,
                )
            )
    return tasks


def load_state(state_path: Path, repo_root: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {
            "schema_version": STATE_SCHEMA_VERSION,
            "created_at": utc_now(),
            "repo_root": str(repo_root),
            "tasks": {},
        }
    with state_path.open(encoding="utf-8") as handle:
        state = json.load(handle)
    if state.get("schema_version") != STATE_SCHEMA_VERSION:
        raise RuntimeError(f"Unsupported state schema version in {state_path}")
    return state


def write_state(state_path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = utc_now()
    write_json_atomic(state_path, state)


def ensure_task_record(state: dict[str, Any], task: QueueTask) -> dict[str, Any]:
    records = state.setdefault("tasks", {})
    record = records.get(task.task_id)
    if record is None:
        record = {
            "task_id": task.task_id,
            "pack_id": task.pack.pack_id,
            "domain": task.pack.domain,
            "stage": task.stage,
            "prompt_hash": task.prompt_hash,
            "status": PENDING_STATUS,
            "attempts": [],
        }
        records[task.task_id] = record
    else:
        record.setdefault("attempts", [])
        record.update(
            {
                "pack_id": task.pack.pack_id,
                "domain": task.pack.domain,
                "stage": task.stage,
            }
        )
        previous_hash = record.get("prompt_hash")
        if previous_hash != task.prompt_hash:
            prior_hashes = record.setdefault("previous_prompt_hashes", [])
            if previous_hash and previous_hash not in prior_hashes:
                prior_hashes.append(previous_hash)
            record["prompt_hash"] = task.prompt_hash
            record["prompt_hash_updated_at"] = utc_now()
    return record


def prune_obsolete_task_records(state: dict[str, Any], tasks: list[QueueTask]) -> int:
    current_task_ids = {task.task_id for task in tasks}
    records = state.setdefault("tasks", {})
    obsolete_task_ids = sorted(set(records) - current_task_ids)
    for task_id in obsolete_task_ids:
        records.pop(task_id, None)
    return len(obsolete_task_ids)


def reset_usage_limited_task_records(state: dict[str, Any], tasks: list[QueueTask]) -> int:
    reset_count = 0
    task_ids = {task.task_id for task in tasks}
    for task_id, record in state.setdefault("tasks", {}).items():
        if task_id not in task_ids or record.get("status") != USAGE_LIMIT_STATUS:
            continue
        record["status"] = PENDING_STATUS
        record["reset_from_usage_limited_at"] = utc_now()
        record.pop("next_retry_at", None)
        reset_count += 1
    return reset_count


def task_status(state: dict[str, Any], task: QueueTask) -> str:
    record = state.get("tasks", {}).get(task.task_id)
    if not record:
        return PENDING_STATUS
    return str(record.get("status") or PENDING_STATUS)


def summarize(tasks: list[QueueTask], state: dict[str, Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for task in tasks:
        counts[task_status(state, task)] += 1
    return counts


def pending_tasks(tasks: list[QueueTask], state: dict[str, Any]) -> list[QueueTask]:
    return [task for task in tasks if task_status(state, task) != COMPLETE_STATUS]


def print_summary(tasks: list[QueueTask], state: dict[str, Any]) -> None:
    counts = summarize(tasks, state)
    print(
        "Summary: "
        f"{counts[COMPLETE_STATUS]} complete, "
        f"{counts[FAILED_STATUS]} failed, "
        f"{counts[VALIDATION_FAILED_STATUS]} validation_failed, "
        f"{counts[USAGE_LIMIT_STATUS]} usage_limited, "
        f"{counts[AGENT_UNAVAILABLE_STATUS]} agent_unavailable, "
        f"{counts[PENDING_STATUS]} pending / {len(tasks)} total"
    )


def write_progress_markdown(progress_path: Path, tasks: list[QueueTask], state: dict[str, Any]) -> None:
    counts = summarize(tasks, state)
    lines = [
        "# Experimental ADES Vector Pack Readiness Queue",
        "",
        f"- Updated: {utc_now()}",
        f"- Repo root: {state.get('repo_root')}",
        f"- Run root: {state.get('run_root')}",
        f"- Agent: {state.get('agent')}",
        f"- Total tasks: {len(tasks)}",
        f"- Complete: {counts[COMPLETE_STATUS]}",
        f"- Failed: {counts[FAILED_STATUS]}",
        f"- Validation failed: {counts[VALIDATION_FAILED_STATUS]}",
        f"- Usage limited: {counts[USAGE_LIMIT_STATUS]}",
        f"- Agent unavailable: {counts[AGENT_UNAVAILABLE_STATUS]}",
        f"- Pending: {counts[PENDING_STATUS]}",
        "",
        "## Packs",
        "",
    ]
    for pack_id in sorted({task.pack.pack_id for task in tasks}):
        scoped = [task for task in tasks if task.pack.pack_id == pack_id]
        done = sum(1 for task in scoped if task_status(state, task) == COMPLETE_STATUS)
        lines.append(f"- {pack_id}: {done}/{len(scoped)}")
    lines.extend(["", "## Next Incomplete Tasks", ""])
    for task in pending_tasks(tasks, state)[:20]:
        lines.append(f"- `{task.task_id}`: {task_status(state, task)}, {task.stage}")
    if not pending_tasks(tasks, state):
        lines.append("- None.")
    lines.append("")
    write_text_atomic(progress_path, "\n".join(lines))


def prompt_path_for_task(state_dir: Path, task: QueueTask) -> Path:
    return state_dir / "prompts" / f"{task.task_id}.txt"


def log_path_for_task(state_dir: Path, task: QueueTask, attempt: int) -> Path:
    return state_dir / "logs" / f"{task.task_id}.attempt-{attempt:03d}.log"


def command_for_task(template: str, agent: str, task: QueueTask, repo_root: Path, prompt_file: Path) -> list[str]:
    rendered = template.format(
        agent=agent,
        task_id=task.task_id,
        pack=task.pack.pack_id,
        stage=task.stage,
        prompt_file=str(prompt_file),
        repo_root=str(repo_root),
    )
    return shlex.split(rendered)


def parse_json_list(raw: str) -> list[Any]:
    text = raw.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"(\[.*\]|\{.*\})", text, flags=re.DOTALL)
        if not match:
            return []
        try:
            parsed = json.loads(match.group(1))
        except json.JSONDecodeError:
            return []
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for key in ("agents", "items", "data", "records", "limits"):
            value = parsed.get(key)
            if isinstance(value, list):
                return value
        return [parsed]
    return []


def value_contains_blocked_term(value: Any) -> bool:
    if isinstance(value, dict):
        return any(value_contains_blocked_term(item) for item in value.values())
    if isinstance(value, list):
        return any(value_contains_blocked_term(item) for item in value)
    text = str(value).lower()
    blocked_terms = (
        "exhausted",
        "usage limited",
        "usage_limited",
        "limit exceeded",
        "quota exceeded",
    )
    ok_terms = ("not exhausted", "not limited", "unlimited", "available")
    return (
        "usage limit" in text
        or any(term in text for term in blocked_terms)
        or re.search(r"\blimited\b", text) is not None
    ) and not any(term in text for term in ok_terms)


def parse_reset_datetime(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def collect_reset_datetimes(value: Any) -> list[dt.datetime]:
    reset_times: list[dt.datetime] = []
    if isinstance(value, dict):
        for key, item in value.items():
            normalized_key = str(key).lower().replace("_", "")
            if normalized_key in {"resetat", "effectiveresetat", "estimatedresetat"}:
                parsed = parse_reset_datetime(item)
                if parsed is not None:
                    reset_times.append(parsed)
            reset_times.extend(collect_reset_datetimes(item))
    elif isinstance(value, list):
        for item in value:
            reset_times.extend(collect_reset_datetimes(item))
    return reset_times


def value_mentions_active_block(value: Any) -> bool:
    if not value_contains_blocked_term(value):
        return False
    reset_times = collect_reset_datetimes(value)
    if reset_times and max(reset_times) <= dt.datetime.now(dt.timezone.utc):
        return False
    return True


def ensure_usage_available(args: argparse.Namespace, repo_root: Path) -> tuple[bool, str]:
    if args.skip_usage_limit_check:
        return True, "skipped"
    command = shlex.split(args.usage_limit_command_template.format(agent=args.agent))
    try:
        result = subprocess.run(
            command,
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=args.usage_limit_timeout_seconds or None,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"usage-limit check failed for {args.agent}: {exc}"
    output = result.stdout or ""
    records = parse_json_list(output)
    if result.returncode != 0 and not records:
        return False, f"usage-limit check failed for {args.agent}: {output[-1000:]}"
    if any(value_mentions_active_block(record) for record in records):
        return False, f"usage appears limited for {args.agent}: {output[-1000:]}"
    return True, "available"


def health_status_from_agent(item: dict[str, Any]) -> str:
    for key in ("health_status", "healthStatus", "status"):
        value = item.get(key)
        if value:
            return str(value).lower()
    health = item.get("health")
    if isinstance(health, dict):
        return str(health.get("status") or health.get("health_status") or "unknown").lower()
    if health:
        return str(health).lower()
    return "unknown"


def ensure_agent_health(repo_root: Path, args: argparse.Namespace) -> tuple[bool, str]:
    if args.skip_agent_health_check:
        return True, "skipped"
    command_options = (
        ["mcoda", "agent", "list", "--json", "--refresh-health"],
        ["mcoda", "agent", "list", "--json"],
    )
    for command in command_options:
        try:
            result = subprocess.run(
                command,
                cwd=repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=600,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode != 0:
            continue
        by_slug: dict[str, dict[str, Any]] = {}
        for item in parse_json_list(result.stdout or ""):
            if not isinstance(item, dict):
                continue
            for key in ("agentSlug", "slug", "id", "agent_id", "name"):
                value = item.get(key)
                if value:
                    by_slug[str(value)] = item
        agent = by_slug.get(args.agent)
        if agent is None:
            return False, f"{args.agent}=missing"
        status = health_status_from_agent(agent)
        return status in {"healthy", "ok", "unknown", "-"}, f"{args.agent}={status}"
    return False, "could not read mcoda agent health"


def read_runner_lock_info(lock_path: Path) -> RunnerLockInfo:
    if not lock_path.exists():
        return RunnerLockInfo(pid=None, created_at=None)
    try:
        raw = json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return RunnerLockInfo(pid=None, created_at=None)
    pid = raw.get("pid")
    return RunnerLockInfo(pid=pid if isinstance(pid, int) else None, created_at=raw.get("created_at"))


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def lock_age_seconds(info: RunnerLockInfo) -> int | None:
    if not info.created_at:
        return None
    try:
        created = dt.datetime.fromisoformat(info.created_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=dt.timezone.utc)
    return max(0, int((dt.datetime.now(dt.timezone.utc) - created).total_seconds()))


class RunnerLock:
    def __init__(self, lock_path: Path, args: argparse.Namespace) -> None:
        self.lock_path = lock_path
        self.args = args
        self.acquired = False

    def __enter__(self) -> "RunnerLock":
        started = time.monotonic()
        while self.lock_path.exists() and not self.args.ignore_lock:
            info = read_runner_lock_info(self.lock_path)
            age = lock_age_seconds(info)
            stale_by_age = age is not None and age >= self.args.lock_stale_seconds
            stale_by_pid = info.pid is not None and not process_alive(info.pid)
            if stale_by_age or stale_by_pid:
                if info.pid and process_alive(info.pid) and self.args.kill_stale_lock_process:
                    warn(f"killing stale lock process pid={info.pid}")
                    os.kill(info.pid, signal.SIGTERM)
                    time.sleep(3)
                warn(f"removing stale runner lock at {self.lock_path}")
                self.lock_path.unlink(missing_ok=True)
                continue
            waited = int(time.monotonic() - started)
            if self.args.lock_max_wait_seconds and waited >= self.args.lock_max_wait_seconds:
                raise RuntimeError(
                    f"Runner lock exists at {self.lock_path} "
                    f"(pid={info.pid}, created_at={info.created_at})"
                )
            warn(f"runner lock exists at {self.lock_path}; waiting {self.args.lock_retry_delay_seconds}s")
            time.sleep(self.args.lock_retry_delay_seconds)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        write_json_atomic(self.lock_path, {"pid": os.getpid(), "created_at": utc_now()})
        self.acquired = True
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self.acquired:
            self.lock_path.unlink(missing_ok=True)


def scan_merge_conflict_markers(repo_root: Path) -> list[str]:
    errors: list[str] = []
    marker_pattern = re.compile(r"^(<<<<<<<|=======|>>>>>>>)(?:\s|$)")
    for path in repo_root.rglob("*"):
        if not path.is_file() or any(part in EXCLUDED_SCAN_DIRS for part in path.parts):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not marker_pattern.match(line):
                continue
            errors.append(
                f"merge conflict marker in {path.relative_to(repo_root)}:{line_number}"
            )
            break
        if len(errors) >= 20:
            break
    return errors


def validate_repo_after_task(repo_root: Path) -> list[str]:
    errors = scan_merge_conflict_markers(repo_root)
    try:
        result = subprocess.run(
            ["git", "diff", "--check"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        errors.append(f"git diff --check failed to run: {exc}")
    else:
        if result.returncode != 0:
            errors.append((result.stdout or "git diff --check failed").strip())
    return errors


def classify_failed_log(log_path: Path) -> str:
    try:
        lowered = log_path.read_text(encoding="utf-8", errors="replace").lower()
    except OSError:
        lowered = ""
    if "reasoning.effort" in lowered and "invalid value" in lowered:
        return FAILED_STATUS
    if any(
        term in lowered
        for term in (
            "usage limit",
            "usage_limited",
            "quota exceeded",
            "limit exceeded",
            "you've hit your usage limit",
        )
    ):
        return USAGE_LIMIT_STATUS
    if any(term in lowered for term in ("agent_unavailable", "unreachable", "connection refused")):
        return AGENT_UNAVAILABLE_STATUS
    return FAILED_STATUS


def wait_with_state(
    *,
    task: QueueTask,
    tasks: list[QueueTask],
    state: dict[str, Any],
    state_path: Path,
    progress_path: Path,
    delay_seconds: int,
    reason: str,
) -> None:
    record = ensure_task_record(state, task)
    retry_at = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=max(0, delay_seconds))
    record["next_retry_at"] = retry_at.isoformat(timespec="seconds")
    record["last_retry_reason"] = reason
    write_state(state_path, state)
    write_progress_markdown(progress_path, tasks, state)
    print(f"{task.task_id}: {reason}; retrying in {delay_seconds}s")
    if delay_seconds > 0:
        time.sleep(delay_seconds)


def run_task(
    *,
    task: QueueTask,
    tasks: list[QueueTask],
    index: int,
    total: int,
    repo_root: Path,
    state_dir: Path,
    state_path: Path,
    progress_path: Path,
    state: dict[str, Any],
    args: argparse.Namespace,
) -> bool:
    record = ensure_task_record(state, task)
    attempts = record.setdefault("attempts", [])
    attempt_no = len(attempts) + 1
    prompt_file = prompt_path_for_task(state_dir, task)
    log_file = log_path_for_task(state_dir, task, attempt_no)
    write_text_atomic(prompt_file, task.prompt)
    command = command_for_task(args.command_template, args.agent, task, repo_root, prompt_file)
    started_at = utc_now()
    attempt = {
        "attempt": attempt_no,
        "agent": args.agent,
        "started_at": started_at,
        "prompt_path": str(prompt_file),
        "log_path": str(log_file),
        "command": command,
    }
    attempts.append(attempt)
    record.update(
        {
            "status": RUNNING_STATUS,
            "agent": args.agent,
            "last_started_at": started_at,
            "last_prompt_path": str(prompt_file),
            "last_log_path": str(log_file),
            "command": command,
        }
    )
    write_state(state_path, state)
    write_progress_markdown(progress_path, tasks, state)
    print(f"[{index}/{total}] Running {task.task_id} ({task.stage}, attempt={attempt_no})")
    print(f"Log: {log_file}")
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w", encoding="utf-8") as handle:
        handle.write(f"Task: {task.task_id}\n")
        handle.write(f"Pack: {task.pack.pack_id}\n")
        handle.write(f"Started: {started_at}\n")
        handle.write(f"Command: {shlex.join(command)}\n\n")
        handle.flush()
        try:
            result = subprocess.run(
                command,
                cwd=repo_root,
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=None if args.timeout_seconds <= 0 else args.timeout_seconds,
                check=False,
            )
            exit_code = result.returncode
            timed_out = False
        except subprocess.TimeoutExpired:
            exit_code = -1
            timed_out = True
            handle.write(f"\nTimed out after {args.timeout_seconds}s\n")
        except KeyboardInterrupt:
            record["status"] = INTERRUPTED_STATUS
            raise
        except OSError as exc:
            exit_code = -1
            timed_out = False
            handle.write(f"\nFailed to start command: {exc}\n")

    completed_at = utc_now()
    attempt.update({"completed_at": completed_at, "exit_code": exit_code, "timed_out": timed_out})
    record.update(
        {
            "last_completed_at": completed_at,
            "last_exit_code": exit_code,
            "timed_out": timed_out,
        }
    )
    if exit_code != 0:
        record["status"] = classify_failed_log(log_file)
        write_state(state_path, state)
        write_progress_markdown(progress_path, tasks, state)
        return False

    validation_errors = validate_repo_after_task(repo_root)
    if validation_errors:
        record["status"] = VALIDATION_FAILED_STATUS
        record["validation_errors"] = validation_errors
        attempt["validation_errors"] = validation_errors
        write_state(state_path, state)
        write_progress_markdown(progress_path, tasks, state)
        return False

    record["status"] = COMPLETE_STATUS
    write_state(state_path, state)
    write_progress_markdown(progress_path, tasks, state)
    print(f"[{index}/{total}] Complete {task.task_id}")
    return True


def list_packs(packs: Iterable[ExperimentalPackSpec]) -> None:
    for pack in packs:
        print(f"{pack.pack_id}: domain={pack.domain} title={pack.title}")


def list_tasks(tasks: list[QueueTask], state: dict[str, Any], show_prompts: bool) -> None:
    for task in tasks:
        print(f"{task.task_id}: {task_status(state, task)}, {task.stage}")
        if show_prompts:
            print("\n".join(f"  {line}" for line in task.prompt.splitlines()))


def print_build_context(
    *,
    args: argparse.Namespace,
    packs: list[ExperimentalPackSpec],
    tasks: list[QueueTask],
    run_root: Path,
    state_dir: Path,
    plan_path: Path,
) -> None:
    print(
        "Experimental ADES vector readiness queue: "
        f"{len(packs)} pack(s), {len(tasks)} task(s), agent={args.agent}"
    )
    print(f"Mcoda command template: {args.command_template}")
    print(f"Run root: {run_root}")
    print(f"Plan: {plan_path}")
    print(f"Queue state: {state_dir}")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    repo_root = args.repo_root.resolve()
    output_root = args.output_root.resolve()
    run_root = output_root / args.run_id
    state_dir = (
        resolve_under_repo(repo_root, args.state_dir)
        if args.state_dir
        else run_root / "queue"
    )
    progress_path = resolve_under_repo(repo_root, args.progress) if args.progress else state_dir / "progress.md"
    state_path = state_dir / "state.json"
    lock_path = state_dir / "runner.lock"
    plan_path = run_root / "prod_readiness_plan.md"
    packs = selected_packs(args)

    if args.list_packs:
        list_packs(packs)
        return 0

    reports: dict[str, dict[str, Any]] = {}
    if not args.skip_bootstrap:
        reports = bootstrap_current_pack_reports(
            repo_root=repo_root,
            run_root=run_root,
            version=args.version,
            packs=packs,
        )
    write_readiness_plan(
        plan_path=plan_path,
        repo_root=repo_root,
        run_root=run_root,
        packs=packs,
        reports=reports,
        args=args,
    )

    tasks = build_tasks(
        repo_root=repo_root,
        run_root=run_root,
        plan_path=plan_path,
        packs=packs,
        reports=reports,
        args=args,
    )

    state = load_state(state_path, repo_root)
    state.update(
        {
            "agent": args.agent,
            "repo_root": str(repo_root),
            "run_root": str(run_root),
            "plan_path": str(plan_path),
            "version": args.version,
            "pack_count": len(packs),
            "task_count": len(tasks),
            "allow_publish": args.allow_publish,
            "allow_git_push": args.allow_git_push,
            "allow_prod_server_deploy": args.allow_prod_server_deploy,
            "bootstrap_skipped": args.skip_bootstrap,
        }
    )
    for task in tasks:
        ensure_task_record(state, task)
    pruned = prune_obsolete_task_records(state, tasks)
    reset_usage_limited = reset_usage_limited_task_records(state, tasks)
    write_state(state_path, state)
    write_progress_markdown(progress_path, tasks, state)
    if pruned:
        print(f"Pruned {pruned} obsolete task record(s) from queue state.")
    if reset_usage_limited:
        print(f"Reset {reset_usage_limited} usage-limited task record(s) for fresh retry.")
    print_summary(tasks, state)

    if args.bootstrap_only:
        print(f"Bootstrap complete. Plan written to: {plan_path}")
        return 0
    if args.list_tasks:
        list_tasks(tasks, state, args.show_prompts)
        return 0

    print_build_context(
        args=args,
        packs=packs,
        tasks=tasks,
        run_root=run_root,
        state_dir=state_dir,
        plan_path=plan_path,
    )
    if args.dry_run:
        limit = args.max_runs if args.max_runs > 0 else 25
        print(f"Dry run. Showing up to {limit} incomplete task(s); no agents will be launched.")
        for task in pending_tasks(tasks, state)[:limit]:
            print(f"- {task.task_id}: {task.stage}, agent={args.agent}")
            if args.show_prompts:
                print("\n".join(f"  {line}" for line in task.prompt.splitlines()))
        return 0

    health_retry = 0
    while True:
        ok, health_reason = ensure_agent_health(repo_root, args)
        if ok:
            if health_reason not in {"healthy", "skipped"}:
                print(f"Agent health: {health_reason}")
            break
        if args.fail_fast:
            print(f"Cannot start queue because agent health is not ready: {health_reason}")
            return 2
        health_retry += 1
        if args.agent_unavailable_max_retries and health_retry > args.agent_unavailable_max_retries:
            print(f"Cannot start queue after health retries: {health_reason}")
            return 2
        try:
            print(
                f"Agent health not ready: {health_reason}; retry {health_retry} "
                f"in {args.agent_unavailable_retry_delay_seconds}s."
            )
            time.sleep(args.agent_unavailable_retry_delay_seconds)
        except KeyboardInterrupt:
            print("Interrupted while waiting for agent health retry.")
            return 130

    attempted = 0
    with RunnerLock(lock_path, args):
        for index, task in enumerate(tasks, start=1):
            consecutive: Counter[str] = Counter()
            while task_status(state, task) != COMPLETE_STATUS:
                if args.max_runs > 0 and attempted >= args.max_runs:
                    print(f"Reached --max-runs={args.max_runs}; stopping.")
                    return 0

                usage_ok, usage_reason = ensure_usage_available(args, repo_root)
                if not usage_ok:
                    record = ensure_task_record(state, task)
                    record["status"] = USAGE_LIMIT_STATUS
                    record["usage_limit_reason"] = usage_reason
                    write_state(state_path, state)
                    write_progress_markdown(progress_path, tasks, state)
                    print_summary(tasks, state)
                    if args.fail_fast:
                        print(f"Stopping before {task.task_id}: {usage_reason}")
                        return 1
                    consecutive[USAGE_LIMIT_STATUS] += 1
                    if (
                        args.usage_limit_max_retries
                        and consecutive[USAGE_LIMIT_STATUS] > args.usage_limit_max_retries
                    ):
                        print(f"Continuing after usage-limit retry budget for {task.task_id}.")
                        break
                    try:
                        wait_with_state(
                            task=task,
                            tasks=tasks,
                            state=state,
                            state_path=state_path,
                            progress_path=progress_path,
                            delay_seconds=args.usage_limit_retry_delay_seconds,
                            reason=usage_reason,
                        )
                    except KeyboardInterrupt:
                        print("Interrupted while waiting for usage-limit retry; state saved.")
                        return 130
                    continue

                attempted += 1
                try:
                    ok = run_task(
                        task=task,
                        tasks=tasks,
                        index=index,
                        total=len(tasks),
                        repo_root=repo_root,
                        state_dir=state_dir,
                        state_path=state_path,
                        progress_path=progress_path,
                        state=state,
                        args=args,
                    )
                except KeyboardInterrupt:
                    record = ensure_task_record(state, task)
                    record["status"] = INTERRUPTED_STATUS
                    write_state(state_path, state)
                    write_progress_markdown(progress_path, tasks, state)
                    print("Interrupted; state saved.")
                    return 130

                print_summary(tasks, state)
                if ok:
                    break

                status = task_status(state, task)
                if args.fail_fast:
                    print(f"Stopping after {status}. Rerun to retry from this task.")
                    return 1
                if status not in RETRYABLE_STATUSES:
                    print(f"Continuing after non-retryable status {status} for {task.task_id}.")
                    break
                consecutive[status] += 1
                retry_limit = {
                    USAGE_LIMIT_STATUS: args.usage_limit_max_retries,
                    AGENT_UNAVAILABLE_STATUS: args.agent_unavailable_max_retries,
                }.get(status, args.task_failure_max_retries)
                if retry_limit and consecutive[status] > retry_limit:
                    print(f"Continuing after {retry_limit} retry(s) for {task.task_id} ({status}).")
                    break
                delay = {
                    USAGE_LIMIT_STATUS: args.usage_limit_retry_delay_seconds,
                    AGENT_UNAVAILABLE_STATUS: args.agent_unavailable_retry_delay_seconds,
                }.get(status, args.task_failure_retry_delay_seconds)
                try:
                    wait_with_state(
                        task=task,
                        tasks=tasks,
                        state=state,
                        state_path=state_path,
                        progress_path=progress_path,
                        delay_seconds=delay,
                        reason=status,
                    )
                except KeyboardInterrupt:
                    print("Interrupted while waiting for retry; state saved.")
                    return 130

    remaining = len(pending_tasks(tasks, state))
    if remaining:
        print(f"Queue finished a pass with {remaining} incomplete task(s). Rerun to retry.")
        return 1
    print("All experimental ADES vector-pack readiness tasks are complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
