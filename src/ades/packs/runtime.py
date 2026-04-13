"""Load installed pack content for runtime tagging."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Pattern

from .registry import PackRegistry
from .rule_validation import ReviewedRule, compile_reviewed_rule


_DEFAULT_DENSITY_THRESHOLDS = {
    "general": 0.25,
    "finance": 0.35,
    "medical": 0.35,
}


@dataclass(frozen=True)
class RuleDefinition:
    """Single deterministic extraction rule."""

    name: str
    label: str
    pattern: str
    compiled_pattern: Pattern[str]
    engine: str
    source_pack: str
    source_domain: str


@dataclass(frozen=True)
class MatcherDefinition:
    """One installed compiled matcher artifact."""

    pack_id: str
    source_domain: str
    artifact_path: str
    entries_path: str


@dataclass(frozen=True)
class PackRuntime:
    """Merged runtime view for an installed pack and its dependencies."""

    schema_version: int
    pack_id: str
    pack_version: str
    language: str
    domain: str
    labels: list[str] = field(default_factory=list)
    rules: list[RuleDefinition] = field(default_factory=list)
    matchers: list[MatcherDefinition] = field(default_factory=list)
    pack_chain: list[str] = field(default_factory=list)
    min_entities_per_100_tokens_warning: float | None = None


def load_pack_runtime(
    storage_root: Path,
    pack_id: str,
    *,
    registry: PackRegistry | None = None,
) -> PackRuntime | None:
    """Load the requested installed pack and all dependency data."""

    registry = registry or PackRegistry(storage_root)
    manifest = registry.get_pack(pack_id, active_only=True)
    if manifest is None:
        return None

    labels: list[str] = []
    rules: list[RuleDefinition] = []
    matchers: list[MatcherDefinition] = []
    seen_packs: set[str] = set()
    pack_chain: list[str] = []

    def visit(current_pack_id: str) -> None:
        if current_pack_id in seen_packs:
            return
        current_manifest = registry.get_pack(current_pack_id, active_only=True)
        if current_manifest is None:
            return

        for dependency in current_manifest.dependencies:
            visit(dependency)

        labels.extend(registry.list_pack_labels(current_pack_id))
        rules.extend(
            _build_rule_definition(
                item=item,
                source_pack=current_manifest.pack_id,
            )
            for item in registry.list_pack_rules(current_pack_id)
        )
        if current_manifest.matcher is not None:
            artifact_path = registry.layout.packs_dir / current_manifest.pack_id / current_manifest.matcher.artifact_path
            entries_path = registry.layout.packs_dir / current_manifest.pack_id / current_manifest.matcher.entries_path
            if artifact_path.exists() and entries_path.exists():
                matchers.append(
                    MatcherDefinition(
                        pack_id=current_manifest.pack_id,
                        source_domain=current_manifest.domain,
                        artifact_path=str(artifact_path),
                        entries_path=str(entries_path),
                    )
                )
        seen_packs.add(current_pack_id)
        pack_chain.append(current_pack_id)

    visit(pack_id)
    deduped_labels = list(dict.fromkeys(labels))
    return PackRuntime(
        schema_version=manifest.schema_version,
        pack_id=manifest.pack_id,
        pack_version=manifest.version,
        language=manifest.language,
        domain=manifest.domain,
        labels=deduped_labels,
        rules=rules,
        matchers=matchers,
        pack_chain=pack_chain,
        min_entities_per_100_tokens_warning=(
            manifest.min_entities_per_100_tokens_warning
            if manifest.min_entities_per_100_tokens_warning is not None
            else _DEFAULT_DENSITY_THRESHOLDS.get(manifest.domain)
        ),
    )


def _build_rule_definition(
    *,
    item: dict[str, str],
    source_pack: str,
) -> RuleDefinition:
    reviewed_rule = compile_reviewed_rule(item["pattern"])
    return RuleDefinition(
        name=item["name"],
        label=item["label"],
        pattern=item["pattern"],
        compiled_pattern=reviewed_rule.compiled,
        engine=reviewed_rule.engine,
        source_pack=source_pack,
        source_domain=item["source_domain"],
    )
