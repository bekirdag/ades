"""Load installed pack content for runtime tagging."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .registry import PackRegistry


@dataclass(frozen=True)
class RuleDefinition:
    """Single deterministic extraction rule."""

    name: str
    label: str
    pattern: str
    source_pack: str
    source_domain: str


@dataclass(frozen=True)
class PackRuntime:
    """Merged runtime view for an installed pack and its dependencies."""

    pack_id: str
    language: str
    domain: str
    labels: list[str] = field(default_factory=list)
    rules: list[RuleDefinition] = field(default_factory=list)
    pack_chain: list[str] = field(default_factory=list)


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
            RuleDefinition(
                name=item["name"],
                label=item["label"],
                pattern=item["pattern"],
                source_pack=current_manifest.pack_id,
                source_domain=item["source_domain"],
            )
            for item in registry.list_pack_rules(current_pack_id)
        )
        seen_packs.add(current_pack_id)
        pack_chain.append(current_pack_id)

    visit(pack_id)
    deduped_labels = list(dict.fromkeys(labels))
    return PackRuntime(
        pack_id=manifest.pack_id,
        language=manifest.language,
        domain=manifest.domain,
        labels=deduped_labels,
        rules=rules,
        pack_chain=pack_chain,
    )
