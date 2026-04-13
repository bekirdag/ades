"""Shared structural gates for generated `general-en` packs."""

from __future__ import annotations

from typing import Any


GENERAL_REAL_PACK_ENTITY_THRESHOLD = 10_000
GENERAL_SMALL_PACK_MIN_PERSON_COUNT = 3
GENERAL_SMALL_PACK_MIN_ORGANIZATION_COUNT = 3
GENERAL_SMALL_PACK_MIN_LOCATION_COUNT = 1
GENERAL_REAL_PACK_MIN_PERSON_COUNT = 1_000
GENERAL_REAL_PACK_MIN_ORGANIZATION_COUNT = 1_000
GENERAL_REAL_PACK_MIN_LOCATION_COUNT = 1_000
GENERAL_REAL_PACK_MIN_CORE_RATIO = 0.01
GENERAL_REAL_PACK_MAX_LOCATION_RATIO = 0.995


def evaluate_general_pack_structure(build_payload: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Return structural failures and warnings for one generated general-en build."""

    label_distribution = build_payload.get("entity_label_distribution")
    if label_distribution is None:
        label_distribution = build_payload.get("label_distribution", {})
    if not isinstance(label_distribution, dict):
        return [], []

    person_count = int(label_distribution.get("person", 0) or 0)
    organization_count = int(label_distribution.get("organization", 0) or 0)
    location_count = int(label_distribution.get("location", 0) or 0)
    included_entity_count = int(build_payload.get("included_entity_count", 0) or 0)
    core_count = person_count + organization_count

    failures: list[str] = []
    warnings: list[str] = []

    if included_entity_count >= GENERAL_REAL_PACK_ENTITY_THRESHOLD:
        if person_count < GENERAL_REAL_PACK_MIN_PERSON_COUNT:
            failures.append(
                "General-en structural gate failed: "
                f"person count {person_count} is below "
                f"{GENERAL_REAL_PACK_MIN_PERSON_COUNT}."
            )
        if organization_count < GENERAL_REAL_PACK_MIN_ORGANIZATION_COUNT:
            failures.append(
                "General-en structural gate failed: "
                f"organization count {organization_count} is below "
                f"{GENERAL_REAL_PACK_MIN_ORGANIZATION_COUNT}."
            )
        if location_count < GENERAL_REAL_PACK_MIN_LOCATION_COUNT:
            failures.append(
                "General-en structural gate failed: "
                f"location count {location_count} is below "
                f"{GENERAL_REAL_PACK_MIN_LOCATION_COUNT}."
            )
        core_ratio = core_count / included_entity_count if included_entity_count else 0.0
        location_ratio = location_count / included_entity_count if included_entity_count else 0.0
        if core_ratio < GENERAL_REAL_PACK_MIN_CORE_RATIO:
            failures.append(
                "General-en structural gate failed: "
                f"person+organization ratio {core_ratio:.4f} is below "
                f"{GENERAL_REAL_PACK_MIN_CORE_RATIO:.4f}."
            )
        if location_ratio > GENERAL_REAL_PACK_MAX_LOCATION_RATIO:
            failures.append(
                "General-en structural gate failed: "
                f"location ratio {location_ratio:.4f} exceeds "
                f"{GENERAL_REAL_PACK_MAX_LOCATION_RATIO:.4f}."
            )
    else:
        if person_count < GENERAL_SMALL_PACK_MIN_PERSON_COUNT:
            failures.append(
                "General-en structural gate failed: "
                f"person count {person_count} is below "
                f"{GENERAL_SMALL_PACK_MIN_PERSON_COUNT}."
            )
        if organization_count < GENERAL_SMALL_PACK_MIN_ORGANIZATION_COUNT:
            failures.append(
                "General-en structural gate failed: "
                f"organization count {organization_count} is below "
                f"{GENERAL_SMALL_PACK_MIN_ORGANIZATION_COUNT}."
            )
        if location_count < GENERAL_SMALL_PACK_MIN_LOCATION_COUNT:
            failures.append(
                "General-en structural gate failed: "
                f"location count {location_count} is below "
                f"{GENERAL_SMALL_PACK_MIN_LOCATION_COUNT}."
            )

    if not failures and included_entity_count:
        warnings.append(
            "general-en structural summary: "
            f"person={person_count}, organization={organization_count}, "
            f"location={location_count}, total={included_entity_count}"
        )

    return failures, warnings
