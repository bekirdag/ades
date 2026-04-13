import pytest

from ades.packs.rule_validation import compile_reviewed_rule, validate_rule_pattern


def test_compile_reviewed_rule_accepts_safe_patterns() -> None:
    reviewed = compile_reviewed_rule(r"ENTITY-[0-9]+")

    assert reviewed.pattern == r"ENTITY-[0-9]+"
    assert reviewed.engine in {"re2", "python-re-reviewed"}
    assert reviewed.compiled.search("entity-42") is not None


@pytest.mark.parametrize(
    "pattern",
    [
        r"(entity)\1",
        r"(?<=entity) alpha",
    ],
)
def test_validate_rule_pattern_rejects_high_risk_constructs(pattern: str) -> None:
    with pytest.raises(ValueError, match="unsupported high-risk construct"):
        validate_rule_pattern(pattern)


def test_validate_rule_pattern_rejects_nested_repeats() -> None:
    with pytest.raises(ValueError, match="nested repeats"):
        validate_rule_pattern(r"(entity+)+")


def test_validate_rule_pattern_allows_optional_wrapper_around_repeated_group() -> None:
    validate_rule_pattern(r"(entity+)?")
