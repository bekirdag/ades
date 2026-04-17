import importlib.util
from pathlib import Path


def _load_script_module() -> object:
    script_path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "run_general_alias_batch_review_with_mcoda.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_general_alias_batch_review_with_mcoda_script",
        script_path,
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_parse_batch_review_text_extracts_first_json_object_from_prose() -> None:
    module = _load_script_module()
    raw_output = """
Thinking...
Some prose first.
{"drops":[{"line":1,"reason_code":"generic_retained_alias_from_batch_review","reason":"too generic for general-en"}]}
Extra trailing prose.
"""

    parsed = module.parse_batch_review_text(
        raw_output,
        expected_review_keys={"k1", "k2"},
        review_key_by_line_number={1: "k1", 2: "k2"},
    )

    assert parsed == [
        {
            "review_key": "k1",
            "decision": "drop",
            "reason_code": "generic_retained_alias_from_batch_review",
            "reason": "too generic for general-en",
        }
    ]


def test_parse_batch_review_text_skips_placeholder_schema_object() -> None:
    module = _load_script_module()
    raw_output = """
Thinking...
Example schema: {"drops":[{"line":0,"reason_code":"short_snake_case","reason":"short reason"}]}
Actual answer: {"drops":[{"line":1,"reason_code":"generic_retained_alias_from_batch_review","reason":"too generic for general-en"}]}
"""

    parsed = module.parse_batch_review_text(
        raw_output,
        expected_review_keys={"k1", "k2"},
        review_key_by_line_number={1: "k1", 2: "k2"},
    )

    assert parsed == [
        {
            "review_key": "k1",
            "decision": "drop",
            "reason_code": "generic_retained_alias_from_batch_review",
            "reason": "too generic for general-en",
        }
    ]


def test_parse_batch_review_text_accepts_full_reviews_payload() -> None:
    module = _load_script_module()
    raw_output = """
{"reviews":[
  {"review_key":"k1","decision":"drop","reason_code":"generic_retained_alias_from_batch_review","reason":"too generic"},
  {"review_key":"k2","decision":"keep","reason_code":"specific_retained_alias","reason":"specific enough"}
]}
"""

    parsed = module.parse_batch_review_text(
        raw_output,
        expected_review_keys={"k1", "k2"},
        review_key_by_line_number={1: "k1", 2: "k2"},
    )

    assert parsed == [
        {
            "review_key": "k1",
            "decision": "drop",
            "reason_code": "generic_retained_alias_from_batch_review",
            "reason": "too generic",
        },
        {
            "review_key": "k2",
            "decision": "keep",
            "reason_code": "specific_retained_alias",
            "reason": "specific enough",
        },
    ]


def test_review_alias_batch_retries_with_repair_prompt() -> None:
    module = _load_script_module()
    aliases = [
        {
            "review_key": "k1",
            "text": "Cars",
            "label": "organization",
            "canonical_text": "Cars",
            "entity_id": "organization:cars",
            "source_name": "curated-general",
            "runtime_tier": "runtime_exact_high_precision",
        },
        {
            "review_key": "k2",
            "text": "North Harbor",
            "label": "location",
            "canonical_text": "North Harbor",
            "entity_id": "location:north-harbor",
            "source_name": "curated-general",
            "runtime_tier": "runtime_exact_high_precision",
        },
    ]
    prompts: list[str] = []
    responses = iter(
        [
            "Thinking...\nI am not sure.\n",
            '{"drops":[{"line":1,"reason_code":"generic_retained_alias_from_batch_review","reason":"too generic for general-en"}]}',
        ]
    )

    def runner(prompt: str) -> str:
        prompts.append(prompt)
        return next(responses)

    reviews = module.review_alias_batch(
        aliases,
        runner=runner,
        max_retries=1,
    )

    assert reviews == [
        {
            "review_key": "k1",
            "decision": "drop",
            "reason_code": "generic_retained_alias_from_batch_review",
            "reason": "too generic for general-en",
        },
        {
            "review_key": "k2",
            "decision": "keep",
            "reason_code": "implicit_keep_from_batch_review",
            "reason": "Not listed among generic aliases in chunk review.",
        },
    ]
    assert len(prompts) == 2
    assert "Your previous answer was not valid JSON." in prompts[1]
    assert "Cars" in prompts[0]
    assert "North Harbor" in prompts[0]
    assert '"review_key"' not in prompts[0]
    assert '"line":0' in prompts[0]


def test_review_alias_batch_implicit_keep_for_omitted_aliases() -> None:
    module = _load_script_module()
    aliases = [
        {
            "review_key": "k1",
            "text": "Cars",
            "label": "organization",
            "canonical_text": "Cars",
            "entity_id": "organization:cars",
            "source_name": "curated-general",
            "runtime_tier": "runtime_exact_high_precision",
        },
        {
            "review_key": "k2",
            "text": "1+1 Media Group",
            "label": "organization",
            "canonical_text": "1+1 Media Group",
            "entity_id": "organization:1-plus-1-media-group",
            "source_name": "curated-general",
            "runtime_tier": "runtime_exact_high_precision",
        },
    ]

    reviews = module.review_alias_batch(
        aliases,
        runner=lambda _prompt: '{"drops":[{"line":1,"reason_code":"generic_retained_alias_from_batch_review","reason":"too generic"}]}',
        max_retries=0,
    )

    assert reviews == [
        {
            "review_key": "k1",
            "decision": "drop",
            "reason_code": "generic_retained_alias_from_batch_review",
            "reason": "too generic",
        },
        {
            "review_key": "k2",
            "decision": "keep",
            "reason_code": "implicit_keep_from_batch_review",
            "reason": "Not listed among generic aliases in chunk review.",
        },
    ]


def test_build_batch_review_prompt_uses_one_entity_per_line() -> None:
    module = _load_script_module()
    prompt = module.build_batch_review_prompt(
        [
            {
                "review_key": "k1",
                "text": "Some value",
                "label": "organization",
                "canonical_text": "Some value",
            },
            {
                "review_key": "k2",
                "text": "Another item",
                "label": "location",
                "canonical_text": "Another item",
            },
        ]
    )

    assert "Entities:\nSome value\nAnother item" in prompt
    assert '"line":0' in prompt
    assert '"review_key"' not in prompt
