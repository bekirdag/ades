from ades.packs.general_structure import evaluate_general_pack_structure


def test_evaluate_general_pack_structure_prefers_entity_label_distribution() -> None:
    failures, warnings = evaluate_general_pack_structure(
        {
            "included_entity_count": 2806081,
            "entity_label_distribution": {
                "person": 788064,
                "organization": 190985,
                "location": 1827032,
            },
            "label_distribution": {
                "person": 1820263,
                "organization": 368137,
                "location": 3447234,
            },
        }
    )

    assert failures == []
    assert warnings == [
        "general-en structural summary: person=788064, organization=190985, location=1827032, total=2806081"
    ]


def test_evaluate_general_pack_structure_falls_back_to_label_distribution() -> None:
    failures, _warnings = evaluate_general_pack_structure(
        {
            "included_entity_count": 1_417_921,
            "label_distribution": {
                "person": 26,
                "organization": 14,
                "location": 1_417_881,
            },
        }
    )

    assert failures
    assert any("person count 26 is below 1000" in item for item in failures)
