import json
from pathlib import Path


def create_general_raw_snapshots(root: Path) -> dict[str, Path]:
    raw_dir = root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    wikidata_path = raw_dir / "wikidata_general_entities.json"
    wikidata_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "id": "Q1",
                        "entity_type": "person",
                        "label": "Tim Cook",
                        "aliases": ["Timothy Cook"],
                    },
                    {
                        "id": "Q2",
                        "entity_type": "organization",
                        "label": "OpenAI",
                        "aliases": ["Open AI"],
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    geonames_path = raw_dir / "geonames_places.txt"
    geonames_path.write_text(
        "\n".join(
            [
                "geonameid|name|alternatenames|feature_class|feature_code|country_code",
                "745044|Istanbul|Constantinople,Byzantium|P|PPLA|TR",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    curated_path = raw_dir / "curated_general_entities.json"
    curated_path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "entity_type": "organization",
                        "canonical_text": "Apple",
                        "aliases": [],
                    }
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "wikidata_entities": wikidata_path,
        "geonames_places": geonames_path,
        "curated_entities": curated_path,
    }
