from pathlib import Path

from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import _should_skip_single_token_lookup_candidate, tag_text
from tests.pack_registry_helpers import delete_installed_pack_metadata


def test_finance_pack_tags_aliases_and_rules(tmp_path: Path) -> None:
    installer = PackInstaller(tmp_path)
    installer.install("finance-en")

    response = tag_text(
        text="TICKA rallied on EXCHX after USD 12.5 earnings guidance.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    labels = {entity.label for entity in response.entities}
    topics = {topic.label for topic in response.topics}

    assert "ticker" in labels
    assert "exchange" in labels
    assert "currency_amount" in labels
    assert "finance" in topics


def test_finance_pack_alias_tagging_recovers_after_metadata_row_deletion(tmp_path: Path) -> None:
    installer = PackInstaller(tmp_path)
    installer.install("finance-en")

    delete_installed_pack_metadata(tmp_path, "finance-en")

    response = tag_text(
        text="TICKA traded on EXCHX.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("TICKA", "ticker") in pairs
    assert ("EXCHX", "exchange") in pairs


def test_missing_pack_warns(tmp_path: Path) -> None:
    response = tag_text(
        text="anything",
        pack="missing-pack",
        content_type="text/plain",
        storage_root=tmp_path,
    )
    assert "pack_not_installed:missing-pack" in response.warnings


def test_single_token_lookup_filter_skips_low_information_people_and_orgs() -> None:
    assert _should_skip_single_token_lookup_candidate(
        matched_text="investor",
        candidate_value="Investor",
        candidate_label="organization",
    )
    assert _should_skip_single_token_lookup_candidate(
        matched_text="April",
        candidate_value="April",
        candidate_label="organization",
    )
    assert not _should_skip_single_token_lookup_candidate(
        matched_text="Reuters",
        candidate_value="Reuters",
        candidate_label="organization",
    )


def test_single_token_lookup_filter_skips_general_titlecase_fragments() -> None:
    assert _should_skip_single_token_lookup_candidate(
        matched_text="Daniel",
        candidate_value="Daniel",
        candidate_label="person",
        candidate_domain="general",
        segment_text="Daniel Loeb wrote a letter.",
        start=0,
        end=6,
    )
    assert _should_skip_single_token_lookup_candidate(
        matched_text="YORK",
        candidate_value="York",
        candidate_label="location",
        candidate_domain="general",
        segment_text="NEW YORK opened lower.",
        start=4,
        end=8,
    )


def test_single_token_lookup_filter_skips_general_lowercase_multi_token_nouns() -> None:
    assert _should_skip_single_token_lookup_candidate(
        matched_text="real estate",
        candidate_value="Real Estate",
        candidate_label="organization",
        candidate_domain="general",
    )
    assert not _should_skip_single_token_lookup_candidate(
        matched_text="new york",
        candidate_value="New York",
        candidate_label="location",
        candidate_domain="general",
    )
