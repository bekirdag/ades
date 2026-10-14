"""Regressions found in retained TNL texts and ADES impact outputs."""
from ades.packs.finance_sources import _looks_like_finance_person_name
from ades.packs.finance_bundle import _build_sec_company_issuers
from ades.pipeline.tagger import (
    _should_skip_single_token_lookup_candidate,
    _should_skip_multi_token_lookup_candidate,
)
from ades.impact.expansion import _mentions_from_entities
from ades.service.app import _direct_terminal_source_entity_is_story_anchored
from ades.service.models import EntityMatch, EntityLink, ImpactSourceEntity
import pytest


@pytest.mark.parametrize('name', ['Middle East', 'West Bank', 'New York', 'South Africa', 'In July', 'Investor Relations', 'Samsung Electronics', '• Latin America', 'San Francisco', 'Harvard University'])
def test_geographic_names_cannot_become_finance_people(name):
    assert not _looks_like_finance_person_name(name, raw_value=name)
    assert _should_skip_multi_token_lookup_candidate(
        matched_text=name, candidate_value=name, canonical_text=name,
        candidate_label='person', candidate_domain='finance',
    )


def test_geographic_guard_preserves_legitimate_directional_surnames():
    assert _looks_like_finance_person_name('John West', raw_value='John West')
    assert _looks_like_finance_person_name('Sarah East', raw_value='Sarah East')


def test_single_mention_issuer_ticker_is_allowed_in_reporting_context():
    assert not _should_skip_single_token_lookup_candidate(
        matched_text='Apple', candidate_value='Apple', canonical_text='AAPL',
        candidate_label='ticker', candidate_domain='finance',
        segment_text='Apple reported quarterly earnings.', start=0, end=5,
    )


@pytest.mark.parametrize('text', ['Apple harvests were abundant.', 'Apple farmers reported quarterly earnings.', 'Apple juice sales rose.'])
def test_ambiguous_company_alias_keeps_common_noun_guard(text):
    assert _should_skip_single_token_lookup_candidate(
        matched_text='Apple', candidate_value='Apple', canonical_text='Apple Inc.',
        candidate_label='organization', candidate_domain='finance',
        segment_text=text, start=0, end=5,
    )


def test_sec_issuer_short_name_alias_is_source_derived():
    entities, _ = _build_sec_company_issuers(
        [{'title':'Apple Inc.', 'cik_str':320193, 'ticker':'AAPL'}],
        submission_profiles={}, companyfacts_profiles={}, symbol_profiles={},
    )
    assert 'Apple' in entities[0]['aliases']
    assert entities[0]['entity_id'] == 'sec-cik:0000320193'


def test_impact_anchoring_preserves_extracted_surface_without_changing_identity():
    e=EntityMatch(text='NVIDIA', label='ticker', start=0, end=6, confidence=.8,
        link=EntityLink(entity_id='finance-us-ticker:NVDA', canonical_text='NVDA', provider='lookup.alias.exact'))
    mention=_mentions_from_entities([e])['finance-us-ticker:NVDA']
    assert mention.name == 'NVDA'
    assert mention.mention_texts == ('NVIDIA',)
    source=ImpactSourceEntity(entity_ref='finance-us-ticker:NVDA', name=mention.name,
        mention_texts=list(mention.mention_texts), entity_type='ticker', is_tradable=True)
    assert _direct_terminal_source_entity_is_story_anchored(source,'finance-us-ticker:NVDA','NVIDIA reported quarterly earnings.')
    assert not _direct_terminal_source_entity_is_story_anchored(source,'finance-us-ticker:NVDA','A completely unrelated company announced results.')


def test_sqlite_metadata_reads_release_file_descriptors_without_gc(tmp_path):
    import gc
    from pathlib import Path
    from ades.storage.registry_db import PackMetadataStore
    from ades.storage.paths import build_storage_layout, ensure_storage_layout
    if not Path('/proc/self/fd').exists():
        pytest.skip('Descriptor inspection requires Linux procfs')
    store=PackMetadataStore(ensure_storage_layout(build_storage_layout(tmp_path)))
    gc.collect()
    was_enabled=gc.isenabled()
    gc.disable()
    try:
        before=len(list(Path('/proc/self/fd').iterdir()))
        for _ in range(200):
            assert store.count_installed_packs() == 0
        after=len(list(Path('/proc/self/fd').iterdir()))
        assert after <= before + 2
    finally:
        if was_enabled:gc.enable()


def test_news_analysis_keeps_surface_across_refs_only_expansion():
    from ades.service.app import _source_entities_with_mention_texts
    source=ImpactSourceEntity(entity_ref='finance-us-ticker:NVDA',name='NVDA',same_as_refs=['sec-cik:0001045810'])
    e=EntityMatch(text='NVIDIA',label='organization',start=0,end=6,
        link=EntityLink(entity_id='sec-cik:0001045810',canonical_text='NVIDIA CORP',provider='lookup.alias.exact'))
    result=_source_entities_with_mention_texts([source],[e],text='NVIDIA reported earnings.')
    assert result[0].mention_texts == ['NVIDIA']
    assert result[0].name == 'NVDA'
    assert _source_entities_with_mention_texts([source],[e],text='Unrelated reporting.')[0].mention_texts == []


@pytest.mark.parametrize('name', ['New York', 'Middle East', 'Los Angeles', 'Hong Kong'])
def test_geographic_phrase_cannot_become_short_finance_issuer_alias(name):
    from ades.pipeline.tagger import _runtime_finance_country_short_alias_surface_allowed
    assert not _runtime_finance_country_short_alias_surface_allowed(name)
