"""Source-tier helpers for market impact relationship graph lanes."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

SOURCE_TIER_OFFICIAL = "official"
SOURCE_TIER_ISSUER_DISCLOSED = "issuer_disclosed"
SOURCE_TIER_EXCHANGE = "exchange"
SOURCE_TIER_REGULATOR = "regulator"
SOURCE_TIER_GOVERNMENT = "government"
SOURCE_TIER_LICENSED = "licensed"
SOURCE_TIER_INDUSTRY_ASSOCIATION = "industry_association"
SOURCE_TIER_WIKIDATA_BRIDGE = "wikidata_bridge"
SOURCE_TIER_REVIEWED_PROPOSAL = "reviewed_proposal"
SOURCE_TIER_LOCAL_PACK_METADATA = "local_pack_metadata"
SOURCE_TIER_TEST_FIXTURE = "test_fixture"
SOURCE_TIER_UNKNOWN = "unknown"

PROMOTION_ELIGIBLE_SOURCE_TIERS = {
    SOURCE_TIER_OFFICIAL,
    SOURCE_TIER_ISSUER_DISCLOSED,
    SOURCE_TIER_EXCHANGE,
    SOURCE_TIER_REGULATOR,
    SOURCE_TIER_GOVERNMENT,
    SOURCE_TIER_LICENSED,
    SOURCE_TIER_INDUSTRY_ASSOCIATION,
    SOURCE_TIER_REVIEWED_PROPOSAL,
    SOURCE_TIER_LOCAL_PACK_METADATA,
}

_OFFICIAL_HOST_FRAGMENTS = (
    "sec.gov",
    "companieshouse.gov.uk",
    "legislation.gov.uk",
    "parliament.uk",
    "federalregister.gov",
    "congress.gov",
    "eur-lex.europa.eu",
    "ecb.europa.eu",
    "iea.org",
    "worldbank.org",
    "opec.org",
    "pancanal.com",
    "usgs.gov",
    "eia.gov",
    "gleif.org",
    "leidata.gleif.org",
    "iso.org",
    "iso20022.org",
    "census.gov",
    "unstats.un.org",
    "ec.europa.eu",
    "eurostat.ec.europa.eu",
    "imf.org",
    "bis.org",
    "oecd.org",
    "world-exchanges.org",
    "wto.org",
    "fao.org",
    "ilo.org",
    "ojk.go.id",
    "setkab.go.id",
    "kemenkeu.go.id",
    "bumn.go.id",
    "boletinoficial.gob.ar",
    "datos.gob.ar",
    "indec.gob.ar",
    "rionegro.gov.ar",
    "crear.rionegro.gov.ar",
    "boletinoficial.rionegro.gov.ar",
    "data.gov.au",
    "rba.gov.au",
    "abs.gov.au",
    "ato.gov.au",
    "business.gov.au",
    "legislation.gov.au",
    "grants.gov.au",
    "firsthomebuyers.gov.au",
    "housingaustralia.gov.au",
    "cefc.com.au",
    "finance.gov.au",
    "arena.gov.au",
    "nrf.gov.au",
    "nsw.gov.au",
    "transport.nsw.gov.au",
)
_EXCHANGE_HOST_FRAGMENTS = (
    "nasdaq.com",
    "nyse.com",
    "londonstockexchange.com",
    "lse.co.uk",
    "asx.com.au",
    "tsx.com",
    "b3.com.br",
    "hkex.com.hk",
    "jpx.co.jp",
    "borsaistanbul.com",
    "cmegroup.com",
    "idx.co.id",
    "sgx.com",
    "deutsche-boerse.com",
    "boerse-frankfurt.de",
    "six-group.com",
    "bseindia.com",
    "nseindia.com",
    "krx.co.kr",
    "twse.com.tw",
    "byma.com.ar",
    "open.bymadata.com.ar",
    "asxonline.com",
)
_REGULATOR_HOST_FRAGMENTS = (
    "fca.org.uk",
    "sec.gov",
    "cftc.gov",
    "esma.europa.eu",
    "ofgem.gov.uk",
    "ofcom.org.uk",
    "cma.gov.uk",
    "epa.gov",
    "bankofengland.co.uk",
    "federalreserve.gov",
    "bis.org",
    "esma.europa.eu",
    "iosco.org",
    "mas.gov.sg",
    "sfc.hk",
    "asic.gov.au",
    "connectonline.asic.gov.au",
    "apra.gov.au",
    "tga.gov.au",
    "bcra.gob.ar",
    "cnv.gov.ar",
    "aif2.cnv.gov.ar",
)
_LICENSED_HOST_FRAGMENTS = ("msci.com", "openfigi.com")
_INDUSTRY_ASSOCIATION_HOST_FRAGMENTS = ("semiconductors.org",)
_ISSUER_DISCLOSED_HOST_FRAGMENTS = (
    "pnm.co.id",
    "bri.co.id",
    "ir-bri.com",
    "bancopatagonia.com.ar",
    "bp.bancopatagonia.com.ar",
    "commbank.com.au",
    "bhp.com",
    "nab.com.au",
    "westpac.com.au",
    "anz.com.au",
    "macquarie.com",
    "woodside.com",
    "riotinto.com",
    "telstra.com.au",
    "colesgroup.com.au",
    "woolworthsgroup.com.au",
)


@dataclass(frozen=True)
class SourceAttribution:
    source_name: str
    source_url: str
    source_snapshot: str
    source_tier: str
    promotion_eligible: bool


def _host(source_url: str) -> str:
    parsed = urlparse(source_url)
    return (parsed.netloc or "").casefold()


def _contains_any(value: str, fragments: tuple[str, ...]) -> bool:
    return any(fragment in value for fragment in fragments)


def classify_source_tier(source_name: str | None, source_url: str | None) -> str:
    """Classify a source into a coarse promotion tier without requiring new TSV columns."""

    name = (source_name or "").casefold()
    url = (source_url or "").strip()
    url_lower = url.casefold()
    host = _host(url)

    if not url:
        return SOURCE_TIER_UNKNOWN
    if url_lower.startswith("file://"):
        return SOURCE_TIER_LOCAL_PACK_METADATA
    if host.endswith(".test") or "example.com" in host or "example.test" in host:
        return SOURCE_TIER_TEST_FIXTURE
    if "wikidata" in host or "wikidata" in name:
        return SOURCE_TIER_WIKIDATA_BRIDGE
    if "reviewed proposal" in name or "reviewed-proposal" in name:
        return SOURCE_TIER_REVIEWED_PROPOSAL
    if "licensed" in name or "openfigi" in name or _contains_any(host, _LICENSED_HOST_FRAGMENTS):
        return SOURCE_TIER_LICENSED
    if _contains_any(host, _INDUSTRY_ASSOCIATION_HOST_FRAGMENTS):
        return SOURCE_TIER_INDUSTRY_ASSOCIATION
    if _contains_any(host, _REGULATOR_HOST_FRAGMENTS) or "regulator" in name:
        return SOURCE_TIER_REGULATOR
    if _contains_any(host, _EXCHANGE_HOST_FRAGMENTS) or "exchange" in name:
        return SOURCE_TIER_EXCHANGE
    if (
        host.endswith(".gov")
        or host.endswith(".gov.uk")
        or host.endswith(".gov.au")
        or _contains_any(host, _OFFICIAL_HOST_FRAGMENTS)
    ):
        return SOURCE_TIER_GOVERNMENT
    if (
        "investor" in name
        or "annual report" in name
        or "/investor" in url_lower
        or host.startswith("ir.")
        or _contains_any(host, _ISSUER_DISCLOSED_HOST_FRAGMENTS)
    ):
        return SOURCE_TIER_ISSUER_DISCLOSED
    return SOURCE_TIER_UNKNOWN


def build_source_attribution(
    *,
    source_name: str,
    source_url: str,
    source_snapshot: str,
) -> SourceAttribution:
    source_tier = classify_source_tier(source_name, source_url)
    return SourceAttribution(
        source_name=source_name,
        source_url=source_url,
        source_snapshot=source_snapshot,
        source_tier=source_tier,
        promotion_eligible=source_tier in PROMOTION_ELIGIBLE_SOURCE_TIERS,
    )


def validate_source_attribution(
    *,
    source_name: str | None,
    source_url: str | None,
    source_snapshot: str | None,
) -> list[str]:
    warnings: list[str] = []
    if not source_name:
        warnings.append("missing_source_name")
    if not source_url:
        warnings.append("missing_source_url")
    if not source_snapshot:
        warnings.append("missing_source_snapshot")
    if warnings:
        return warnings
    source_tier = classify_source_tier(source_name, source_url)
    if source_tier == SOURCE_TIER_UNKNOWN:
        warnings.append("unknown_source_tier")
    if source_tier == SOURCE_TIER_WIKIDATA_BRIDGE:
        warnings.append("bridge_source_requires_supporting_source_before_promotion")
    if source_tier == SOURCE_TIER_TEST_FIXTURE:
        warnings.append("test_fixture_source_not_promotable")
    return warnings
