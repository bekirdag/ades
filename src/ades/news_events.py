"""Deterministic market-event signal extraction for news analysis."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, Sequence

from .service.models import ImpactCandidate, NewsEventSignal


_MAX_EVIDENCE_CHARS = 180


@dataclass(frozen=True)
class _EventRule:
    event_type: str
    patterns: tuple[re.Pattern[str], ...]
    compatible_asset_families: tuple[str, ...]
    confidence: float


@dataclass(frozen=True)
class EventCompatibilityFamily:
    family: str
    event_types: tuple[str, ...]
    compatible_asset_families: tuple[str, ...]
    terminal_types: tuple[str, ...]
    relation_families: tuple[str, ...]


@dataclass(frozen=True)
class _SentenceSpan:
    text: str
    start: int
    end: int


def _rx(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE | re.MULTILINE)


_EVENT_RULES: tuple[_EventRule, ...] = (
    _EventRule(
        event_type="policy_rate_cut",
        patterns=(
            _rx(
                r"\b(?:cut|cuts|cutting|lower|lowers|lowered|reduce|reduces|reduced|slash|slashes|slashed)\b.{0,90}\b(?:policy\s+rate|interest\s+rate|benchmark\s+rate|key\s+rate|deposit\s+rate|lending\s+rate|rates?)\b"
            ),
            _rx(
                r"\b(?:policy\s+rate|interest\s+rate|benchmark\s+rate|key\s+rate|deposit\s+rate|lending\s+rate|rates?)\b.{0,90}\b(?:cut|cuts|cutting|lowered|reduced|slashed)\b"
            ),
        ),
        compatible_asset_families=("rates", "currency", "bonds", "equity_index", "banking"),
        confidence=0.91,
    ),
    _EventRule(
        event_type="policy_rate_hike",
        patterns=(
            _rx(
                r"\b(?:raise|raises|raised|hike|hikes|hiked|increase|increases|increased|tighten|tightens|tightened)\b.{0,90}\b(?:policy\s+rate|interest\s+rate|benchmark\s+rate|key\s+rate|deposit\s+rate|lending\s+rate|rates?)\b"
            ),
            _rx(
                r"\b(?:policy\s+rate|interest\s+rate|benchmark\s+rate|key\s+rate|deposit\s+rate|lending\s+rate|rates?)\b.{0,90}\b(?:raised|hiked|increased|tightened)\b"
            ),
        ),
        compatible_asset_families=("rates", "currency", "bonds", "equity_index", "banking"),
        confidence=0.91,
    ),
    _EventRule(
        event_type="policy_rate_hold",
        patterns=(
            _rx(
                r"\b(?:kept|hold|holds|held|left|leave|leaves|maintained)\b.{0,80}\b(?:policy\s+rate|interest\s+rate|benchmark\s+rate|key\s+rate|deposit\s+rate|lending\s+rate|rates?)\b.{0,80}\b(?:unchanged|steady|on\s+hold|at)\b"
            ),
            _rx(
                r"\b(?:policy\s+rate|interest\s+rate|benchmark\s+rate|key\s+rate|deposit\s+rate|lending\s+rate|rates?)\b.{0,80}\b(?:unchanged|steady|on\s+hold|held|maintained)\b"
            ),
            _rx(
                r"\b(?:kept|hold|holds|held|left|leave|leaves|maintained)\b.{0,80}\b(?:policy\s+rate|interest\s+rate|benchmark\s+rate|key\s+rate|deposit\s+rate|lending\s+rate|rates?)\b"
            ),
        ),
        compatible_asset_families=("rates", "currency", "bonds", "equity_index", "banking"),
        confidence=0.86,
    ),
    _EventRule(
        event_type="inflation_shock",
        patterns=(
            _rx(
                r"\b(?:inflation|cpi|ppi|consumer\s+prices|producer\s+prices)\b.{0,90}\b(?:hotter|above\s+(?:forecast|expectations?|estimates?)|unexpected|surprise|accelerat(?:e|ed|es|ing)|surge(?:d|s)?|jump(?:ed|s)?|spik(?:e|ed|es|ing))\b"
            ),
            _rx(
                r"\b(?:hotter|above\s+(?:forecast|expectations?|estimates?)|unexpected|surprise|accelerat(?:e|ed|es|ing)|surge(?:d|s)?|jump(?:ed|s)?|spik(?:e|ed|es|ing))\b.{0,90}\b(?:inflation|cpi|ppi|consumer\s+prices|producer\s+prices)\b"
            ),
        ),
        compatible_asset_families=("rates", "currency", "bonds", "equity_index", "consumer"),
        confidence=0.88,
    ),
    _EventRule(
        event_type="future_policy_expectation",
        patterns=(
            _rx(
                r"\b(?:expect|expects|expected|forecast|forecasts|project|projects|projected|price\s+in|priced\s+in|bets?\s+on|odds\s+of)\b.{0,100}\b(?:rate\s+cut|rate\s+hike|rate\s+increase|rate\s+reduction|policy\s+easing|policy\s+tightening|monetary\s+easing|monetary\s+tightening)\b"
            ),
            _rx(
                r"\b(?:rate\s+cut|rate\s+hike|rate\s+increase|rate\s+reduction|policy\s+easing|policy\s+tightening|monetary\s+easing|monetary\s+tightening)\b.{0,100}\b(?:expect|expects|expected|forecast|forecasts|projected|priced\s+in|bets?|odds)\b"
            ),
            _rx(
                r"\b(?:confirm(?:s|ed|ation)?|appoint(?:s|ed|ment)?|nominate(?:s|d|ion)?|name(?:s|d)?|select(?:s|ed)?|pick(?:s|ed)?)\b.{0,120}\b(?:fed(?:eral\s+reserve)?|central\s+bank|ecb|boe|bank\s+of\s+japan|boj|pboc|cbrt|monetary\s+policy)\b.{0,80}\b(?:chair|chairman|chairwoman|governor|president|chief|member|board|committee|leader|leadership)\b"
            ),
            _rx(
                r"\b(?:fed(?:eral\s+reserve)?|central\s+bank|ecb|boe|bank\s+of\s+japan|boj|pboc|cbrt|monetary\s+policy)\b.{0,100}\b(?:chair|chairman|chairwoman|governor|president|chief|member|board|committee|leader|leadership)\b.{0,120}\b(?:confirm(?:s|ed|ation)?|appoint(?:s|ed|ment)?|nominate(?:s|d|ion)?|name(?:s|d)?|select(?:s|ed)?|pick(?:s|ed)?)\b"
            ),
        ),
        compatible_asset_families=("rates", "currency", "bonds", "equity_index", "banking"),
        confidence=0.84,
    ),
    _EventRule(
        event_type="fiscal_expansion",
        patterns=(
            _rx(
                r"\b(?:stimulus|spending\s+package|fiscal\s+support|budget\s+expansion|tax\s+cut|public\s+spending)\b"
            ),
        ),
        compatible_asset_families=("rates", "currency", "bonds", "equity_index", "country_risk"),
        confidence=0.78,
    ),
    _EventRule(
        event_type="fiscal_austerity",
        patterns=(
            _rx(
                r"\b(?:austerity|spending\s+cut|budget\s+cut|fiscal\s+tightening|tax\s+hike|deficit\s+reduction)\b"
            ),
        ),
        compatible_asset_families=("rates", "currency", "bonds", "equity_index", "country_risk"),
        confidence=0.78,
    ),
    _EventRule(
        event_type="sanctions",
        patterns=(
            _rx(
                r"\b(?:sanction|sanctions|blacklist|asset\s+freeze|embargo|secondary\s+sanctions)\b"
            ),
        ),
        compatible_asset_families=("currency", "commodity", "equity", "shipping", "country_risk"),
        confidence=0.87,
    ),
    _EventRule(
        event_type="tariff",
        patterns=(
            _rx(
                r"\b(?:tariff|tariffs|import\s+dut(?:y|ies)|customs\s+dut(?:y|ies)|trade\s+levy|levies)\b"
            ),
        ),
        compatible_asset_families=("equity", "commodity", "currency", "sector", "country_risk"),
        confidence=0.85,
    ),
    _EventRule(
        event_type="export_control",
        patterns=(
            _rx(
                r"\b(?:export\s+control|export\s+controls|export\s+ban|export\s+curb|export\s+curbs|export\s+restriction|export\s+restrictions|license\s+requirement)\b"
            ),
        ),
        compatible_asset_families=("equity", "commodity", "currency", "sector", "country_risk"),
        confidence=0.87,
    ),
    _EventRule(
        event_type="sector_policy_change",
        patterns=(
            _rx(
                r"\b(?:law|laws|bill|bills|legislation|act|rule|rules|regulation|regulations|parliament|lawmakers?|government|minister|ministry|regulator|agency)\b.{0,180}\b(?:pass(?:es|ed)?|approv(?:e|es|ed|al)|adopt(?:s|ed)?|enact(?:s|ed)?|introduc(?:e|es|ed)|propos(?:e|es|ed)|implement(?:s|ed)?|tighten(?:s|ed)?|relax(?:es|ed)?|rais(?:e|es|ed)|cut(?:s)?|lower(?:s|ed)?|ban(?:s|ned)?|limit(?:s|ed)?|cap(?:s|ped)?|subsid(?:y|ies|ise|ize|ised|ized))\b.{0,180}\b(?:mining|miners?|metals?|gold|silver|copper|lithium|uranium|oil|gas|energy|power|utilities|banks?|banking|financial|insurers?|telecom|technology|tech|semiconductor|pharma|drugmakers?|healthcare|transport|shipping|airlines?|agriculture|food|crypto|digital\s+asset|environmental|emissions?|carbon|royalt(?:y|ies)|tax|permit|permits|licen[cs]e|quota)\b"
            ),
            _rx(
                r"\b(?:mining|miners?|metals?|gold|silver|copper|lithium|uranium|oil|gas|energy|power|utilities|banks?|banking|financial|insurers?|telecom|technology|tech|semiconductor|pharma|drugmakers?|healthcare|transport|shipping|airlines?|agriculture|food|crypto|digital\s+asset|environmental|emissions?|carbon)\b.{0,180}\b(?:law|laws|bill|bills|legislation|act|rule|rules|regulation|regulations|policy|royalt(?:y|ies)|tax|permit|permits|licen[cs]e|quota|subsid(?:y|ies)|ban|caps?|limit|limits)\b"
            ),
            _rx(
                r"\b(?:royalt(?:y|ies)|windfall\s+tax|resource\s+rent\s+tax|mining\s+tax|export\s+levy|permit(?:ting)?\s+rule|licen[cs]ing\s+rule|environmental\s+rule|emissions?\s+rule)\b"
            ),
        ),
        compatible_asset_families=(
            "equity",
            "ticker",
            "sector",
            "equity_index",
            "commodity",
            "country_risk",
        ),
        confidence=0.8,
    ),
    _EventRule(
        event_type="regulatory_enforcement",
        patterns=(
            _rx(
                r"\b(?:regulator|regulators|agency|watchdog|competition\s+authority|antitrust|securities\s+commission|environmental\s+agency|central\s+bank|court|courts|tribunal|judge)\b.{0,180}\b(?:fine(?:s|d)?|penalt(?:y|ies)|probe|probes|investigat(?:e|es|ed|ion)|enforcement|crackdown|lawsuit|class\s+action|legal\s+claims?|price\s+conduct|sue(?:s|d)?|order(?:s|ed)?|rul(?:e|es|ed|ing)|judg(?:e|es|ed|ment)?|injunction|ban(?:s|ned)?|suspend(?:s|ed)?|revoke(?:s|d)?|licen[cs]e|settlement)\b"
            ),
            _rx(
                r"\b(?:fine(?:s|d)?|penalt(?:y|ies)|probe|probes|investigat(?:e|es|ed|ion)|enforcement|crackdown|lawsuit|class\s+action|legal\s+claims?|price\s+conduct|sue(?:s|d)?|order(?:s|ed)?|rul(?:e|es|ed|ing)|judg(?:e|es|ed|ment)?|injunction|ban(?:s|ned)?|suspend(?:s|ed)?|revoke(?:s|d)?|settlement)\b.{0,180}\b(?:regulator|regulators|agency|watchdog|competition\s+authority|antitrust|securities\s+commission|environmental\s+agency|central\s+bank|court|courts|tribunal|judge|banks?|miners?|utilities|telecom|pharma|technology|crypto|energy|oil|gas|airlines?|shipping|housebuilders?|homebuilders?|builders?|housing|construction|real\s+estate|property|developers?)\b"
            ),
            _rx(
                r"\b(?:banks?|miners?|utilities|telecom|pharma|technology|crypto|energy|oil|gas|airlines?|shipping|housebuilders?|homebuilders?|builders?|housing|construction|real\s+estate|property|developers?)\b.{0,180}\b(?:fine(?:s|d)?|penalt(?:y|ies)|probe|probes|investigat(?:e|es|ed|ion)|enforcement|crackdown|lawsuit|class\s+action|legal\s+claims?|price\s+conduct|sue(?:s|d)?|order(?:s|ed)?|rul(?:e|es|ed|ing)|judg(?:e|es|ed|ment)?|injunction|ban(?:s|ned)?|suspend(?:s|ed)?|revoke(?:s|d)?|settlement)\b"
            ),
        ),
        compatible_asset_families=(
            "equity",
            "ticker",
            "sector",
            "equity_index",
            "country_risk",
        ),
        confidence=0.81,
    ),
    _EventRule(
        event_type="supply_disruption",
        patterns=(
            _rx(
                r"\b(?:supply|exports?|shipments?|deliveries|pipeline|port|mine|refinery|plant|terminal)\b.{0,100}\b(?:disrupt(?:ion|ed|s)?|halt(?:ed|s)?|shutdown|shut\s+down|shortage|blocked|closed|outage|suspension|suspend(?:ed|s)?)\b"
            ),
            _rx(
                r"\b(?:disrupt(?:ion|ed|s)?|halt(?:ed|s)?|shutdown|shut\s+down|shortage|blocked|closed|outage|suspension|suspend(?:ed|s)?)\b.{0,100}\b(?:supply|exports?|shipments?|deliveries|pipeline|port|mine|refinery|plant|terminal)\b"
            ),
            _rx(
                r"\b(?:oil|crude|gas|lng|fuel|energy)\b.{0,80}\b(?:flows?|shipments?|exports?|traffic|transit|volumes?)\b.{0,100}\b(?:fell|falls|fall|drop(?:ped|s)?|declin(?:e|ed|es|ing)|slump(?:ed|s)?|slow(?:ed|s|ing)?|reduc(?:e|ed|es|ing)|down)\b"
            ),
            _rx(
                r"\b(?:fell|falls|fall|drop(?:ped|s)?|declin(?:e|ed|es|ing)|slump(?:ed|s)?|slow(?:ed|s|ing)?|reduc(?:e|ed|es|ing)|down)\b.{0,100}\b(?:oil|crude|gas|lng|fuel|energy)\b.{0,80}\b(?:flows?|shipments?|exports?|traffic|transit|volumes?)\b"
            ),
        ),
        compatible_asset_families=("commodity", "energy", "agriculture", "shipping", "sector"),
        confidence=0.88,
    ),
    _EventRule(
        event_type="production_decrease",
        patterns=(
            _rx(
                r"\b(?:production|output|supply|capacity)\b.{0,90}\b(?:fell|falls|drop(?:ped|s)?|declin(?:e|ed|es|ing)|cut|cuts|curtail(?:ed|s)?|reduc(?:e|ed|es|ing))\b"
            ),
            _rx(
                r"\b(?:cut|cuts|curtail(?:ed|s)?|reduc(?:e|ed|es|ing)|lower(?:ed|s)?|drop(?:ped|s)?|declin(?:e|ed|es|ing))\b.{0,90}\b(?:production|output|supply|capacity)\b"
            ),
        ),
        compatible_asset_families=(
            "commodity",
            "energy",
            "agriculture",
            "sector",
            "equity",
            "ticker",
        ),
        confidence=0.84,
    ),
    _EventRule(
        event_type="production_increase",
        patterns=(
            _rx(
                r"\b(?:production|output|supply|capacity)\b.{0,90}\b(?:rose|rises|rise|jump(?:ed|s)?|increas(?:e|ed|es|ing)|boost(?:ed|s)?|expand(?:ed|s|ing)|ramp(?:ed)?\s+up)\b"
            ),
            _rx(
                r"\b(?:increase|boost|expand|ramp\s+up|raise|raised)\b.{0,90}\b(?:production|output|supply|capacity)\b"
            ),
        ),
        compatible_asset_families=(
            "commodity",
            "energy",
            "agriculture",
            "sector",
            "equity",
            "ticker",
        ),
        confidence=0.84,
    ),
    _EventRule(
        event_type="shipping_chokepoint_disruption",
        patterns=(
            _rx(
                r"\b(?:strait|canal|shipping\s+lane|sea\s+lane|chokepoint|port)\b.{0,100}\b(?:blocked|closed|attack(?:ed|s)?|disrupt(?:ed|ion|s)?|halt(?:ed|s)?|suspend(?:ed|s)?|rerout(?:e|ed|es|ing))\b"
            ),
            _rx(
                r"\b(?:blocked|closed|attack(?:ed|s)?|disrupt(?:ed|ion|s)?|halt(?:ed|s)?|suspend(?:ed|s)?|rerout(?:e|ed|es|ing))\b.{0,100}\b(?:strait|canal|shipping\s+lane|sea\s+lane|chokepoint|port)\b"
            ),
            _rx(
                r"\b(?:strait|canal|shipping\s+lane|sea\s+lane|chokepoint|port|transit|tankers?|vessels?)\b.{0,100}\b(?:risk|risks|threat|threats|tension|tensions|danger|closure)\b"
            ),
            _rx(
                r"\b(?:risk|risks|threat|threats|tension|tensions|danger|closure)\b.{0,100}\b(?:strait|canal|shipping\s+lane|sea\s+lane|chokepoint|port|transit|tankers?|vessels?)\b"
            ),
            _rx(
                r"\b(?:oil|crude|gas|lng|fuel|energy|cargo|freight|shipments?|exports?|tankers?|vessels?|traffic|flows?|transit|volumes?)\b.{0,120}\b(?:strait|canal|shipping\s+lane|sea\s+lane|chokepoint|port|hormuz|suez|panama|malacca|bab\s+el[-\s]?mandeb)\b.{0,120}\b(?:fell|falls|fall|drop(?:ped|s)?|declin(?:e|ed|es|ing)|slump(?:ed|s)?|slow(?:ed|s|ing)?|reduc(?:e|ed|es|ing)|down)\b"
            ),
            _rx(
                r"\b(?:fell|falls|fall|drop(?:ped|s)?|declin(?:e|ed|es|ing)|slump(?:ed|s)?|slow(?:ed|s|ing)?|reduc(?:e|ed|es|ing)|down)\b.{0,120}\b(?:oil|crude|gas|lng|fuel|energy|cargo|freight|shipments?|exports?|tankers?|vessels?|traffic|flows?|transit|volumes?)\b.{0,120}\b(?:strait|canal|shipping\s+lane|sea\s+lane|chokepoint|port|hormuz|suez|panama|malacca|bab\s+el[-\s]?mandeb)\b"
            ),
        ),
        compatible_asset_families=("commodity", "energy", "shipping", "sector"),
        confidence=0.9,
    ),
    _EventRule(
        event_type="earnings_beat",
        patterns=(
            _rx(
                r"\b(?:earnings|profit|revenue|sales|eps)\b.{0,90}\b(?:beat|beats|beat\s+estimates|above\s+(?:forecast|expectations?|estimates?))\b"
            ),
            _rx(
                r"\b(?:beat|beats|above\s+(?:forecast|expectations?|estimates?))\b.{0,90}\b(?:earnings|profit|revenue|sales|eps)\b"
            ),
        ),
        compatible_asset_families=("equity", "ticker"),
        confidence=0.86,
    ),
    _EventRule(
        event_type="earnings_miss",
        patterns=(
            _rx(
                r"\b(?:earnings|profit|revenue|sales|eps)\b.{0,90}\b(?:miss|misses|missed|below\s+(?:forecast|expectations?|estimates?))\b"
            ),
            _rx(
                r"\b(?:miss|misses|missed|below\s+(?:forecast|expectations?|estimates?))\b.{0,90}\b(?:earnings|profit|revenue|sales|eps)\b"
            ),
        ),
        compatible_asset_families=("equity", "ticker"),
        confidence=0.86,
    ),
    _EventRule(
        event_type="guidance_raise",
        patterns=(
            _rx(
                r"\b(?:raise|raises|raised|lift|lifts|lifted|upgrade|upgrades|upgraded)\b.{0,80}\b(?:guidance|outlook|forecast|target)\b"
            ),
            _rx(
                r"\b(?:guidance|outlook|forecast|target)\b.{0,80}\b(?:raised|lifted|upgraded|higher)\b"
            ),
        ),
        compatible_asset_families=("equity", "ticker"),
        confidence=0.85,
    ),
    _EventRule(
        event_type="guidance_cut",
        patterns=(
            _rx(
                r"\b(?:cut|cuts|cutting|lower|lowers|lowered|downgrade|downgrades|downgraded|reduce|reduced)\b.{0,80}\b(?:guidance|outlook|forecast|target)\b"
            ),
            _rx(
                r"\b(?:guidance|outlook|forecast|target)\b.{0,80}\b(?:cut|lowered|downgraded|reduced|weaker)\b"
            ),
        ),
        compatible_asset_families=("equity", "ticker"),
        confidence=0.85,
    ),
    _EventRule(
        event_type="acquisition",
        patterns=(
            _rx(
                r"\b(?:acquire|acquires|acquired|buy|buys|bought|takeover|take\s+over|purchase|purchases|purchased)\b.{0,90}\b(?:company|stake|shares?|business|unit|assets?)\b"
            ),
            _rx(
                r"\b(?:back|backs|backed|backing|recommend(?:s|ed|ing)?|agree(?:s|d|ing)?\s+to|accept(?:s|ed|ing)?|reject(?:s|ed|ing)?|rebuff(?:s|ed|ing)?)\b.{0,100}\b(?:takeover|buyout|bid|approach|offer)\b"
            ),
            _rx(r"\b(?:takeover|buyout)\b.{0,100}\b(?:bid|approach|offer|from|by)\b"),
            _rx(r"\b(?:bid|approach|offer)\b.{0,100}\b(?:takeover|buyout)\b"),
        ),
        compatible_asset_families=("equity", "ticker", "sector"),
        confidence=0.82,
    ),
    _EventRule(
        event_type="merger",
        patterns=(
            _rx(
                r"\b(?:merger|merge|merges|merged|combine|combines|combined)\b.{0,90}\b(?:company|business|unit|operations?|assets?)\b"
            ),
        ),
        compatible_asset_families=("equity", "ticker", "sector"),
        confidence=0.82,
    ),
    _EventRule(
        event_type="divestiture",
        patterns=(
            _rx(
                r"\b(?:divest|divests|divested|sell|sells|sold|spin\s*off|spinoff|carve[-\s]?out)\b.{0,90}\b(?:stake|shares?|unit|business|assets?|subsidiary)\b"
            ),
        ),
        compatible_asset_families=("equity", "ticker", "sector"),
        confidence=0.82,
    ),
    _EventRule(
        event_type="key_person_ownership_governance",
        patterns=(
            _rx(
                r"\b(?:trial|lawsuit|court|legal\s+dispute|testif(?:y|ies|ied|ying))\b.{0,140}\b(?:stake|shareholding|shareholder|ownership|control|board|leadership|founder|ceo|chair|executive)\b"
            ),
            _rx(
                r"\b(?:stake|shareholding|shareholder|ownership|control|board|leadership|founder|ceo|chair|executive)\b.{0,140}\b(?:trial|lawsuit|court|legal\s+dispute|testif(?:y|ies|ied|ying))\b"
            ),
            _rx(
                r"\b(?:sought|wanted|asked|demanded|proposed|offered|bid|bids?|pursued)\b.{0,120}\b(?:stake|shareholding|shareholder|ownership|control|board|leadership)\b"
            ),
            _rx(
                r"\b(?:stake|shareholding|shareholder|ownership|control|board|leadership)\b.{0,120}\b(?:sought|wanted|asked|demanded|proposed|offered|bid|bids?|pursued)\b"
            ),
            _rx(
                r"\b(?:resign(?:s|ed|ation)?|step(?:s|ped)?\s+down|depart(?:s|ed|ure)?|leave|leaves|left|oust(?:ed|s)?|fired|dismiss(?:ed|es)?|appoint(?:ed|s)?|named|replace(?:d|s)?|succession|successor|died|dies|death|illness|hospitali[sz]ed|arrest(?:ed|s)?|investigat(?:e|ed|es|ion)|probe|fraud)\b.{0,160}\b(?:ceo|chief\s+executive|cfo|chief\s+financial\s+officer|founder|chair|chairman|chairwoman|board|director|executive|owner|shareholder|leadership)\b"
            ),
            _rx(
                r"\b(?:ceo|chief\s+executive|cfo|chief\s+financial\s+officer|founder|chair|chairman|chairwoman|board|director|executive|owner|shareholder|leadership)\b.{0,160}\b(?:resign(?:s|ed|ation)?|step(?:s|ped)?\s+down|depart(?:s|ed|ure)?|leave|leaves|left|oust(?:ed|s)?|fired|dismiss(?:ed|es)?|appoint(?:ed|s)?|named|replace(?:d|s)?|succession|successor|died|dies|death|illness|hospitali[sz]ed|arrest(?:ed|s)?|investigat(?:e|ed|es|ion)|probe|fraud)\b"
            ),
        ),
        compatible_asset_families=("equity", "ticker", "sector"),
        confidence=0.8,
    ),
    _EventRule(
        event_type="default",
        patterns=(
            _rx(
                r"\b(?:default|defaults|defaulted|debt\s+default|missed\s+(?:bond|debt|coupon)\s+payment|restructur(?:e|ed|ing)\s+debt)\b"
            ),
        ),
        compatible_asset_families=("equity", "credit", "bonds", "country_risk"),
        confidence=0.88,
    ),
    _EventRule(
        event_type="bankruptcy",
        patterns=(
            _rx(
                r"\b(?:bankruptcy|bankrupt|insolvency|insolvent|chapter\s+11|administration|liquidation)\b"
            ),
        ),
        compatible_asset_families=("equity", "credit", "bonds"),
        confidence=0.9,
    ),
    _EventRule(
        event_type="strike_labor_disruption",
        patterns=(
            _rx(
                r"\b(?:strike|strikes|walkout|labor\s+dispute|labour\s+dispute|union\s+action|work\s+stoppage)\b"
            ),
        ),
        compatible_asset_families=("equity", "commodity", "sector"),
        confidence=0.81,
    ),
    _EventRule(
        event_type="safe_haven_commodity_move",
        patterns=(
            _rx(
                r"\b(?:gold|silver|platinum|palladium|precious\s+metals?)\b.{0,90}\b(?:slip(?:s|ped|ping)?|fall(?:s|en|ing)?|fell|drop(?:s|ped|ping)?|declin(?:e|ed|es|ing)|rise|rises|rose|rally|rallies|rallied|gain(?:s|ed|ing)?|jump(?:s|ed|ing)?|surg(?:e|ed|es|ing)|trade(?:s|d|ing)?|edge(?:s|d|ing)?)\b"
            ),
            _rx(
                r"\b(?:slip(?:s|ped|ping)?|fall(?:s|en|ing)?|fell|drop(?:s|ped|ping)?|declin(?:e|ed|es|ing)|rise|rises|rose|rally|rallies|rallied|gain(?:s|ed|ing)?|jump(?:s|ed|ing)?|surg(?:e|ed|es|ing)|trade(?:s|d|ing)?|edge(?:s|d|ing)?)\b.{0,90}\b(?:gold|silver|platinum|palladium|precious\s+metals?)\b"
            ),
        ),
        compatible_asset_families=(
            "commodity",
            "currency",
            "equity_index",
            "country_risk",
            "safe_haven",
        ),
        confidence=0.83,
    ),
    _EventRule(
        event_type="commodity_price_move",
        patterns=(
            _rx(
                r"\b(?:oil|crude|brent|wti|natural\s+gas|gasoline|diesel|coffee|cocoa|sugar|wheat|corn|soybeans?|copper|iron\s+ore|coal|uranium)\b.{0,120}\b(?:slip(?:s|ped|ping)?|fall(?:s|en|ing)?|fell|drop(?:s|ped|ping)?|declin(?:e|ed|es|ing)|rise|rises|rose|rally|rallies|rallied|gain(?:s|ed|ing)?|jump(?:s|ed|ing)?|surg(?:e|ed|es|ing)|climb(?:s|ed|ing)?|trade(?:s|d|ing)?|edge(?:s|d|ing)?)\b"
            ),
            _rx(
                r"\b(?:slip(?:s|ped|ping)?|fall(?:s|en|ing)?|fell|drop(?:s|ped|ping)?|declin(?:e|ed|es|ing)|rise|rises|rose|rally|rallies|rallied|gain(?:s|ed|ing)?|jump(?:s|ed|ing)?|surg(?:e|ed|es|ing)|climb(?:s|ed|ing)?|trade(?:s|d|ing)?|edge(?:s|d|ing)?)\b.{0,120}\b(?:oil|crude|brent|wti|natural\s+gas|gasoline|diesel|coffee|cocoa|sugar|wheat|corn|soybeans?|copper|iron\s+ore|coal|uranium)\b"
            ),
        ),
        compatible_asset_families=("commodity", "energy", "agriculture", "safe_haven"),
        confidence=0.82,
    ),
    _EventRule(
        event_type="crypto_market_move",
        patterns=(
            _rx(
                r"\b(?:bitcoin|btc|ether|ethereum|crypto(?:currency|currencies)?|digital\s+assets?|tokens?)\b.{0,120}\b(?:slip(?:s|ped|ping)?|fall(?:s|en|ing)?|fell|drop(?:s|ped|ping)?|declin(?:e|ed|es|ing)|rise|rises|rose|rally|rallies|rallied|gain(?:s|ed|ing)?|jump(?:s|ed|ing)?|surg(?:e|ed|es|ing)|trade(?:s|d|ing)?|etf|flows?)\b"
            ),
            _rx(
                r"\b(?:slip(?:s|ped|ping)?|fall(?:s|en|ing)?|fell|drop(?:s|ped|ping)?|declin(?:e|ed|es|ing)|rise|rises|rose|rally|rallies|rallied|gain(?:s|ed|ing)?|jump(?:s|ed|ing)?|surg(?:e|ed|es|ing)|trade(?:s|d|ing)?|etf|flows?)\b.{0,120}\b(?:bitcoin|btc|ether|ethereum|crypto(?:currency|currencies)?|digital\s+assets?|tokens?)\b"
            ),
        ),
        compatible_asset_families=("crypto", "currency", "equity_index"),
        confidence=0.82,
    ),
    _EventRule(
        event_type="currency_market_move",
        patterns=(
            _rx(
                r"\b(?:usd[/\-\s]?brl|dollar[/\-\s]?real|usd[/\-\s]?try|dollar[/\-\s]?lira|usd[/\-\s]?idr|dollar[/\-\s]?rupiah|currency|currencies|fx|forex|real|lira|rupiah)\b.{0,120}\b(?:slip(?:s|ped|ping)?|fall(?:s|en|ing)?|fell|drop(?:s|ped|ping)?|declin(?:e|ed|es|ing)|weak(?:en|ened|ens|ening)|rise|rises|rose|rally|rallies|rallied|gain(?:s|ed|ing)?|jump(?:s|ed|ing)?|surg(?:e|ed|es|ing)|strengthen(?:s|ed|ing)?|trade(?:s|d|ing)?)\b"
            ),
            _rx(
                r"\b(?:slip(?:s|ped|ping)?|fall(?:s|en|ing)?|fell|drop(?:s|ped|ping)?|declin(?:e|ed|es|ing)|weak(?:en|ened|ens|ening)|rise|rises|rose|rally|rallies|rallied|gain(?:s|ed|ing)?|jump(?:s|ed|ing)?|surg(?:e|ed|es|ing)|strengthen(?:s|ed|ing)?|trade(?:s|d|ing)?)\b.{0,120}\b(?:usd[/\-\s]?brl|dollar[/\-\s]?real|usd[/\-\s]?try|dollar[/\-\s]?lira|usd[/\-\s]?idr|dollar[/\-\s]?rupiah|currency|currencies|fx|forex|real|lira|rupiah)\b"
            ),
        ),
        compatible_asset_families=("currency", "rates", "equity_index", "country_risk"),
        confidence=0.82,
    ),
    _EventRule(
        event_type="fx_intervention",
        patterns=(
            _rx(
                r"\b(?:central\s+bank|monetary\s+authority|finance\s+ministry|treasury)\b.{0,120}\b(?:interven(?:e|ed|es|ing|tion)|bought|sold|sell|buy|support(?:ed|s)?|defend(?:ed|s)?)\b.{0,120}\b(?:currency|currencies|fx|forex|dollar|euro|yen|yuan|real|lira|rupiah|peso|rupee|exchange\s+rate)\b"
            ),
            _rx(
                r"\b(?:currency|currencies|fx|forex|dollar|euro|yen|yuan|real|lira|rupiah|peso|rupee|exchange\s+rate)\b.{0,120}\b(?:interven(?:e|ed|es|ing|tion)|support(?:ed|s)?|defend(?:ed|s)?)\b"
            ),
        ),
        compatible_asset_families=("currency", "rates", "bonds", "equity_index", "country_risk"),
        confidence=0.86,
    ),
    _EventRule(
        event_type="trade_balance_signal",
        patterns=(
            _rx(
                r"\b(?:trade\s+(?:balance|deficit|surplus)|current\s+account|exports?|imports?)\b.{0,120}\b(?:widen(?:ed|s|ing)?|narrow(?:ed|s|ing)?|surplus|deficit|fell|falls|fall|rose|rises|rise|jump(?:ed|s)?|drop(?:ped|s)?|declin(?:e|ed|es|ing)|increase(?:d|s|ing)?)\b"
            ),
            _rx(
                r"\b(?:widen(?:ed|s|ing)?|narrow(?:ed|s|ing)?|surplus|deficit|fell|falls|fall|rose|rises|rise|jump(?:ed|s)?|drop(?:ped|s)?|declin(?:e|ed|es|ing)|increase(?:d|s|ing)?)\b.{0,120}\b(?:trade\s+(?:balance|deficit|surplus)|current\s+account|exports?|imports?)\b"
            ),
        ),
        compatible_asset_families=("currency", "rates", "bonds", "equity_index", "country_risk"),
        confidence=0.8,
    ),
    _EventRule(
        event_type="capital_flow_signal",
        patterns=(
            _rx(
                r"\b(?:capital|portfolio|foreign\s+investment|fund)\s+(?:inflows?|outflows?|flows?)\b.{0,120}\b(?:rise|rises|rose|jump(?:ed|s)?|surge(?:d|s)?|fall|falls|fell|drop(?:ped|s)?|accelerat(?:e|ed|es|ing)|reverse(?:d|s)?)\b"
            ),
            _rx(
                r"\b(?:inflows?|outflows?)\b.{0,120}\b(?:bonds?|equities|stocks?|currency|fx|forex|emerging\s+markets?|foreign\s+investors?)\b"
            ),
        ),
        compatible_asset_families=("currency", "rates", "bonds", "equity_index", "country_risk"),
        confidence=0.82,
    ),
    _EventRule(
        event_type="sovereign_debt_signal",
        patterns=(
            _rx(
                r"\b(?:sovereign|government|treasury|gilt|bond|debt)\b.{0,120}\b(?:yield|yields|auction|issuance|issue|spread|spreads|default|downgrade|rating)\b"
            ),
            _rx(
                r"\b(?:yield|yields|auction|issuance|issue|spread|spreads|default|downgrade|rating)\b.{0,120}\b(?:sovereign|government|treasury|gilt|bond|debt)\b"
            ),
        ),
        compatible_asset_families=("rates", "currency", "bonds", "equity_index", "country_risk"),
        confidence=0.83,
    ),
    _EventRule(
        event_type="equity_listing",
        patterns=(
            _rx(
                r"\b(?:ipo|initial\s+public\s+offering|market\s+debut|public\s+listing|listing|listed|lists|float|flotation)\b.{0,120}\b(?:shares?|stock|ticker|exchange|bourse|market|company|miner|mining|drugmaker|pharma|telecom)\b"
            ),
            _rx(
                r"\b(?:shares?|stock|ticker|exchange|bourse|market|company|miner|mining|drugmaker|pharma|telecom)\b.{0,120}\b(?:ipo|initial\s+public\s+offering|market\s+debut|public\s+listing|listing|listed|lists|float|flotation)\b"
            ),
        ),
        compatible_asset_families=("equity", "ticker", "equity_index", "sector"),
        confidence=0.82,
    ),
    _EventRule(
        event_type="war_escalation",
        patterns=(
            _rx(
                r"\b(?:war|invasion|attack|attacks|missile|airstrike|escalat(?:e|ed|es|ion)|clashes|conflict|military\s+operation|troops?)\b"
            ),
        ),
        compatible_asset_families=(
            "commodity",
            "currency",
            "equity_index",
            "country_risk",
            "safe_haven",
        ),
        confidence=0.82,
    ),
    _EventRule(
        event_type="ceasefire_risk_relief",
        patterns=(
            _rx(
                r"\b(?:ceasefire|truce|de-escalat(?:e|ed|es|ion)|peace\s+deal|peace\s+talks|risk\s+relief|hostilities\s+paused)\b"
            ),
        ),
        compatible_asset_families=(
            "commodity",
            "currency",
            "equity_index",
            "country_risk",
            "safe_haven",
        ),
        confidence=0.84,
    ),
)


REQUIRED_EVENT_COMPATIBILITY_FAMILIES = (
    "company",
    "sector",
    "policy",
    "legal_regulatory",
    "commodity",
    "supply_chain",
    "macro",
    "central_bank",
    "product",
    "program",
)

EVENT_COMPATIBILITY_MATRIX: dict[str, EventCompatibilityFamily] = {
    "company": EventCompatibilityFamily(
        family="company",
        event_types=(
            "earnings_beat",
            "earnings_miss",
            "guidance_raise",
            "guidance_cut",
            "acquisition",
            "merger",
            "divestiture",
            "key_person_ownership_governance",
            "default",
            "bankruptcy",
            "strike_labor_disruption",
            "equity_listing",
        ),
        compatible_asset_families=("equity", "ticker", "credit", "bonds", "sector"),
        terminal_types=("issuer", "security", "ticker", "index"),
        relation_families=(
            "identity_listing",
            "person_to_issuer",
            "organization_control",
            "issuer_exposure",
            "market_access_status",
        ),
    ),
    "sector": EventCompatibilityFamily(
        family="sector",
        event_types=(
            "sector_policy_change",
            "regulatory_enforcement",
            "tariff",
            "export_control",
            "sanctions",
            "supply_disruption",
            "production_decrease",
            "production_increase",
            "strike_labor_disruption",
            "acquisition",
            "merger",
            "divestiture",
            "equity_listing",
        ),
        compatible_asset_families=(
            "sector",
            "equity",
            "equity_index",
            "ticker",
            "commodity",
            "country_risk",
        ),
        terminal_types=("sector_index", "index", "issuer", "security", "ticker", "commodity"),
        relation_families=(
            "policy_sector_exposure",
            "sector_exposure",
            "global_equity_proxy",
            "issuer_exposure",
        ),
    ),
    "policy": EventCompatibilityFamily(
        family="policy",
        event_types=(
            "sector_policy_change",
            "fiscal_expansion",
            "fiscal_austerity",
            "tariff",
            "export_control",
            "sanctions",
        ),
        compatible_asset_families=(
            "sector",
            "equity",
            "equity_index",
            "commodity",
            "currency",
            "rates",
            "bonds",
            "country_risk",
        ),
        terminal_types=(
            "sector_index",
            "index",
            "issuer",
            "security",
            "ticker",
            "commodity",
            "currency",
            "rates_proxy",
        ),
        relation_families=(
            "policy_sector_exposure",
            "country_macro_policy",
            "currency_proxy",
            "global_equity_proxy",
            "risk_proxy",
        ),
    ),
    "legal_regulatory": EventCompatibilityFamily(
        family="legal_regulatory",
        event_types=(
            "regulatory_enforcement",
            "sector_policy_change",
            "sanctions",
            "export_control",
            "default",
            "bankruptcy",
        ),
        compatible_asset_families=(
            "equity",
            "ticker",
            "sector",
            "equity_index",
            "country_risk",
            "commodity",
        ),
        terminal_types=("issuer", "security", "ticker", "sector_index", "index", "commodity"),
        relation_families=(
            "policy_sector_exposure",
            "market_access_status",
            "sanctions",
            "issuer_exposure",
        ),
    ),
    "commodity": EventCompatibilityFamily(
        family="commodity",
        event_types=(
            "commodity_price_move",
            "safe_haven_commodity_move",
            "supply_disruption",
            "production_decrease",
            "production_increase",
            "shipping_chokepoint_disruption",
            "strike_labor_disruption",
            "sanctions",
            "tariff",
            "export_control",
            "war_escalation",
            "ceasefire_risk_relief",
            "inflation_shock",
            "currency_market_move",
        ),
        compatible_asset_families=(
            "commodity",
            "energy",
            "agriculture",
            "safe_haven",
            "shipping",
            "sector",
            "currency",
            "equity_index",
            "country_risk",
        ),
        terminal_types=(
            "commodity",
            "sector_index",
            "index",
            "issuer",
            "security",
            "ticker",
            "currency",
        ),
        relation_families=(
            "geography_commodity",
            "commodity_flow",
            "issuer_exposure",
            "supply_chain_counterparty",
            "policy_sector_exposure",
        ),
    ),
    "supply_chain": EventCompatibilityFamily(
        family="supply_chain",
        event_types=(
            "supply_disruption",
            "production_decrease",
            "production_increase",
            "shipping_chokepoint_disruption",
            "tariff",
            "export_control",
            "sanctions",
            "strike_labor_disruption",
        ),
        compatible_asset_families=(
            "commodity",
            "energy",
            "agriculture",
            "shipping",
            "sector",
            "equity",
            "ticker",
        ),
        terminal_types=("commodity", "sector_index", "index", "issuer", "security", "ticker"),
        relation_families=(
            "supply_chain_counterparty",
            "commodity_flow",
            "infrastructure_asset",
            "issuer_exposure",
            "policy_sector_exposure",
        ),
    ),
    "macro": EventCompatibilityFamily(
        family="macro",
        event_types=(
            "policy_rate_cut",
            "policy_rate_hike",
            "policy_rate_hold",
            "inflation_shock",
            "future_policy_expectation",
            "fiscal_expansion",
            "fiscal_austerity",
            "currency_market_move",
            "crypto_market_move",
            "fx_intervention",
            "trade_balance_signal",
            "capital_flow_signal",
            "sovereign_debt_signal",
            "sanctions",
            "tariff",
            "export_control",
            "war_escalation",
            "ceasefire_risk_relief",
            "default",
            "bankruptcy",
        ),
        compatible_asset_families=(
            "currency",
            "crypto",
            "rates",
            "bonds",
            "equity_index",
            "country_risk",
            "safe_haven",
            "commodity",
            "equity",
        ),
        terminal_types=(
            "currency",
            "crypto",
            "rates_proxy",
            "index",
            "commodity",
            "issuer",
            "security",
            "ticker",
        ),
        relation_families=(
            "country_macro_policy",
            "currency_proxy",
            "dxy_proxy",
            "risk_proxy",
            "global_equity_proxy",
        ),
    ),
    "central_bank": EventCompatibilityFamily(
        family="central_bank",
        event_types=(
            "policy_rate_cut",
            "policy_rate_hike",
            "policy_rate_hold",
            "inflation_shock",
            "future_policy_expectation",
            "fx_intervention",
            "currency_market_move",
            "capital_flow_signal",
            "sovereign_debt_signal",
        ),
        compatible_asset_families=(
            "rates",
            "currency",
            "bonds",
            "equity_index",
            "banking",
            "country_risk",
        ),
        terminal_types=("currency", "rates_proxy", "index", "sector_index"),
        relation_families=("country_macro_policy", "currency_proxy"),
    ),
    "product": EventCompatibilityFamily(
        family="product",
        event_types=(
            "sector_policy_change",
            "regulatory_enforcement",
            "earnings_beat",
            "earnings_miss",
            "guidance_raise",
            "guidance_cut",
            "acquisition",
            "merger",
            "divestiture",
            "commodity_price_move",
            "supply_disruption",
            "production_decrease",
            "production_increase",
            "tariff",
            "export_control",
            "sanctions",
        ),
        compatible_asset_families=(
            "equity",
            "ticker",
            "sector",
            "commodity",
            "energy",
            "agriculture",
        ),
        terminal_types=("issuer", "security", "ticker", "sector_index", "commodity"),
        relation_families=(
            "program_org_relationship",
            "policy_sector_exposure",
            "issuer_exposure",
            "supply_chain_counterparty",
        ),
    ),
    "program": EventCompatibilityFamily(
        family="program",
        event_types=(
            "sector_policy_change",
            "regulatory_enforcement",
            "fiscal_expansion",
            "fiscal_austerity",
            "tariff",
            "export_control",
            "sanctions",
            "supply_disruption",
            "production_decrease",
            "production_increase",
            "shipping_chokepoint_disruption",
            "earnings_miss",
            "default",
            "bankruptcy",
        ),
        compatible_asset_families=("equity", "ticker", "sector", "country_risk", "commodity"),
        terminal_types=("issuer", "security", "ticker", "sector_index", "index", "commodity"),
        relation_families=(
            "program_org_relationship",
            "program_country_relationship",
            "program_project_relationship",
            "project_org_relationship",
            "policy_sector_exposure",
            "organization_control",
        ),
    ),
}


def event_compatibility_families_for_event_type(event_type: str) -> tuple[str, ...]:
    """Return compatibility families that include an extracted event type."""

    normalized = event_type.casefold()
    return tuple(
        family
        for family, definition in EVENT_COMPATIBILITY_MATRIX.items()
        if normalized in {event.casefold() for event in definition.event_types}
    )


_POLICY_SIGNAL_TYPES = {
    "policy_rate_cut",
    "policy_rate_hike",
    "policy_rate_hold",
    "inflation_shock",
    "future_policy_expectation",
    "fiscal_expansion",
    "fiscal_austerity",
}

_COMMODITY_SIGNAL_TYPES = {
    "sanctions",
    "tariff",
    "export_control",
    "sector_policy_change",
    "supply_disruption",
    "production_decrease",
    "production_increase",
    "shipping_chokepoint_disruption",
    "strike_labor_disruption",
    "war_escalation",
    "ceasefire_risk_relief",
    "commodity_price_move",
}

_EQUITY_EVENT_SIGNAL_TYPES = {
    "tariff",
    "export_control",
    "sector_policy_change",
    "regulatory_enforcement",
    "sanctions",
    "supply_disruption",
    "production_decrease",
    "production_increase",
    "earnings_beat",
    "earnings_miss",
    "guidance_raise",
    "guidance_cut",
    "acquisition",
    "merger",
    "divestiture",
    "key_person_ownership_governance",
    "default",
    "bankruptcy",
    "strike_labor_disruption",
    "equity_listing",
}

_BROAD_PROXY_SIGNAL_TYPES = {
    *_POLICY_SIGNAL_TYPES,
    "sanctions",
    "tariff",
    "export_control",
    "war_escalation",
    "ceasefire_risk_relief",
    "default",
    "bankruptcy",
    "currency_market_move",
    "fx_intervention",
    "trade_balance_signal",
    "capital_flow_signal",
    "sovereign_debt_signal",
}

_MACRO_FX_RATES_SIGNAL_TYPES = {
    *_POLICY_SIGNAL_TYPES,
    "currency_market_move",
    "fx_intervention",
    "tariff",
    "export_control",
    "trade_balance_signal",
    "capital_flow_signal",
    "sovereign_debt_signal",
}

_BROAD_PROXY_TEXT_TOKENS = (
    "country-risk",
    "country risk",
    "dxy",
    "dollar index",
    "global-equity",
    "global equity",
    "world equity",
    "msci world",
    "s&p 500",
    "sp500",
)

_DIRECT_LISTING_OPERATING_SIGNAL_TYPES = {
    "sector_policy_change",
    "regulatory_enforcement",
    "tariff",
    "export_control",
    "sanctions",
    "supply_disruption",
    "production_decrease",
    "production_increase",
}

_DIRECT_LISTING_RELATIONS = {
    "issuer_has_exchange_listing",
    "issuer_has_listed_ticker",
    "ticker_represents_issuer",
}


def _sentence_spans(text: str) -> list[_SentenceSpan]:
    spans: list[_SentenceSpan] = []
    for match in re.finditer(r"[^.!?\n]+(?:[.!?]+|$)", text):
        sentence = match.group(0).strip()
        if not sentence:
            continue
        leading_ws = len(match.group(0)) - len(match.group(0).lstrip())
        trailing_ws = len(match.group(0)) - len(match.group(0).rstrip())
        spans.append(
            _SentenceSpan(
                text=sentence,
                start=match.start() + leading_ws,
                end=match.end() - trailing_ws,
            )
        )
    if not spans and text.strip():
        leading_ws = len(text) - len(text.lstrip())
        trailing_ws = len(text) - len(text.rstrip())
        spans.append(
            _SentenceSpan(
                text=text.strip(),
                start=leading_ws,
                end=len(text) - trailing_ws,
            )
        )
    return spans


def _evidence_text(sentence: str, match: re.Match[str]) -> str:
    evidence = match.group(0).strip()
    if len(evidence) <= _MAX_EVIDENCE_CHARS:
        return evidence
    center = max(0, min(len(sentence), match.start() + len(match.group(0)) // 2))
    start = max(0, center - _MAX_EVIDENCE_CHARS // 2)
    end = min(len(sentence), start + _MAX_EVIDENCE_CHARS)
    return sentence[start:end].strip()


def extract_news_event_signals(text: str, *, max_signals: int = 24) -> list[NewsEventSignal]:
    """Extract market event signals with sentence-local evidence spans."""

    normalized_text = text or ""
    best_by_type: dict[str, NewsEventSignal] = {}
    for span in _sentence_spans(normalized_text):
        for rule in _EVENT_RULES:
            if rule.event_type in best_by_type:
                continue
            for pattern in rule.patterns:
                match = pattern.search(span.text)
                if match is None:
                    continue
                signal = NewsEventSignal(
                    event_type=rule.event_type,
                    confidence=rule.confidence,
                    evidence_text=_evidence_text(span.text, match),
                    evidence_start=span.start + match.start(),
                    evidence_end=span.start + match.end(),
                    source_sentence=span.text[:320],
                    compatible_asset_families=list(rule.compatible_asset_families),
                )
                best_by_type[rule.event_type] = signal
                break
    return sorted(
        best_by_type.values(),
        key=lambda item: (-item.confidence, item.event_type),
    )[:max_signals]


def _signal_types(signals: Sequence[NewsEventSignal]) -> set[str]:
    return {signal.event_type for signal in signals}


def _candidate_text(candidate: ImpactCandidate) -> str:
    return f"{candidate.entity_ref} {candidate.entity_type or ''} {candidate.name}".casefold()


def _candidate_families(candidate: ImpactCandidate) -> set[str]:
    text = _candidate_text(candidate)
    families: set[str] = set()
    if "ticker" in text or "equity" in text or "stock" in text or "issuer" in text:
        families.update(("equity", "ticker"))
    if "otc" in text:
        families.update(("equity", "ticker", "microcap"))
    if (
        "commodity" in text
        or "crude" in text
        or "oil" in text
        or "gas" in text
        or "copper" in text
        or "gold" in text
    ):
        families.add("commodity")
    if "energy" in text or "oil" in text or "gas" in text:
        families.add("energy")
    if (
        "crypto" in text
        or "bitcoin" in text
        or "btc" in text
        or "ether" in text
        or "ethereum" in text
        or "digital-asset" in text
        or "digital asset" in text
    ):
        families.add("crypto")
    if "currency" in text or "forex" in text or "fx" in text or "dxy" in text:
        families.add("currency")
    if "policy-rate" in text or "interest-rate" in text or "rate" in text:
        families.add("rates")
    if "bond" in text or "yield" in text:
        families.add("bonds")
    if "index" in text or "market" in text or "equity-index" in text:
        families.add("equity_index")
    if "country-risk" in text or "risk" in text:
        families.add("country_risk")
    return families


def _candidate_has_strong_path(candidate: ImpactCandidate) -> bool:
    if candidate.evidence_level == "direct":
        return True
    if (
        candidate.evidence_level == "shallow"
        and candidate.confidence >= 0.78
        and candidate.relationship_paths
    ):
        return True
    for path in candidate.relationship_paths:
        if path.path_depth <= 1 and any(edge.evidence_level == "direct" for edge in path.edges):
            return True
        if path.edges and all(
            edge.evidence_level in {"direct", "shallow"} and edge.confidence >= 0.80
            for edge in path.edges
        ):
            return True
    return False


def _candidate_has_direct_evidence(candidate: ImpactCandidate) -> bool:
    return candidate.evidence_level == "direct"


def _candidate_has_sector_index_path(candidate: ImpactCandidate) -> bool:
    text = _candidate_text(candidate)
    if "sector" in text:
        return True
    for path in candidate.relationship_paths:
        if any(edge.relation == "sector_affects_index" for edge in path.edges):
            return True
    return False


def _candidate_has_direct_listing_path(candidate: ImpactCandidate) -> bool:
    for path in candidate.relationship_paths:
        for edge in path.edges:
            if (
                edge.relation in _DIRECT_LISTING_RELATIONS
                and edge.evidence_level == "direct"
                and edge.confidence >= 0.80
            ):
                return True
    return False


def _candidate_is_broad_proxy(candidate: ImpactCandidate, families: set[str]) -> bool:
    text = _candidate_text(candidate)
    if any(token in text for token in _BROAD_PROXY_TEXT_TOKENS):
        return True
    if "country_risk" in families:
        return True
    if "equity_index" in families and not _candidate_has_sector_index_path(candidate):
        return True
    return False


def _candidate_requires_macro_fx_rates_signal(
    candidate: ImpactCandidate,
    families: set[str],
) -> bool:
    return bool(families & {"currency", "rates"}) or _candidate_is_broad_proxy(candidate, families)


def _candidate_edge_event_types(candidate: ImpactCandidate) -> set[str]:
    event_types = {event.casefold() for event in candidate.compatible_event_types}
    for path in candidate.relationship_paths:
        for edge in path.edges:
            event_types.update(event.casefold() for event in edge.compatible_event_types)
    return event_types


def _compatible_events_for_candidate(
    candidate: ImpactCandidate,
    signals: Sequence[NewsEventSignal],
) -> list[str]:
    edge_event_types = _candidate_edge_event_types(candidate)
    if edge_event_types:
        return sorted(
            {
                signal.event_type
                for signal in signals
                if signal.event_type.casefold() in edge_event_types
            }
        )
    families = _candidate_families(candidate)
    if not families:
        return []
    compatible: list[str] = []
    for signal in signals:
        signal_families = {family.casefold() for family in signal.compatible_asset_families}
        if families & signal_families:
            compatible.append(signal.event_type)
    return sorted(set(compatible))


def gate_terminal_candidates_by_event_signals(
    candidates: Iterable[ImpactCandidate],
    signals: Sequence[NewsEventSignal],
) -> tuple[list[ImpactCandidate], list[str]]:
    """Return candidates that satisfy event-compatibility guardrails."""

    signal_types = _signal_types(signals)
    gated: list[ImpactCandidate] = []
    dropped_by_reason: dict[str, int] = {}

    for candidate in candidates:
        compatible_events = _compatible_events_for_candidate(candidate, signals)
        edge_event_types = _candidate_edge_event_types(candidate)
        families = _candidate_families(candidate)
        direct_evidence = _candidate_has_direct_evidence(candidate)
        keep = True
        reason: str | None = None

        if _candidate_requires_macro_fx_rates_signal(candidate, families) and not (
            signal_types & _MACRO_FX_RATES_SIGNAL_TYPES
        ):
            keep = False
            reason = "missing_macro_fx_rates_event_signal"
        elif edge_event_types and not (signal_types & edge_event_types):
            operating_signal_types = signal_types & _DIRECT_LISTING_OPERATING_SIGNAL_TYPES
            if (
                families & {"equity", "ticker"}
                and operating_signal_types
                and _candidate_has_direct_listing_path(candidate)
            ):
                compatible_events = sorted(operating_signal_types)
            else:
                keep = False
                reason = "missing_relationship_event_compatibility"
        elif ("commodity" in families or "energy" in families) and not direct_evidence:
            if not (signal_types & _COMMODITY_SIGNAL_TYPES):
                keep = False
                reason = "missing_commodity_event_signal"
        elif families & {"equity", "ticker", "microcap"}:
            if not direct_evidence and not (
                (signal_types & _EQUITY_EVENT_SIGNAL_TYPES)
                and _candidate_has_strong_path(candidate)
            ):
                keep = False
                reason = "missing_equity_direct_or_strong_event_path"

        if keep:
            gated.append(
                candidate.model_copy(
                    update={
                        "compatible_event_types": compatible_events,
                    }
                )
            )
            continue

        if reason:
            dropped_by_reason[reason] = dropped_by_reason.get(reason, 0) + 1

    warnings = [
        f"event_signal_gate_filtered:{reason}:{count}"
        for reason, count in sorted(dropped_by_reason.items())
    ]
    return gated, warnings
