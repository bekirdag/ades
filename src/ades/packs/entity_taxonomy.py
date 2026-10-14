"""Exact geographic names that must not become finance person aliases.

Use whole-name exclusions, rather than directional stop words, to preserve
legitimate names such as East, West, or North in human names.
"""
GEOGRAPHIC_PERSON_EXCLUSIONS = frozenset({
    "middle east", "west bank", "latin america", "north america", "south america",
    "central america", "eastern europe", "western europe", "southeast asia",
    "south east asia", "south asia", "east asia", "north africa", "south africa",
    "europe", "asia", "africa", "united states", "united kingdom", "hong kong",
    "new york", "new zealand", "saudi arabia", "south korea", "north korea",
    "european union", "gulf of oman", "strait of hormuz", "south china sea",
    "los angeles", "san francisco", "washington, d.c.", "washington, d.c", "washington d.c.",
    "north carolina", "south carolina", "north dakota", "south dakota", "new jersey",
    "new hampshire", "new mexico", "new delhi", "new york city", "san diego",
    "salt lake city", "san antonio", "las vegas", "united arab emirates",
})


def is_geographic_person_name(value: str) -> bool:
    """Recognize an exact geographical phrase mislabeled as a person."""
    return " ".join(value.strip().casefold().split()).removeprefix("the ") in GEOGRAPHIC_PERSON_EXCLUSIONS

# These are complete non-person phrases observed in SEC table extraction,
# not token-level surname exclusions.
_NON_PERSON_FINANCE_PHRASES = frozenset({
    "investor relations", "research analyst", "share price", "first amendment",
    "exchange commission", "international monetary fund", "thank you",
    "national security council", "federal trade commission", "human rights",
    "deutsche bank", "samsung electronics", "u.s. navy", "u.s. army", "u.s. senate",
    "us navy", "us army", "us senate", "u.s. government", "us government",
    "protection act", "postal service", "president/ ceo", "president/ceo",
    "real estate", "life insurance", "growth fund", "class i", "fiscal year",
    "eastern time", "british army", "fourth quarter", "exchange act", "cyber security",
    "adjusted ebitda", "high school", "voting rights", "press release", "medical center",
    "data protection", "air lines", "what we don't do:", "former us",
})


def is_invalid_finance_person_name(value: str) -> bool:
    """Reject confirmed place, institutional, and date phrases in legacy people lanes."""
    import re
    normalized = " ".join(value.strip(" \t\r\n•§*-,.").casefold().split())
    if is_geographic_person_name(normalized) or normalized in _NON_PERSON_FINANCE_PHRASES:
        return True
    if re.fullmatch(
        r"(?:in|on) (?:january|february|march|april|may|june|july|august|september|october|november|december)",
        normalized,
    ):
        return True
    return bool(re.search(r"\b(?:university|commission|committee|council|agency|government|college|school|act)$", normalized))
