"""Live newswire feedback evaluation for entity extraction quality."""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET

import httpx

from .packs.registry import PackRegistry
from .pipeline.tagger import tag_text
from .service.models import EntityMatch, TagResponse
from .text_processing import normalize_lookup_text

DEFAULT_NEWS_FEEDBACK_ROOT = Path("/mnt/githubActions/ades_big_data/extraction_quality")
DEFAULT_NEWS_HTTP_TIMEOUT_S = 20.0
DEFAULT_MAX_FEED_ITEM_AGE_DAYS = 0
DEFAULT_NEWS_HTTP_HEADERS = {
    "user-agent": "ades/0.1.0 (+https://local.ades)",
    "accept-encoding": "gzip, deflate, identity",
}
DEFAULT_LIVE_NEWS_FEEDS: tuple[tuple[str, str], ...] = (
    ("abcnews", "https://abcnews.go.com/abcnews/topstories"),
    ("arab_news", "https://www.arabnews.com/rss.xml"),
    ("bbc", "https://feeds.bbci.co.uk/news/rss.xml"),
    ("belfast_telegraph", "https://www.belfasttelegraph.co.uk/rss"),
    ("brisbane_times", "https://www.brisbanetimes.com.au/rss/feed.xml"),
    ("calgary_herald", "https://calgaryherald.com/feed"),
    ("canberra_times", "https://www.canberratimes.com.au/rss.xml"),
    ("cbsnews", "https://www.cbsnews.com/latest/rss/main"),
    ("cityam", "https://www.cityam.com/feed/"),
    ("cnn", "http://rss.cnn.com/rss/edition.rss"),
    ("csmonitor", "https://www.csmonitor.com/feeds/all"),
    ("daily_mail", "https://www.dailymail.co.uk/home/index.rss"),
    ("daily_mirror", "https://www.mirror.co.uk/rss.xml"),
    ("daily_record", "https://www.dailyrecord.co.uk/rss.xml"),
    ("daily_trust", "https://dailytrust.com/rss"),
    ("dw", "https://rss.dw.com/xml/rss-en-top"),
    ("edmonton_journal", "https://edmontonjournal.com/feed"),
    ("euronews", "https://www.euronews.com/rss?level=theme&name=news"),
    ("evening_standard", "https://www.standard.co.uk/rss"),
    ("foxnews", "https://moxie.foxnews.com/google-publisher/world.xml"),
    ("france24", "https://www.france24.com/en/rss"),
    ("guardian", "https://www.theguardian.com/world/rss"),
    ("gulf_news", "https://gulfnews.com/feed"),
    ("independent", "https://www.independent.co.uk/rss"),
    ("inews", "https://inews.co.uk/rss"),
    ("irish_independent", "https://www.independent.ie/rss"),
    ("irish_times", "https://www.irishtimes.com/arc/outboundfeeds/rss/"),
    ("khaleej_times", "https://www.khaleejtimes.com/api/v1/collections/top-section.rss"),
    ("korea_times", "https://feed.koreatimes.co.kr/k/allnews.xml"),
    ("latimes", "https://www.latimes.com/index.rss"),
    ("mail_guardian", "https://mg.co.za/feed/"),
    ("metro_uk", "https://metro.co.uk/feed/"),
    ("montreal_gazette", "https://montrealgazette.com/rss"),
    ("national_post", "https://nationalpost.com/feed"),
    ("nbcnews", "https://feeds.nbcnews.com/nbcnews/public/news"),
    ("newsletter", "https://www.newsletter.co.uk/rss"),
    ("newsweek", "https://www.newsweek.com/rss"),
    ("otago_daily_times", "https://www.odt.co.nz/news/feed"),
    ("ottawa_citizen", "https://ottawacitizen.com/feed"),
    ("philippine_daily_inquirer", "https://newsinfo.inquirer.net/rss"),
    ("politico", "https://www.politico.com/rss/politicopicks.xml"),
    ("punch", "https://punchng.com/feed/"),
    ("scmp", "https://www.scmp.com/rss/feed"),
    ("scotsman", "https://www.scotsman.com/rss"),
    ("smh", "https://www.smh.com.au/rss/feed.xml"),
    ("telegraph", "https://www.telegraph.co.uk/rss.xml"),
    ("the_age", "https://www.theage.com.au/rss/feed.xml"),
    ("the_hindu", "https://www.thehindu.com/feeder/default.rss"),
    ("the_province", "https://theprovince.com/feed"),
    ("the_sun", "https://www.thesun.co.uk/feed/"),
    ("thehill", "https://thehill.com/feed/"),
    ("upi", "https://rss.upi.com/news/news.rss"),
    ("vancouver_sun", "https://vancouversun.com/feed"),
    ("watoday", "https://www.watoday.com.au/rss/feed.xml"),
    ("west_australian", "https://thewest.com.au/rss"),
    ("winnipeg_free_press", "https://www.winnipegfreepress.com/feed"),
)

_GENERIC_SINGLE_TOKEN_WORDS = {
    "a",
    "an",
    "and",
    "april",
    "august",
    "bank",
    "company",
    "december",
    "eight",
    "eleven",
    "february",
    "five",
    "four",
    "friday",
    "january",
    "july",
    "june",
    "march",
    "monday",
    "nine",
    "november",
    "october",
    "one",
    "report",
    "saturday",
    "seven",
    "six",
    "stock",
    "sunday",
    "ten",
    "that",
    "the",
    "these",
    "this",
    "three",
    "thursday",
    "tuesday",
    "twelve",
    "two",
    "university",
    "wednesday",
}
_STRUCTURAL_ORG_SUFFIXES = {
    "agency",
    "association",
    "bank",
    "bureau",
    "committee",
    "consultancy",
    "consulting",
    "corporation",
    "council",
    "group",
    "information",
    "institute",
    "intelligence",
    "media",
    "ministry",
    "organization",
    "research",
    "solutions",
    "systems",
    "technologies",
    "technology",
    "university",
}
_STRUCTURAL_LOCATION_HEADS = {
    "bay",
    "cape",
    "delta",
    "gulf",
    "island",
    "lake",
    "mount",
    "mountain",
    "river",
    "sea",
    "strait",
}
_PARTIAL_SPAN_CONNECTORS = {"and", "for", "in", "of", "on", "or", "the", "to", "with"}
_PARTIAL_SPAN_FRAGMENT_TOKENS = {"d", "ll", "m", "re", "s", "t", "ve"}
_PARTIAL_SPAN_ROLE_TOKENS = {
    "chair",
    "chief",
    "director",
    "editor",
    "governor",
    "mayor",
    "minister",
    "president",
    "principal",
    "reporter",
    "secretary",
    "spokesperson",
}
_LEADING_CONTEXT_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "but",
    "by",
    "click",
    "download",
    "for",
    "from",
    "here",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "this",
    "that",
    "these",
    "those",
    "to",
    "watch",
    "with",
}
_ACRONYM_STOPWORDS = {
    "app",
    "as",
    "at",
    "by",
    "click",
    "for",
    "from",
    "here",
    "in",
    "into",
    "live",
    "news",
    "of",
    "on",
    "or",
    "the",
    "to",
    "via",
    "watch",
}
_BOILERPLATE_CONTEXT_TOKENS = {
    "app",
    "download",
    "here",
    "live",
    "news",
    "video",
    "watch",
}
_PUBLICATION_SUFFIX_TOKENS = {
    "app",
    "news",
    "times",
}
_WEAK_STRUCTURAL_ORG_SUFFIXES = {
    "group",
    "information",
    "intelligence",
    "research",
}
_GENERIC_STRUCTURED_ORG_CANDIDATES = {
    "social media",
}
_HYPHENATED_GEO_SUFFIXES = {
    "backed",
    "based",
    "centered",
    "centred",
    "driven",
    "focused",
    "led",
    "linked",
    "owned",
    "related",
    "run",
    "sourced",
    "targeted",
}
_GENERIC_FIX_CLASS_BY_ISSUE = {
    "generic_single_token_entity": "tighten_alias_class_filtering",
    "single_token_name_fragment": "tighten_fragment_suppression",
    "partial_span_candidate": "improve_longest_span_preference",
    "missing_acronym_candidate": "strengthen_acronym_retention",
    "missing_hyphenated_prefix_candidate": "strengthen_hyphenated_prefix_recovery",
    "missing_structured_organization_candidate": "strengthen_structural_organization_retention",
    "missing_structured_location_candidate": "strengthen_structural_location_retention",
}


@dataclass(frozen=True)
class LiveNewsFeedSpec:
    """One configured RSS feed source."""

    source: str
    feed_url: str


@dataclass(frozen=True)
class LiveNewsFeedItem:
    """One feed item discovered from one RSS document."""

    source: str
    feed_url: str
    title: str
    article_url: str
    published_at: str | None = None


@dataclass(frozen=True)
class LiveNewsEntity:
    """Condensed extracted entity for reporting."""

    text: str
    label: str
    start: int
    end: int


@dataclass(frozen=True)
class LiveNewsIssue:
    """One heuristic issue found in one tagged article."""

    issue_type: str
    message: str
    entity_text: str | None = None
    label: str | None = None
    candidate_text: str | None = None


@dataclass(frozen=True)
class LiveNewsFailure:
    """One feed or article fetch/extraction failure."""

    source: str
    stage: str
    url: str
    message: str
    title: str | None = None


@dataclass(frozen=True)
class LiveNewsArticleResult:
    """One evaluated live article."""

    source: str
    title: str
    article_url: str
    published_at: str | None
    text_source: str
    word_count: int
    entity_count: int
    timing_ms: int
    warnings: list[str] = field(default_factory=list)
    issue_types: list[str] = field(default_factory=list)
    issues: list[LiveNewsIssue] = field(default_factory=list)
    entities: list[LiveNewsEntity] = field(default_factory=list)


@dataclass(frozen=True)
class LiveNewsFeedbackReport:
    """Aggregate live-news feedback report for one installed pack."""

    pack_id: str
    generated_at: str
    requested_article_count: int
    collected_article_count: int
    feed_count: int
    successful_feed_count: int
    p50_latency_ms: int
    p95_latency_ms: int
    per_source_article_counts: dict[str, int] = field(default_factory=dict)
    per_issue_counts: dict[str, int] = field(default_factory=dict)
    suggested_fix_classes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    feed_failures: list[LiveNewsFailure] = field(default_factory=list)
    article_failures: list[LiveNewsFailure] = field(default_factory=list)
    articles: list[LiveNewsArticleResult] = field(default_factory=list)


@dataclass(frozen=True)
class LiveNewsFixSuggestion:
    """One merged fix suggestion derived from one or more live-news issues."""

    issue_type: str
    fix_class: str
    issue_count: int
    recommendation: str
    cluster_indexes: list[int] = field(default_factory=list)
    sample_titles: list[str] = field(default_factory=list)
    sample_urls: list[str] = field(default_factory=list)
    sample_candidate_texts: list[str] = field(default_factory=list)
    sample_entity_texts: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class LiveNewsProcessedArticle:
    """One article URL already consumed by the live-news digestion workflow."""

    article_url: str
    source: str
    title: str
    published_at: str | None = None
    first_processed_at: str | None = None
    last_processed_at: str | None = None
    processed_count: int = 1
    last_status: str = "success"
    last_message: str | None = None


@dataclass(frozen=True)
class LiveNewsDigestionClusterSummary:
    """Summary for one fixed-size digestion cluster."""

    cluster_index: int
    requested_article_count: int
    collected_article_count: int
    p50_latency_ms: int
    p95_latency_ms: int
    per_source_article_counts: dict[str, int] = field(default_factory=dict)
    per_issue_counts: dict[str, int] = field(default_factory=dict)
    suggested_fix_classes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    article_failures: list[LiveNewsFailure] = field(default_factory=list)
    suggestions: list[LiveNewsFixSuggestion] = field(default_factory=list)
    report_path: str | None = None
    suggestion_path: str | None = None


@dataclass(frozen=True)
class LiveNewsDigestionRunReport:
    """Aggregate multi-cluster digestion run report."""

    pack_id: str
    generated_at: str
    requested_cluster_count: int
    completed_cluster_count: int
    cluster_size: int
    requested_article_count: int
    collected_article_count: int
    feed_count: int
    successful_feed_count: int
    previously_processed_article_count: int
    newly_processed_article_count: int
    known_processed_article_count: int
    p50_latency_ms: int
    p95_latency_ms: int
    per_source_article_counts: dict[str, int] = field(default_factory=dict)
    per_issue_counts: dict[str, int] = field(default_factory=dict)
    suggested_fix_classes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    feed_failures: list[LiveNewsFailure] = field(default_factory=list)
    article_failures: list[LiveNewsFailure] = field(default_factory=list)
    clusters: list[LiveNewsDigestionClusterSummary] = field(default_factory=list)
    merged_suggestions: list[LiveNewsFixSuggestion] = field(default_factory=list)
    suggestion_dir: str | None = None
    merged_suggestion_path: str | None = None
    processed_store_dir: str | None = None
    processed_store_path: str | None = None
    run_report_path: str | None = None


def live_news_feedback_report_path(
    pack_id: str,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> Path:
    """Return the default on-disk path for one live-news feedback report."""

    return root / "reports" / f"{pack_id}.live-news-feedback.json"


def write_live_news_feedback_report(
    path: str | Path,
    report: LiveNewsFeedbackReport,
    ) -> Path:
    """Persist one live-news feedback report as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(asdict(report), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return destination


def live_news_digestion_reports_dir(
    pack_id: str,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> Path:
    """Return the directory for per-cluster digestion reports."""

    return root / "reports" / "live-news-digestion" / pack_id


def live_news_digestion_cluster_report_path(
    pack_id: str,
    cluster_index: int,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> Path:
    """Return the JSON path for one digestion-cluster report."""

    return live_news_digestion_reports_dir(pack_id, root=root) / f"cluster-{cluster_index:02d}.json"


def live_news_digestion_run_report_path(
    pack_id: str,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> Path:
    """Return the JSON path for one multi-cluster digestion run."""

    return live_news_digestion_reports_dir(pack_id, root=root) / "run-summary.json"


def live_news_feedback_suggestions_dir(
    pack_id: str,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> Path:
    """Return the suggestion folder for live-news issue-fix guidance."""

    return root / "suggestions" / "live-news-digestion" / pack_id


def live_news_feedback_cluster_suggestions_path(
    pack_id: str,
    cluster_index: int,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> Path:
    """Return the JSON path for one cluster's fix suggestions."""

    return live_news_feedback_suggestions_dir(pack_id, root=root) / f"cluster-{cluster_index:02d}.suggestions.json"


def live_news_feedback_merged_suggestions_path(
    pack_id: str,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> Path:
    """Return the JSON path for merged multi-cluster suggestions."""

    return live_news_feedback_suggestions_dir(pack_id, root=root) / "merged.suggestions.json"


def live_news_processed_store_dir(
    pack_id: str,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> Path:
    """Return the folder that stores processed live-news URLs for one pack."""

    return root / "processed-news" / "live-news-digestion" / pack_id


def live_news_processed_articles_path(
    pack_id: str,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> Path:
    """Return the JSON path for the durable processed live-news ledger."""

    return live_news_processed_store_dir(pack_id, root=root) / "processed-articles.json"


def write_live_news_digestion_run_report(
    path: str | Path,
    report: LiveNewsDigestionRunReport,
) -> Path:
    """Persist one multi-cluster digestion run report as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(asdict(report), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return destination


def write_live_news_fix_suggestions(
    path: str | Path,
    *,
    pack_id: str,
    generated_at: str,
    suggestions: list[LiveNewsFixSuggestion],
    cluster_index: int | None = None,
) -> Path:
    """Persist one cluster or merged suggestion bundle as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pack_id": pack_id,
        "generated_at": generated_at,
        "cluster_index": cluster_index,
        "suggestions": [asdict(item) for item in suggestions],
    }
    destination.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return destination


def write_live_news_processed_articles(
    path: str | Path,
    *,
    pack_id: str,
    generated_at: str,
    articles: list[LiveNewsProcessedArticle],
) -> Path:
    """Persist the durable processed live-news ledger as JSON."""

    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    ordered_articles = sorted(
        articles,
        key=lambda item: (
            item.first_processed_at or item.last_processed_at or "",
            item.source,
            item.article_url,
        ),
    )
    payload = {
        "pack_id": pack_id,
        "generated_at": generated_at,
        "article_count": len(ordered_articles),
        "articles": [asdict(item) for item in ordered_articles],
    }
    destination.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return destination


def load_live_news_processed_articles(
    pack_id: str,
    *,
    root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
) -> dict[str, LiveNewsProcessedArticle]:
    """Load the durable processed live-news ledger, bootstrapping from old reports."""

    destination = live_news_processed_articles_path(pack_id, root=root)
    records = _read_live_news_processed_articles(destination, pack_id=pack_id)
    historical_records = _bootstrap_live_news_processed_articles(pack_id, search_root=root)
    for article in historical_records.values():
        _merge_processed_live_news_article(
            records,
            article_url=article.article_url,
            source=article.source,
            title=article.title,
            published_at=article.published_at,
            processed_at=article.last_processed_at or article.first_processed_at,
            status=article.last_status,
            message=article.last_message,
            increment_count=False,
        )
    if records:
        write_live_news_processed_articles(
            destination,
            pack_id=pack_id,
            generated_at=datetime.now(timezone.utc).isoformat(),
            articles=list(records.values()),
        )
    return records


def _read_live_news_processed_articles(
    path: Path,
    *,
    pack_id: str,
) -> dict[str, LiveNewsProcessedArticle]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if payload.get("pack_id") not in {None, pack_id}:
        return {}

    records: dict[str, LiveNewsProcessedArticle] = {}
    for item in payload.get("articles", []):
        article_url = str(item.get("article_url") or "").strip()
        if not article_url:
            continue
        records[article_url] = LiveNewsProcessedArticle(
            article_url=article_url,
            source=str(item.get("source") or ""),
            title=str(item.get("title") or ""),
            published_at=_optional_string(item.get("published_at")),
            first_processed_at=_optional_string(item.get("first_processed_at")),
            last_processed_at=_optional_string(item.get("last_processed_at")),
            processed_count=max(int(item.get("processed_count") or 1), 1),
            last_status=str(item.get("last_status") or "success"),
            last_message=_optional_string(item.get("last_message")),
        )
    return records


def _bootstrap_live_news_processed_articles(
    pack_id: str,
    *,
    search_root: Path,
) -> dict[str, LiveNewsProcessedArticle]:
    records: dict[str, LiveNewsProcessedArticle] = {}
    if not search_root.exists():
        return records

    for report_path in _iter_bootstrap_live_news_report_paths(pack_id, search_root=search_root):
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("pack_id") not in {None, pack_id}:
            continue
        processed_at = _optional_string(payload.get("generated_at"))
        for article in payload.get("articles", []):
            _merge_processed_live_news_article(
                records,
                article_url=str(article.get("article_url") or ""),
                source=str(article.get("source") or ""),
                title=str(article.get("title") or ""),
                published_at=_optional_string(article.get("published_at")),
                processed_at=processed_at,
                status="success",
                message=None,
                increment_count=False,
            )
        for failure in payload.get("article_failures", []):
            _merge_processed_live_news_article(
                records,
                article_url=str(failure.get("url") or ""),
                source=str(failure.get("source") or ""),
                title=str(failure.get("title") or ""),
                published_at=None,
                processed_at=processed_at,
                status=str(failure.get("stage") or "article_failure"),
                message=_optional_string(failure.get("message")),
                increment_count=False,
            )
    return records


def _iter_bootstrap_live_news_report_paths(
    pack_id: str,
    *,
    search_root: Path,
) -> list[Path]:
    paths: set[Path] = set()
    for path in search_root.rglob(f"{pack_id}.live-news-feedback.json"):
        if path.is_file():
            paths.add(path.resolve())
    for path in search_root.rglob("cluster-*.json"):
        if path.is_file() and _is_live_news_cluster_report_path(path, pack_id):
            paths.add(path.resolve())
    return sorted(paths)


def _is_live_news_cluster_report_path(path: Path, pack_id: str) -> bool:
    if path.name.endswith(".suggestions.json"):
        return False
    if not re.fullmatch(r"cluster-\d{2}\.json", path.name):
        return False
    parts = path.parts
    return len(parts) >= 4 and parts[-4] == "reports" and parts[-3] == "live-news-digestion" and parts[-2] == pack_id


def _merge_processed_live_news_article(
    records: dict[str, LiveNewsProcessedArticle],
    *,
    article_url: str,
    source: str,
    title: str,
    published_at: str | None,
    processed_at: str | None,
    status: str,
    message: str | None,
    increment_count: bool,
) -> None:
    normalized_url = article_url.strip()
    if not normalized_url:
        return

    existing = records.get(normalized_url)
    if existing is None:
        records[normalized_url] = LiveNewsProcessedArticle(
            article_url=normalized_url,
            source=source,
            title=title,
            published_at=published_at,
            first_processed_at=processed_at,
            last_processed_at=processed_at,
            processed_count=1,
            last_status=status,
            last_message=message,
        )
        return

    records[normalized_url] = replace(
        existing,
        source=existing.source or source,
        title=existing.title or title,
        published_at=existing.published_at or published_at,
        first_processed_at=_earliest_timestamp(existing.first_processed_at, processed_at),
        last_processed_at=_latest_timestamp(existing.last_processed_at, processed_at),
        processed_count=existing.processed_count + 1 if increment_count else existing.processed_count,
        last_status=status or existing.last_status,
        last_message=message if message is not None else existing.last_message,
    )


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _earliest_timestamp(*values: str | None) -> str | None:
    non_empty = [value for value in values if value]
    if not non_empty:
        return None
    return min(non_empty)


def _latest_timestamp(*values: str | None) -> str | None:
    non_empty = [value for value in values if value]
    if not non_empty:
        return None
    return max(non_empty)


def evaluate_live_news_feedback(
    pack_id: str,
    *,
    storage_root: Path,
    article_limit: int = 10,
    per_feed_limit: int = 10,
    feeds: tuple[LiveNewsFeedSpec, ...] | None = None,
    timeout_s: float = DEFAULT_NEWS_HTTP_TIMEOUT_S,
    max_feed_item_age_days: int = DEFAULT_MAX_FEED_ITEM_AGE_DAYS,
) -> LiveNewsFeedbackReport:
    """Fetch live RSS items, tag article text, and summarize generic issue classes."""

    active_feeds = _resolve_live_news_feeds(feeds)
    registry = PackRegistry(storage_root)

    with httpx.Client(
        follow_redirects=True,
        timeout=timeout_s,
        headers=DEFAULT_NEWS_HTTP_HEADERS,
    ) as client:
        successful_feed_count, feed_failures, feed_queues = _load_live_news_feed_queues(
            client,
            active_feeds,
            per_feed_limit=per_feed_limit,
            max_feed_item_age_days=max_feed_item_age_days,
        )
        article_results, article_failures = _collect_live_news_article_results(
            client,
            feed_queues,
            article_limit=article_limit,
            pack_id=pack_id,
            storage_root=storage_root,
            registry=registry,
        )

    return _build_live_news_feedback_report(
        pack_id,
        requested_article_count=article_limit,
        active_feeds=active_feeds,
        successful_feed_count=successful_feed_count,
        feed_failures=feed_failures,
        article_failures=article_failures,
        article_results=article_results,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


def run_live_news_digestion_clusters(
    pack_id: str,
    *,
    storage_root: Path,
    cluster_count: int = 10,
    cluster_size: int = 10,
    per_feed_limit: int = 30,
    feeds: tuple[LiveNewsFeedSpec, ...] | None = None,
    timeout_s: float = DEFAULT_NEWS_HTTP_TIMEOUT_S,
    max_feed_item_age_days: int = DEFAULT_MAX_FEED_ITEM_AGE_DAYS,
    artifact_root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
    processed_root: Path = DEFAULT_NEWS_FEEDBACK_ROOT,
    write_artifacts: bool = True,
) -> LiveNewsDigestionRunReport:
    """Run multiple fixed-size live-news digestion clusters and merge fix suggestions."""

    active_feeds = _resolve_live_news_feeds(feeds)
    registry = PackRegistry(storage_root)
    processed_articles = load_live_news_processed_articles(pack_id, root=processed_root)
    seen_article_urls: set[str] = set(processed_articles)
    previous_processed_article_count = len(processed_articles)
    cluster_summaries: list[LiveNewsDigestionClusterSummary] = []
    all_article_results: list[LiveNewsArticleResult] = []
    all_article_failures: list[LiveNewsFailure] = []
    collected_suggestions: list[LiveNewsFixSuggestion] = []
    warnings: list[str] = []
    generated_at = datetime.now(timezone.utc).isoformat()

    with httpx.Client(
        follow_redirects=True,
        timeout=timeout_s,
        headers=DEFAULT_NEWS_HTTP_HEADERS,
    ) as client:
        successful_feed_count, feed_failures, feed_queues = _load_live_news_feed_queues(
            client,
            active_feeds,
            per_feed_limit=per_feed_limit,
            max_feed_item_age_days=max_feed_item_age_days,
        )

        for cluster_index in range(1, cluster_count + 1):
            article_results, article_failures = _collect_live_news_article_results(
                client,
                feed_queues,
                article_limit=cluster_size,
                pack_id=pack_id,
                storage_root=storage_root,
                registry=registry,
                seen_article_urls=seen_article_urls,
            )
            if not article_results:
                warnings.append(f"cluster_{cluster_index:02d}_no_articles_collected")
                break

            cluster_report = _build_live_news_feedback_report(
                pack_id,
                requested_article_count=cluster_size,
                active_feeds=active_feeds,
                successful_feed_count=successful_feed_count,
                feed_failures=[],
                article_failures=article_failures,
                article_results=article_results,
                generated_at=datetime.now(timezone.utc).isoformat(),
            )
            cluster_suggestions = _build_live_news_fix_suggestions(
                cluster_report,
                cluster_index=cluster_index,
            )

            report_path: str | None = None
            suggestion_path: str | None = None
            if write_artifacts:
                report_destination = live_news_digestion_cluster_report_path(
                    pack_id,
                    cluster_index,
                    root=artifact_root,
                )
                report_path = str(write_live_news_feedback_report(report_destination, cluster_report))
                suggestion_destination = live_news_feedback_cluster_suggestions_path(
                    pack_id,
                    cluster_index,
                    root=artifact_root,
                )
                suggestion_path = str(
                    write_live_news_fix_suggestions(
                        suggestion_destination,
                        pack_id=pack_id,
                        generated_at=cluster_report.generated_at,
                        suggestions=cluster_suggestions,
                        cluster_index=cluster_index,
                    )
                )

            cluster_summaries.append(
                LiveNewsDigestionClusterSummary(
                    cluster_index=cluster_index,
                    requested_article_count=cluster_size,
                    collected_article_count=cluster_report.collected_article_count,
                    p50_latency_ms=cluster_report.p50_latency_ms,
                    p95_latency_ms=cluster_report.p95_latency_ms,
                    per_source_article_counts=cluster_report.per_source_article_counts,
                    per_issue_counts=cluster_report.per_issue_counts,
                    suggested_fix_classes=cluster_report.suggested_fix_classes,
                    warnings=cluster_report.warnings,
                    article_failures=article_failures,
                    suggestions=cluster_suggestions,
                    report_path=report_path,
                    suggestion_path=suggestion_path,
                )
            )
            all_article_results.extend(article_results)
            all_article_failures.extend(article_failures)
            collected_suggestions.extend(cluster_suggestions)

    merged_suggestions = _merge_live_news_fix_suggestions(collected_suggestions)
    merged_suggestion_path: str | None = None
    run_report_path: str | None = None
    if write_artifacts:
        merged_suggestion_destination = live_news_feedback_merged_suggestions_path(
            pack_id,
            root=artifact_root,
        )
        merged_suggestion_path = str(
            write_live_news_fix_suggestions(
                merged_suggestion_destination,
                pack_id=pack_id,
                generated_at=generated_at,
                suggestions=merged_suggestions,
            )
        )

    if len(cluster_summaries) < cluster_count:
        warnings.append("insufficient_unique_articles_for_requested_cluster_count")

    for result in all_article_results:
        _merge_processed_live_news_article(
            processed_articles,
            article_url=result.article_url,
            source=result.source,
            title=result.title,
            published_at=result.published_at,
            processed_at=generated_at,
            status="success",
            message=None,
            increment_count=True,
        )
    for failure in all_article_failures:
        _merge_processed_live_news_article(
            processed_articles,
            article_url=failure.url,
            source=failure.source,
            title=failure.title or "",
            published_at=None,
            processed_at=generated_at,
            status=failure.stage,
            message=failure.message,
            increment_count=True,
        )
    processed_store_destination = live_news_processed_articles_path(
        pack_id,
        root=processed_root,
    )
    processed_store_path = str(
        write_live_news_processed_articles(
            processed_store_destination,
            pack_id=pack_id,
            generated_at=generated_at,
            articles=list(processed_articles.values()),
        )
    )
    known_processed_article_count = len(processed_articles)

    run_report = LiveNewsDigestionRunReport(
        pack_id=pack_id,
        generated_at=generated_at,
        requested_cluster_count=cluster_count,
        completed_cluster_count=len(cluster_summaries),
        cluster_size=cluster_size,
        requested_article_count=cluster_count * cluster_size,
        collected_article_count=len(all_article_results),
        feed_count=len(active_feeds),
        successful_feed_count=successful_feed_count if "successful_feed_count" in locals() else 0,
        previously_processed_article_count=previous_processed_article_count,
        newly_processed_article_count=known_processed_article_count
        - previous_processed_article_count,
        known_processed_article_count=known_processed_article_count,
        p50_latency_ms=_percentile_ms(sorted(result.timing_ms for result in all_article_results), 0.5),
        p95_latency_ms=_percentile_ms(sorted(result.timing_ms for result in all_article_results), 0.95),
        per_source_article_counts=dict(
            sorted(Counter(result.source for result in all_article_results).items())
        ),
        per_issue_counts=dict(
            sorted(
                Counter(
                    issue_type
                    for result in all_article_results
                    for issue_type in result.issue_types
                ).items()
            )
        ),
        suggested_fix_classes=_suggested_fix_classes(
            Counter(
                issue_type
                for result in all_article_results
                for issue_type in result.issue_types
            )
        ),
        warnings=warnings,
        feed_failures=feed_failures if "feed_failures" in locals() else [],
        article_failures=all_article_failures,
        clusters=cluster_summaries,
        merged_suggestions=merged_suggestions,
        suggestion_dir=str(live_news_feedback_suggestions_dir(pack_id, root=artifact_root).resolve())
        if write_artifacts
        else None,
        merged_suggestion_path=merged_suggestion_path,
        processed_store_dir=str(live_news_processed_store_dir(pack_id, root=processed_root).resolve()),
        processed_store_path=processed_store_path,
    )
    if write_artifacts:
        run_report_destination = live_news_digestion_run_report_path(
            pack_id,
            root=artifact_root,
        )
        run_report_path = str(write_live_news_digestion_run_report(run_report_destination, run_report))
        run_report = replace(run_report, run_report_path=run_report_path)
        write_live_news_digestion_run_report(run_report_destination, run_report)
    return run_report


def _resolve_live_news_feeds(
    feeds: tuple[LiveNewsFeedSpec, ...] | None,
) -> tuple[LiveNewsFeedSpec, ...]:
    if feeds:
        return feeds
    return tuple(
        LiveNewsFeedSpec(source=source, feed_url=feed_url)
        for source, feed_url in DEFAULT_LIVE_NEWS_FEEDS
    )


def _load_live_news_feed_queues(
    client: httpx.Client,
    active_feeds: tuple[LiveNewsFeedSpec, ...],
    *,
    per_feed_limit: int,
    max_feed_item_age_days: int,
) -> tuple[int, list[LiveNewsFailure], list[tuple[LiveNewsFeedSpec, deque[LiveNewsFeedItem]]]]:
    feed_failures: list[LiveNewsFailure] = []
    feed_queues: list[tuple[LiveNewsFeedSpec, deque[LiveNewsFeedItem]]] = []
    successful_feed_count = 0

    for feed in active_feeds:
        try:
            items = _load_feed_items(client, feed, per_feed_limit=per_feed_limit)
            items = _filter_recent_feed_items(
                items,
                max_age_days=max_feed_item_age_days,
            )
        except Exception as exc:  # pragma: no cover - exercised via live runs
            feed_failures.append(
                LiveNewsFailure(
                    source=feed.source,
                    stage="feed_fetch",
                    url=feed.feed_url,
                    message=str(exc),
                )
            )
            continue
        if not items:
            feed_failures.append(
                LiveNewsFailure(
                    source=feed.source,
                    stage="feed_parse",
                    url=feed.feed_url,
                    message="no_items",
                )
            )
            continue
        successful_feed_count += 1
        feed_queues.append((feed, deque(items)))

    return successful_feed_count, feed_failures, feed_queues


def _collect_live_news_article_results(
    client: httpx.Client,
    feed_queues: list[tuple[LiveNewsFeedSpec, deque[LiveNewsFeedItem]]],
    *,
    article_limit: int,
    pack_id: str,
    storage_root: Path,
    registry: PackRegistry,
    seen_article_urls: set[str] | None = None,
) -> tuple[list[LiveNewsArticleResult], list[LiveNewsFailure]]:
    article_failures: list[LiveNewsFailure] = []
    article_results: list[LiveNewsArticleResult] = []
    seen_urls = seen_article_urls if seen_article_urls is not None else set()

    while len(article_results) < article_limit and any(queue for _, queue in feed_queues):
        progressed = False
        for _, queue in feed_queues:
            while queue and len(article_results) < article_limit:
                item = queue.popleft()
                if item.article_url in seen_urls:
                    continue
                seen_urls.add(item.article_url)
                progressed = True
                try:
                    result = _evaluate_live_news_item(
                        client,
                        item,
                        pack_id=pack_id,
                        storage_root=storage_root,
                        registry=registry,
                    )
                except Exception as exc:  # pragma: no cover - exercised via live runs
                    article_failures.append(
                        LiveNewsFailure(
                            source=item.source,
                            stage="article_fetch",
                            url=item.article_url,
                            title=item.title,
                            message=str(exc),
                        )
                    )
                    break
                if result is None:
                    article_failures.append(
                        LiveNewsFailure(
                            source=item.source,
                            stage="article_extract",
                            url=item.article_url,
                            title=item.title,
                            message="insufficient_article_text",
                        )
                    )
                    break
                article_results.append(result)
                break
        if not progressed:
            break

    return article_results, article_failures


def _build_live_news_feedback_report(
    pack_id: str,
    *,
    requested_article_count: int,
    active_feeds: tuple[LiveNewsFeedSpec, ...],
    successful_feed_count: int,
    feed_failures: list[LiveNewsFailure],
    article_failures: list[LiveNewsFailure],
    article_results: list[LiveNewsArticleResult],
    generated_at: str,
) -> LiveNewsFeedbackReport:
    warnings: list[str] = []
    if not article_results:
        warnings.append("no_live_articles_collected")

    per_source_article_counts = Counter(result.source for result in article_results)
    per_issue_counts = Counter(
        issue_type
        for result in article_results
        for issue_type in result.issue_types
    )
    latencies = sorted(result.timing_ms for result in article_results)
    return LiveNewsFeedbackReport(
        pack_id=pack_id,
        generated_at=generated_at,
        requested_article_count=requested_article_count,
        collected_article_count=len(article_results),
        feed_count=len(active_feeds),
        successful_feed_count=successful_feed_count,
        p50_latency_ms=_percentile_ms(latencies, 0.5),
        p95_latency_ms=_percentile_ms(latencies, 0.95),
        per_source_article_counts=dict(sorted(per_source_article_counts.items())),
        per_issue_counts=dict(sorted(per_issue_counts.items())),
        suggested_fix_classes=_suggested_fix_classes(per_issue_counts),
        warnings=warnings,
        feed_failures=feed_failures,
        article_failures=article_failures,
        articles=article_results,
    )


def _build_live_news_fix_suggestions(
    report: LiveNewsFeedbackReport,
    *,
    cluster_index: int | None = None,
) -> list[LiveNewsFixSuggestion]:
    grouped: dict[str, list[tuple[LiveNewsArticleResult, LiveNewsIssue]]] = {}
    for article in report.articles:
        for issue in article.issues:
            grouped.setdefault(issue.issue_type, []).append((article, issue))

    suggestions: list[LiveNewsFixSuggestion] = []
    for issue_type, records in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        fix_class = _GENERIC_FIX_CLASS_BY_ISSUE.get(issue_type, "review_live_news_extraction")
        suggestions.append(
            LiveNewsFixSuggestion(
                issue_type=issue_type,
                fix_class=fix_class,
                issue_count=len(records),
                recommendation=_issue_fix_recommendation(issue_type, fix_class),
                cluster_indexes=[cluster_index] if cluster_index is not None else [],
                sample_titles=_unique_non_empty(article.title for article, _ in records),
                sample_urls=_unique_non_empty(article.article_url for article, _ in records),
                sample_candidate_texts=_unique_non_empty(
                    issue.candidate_text for _, issue in records if issue.candidate_text
                ),
                sample_entity_texts=_unique_non_empty(
                    issue.entity_text for _, issue in records if issue.entity_text
                ),
            )
        )
    return suggestions


def _merge_live_news_fix_suggestions(
    suggestions: list[LiveNewsFixSuggestion],
) -> list[LiveNewsFixSuggestion]:
    merged: dict[tuple[str, str], dict[str, object]] = {}
    for suggestion in suggestions:
        key = (suggestion.issue_type, suggestion.fix_class)
        state = merged.setdefault(
            key,
            {
                "issue_type": suggestion.issue_type,
                "fix_class": suggestion.fix_class,
                "issue_count": 0,
                "recommendation": suggestion.recommendation,
                "cluster_indexes": [],
                "sample_titles": [],
                "sample_urls": [],
                "sample_candidate_texts": [],
                "sample_entity_texts": [],
            },
        )
        state["issue_count"] = int(state["issue_count"]) + suggestion.issue_count
        state["cluster_indexes"] = _unique_ints(
            [*state["cluster_indexes"], *suggestion.cluster_indexes],
            limit=10,
        )
        state["sample_titles"] = _unique_non_empty(
            [*state["sample_titles"], *suggestion.sample_titles],
            limit=5,
        )
        state["sample_urls"] = _unique_non_empty(
            [*state["sample_urls"], *suggestion.sample_urls],
            limit=5,
        )
        state["sample_candidate_texts"] = _unique_non_empty(
            [*state["sample_candidate_texts"], *suggestion.sample_candidate_texts],
            limit=6,
        )
        state["sample_entity_texts"] = _unique_non_empty(
            [*state["sample_entity_texts"], *suggestion.sample_entity_texts],
            limit=6,
        )

    merged_suggestions = [
        LiveNewsFixSuggestion(
            issue_type=str(state["issue_type"]),
            fix_class=str(state["fix_class"]),
            issue_count=int(state["issue_count"]),
            recommendation=str(state["recommendation"]),
            cluster_indexes=list(state["cluster_indexes"]),
            sample_titles=list(state["sample_titles"]),
            sample_urls=list(state["sample_urls"]),
            sample_candidate_texts=list(state["sample_candidate_texts"]),
            sample_entity_texts=list(state["sample_entity_texts"]),
        )
        for state in merged.values()
    ]
    return sorted(merged_suggestions, key=lambda item: (-item.issue_count, item.issue_type))


def _issue_fix_recommendation(issue_type: str, fix_class: str) -> str:
    if issue_type == "generic_single_token_entity":
        return "Tighten alias-class filtering for standalone generic capitalized tokens and count/date words."
    if issue_type == "single_token_name_fragment":
        return "Suppress one-token surname or fragment matches unless supported by nearby full-form evidence."
    if issue_type == "partial_span_candidate":
        return "Prefer longest valid spans and suppress coordinated or role-phrase fragment candidates."
    if issue_type == "missing_acronym_candidate":
        return "Retain all-caps acronyms when surrounding context supports geopolitical or organizational usage."
    if issue_type == "missing_hyphenated_prefix_candidate":
        return "Recover the leading location or entity prefix in hyphenated modifiers such as country-based."
    if issue_type == "missing_structured_organization_candidate":
        return "Retain multi-token structural organization names ending in group, council, ministry, bank, or solutions."
    if issue_type == "missing_structured_location_candidate":
        return "Retain full structural location spans such as Strait, Gulf, River, or Mount phrases."
    return f"Review extractor behavior for {fix_class}."


def _unique_non_empty(
    values: list[object] | tuple[object, ...] | set[object] | object,
    *,
    limit: int = 3,
) -> list[str]:
    if isinstance(values, (str, int)):
        iterable = [values]
    else:
        iterable = values
    results: list[str] = []
    seen: set[str] = set()
    for value in iterable:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
        if len(results) >= limit:
            break
    return results


def _unique_ints(
    values: list[int] | tuple[int, ...] | set[int],
    *,
    limit: int = 10,
) -> list[int]:
    results: list[int] = []
    seen: set[int] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        results.append(value)
        if len(results) >= limit:
            break
    return results


def _evaluate_live_news_item(
    client: httpx.Client,
    item: LiveNewsFeedItem,
    *,
    pack_id: str,
    storage_root: Path,
    registry: PackRegistry,
) -> LiveNewsArticleResult | None:
    html_text = _fetch_text(client, item.article_url)
    text_source, article_text = _extract_article_text(html_text)
    word_count = _word_count(article_text)
    if word_count < 80:
        return None
    response = _tag_article_text(
        article_text,
        pack_id=pack_id,
        storage_root=storage_root,
        registry=registry,
    )
    issues = _detect_news_feedback_issues(article_text, response, source=item.source)
    return LiveNewsArticleResult(
        source=item.source,
        title=item.title,
        article_url=item.article_url,
        published_at=item.published_at,
        text_source=text_source,
        word_count=word_count,
        entity_count=len(response.entities),
        timing_ms=response.timing_ms,
        warnings=list(response.warnings),
        issue_types=[issue.issue_type for issue in issues],
        issues=issues,
        entities=[
            LiveNewsEntity(
                text=entity.text,
                label=entity.label,
                start=entity.start,
                end=entity.end,
            )
            for entity in response.entities
        ],
    )


def _tag_article_text(
    text: str,
    *,
    pack_id: str,
    storage_root: Path,
    registry: PackRegistry,
) -> TagResponse:
    return tag_text(
        text=text,
        pack=pack_id,
        content_type="text/plain",
        storage_root=storage_root,
        registry=registry,
    )


def _load_feed_items(
    client: httpx.Client,
    feed: LiveNewsFeedSpec,
    *,
    per_feed_limit: int,
) -> list[LiveNewsFeedItem]:
    xml_text = _fetch_text(client, feed.feed_url)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError(f"invalid_rss_xml:{exc}") from exc
    items: list[LiveNewsFeedItem] = []
    for node in root.findall(".//item"):
        link = (node.findtext("link") or "").strip()
        title = _collapse_whitespace(node.findtext("title") or "")
        if not link or not title:
            continue
        items.append(
            LiveNewsFeedItem(
                source=feed.source,
                feed_url=feed.feed_url,
                title=title,
                article_url=link,
                published_at=_collapse_whitespace(node.findtext("pubDate") or "")
                or None,
            )
        )
        if len(items) >= per_feed_limit:
            break
    return items


def _fetch_text(client: httpx.Client, url: str) -> str:
    response = client.get(url)
    response.raise_for_status()
    return response.text


def _extract_article_text(html_text: str) -> tuple[str, str]:
    article_body = _extract_article_body_from_json_ld(html_text)
    if _word_count(article_body) >= 80:
        return ("json_ld", article_body)
    paragraph_text = _extract_paragraph_text(html_text)
    if _word_count(paragraph_text) >= 80:
        return ("paragraphs", paragraph_text)
    combined = article_body if _word_count(article_body) >= _word_count(paragraph_text) else paragraph_text
    return ("fallback", combined)


def _extract_article_body_from_json_ld(html_text: str) -> str:
    candidates: list[str] = []
    for payload in _iter_json_ld_payloads(html_text):
        for obj in _walk_json_ld(payload):
            if not isinstance(obj, dict):
                continue
            article_body = obj.get("articleBody")
            if isinstance(article_body, str):
                candidates.append(_collapse_whitespace(article_body))
    return max(candidates, key=_word_count, default="")


def _iter_json_ld_payloads(html_text: str) -> list[object]:
    matches = re.findall(
        r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    payloads: list[object] = []
    for raw in matches:
        text = raw.strip()
        if not text:
            continue
        try:
            payloads.append(json.loads(unescape(text)))
        except json.JSONDecodeError:
            continue
    return payloads


def _walk_json_ld(value: object) -> list[object]:
    results: list[object] = []
    queue: list[object] = [value]
    while queue:
        current = queue.pop()
        results.append(current)
        if isinstance(current, list):
            queue.extend(current)
        elif isinstance(current, dict):
            queue.extend(current.values())
    return results


def _extract_paragraph_text(html_text: str) -> str:
    stripped = re.sub(
        r"<(script|style|noscript)[^>]*>.*?</\1>",
        " ",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    candidate_regions = re.findall(
        r"<(article|main)[^>]*>(.*?)</\1>",
        stripped,
        flags=re.IGNORECASE | re.DOTALL,
    )
    regions = [content for _, content in candidate_regions]
    if not regions:
        regions = [stripped]
    best_region = max(regions, key=len)
    paragraphs = re.findall(
        r"<p\b[^>]*>(.*?)</p>",
        best_region,
        flags=re.IGNORECASE | re.DOTALL,
    )
    cleaned: list[str] = []
    seen: set[str] = set()
    for paragraph in paragraphs:
        text = _html_to_text(paragraph)
        if _word_count(text) < 5:
            continue
        normalized = normalize_lookup_text(text)
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(text)
    return "\n\n".join(cleaned)


def _html_to_text(value: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", value)
    return _collapse_whitespace(unescape(no_tags))


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _word_count(text: str) -> int:
    return len(re.findall(r"\b[\w-]+\b", text))


def _detect_news_feedback_issues(
    text: str,
    response: TagResponse,
    *,
    source: str | None = None,
) -> list[LiveNewsIssue]:
    issues: list[LiveNewsIssue] = []
    seen: set[tuple[str, str, str]] = set()
    extracted_norms = {
        normalize_lookup_text(entity.text)
        for entity in response.entities
        if entity.text.strip()
    }
    extracted_tokens = {
        normalize_lookup_text(token)
        for entity in response.entities
        for token in re.findall(r"\b[\w-]+\b", entity.text)
        if token.strip()
    }

    for entity in response.entities:
        entity_text = entity.text.strip()
        if not entity_text:
            continue
        normalized = normalize_lookup_text(entity_text)
        if _is_generic_single_token_entity(entity):
            _append_issue(
                issues,
                seen,
                LiveNewsIssue(
                    issue_type="generic_single_token_entity",
                    entity_text=entity_text,
                    label=entity.label,
                    message="single-token common-word entity survived extraction",
                ),
            )
        elif _is_single_token_name_fragment(text, entity):
            _append_issue(
                issues,
                seen,
                LiveNewsIssue(
                    issue_type="single_token_name_fragment",
                    entity_text=entity_text,
                    label=entity.label,
                    message="single-token non-acronym person/org fragment survived extraction",
                ),
            )

        partial_candidate = _find_partial_span_candidate(
            text,
            entity,
            extracted_norms,
            source=source,
        )
        if partial_candidate is not None:
            _append_issue(
                issues,
                seen,
                LiveNewsIssue(
                    issue_type="partial_span_candidate",
                    entity_text=entity_text,
                    label=entity.label,
                    candidate_text=partial_candidate,
                    message="entity appears to be a fragment of a longer structured span",
                ),
            )

    for candidate in _find_missing_acronym_candidates(
        text,
        extracted_norms,
        extracted_tokens,
        source=source,
    ):
        _append_issue(
            issues,
            seen,
            LiveNewsIssue(
                issue_type="missing_acronym_candidate",
                candidate_text=candidate,
                message="all-caps acronym appears in text but was not extracted",
            ),
        )
    for candidate in _find_missing_hyphenated_prefix_candidates(text, extracted_norms):
        _append_issue(
            issues,
            seen,
            LiveNewsIssue(
                issue_type="missing_hyphenated_prefix_candidate",
                candidate_text=candidate,
                message="hyphenated adjectival geographic prefix appears in text but was not extracted",
            ),
        )
    for candidate in _find_missing_structured_organization_candidates(text, extracted_norms):
        _append_issue(
            issues,
            seen,
            LiveNewsIssue(
                issue_type="missing_structured_organization_candidate",
                candidate_text=candidate,
                message="structured multi-token organization span appears in text but was not extracted",
            ),
        )
    for candidate in _find_missing_structured_location_candidates(text, extracted_norms):
        _append_issue(
            issues,
            seen,
            LiveNewsIssue(
                issue_type="missing_structured_location_candidate",
                candidate_text=candidate,
                message="structured multi-token location span appears in text but was not extracted",
            ),
        )
    return issues


def _append_issue(
    issues: list[LiveNewsIssue],
    seen: set[tuple[str, str, str]],
    issue: LiveNewsIssue,
) -> None:
    key = (
        issue.issue_type,
        normalize_lookup_text(issue.entity_text or ""),
        normalize_lookup_text(issue.candidate_text or ""),
    )
    if key in seen:
        return
    seen.add(key)
    issues.append(issue)


def _is_generic_single_token_entity(entity: EntityMatch) -> bool:
    if _token_count(entity.text) != 1:
        return False
    normalized = normalize_lookup_text(entity.text)
    return normalized in _GENERIC_SINGLE_TOKEN_WORDS


def _is_single_token_name_fragment(text: str, entity: EntityMatch) -> bool:
    if _token_count(entity.text) != 1:
        return False
    if entity.label not in {"organization", "person"}:
        return False
    token = entity.text.strip()
    if not token.isalpha() or token.isupper():
        return False
    normalized = normalize_lookup_text(token)
    if normalized in _GENERIC_SINGLE_TOKEN_WORDS:
        return False
    if entity.label == "person":
        return True
    if not _has_direct_titleish_neighbor(text, start=entity.start, end=entity.end):
        return False
    return _has_fragment_like_context(text, start=entity.start, end=entity.end)


def _find_partial_span_candidate(
    text: str,
    entity: EntityMatch,
    extracted_norms: set[str],
    *,
    source: str | None = None,
) -> str | None:
    entity_text = text[entity.start:entity.end]
    if _token_count(entity_text) == 1:
        left_candidate = _expand_partial_span_left(text, entity.start, entity_text)
        if left_candidate is not None:
            normalized = normalize_lookup_text(left_candidate)
            if normalized not in extracted_norms and not _is_noise_partial_span_candidate(
                left_candidate,
                source=source,
            ):
                return left_candidate
    right_candidate = _expand_partial_span_right(text, entity.start, entity.end, entity_text)
    if right_candidate is None:
        return None
    normalized = normalize_lookup_text(right_candidate)
    if normalized in extracted_norms:
        return None
    if _is_noise_partial_span_candidate(right_candidate, source=source):
        return None
    return right_candidate


def _expand_partial_span_left(text: str, start: int, entity_text: str) -> str | None:
    left_context = text[max(0, start - 48):start]
    match = re.search(
        r"([A-Z][A-Za-z-]+(?:\s+(?:of|for|the|[A-Z][A-Za-z-]+)){1,4})\s*$",
        left_context,
    )
    if match is None:
        return None
    candidate = _collapse_whitespace(f"{match.group(1)} {entity_text}")
    if _token_count(candidate) <= _token_count(entity_text):
        return None
    if candidate.casefold() == entity_text.casefold():
        return None
    if not _is_plausible_partial_span_candidate(candidate):
        return None
    return candidate


def _expand_partial_span_right(
    text: str,
    start: int,
    end: int,
    entity_text: str,
) -> str | None:
    right_context = text[end:end + 64]
    match = re.match(
        r"((?:\s+(?:of|for|the|and|[A-Z][A-Za-z-]+)){1,4})",
        right_context,
    )
    if match is None:
        return None
    suffix = _collapse_whitespace(match.group(1))
    candidate = _collapse_whitespace(f"{entity_text} {suffix}")
    if _token_count(candidate) <= _token_count(entity_text):
        return None
    if not _is_plausible_partial_span_candidate(candidate):
        return None
    return candidate


def _is_plausible_partial_span_candidate(candidate: str) -> bool:
    tokens = re.findall(r"\b[\w-]+\b", candidate)
    if len(tokens) < 2:
        return False
    first = normalize_lookup_text(tokens[0])
    last = normalize_lookup_text(tokens[-1])
    if first in _LEADING_CONTEXT_STOPWORDS:
        return False
    if not tokens[0][:1].isupper() and first not in _STRUCTURAL_LOCATION_HEADS:
        return False
    if last in _PARTIAL_SPAN_CONNECTORS:
        return False
    if first in _PARTIAL_SPAN_FRAGMENT_TOKENS or last in _PARTIAL_SPAN_FRAGMENT_TOKENS:
        return False
    if len(tokens) >= 4 and sum(token.isupper() for token in tokens if token.isalpha()) >= 3:
        return False
    if last in _PUBLICATION_SUFFIX_TOKENS and len(tokens) <= 3:
        return False
    if "@" in candidate and not re.fullmatch(r"\S+@\S+", candidate):
        return False
    lowered_tokens = [normalize_lookup_text(token) for token in tokens]
    if _looks_like_coordination_span(lowered_tokens):
        return False
    if _looks_like_role_phrase(lowered_tokens):
        return False
    return True


def _looks_like_coordination_span(tokens: list[str]) -> bool:
    if "and" not in tokens and "or" not in tokens:
        return False
    for connector in ("and", "or"):
        if connector not in tokens:
            continue
        index = tokens.index(connector)
        left = [token for token in tokens[:index] if token not in _PARTIAL_SPAN_CONNECTORS]
        right = [token for token in tokens[index + 1:] if token not in _PARTIAL_SPAN_CONNECTORS]
        if not left or not right:
            continue
        right_tail = right[-1]
        if right_tail in _STRUCTURAL_ORG_SUFFIXES or right_tail in _STRUCTURAL_LOCATION_HEADS:
            continue
        return True
    return False


def _looks_like_role_phrase(tokens: list[str]) -> bool:
    return any(token in _PARTIAL_SPAN_ROLE_TOKENS for token in tokens)


def _find_missing_acronym_candidates(
    text: str,
    extracted_norms: set[str],
    extracted_tokens: set[str],
    *,
    source: str | None = None,
) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    source_noise_tokens = _source_noise_tokens(source)
    for match in re.finditer(r"\b[A-Z]{2,6}\b", text):
        token = match.group(0)
        normalized = normalize_lookup_text(token)
        if normalized in extracted_norms or normalized in extracted_tokens or normalized in seen:
            continue
        if normalized in _ACRONYM_STOPWORDS or normalized in source_noise_tokens:
            continue
        if _is_noise_acronym_candidate(text, match.start(), match.end(), source_noise_tokens):
            continue
        seen.add(normalized)
        candidates.append(token)
    return candidates


def _find_missing_hyphenated_prefix_candidates(
    text: str,
    extracted_norms: set[str],
) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    pattern = (
        r"\b([A-Z][A-Za-z-]+)-("
        + "|".join(sorted(_HYPHENATED_GEO_SUFFIXES))
        + r")\b"
    )
    for match in re.finditer(pattern, text):
        candidate = match.group(1)
        normalized = normalize_lookup_text(candidate)
        if normalized in extracted_norms or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(candidate)
    return candidates


def _find_missing_structured_organization_candidates(
    text: str,
    extracted_norms: set[str],
) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    org_token_pattern = r"[A-Z][A-Za-z]*(?:-[A-Z][A-Za-z]*)*"
    suffix_variants: set[str] = set()
    for suffix in _STRUCTURAL_ORG_SUFFIXES:
        suffix_variants.add(re.escape(suffix))
        suffix_variants.add(re.escape(suffix.title()))
        suffix_variants.add(re.escape(suffix.upper()))
    suffix_pattern = "|".join(sorted(suffix_variants))
    pattern = (
        r"\b(("
        + org_token_pattern
        + r")(?:\s+"
        + org_token_pattern
        + r"){0,3}\s+(?:"
        + suffix_pattern
        + r"))\b"
    )
    for match in re.finditer(pattern, text):
        candidate = _collapse_whitespace(match.group(1))
        normalized = normalize_lookup_text(candidate)
        if not _is_plausible_structured_organization_candidate(candidate):
            continue
        if normalized in extracted_norms or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(candidate)
    return candidates


def _is_plausible_structured_organization_candidate(candidate: str) -> bool:
    tokens = re.findall(r"\b[\w-]+\b", candidate)
    if len(tokens) < 2:
        return False
    normalized = normalize_lookup_text(candidate)
    if normalized in _GENERIC_STRUCTURED_ORG_CANDIDATES:
        return False
    first = normalize_lookup_text(tokens[0])
    last = normalize_lookup_text(tokens[-1])
    if first in _LEADING_CONTEXT_STOPWORDS:
        return False
    if len(tokens) == 2 and _looks_like_adjectival_modifier(tokens[0]):
        return False
    if last in _WEAK_STRUCTURAL_ORG_SUFFIXES and len(tokens) <= 2:
        return False
    return True


def _find_missing_structured_location_candidates(
    text: str,
    extracted_norms: set[str],
) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    location_head_pattern = "|".join(sorted(_STRUCTURAL_LOCATION_HEADS))
    pattern = (
        r"\b((?:"
        + location_head_pattern
        + r")\s+of\s+[A-Z][A-Za-z-]+(?:\s+[A-Z][A-Za-z-]+){0,2})\b"
    )
    for match in re.finditer(pattern, text):
        candidate = _collapse_whitespace(match.group(1))
        normalized = normalize_lookup_text(candidate)
        tokens = re.findall(r"\b[\w-]+\b", candidate)
        if tokens and _looks_like_adjectival_modifier(tokens[-1]):
            continue
        if normalized in extracted_norms or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(candidate)
    return candidates


def _token_count(value: str) -> int:
    return len(re.findall(r"\b[\w-]+\b", value))


def _has_fragment_like_context(text: str, *, start: int, end: int) -> bool:
    previous_token = _previous_token(text, start)
    next_token = _next_token(text, end)
    previous_lower = normalize_lookup_text(previous_token)
    next_lower = normalize_lookup_text(next_token)
    if _is_titleish_token(previous_token) or _is_titleish_token(next_token):
        return True
    if previous_lower in _PARTIAL_SPAN_CONNECTORS and _has_capitalized_token_before(text, start):
        return True
    if next_lower in _PARTIAL_SPAN_ROLE_TOKENS:
        return True
    return False


def _has_direct_titleish_neighbor(text: str, *, start: int, end: int) -> bool:
    return _is_titleish_token(_previous_token(text, start)) or _is_titleish_token(_next_token(text, end))


def _is_noise_partial_span_candidate(candidate: str, *, source: str | None = None) -> bool:
    tokens = re.findall(r"\b[\w-]+\b", candidate)
    if not tokens:
        return True
    normalized_tokens = [normalize_lookup_text(token) for token in tokens]
    if normalized_tokens[-1] in _PUBLICATION_SUFFIX_TOKENS and len(tokens) <= 3:
        return True
    source_noise_tokens = _source_noise_tokens(source)
    return bool(source_noise_tokens) and normalized_tokens[0] in source_noise_tokens and normalized_tokens[-1] in {
        "app",
        "news",
    }


def _source_noise_tokens(source: str | None) -> set[str]:
    if not source:
        return set()
    normalized = normalize_lookup_text(source)
    tokens = {normalized}
    if normalized.endswith("news") and len(normalized) > len("news"):
        tokens.add(normalized[: -len("news")])
    return {token for token in tokens if token}


def _is_noise_acronym_candidate(
    text: str,
    start: int,
    end: int,
    source_noise_tokens: set[str],
) -> bool:
    previous_lower = normalize_lookup_text(_previous_token(text, start))
    next_lower = normalize_lookup_text(_next_token(text, end))
    if previous_lower in _BOILERPLATE_CONTEXT_TOKENS or next_lower in _BOILERPLATE_CONTEXT_TOKENS:
        return True
    if previous_lower in source_noise_tokens or next_lower in source_noise_tokens:
        return True
    return False


def _looks_like_adjectival_modifier(token: str) -> bool:
    normalized = normalize_lookup_text(token)
    if len(normalized) <= 3:
        return False
    return normalized.endswith(("ese", "ian", "ish", "ist")) or (
        normalized.endswith("i") and len(normalized) > 4
    )


def _has_capitalized_token_before(text: str, start: int) -> bool:
    previous_token = _previous_token(text, start)
    if not previous_token:
        return False
    previous_start = max(0, start - len(previous_token) - 4)
    earlier_token = _previous_token(text, previous_start)
    return _is_titleish_token(previous_token) or _is_titleish_token(earlier_token)


def _previous_token(text: str, start: int) -> str:
    match = re.search(r"([\w][\w.+&/-]*)\W*$", text[:start])
    return match.group(1) if match is not None else ""


def _next_token(text: str, end: int) -> str:
    match = re.match(r"\W*([\w][\w.+&/-]*)", text[end:])
    return match.group(1) if match is not None else ""


def _is_titleish_token(token: str) -> bool:
    compact = "".join(character for character in token if character.isalpha())
    return bool(compact) and (compact.isupper() or compact[:1].isupper())


def _filter_recent_feed_items(
    items: list[LiveNewsFeedItem],
    *,
    max_age_days: int,
    now: datetime | None = None,
) -> list[LiveNewsFeedItem]:
    if max_age_days <= 0:
        return items
    reference_time = now or datetime.now(timezone.utc)
    cutoff = reference_time - timedelta(days=max_age_days)
    dated_items: list[tuple[LiveNewsFeedItem, datetime]] = []
    undated_items: list[LiveNewsFeedItem] = []
    for item in items:
        published_at = _parse_feed_item_datetime(item.published_at)
        if published_at is None:
            undated_items.append(item)
            continue
        dated_items.append((item, published_at))
    if dated_items and max(timestamp for _, timestamp in dated_items) < cutoff:
        raise ValueError(f"stale_feed_items:max_age_days={max_age_days}")
    recent_items = [item for item, timestamp in dated_items if timestamp >= cutoff]
    return recent_items or undated_items


def _parse_feed_item_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _percentile_ms(values: list[int], q: float) -> int:
    if not values:
        return 0
    if len(values) == 1:
        return values[0]
    index = int(round((len(values) - 1) * q))
    index = min(max(index, 0), len(values) - 1)
    return values[index]


def _suggested_fix_classes(per_issue_counts: Counter[str]) -> list[str]:
    suggestions = [
        fix_class
        for issue_type, _count in per_issue_counts.most_common()
        if (fix_class := _GENERIC_FIX_CLASS_BY_ISSUE.get(issue_type)) is not None
    ]
    deduped: list[str] = []
    for item in suggestions:
        if item not in deduped:
            deduped.append(item)
    return deduped
