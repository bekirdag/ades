#!/usr/bin/env python3
"""Probe candidate newspaper homepages for healthy RSS feeds and article fetches."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from typing import Iterable
from urllib.parse import urljoin, urlparse

import httpx

from ades.news_feedback import (
    DEFAULT_NEWS_FEEDBACK_ROOT,
    DEFAULT_NEWS_HTTP_HEADERS,
    LiveNewsFeedItem,
    LiveNewsFeedSpec,
    _extract_article_text,
    _fetch_text,
    _load_feed_items,
    _word_count,
)


AUDIT_OUTPUT_PATH = (
    DEFAULT_NEWS_FEEDBACK_ROOT / "audits" / "live-news-feed-candidates" / "newspaper-feed-audit.json"
)
COMMON_FEED_SUFFIXES: tuple[str, ...] = (
    "/rss.xml",
    "/rss",
    "/feed",
    "/feeds/rss.xml",
)
MAX_DISCOVERED_FEEDS = 6
MAX_WORKERS = 20
HOME_TIMEOUT_S = 6.0
FEED_TIMEOUT_S = 5.0
ARTICLE_TIMEOUT_S = 6.0
MIN_ARTICLE_WORDS = 80
PER_FEED_ITEM_LIMIT = 5


@dataclass(frozen=True)
class FeedCandidate:
    slug: str
    name: str
    homepage_url: str
    feed_hints: tuple[str, ...] = ()


@dataclass(frozen=True)
class FeedAuditResult:
    slug: str
    name: str
    homepage_url: str
    healthy: bool
    feed_url: str | None = None
    article_url: str | None = None
    text_source: str | None = None
    article_word_count: int = 0
    feed_item_count: int = 0
    error: str | None = None
    discovered_feed_urls: list[str] = field(default_factory=list)


class AlternateFeedParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.feed_urls: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "link":
            return
        attr_map = {key.lower(): value or "" for key, value in attrs}
        rel = attr_map.get("rel", "").lower()
        type_value = attr_map.get("type", "").lower()
        href = attr_map.get("href", "")
        if "alternate" not in rel or not href:
            return
        if type_value in {
            "application/rss+xml",
            "application/atom+xml",
            "application/xml",
            "text/xml",
        }:
            self.feed_urls.append(href)


def _candidate_sources() -> tuple[FeedCandidate, ...]:
    return (
        FeedCandidate("bbc", "BBC News", "https://www.bbc.com/", ("https://feeds.bbci.co.uk/news/rss.xml",)),
        FeedCandidate("cnn", "CNN", "https://www.cnn.com/", ("http://rss.cnn.com/rss/edition.rss",)),
        FeedCandidate("euronews", "Euronews", "https://www.euronews.com/", ("https://www.euronews.com/rss?level=theme&name=news",)),
        FeedCandidate("guardian", "The Guardian", "https://www.theguardian.com/", ("https://www.theguardian.com/world/rss",)),
        FeedCandidate("abcnews", "ABC News", "https://abcnews.go.com/", ("https://abcnews.go.com/abcnews/topstories",)),
        FeedCandidate("cbsnews", "CBS News", "https://www.cbsnews.com/", ("https://www.cbsnews.com/latest/rss/main",)),
        FeedCandidate("foxnews", "Fox News", "https://www.foxnews.com/", ("https://moxie.foxnews.com/google-publisher/world.xml",)),
        FeedCandidate("nbcnews", "NBC News", "https://www.nbcnews.com/", ("https://feeds.nbcnews.com/nbcnews/public/news",)),
        FeedCandidate("npr", "NPR", "https://www.npr.org/", ("https://feeds.npr.org/1001/rss.xml", "https://feeds.npr.org/1004/rss.xml")),
        FeedCandidate("nytimes", "The New York Times", "https://www.nytimes.com/", ("https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",)),
        FeedCandidate("washpost", "The Washington Post", "https://www.washingtonpost.com/", ("https://feeds.washingtonpost.com/rss/world",)),
        FeedCandidate("latimes", "Los Angeles Times", "https://www.latimes.com/"),
        FeedCandidate("usatoday", "USA Today", "https://www.usatoday.com/"),
        FeedCandidate("bostonglobe", "The Boston Globe", "https://www.bostonglobe.com/"),
        FeedCandidate("wsj", "The Wall Street Journal", "https://www.wsj.com/"),
        FeedCandidate("ft", "Financial Times", "https://www.ft.com/"),
        FeedCandidate("telegraph", "The Telegraph", "https://www.telegraph.co.uk/", ("https://www.telegraph.co.uk/rss.xml",)),
        FeedCandidate("independent", "The Independent", "https://www.independent.co.uk/"),
        FeedCandidate("politico", "Politico", "https://www.politico.com/", ("https://www.politico.com/rss/politicopicks.xml",)),
        FeedCandidate("thehill", "The Hill", "https://thehill.com/", ("https://thehill.com/feed/",)),
        FeedCandidate("csmonitor", "The Christian Science Monitor", "https://www.csmonitor.com/", ("https://www.csmonitor.com/feeds/all",)),
        FeedCandidate("upi", "UPI", "https://www.upi.com/", ("https://www.upi.com/Top_News/rss/",)),
        FeedCandidate("newsweek", "Newsweek", "https://www.newsweek.com/"),
        FeedCandidate("irish_times", "The Irish Times", "https://www.irishtimes.com/", ("https://www.irishtimes.com/arc/outboundfeeds/rss/",)),
        FeedCandidate("irish_independent", "Irish Independent", "https://www.independent.ie/"),
        FeedCandidate("evening_standard", "The Standard", "https://www.standard.co.uk/"),
        FeedCandidate("metro_uk", "Metro UK", "https://metro.co.uk/"),
        FeedCandidate("daily_mirror", "Daily Mirror", "https://www.mirror.co.uk/"),
        FeedCandidate("daily_mail", "Daily Mail", "https://www.dailymail.co.uk/"),
        FeedCandidate("daily_express", "Daily Express", "https://www.express.co.uk/"),
        FeedCandidate("the_sun", "The Sun", "https://www.thesun.co.uk/"),
        FeedCandidate("daily_record", "Daily Record", "https://www.dailyrecord.co.uk/"),
        FeedCandidate("the_courier", "The Courier", "https://www.thecourier.co.uk/"),
        FeedCandidate("herald_scotland", "The Herald", "https://www.heraldscotland.com/"),
        FeedCandidate("the_times", "The Times", "https://www.thetimes.com/uk"),
        FeedCandidate("inews", "iNews", "https://inews.co.uk/"),
        FeedCandidate("cityam", "City A.M.", "https://www.cityam.com/"),
        FeedCandidate("press_and_journal", "Press and Journal", "https://www.pressandjournal.co.uk/"),
        FeedCandidate("belfast_telegraph", "Belfast Telegraph", "https://www.belfasttelegraph.co.uk/"),
        FeedCandidate("newsletter", "The News Letter", "https://www.newsletter.co.uk/"),
        FeedCandidate("scotsman", "The Scotsman", "https://www.scotsman.com/"),
        FeedCandidate("smh", "Sydney Morning Herald", "https://www.smh.com.au/"),
        FeedCandidate("the_age", "The Age", "https://www.theage.com.au/"),
        FeedCandidate("the_australian", "The Australian", "https://www.theaustralian.com.au/"),
        FeedCandidate("afr", "Australian Financial Review", "https://www.afr.com/"),
        FeedCandidate("canberra_times", "The Canberra Times", "https://www.canberratimes.com.au/"),
        FeedCandidate("brisbane_times", "Brisbane Times", "https://www.brisbanetimes.com.au/"),
        FeedCandidate("watoday", "WAtoday", "https://www.watoday.com.au/"),
        FeedCandidate("west_australian", "The West Australian", "https://thewest.com.au/"),
        FeedCandidate("adelaide_now", "Adelaide Now", "https://www.adelaidenow.com.au/"),
        FeedCandidate("herald_sun", "Herald Sun", "https://www.heraldsun.com.au/"),
        FeedCandidate("daily_telegraph_au", "The Daily Telegraph", "https://www.dailytelegraph.com.au/"),
        FeedCandidate("courier_mail", "The Courier-Mail", "https://www.couriermail.com.au/"),
        FeedCandidate("nz_herald", "NZ Herald", "https://www.nzherald.co.nz/"),
        FeedCandidate("stuff", "Stuff", "https://www.stuff.co.nz/"),
        FeedCandidate("otago_daily_times", "Otago Daily Times", "https://www.odt.co.nz/"),
        FeedCandidate("globe_and_mail", "The Globe and Mail", "https://www.theglobeandmail.com/"),
        FeedCandidate("toronto_star", "Toronto Star", "https://www.thestar.com/"),
        FeedCandidate("national_post", "National Post", "https://nationalpost.com/"),
        FeedCandidate("vancouver_sun", "Vancouver Sun", "https://vancouversun.com/"),
        FeedCandidate("the_province", "The Province", "https://theprovince.com/"),
        FeedCandidate("montreal_gazette", "Montreal Gazette", "https://montrealgazette.com/"),
        FeedCandidate("calgary_herald", "Calgary Herald", "https://calgaryherald.com/"),
        FeedCandidate("edmonton_journal", "Edmonton Journal", "https://edmontonjournal.com/"),
        FeedCandidate("ottawa_citizen", "Ottawa Citizen", "https://ottawacitizen.com/"),
        FeedCandidate("winnipeg_free_press", "Winnipeg Free Press", "https://www.winnipegfreepress.com/"),
        FeedCandidate("hindustan_times", "Hindustan Times", "https://www.hindustantimes.com/"),
        FeedCandidate("the_hindu", "The Hindu", "https://www.thehindu.com/"),
        FeedCandidate("indian_express", "The Indian Express", "https://indianexpress.com/"),
        FeedCandidate("times_of_india", "The Times of India", "https://timesofindia.indiatimes.com/"),
        FeedCandidate("deccan_herald", "Deccan Herald", "https://www.deccanherald.com/"),
        FeedCandidate("telegraph_india", "The Telegraph India", "https://www.telegraphindia.com/"),
        FeedCandidate("japan_times", "The Japan Times", "https://www.japantimes.co.jp/"),
        FeedCandidate("scmp", "South China Morning Post", "https://www.scmp.com/"),
        FeedCandidate("straits_times", "The Straits Times", "https://www.straitstimes.com/"),
        FeedCandidate("philippine_daily_inquirer", "Philippine Daily Inquirer", "https://newsinfo.inquirer.net/"),
        FeedCandidate("manila_times", "The Manila Times", "https://www.manilatimes.net/"),
        FeedCandidate("bangkok_post", "Bangkok Post", "https://www.bangkokpost.com/"),
        FeedCandidate("jakarta_post", "The Jakarta Post", "https://www.thejakartapost.com/"),
        FeedCandidate("taipei_times", "Taipei Times", "https://www.taipeitimes.com/"),
        FeedCandidate("korea_times", "The Korea Times", "https://www.koreatimes.co.kr/"),
        FeedCandidate("jerusalem_post", "The Jerusalem Post", "https://www.jpost.com/"),
        FeedCandidate("haaretz", "Haaretz", "https://www.haaretz.com/"),
        FeedCandidate("arab_news", "Arab News", "https://www.arabnews.com/"),
        FeedCandidate("the_national", "The National", "https://www.thenationalnews.com/"),
        FeedCandidate("khaleej_times", "Khaleej Times", "https://www.khaleejtimes.com/"),
        FeedCandidate("gulf_news", "Gulf News", "https://gulfnews.com/"),
        FeedCandidate("daily_sabah", "Daily Sabah", "https://www.dailysabah.com/"),
        FeedCandidate("mail_guardian", "Mail & Guardian", "https://mg.co.za/"),
        FeedCandidate("daily_maverick", "Daily Maverick", "https://www.dailymaverick.co.za/"),
        FeedCandidate("business_day_sa", "Business Day", "https://www.businesslive.co.za/bd/"),
        FeedCandidate("daily_nation", "Daily Nation", "https://nation.africa/kenya"),
        FeedCandidate("east_african", "The EastAfrican", "https://www.theeastafrican.co.ke/"),
        FeedCandidate("punch", "The Punch", "https://punchng.com/"),
        FeedCandidate("guardian_ng", "The Guardian Nigeria", "https://guardian.ng/"),
        FeedCandidate("daily_trust", "Daily Trust", "https://dailytrust.com/"),
        FeedCandidate("france24", "France 24", "https://www.france24.com/en/", ("https://www.france24.com/en/rss",)),
        FeedCandidate("dw", "Deutsche Welle", "https://www.dw.com/en/top-stories/s-9097", ("https://rss.dw.com/xml/rss-en-top",)),
        FeedCandidate("cbc", "CBC News", "https://www.cbc.ca/news", ("https://www.cbc.ca/webfeed/rss/rss-topstories",)),
        FeedCandidate("skynews", "Sky News", "https://news.sky.com/", ("https://feeds.skynews.com/feeds/rss/home.xml",)),
    )


def _discover_feed_urls(client: httpx.Client, candidate: FeedCandidate) -> list[str]:
    discovered: list[str] = list(candidate.feed_hints)
    try:
        response = client.get(candidate.homepage_url, timeout=HOME_TIMEOUT_S)
        response.raise_for_status()
        parser = AlternateFeedParser()
        parser.feed(feed_response_text(response.text))
        discovered.extend(urljoin(candidate.homepage_url, href) for href in parser.feed_urls)
    except Exception:
        pass

    base = f"{urlparse(candidate.homepage_url).scheme}://{urlparse(candidate.homepage_url).netloc}"
    discovered.extend(urljoin(base, suffix) for suffix in COMMON_FEED_SUFFIXES)

    unique_urls: list[str] = []
    seen: set[str] = set()
    for url in discovered:
        normalized = url.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_urls.append(normalized)
        if len(unique_urls) >= MAX_DISCOVERED_FEEDS:
            break
    return unique_urls


def feed_response_text(text: str) -> str:
    return re.sub(r"<!--.*?-->", " ", text, flags=re.DOTALL)


def _probe_candidate(candidate: FeedCandidate) -> FeedAuditResult:
    transport = httpx.HTTPTransport(retries=1)
    with httpx.Client(
        follow_redirects=True,
        timeout=FEED_TIMEOUT_S,
        headers=DEFAULT_NEWS_HTTP_HEADERS,
        transport=transport,
    ) as client:
        discovered_feed_urls = _discover_feed_urls(client, candidate)
        if not discovered_feed_urls:
            return FeedAuditResult(
                slug=candidate.slug,
                name=candidate.name,
                homepage_url=candidate.homepage_url,
                healthy=False,
                error="no_feed_candidates",
            )

        for feed_url in discovered_feed_urls:
            try:
                items = _load_feed_items(
                    client,
                    LiveNewsFeedSpec(source=candidate.slug, feed_url=feed_url),
                    per_feed_limit=PER_FEED_ITEM_LIMIT,
                )
            except Exception as exc:
                last_error = f"feed_error:{exc}"
                continue
            if not items:
                last_error = "no_feed_items"
                continue
            for item in items:
                try:
                    article_response = client.get(item.article_url, timeout=ARTICLE_TIMEOUT_S)
                    article_response.raise_for_status()
                    html_text = article_response.text
                    text_source, article_text = _extract_article_text(html_text)
                    article_word_count = _word_count(article_text)
                except Exception as exc:
                    last_error = f"article_error:{exc}"
                    continue
                if article_word_count >= MIN_ARTICLE_WORDS:
                    return FeedAuditResult(
                        slug=candidate.slug,
                        name=candidate.name,
                        homepage_url=candidate.homepage_url,
                        healthy=True,
                        feed_url=feed_url,
                        article_url=item.article_url,
                        text_source=text_source,
                        article_word_count=article_word_count,
                        feed_item_count=len(items),
                        discovered_feed_urls=discovered_feed_urls,
                    )
                last_error = f"insufficient_article_text:{article_word_count}"

        return FeedAuditResult(
            slug=candidate.slug,
            name=candidate.name,
            homepage_url=candidate.homepage_url,
            healthy=False,
            error=last_error,
            discovered_feed_urls=discovered_feed_urls,
        )


def _audit_candidates(candidates: Iterable[FeedCandidate]) -> list[FeedAuditResult]:
    results: list[FeedAuditResult] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(_probe_candidate, candidate): candidate for candidate in candidates}
        for future in as_completed(future_map):
            results.append(future.result())
    return sorted(results, key=lambda item: item.slug)


def main() -> int:
    candidates = _candidate_sources()
    results = _audit_candidates(candidates)
    healthy = [item for item in results if item.healthy]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidate_count": len(candidates),
        "healthy_count": len(healthy),
        "healthy_sources": [
            {"slug": item.slug, "name": item.name, "feed_url": item.feed_url}
            for item in healthy
        ],
        "results": [asdict(item) for item in results],
    }
    AUDIT_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"candidate_count={len(candidates)}")
    print(f"healthy_count={len(healthy)}")
    print(f"audit_path={AUDIT_OUTPUT_PATH}")
    for item in healthy:
        print(f"healthy {item.slug} {item.feed_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
