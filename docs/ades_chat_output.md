# ades Architecture Research Notes

Good. I have enough research now. Let me build the full architecture document.Now let me build the architecture diagram and write up the full spec.Click any box to drill into a stage. Here's the full architecture with concrete open source tool assignments, going beyond what ChatGPT suggested:

---

## Stage-by-stage open source tool map

**Stage 1 — Ingestion & normalization**

The most underrated stage. ChatGPT suggested generic "boilerplate removal" without naming a winner.

Use **Trafilatura** (`pip install trafilatura`). It is the leading open-source content extraction library by F1 score across multiple independent benchmarks, used by HuggingFace, IBM, and Microsoft Research. It handles HTML→clean text, metadata (title, author, date), and outputs JSON or XML natively. For PDFs, add **pymupdf** (`fitz`). For language detection, **langdetect** or **lingua-py**. For Unicode normalization, **ftfy**.

**Stage 2 — NLP base layer**

Use **spaCy** (en_core_web_trf or a smaller model). You need tokenization, sentence segmentation, POS, and dependency parse — spaCy gives all of this in one pipeline call. For coreference resolution (optional, high value), add **coreferee** (spaCy-native) or **fastcoref**.

**Stage 3 — Named entity recognition**

This is where the ChatGPT recommendation is outdated. spaCy's built-in NER is limited to fixed types.

Use **GLiNER** instead. GLiNER is a zero-shot NER framework using a bidirectional transformer encoder that can identify any entity type — person, company, ISIN, financial instrument, regulatory body, anything — in a single forward pass. It outperforms ChatGPT and fine-tuned LLMs on zero-shot NER benchmarks while running efficiently on CPU and consumer hardware. The multitask variant (`knowledgator/gliner-multitask-large-v0.5`) also does relation extraction in the same call. For news specifically, `EmergentMethods/gliner_medium_news-v2.1` is fine-tuned for that domain.

For deterministic pattern extraction (ISINs, tickers, currency amounts, URLs, phones), combine spaCy `EntityRuler` with regex patterns. For large gazetteer matching (100k+ entity names), use **FlashText** — it's ~4000x faster than regex at that scale.

**Stage 4 — Topic classification**

Two options depending on whether you have any labeled examples:

- Zero labels: `facebook/bart-large-mnli` via the HuggingFace zero-shot classification pipeline — pass candidate labels like `["mergers & acquisitions", "earnings", "central bank policy"]` and get scores back. No training required.
- A handful of labels (8+ per class): **SetFit** (`pip install setfit`). SetFit achieves competitive accuracy with only 8 labeled examples per class, is an order of magnitude faster to train than standard fine-tuning, and supports multilingual classification via any Sentence Transformer. SetFit is also 5x faster at inference than the NLI pipeline approach.

For IPTC taxonomy tagging specifically, look at `valurank/distilroberta-base-topic-sentence` on HuggingFace.

**Stage 5 — Candidate generation (for entity linking)**

Before you can link "Alpha" to Org Alpha Holdings, you need to generate candidates. The architecture:

1. Build an alias table from a Wikidata dump (map surface forms → entity IDs)
2. Index entities in OpenSearch with BM25 for fuzzy lookup
3. Optionally add ANN (approximate nearest neighbor) retrieval over entity description embeddings using **FAISS** or **Qdrant**

For the Wikidata dump, the **wikid** spaCy project automates download and preprocessing into SQLite.

**Stage 6 — Entity linking & disambiguation**

Three viable open source choices, roughly in order of quality vs. complexity:

- **REL** (Radboud Entity Linker): Wikipedia/Wikidata-based, REST-API deployable, context-aware — the best quality for self-hosting
- **OpenTapioca**: a simple, fast Named Entity Linking system for Wikidata that can be run locally with pre-trained models and stays synchronized with Wikidata in real time. Best if you want near-zero maintenance on KB freshness.
- **spaCy EntityLinker**: integrates natively into the spaCy pipeline, requires building a KnowledgeBase from the Wikidata dump, gives most control for custom domains

For collective disambiguation (the trick where "Alpha" in the same doc as "Person Alpha" and "Device Alpha" biases toward Org Alpha Holdings), you need either a coherence model or a simple graph-based voting pass over all entity candidates in the document.

**Stage 7 — Relation & event extraction**

ChatGPT's suggestion here is architecturally correct but tool-light. Concretely:

- **spaCy dependency patterns** (`DependencyMatcher`) for high-precision rules: "PERSON appointed to ROLE at ORG", "ORG acquired ORG"
- **GLiNER multitask** or **GraphER** for joint entity + relation extraction in one model pass
- **REBEL** (Babelscape/rebel-large): a seq2seq model that reads a sentence and outputs `<subject, relation, object>` triples — strong for news text, covers ~220 relation types from Wikidata

**Stage 8 — Relevance & confidence scoring**

No dedicated library needed here — this is custom logic, but it's simple. Confidence comes from model scores. Relevance is a weighted sum of: title presence (weight 3x), first-paragraph presence (2x), frequency normalized by document length, and whether the entity is the subject of an extracted relation.

**Stage 9 — Storage & search**

- **Postgres** via SQLAlchemy: documents, enrichment jobs, entity records, feedback
- **OpenSearch** (self-hosted) or Elasticsearch: full-text + faceted search over enriched content
- **Redis** + **Celery** or **Dramatiq**: async job queue for batch tagging
- Optional: **Qdrant** for vector search over entity/document embeddings

---

## V1 vs V2 decomposition (revised)

**V1 — build in this order, in this stack:**

```
Trafilatura → spaCy → GLiNER → alias-table candidate lookup 
→ OpenTapioca or spaCy EL → bart-large-mnli topic classifier 
→ basic relevance scorer → FastAPI → Postgres + OpenSearch
```

This gives you a working tagging endpoint in a few weeks. The entity linking quality will be Wikipedia-level — solid for news/business content.

**V2 — add after V1 is stable:**

- Replace OpenTapioca with REL for better disambiguation
- Add REBEL or GLiNER multitask for relation extraction
- Add SetFit classifiers for domain-specific taxonomy (financial, legal, etc.)
- Add collective disambiguation pass
- Add feedback loop (human corrections → retrain alias priors)
- Add multilingual spaCy + multilingual GLiNER models

---

## Where ChatGPT's architecture is correct but worth sharpening

The GPT answer is solid on overall structure. The places it's weakest:

1. It recommends spaCy NER as the primary NER model — GLiNER is significantly better for open-type extraction without retraining.
2. It doesn't mention Trafilatura by name for ingestion — this matters because the extraction quality directly determines downstream tag quality.
3. It treats entity linking as one step with "bi-encoder + cross-encoder" — REL and OpenTapioca give you this out-of-the-box, no need to build it from scratch.
4. SetFit isn't mentioned for topic classification — it's materially faster than the NLI pipeline at inference time, which matters for a high-throughput tagging service.
5. The "hardest parts" list is accurate. Disambiguation, KB maintenance, and taxonomy design are where most of the engineering time will go regardless of which tools you pick.

This is a markdown file request, not a docx. Let me write the full SDS directly.`SDS-ADES-001 v1.0` is ready. Here's what's covered across the 14 sections:

**Sections 1–3** — Introduction, system overview, full pipeline architecture diagram in ASCII with the tech stack table and a feature comparison against OpenCalais.

**Section 4** — The most detailed section: each of the 10 pipeline stages has its own subsection covering responsibility, tool selection with working code snippets, failure modes, and schema for its inputs/outputs.

**Section 5** — Full relational data model with column-level definitions for all 8 tables (DOCUMENT, MENTION, ENTITY, ENTITY_ALIAS, LINKED_ENTITY, TOPIC_TAG, RELATION, FEEDBACK, ENRICHMENT_JOB) plus key index definitions.

**Section 6** — API contract: full JSON examples for both the success response and error response, plus the complete error code table.

**Section 7** — Knowledge base design: Wikidata seed process, alias table construction SQL, and the update schedule for each KB maintenance operation.

**Section 8** — Service decomposition: all 10 named services with their responsibilities, plus the Celery chain showing how they wire together, and the v1 single-host deployment layout.

**Section 9** — Storage architecture: Postgres indexes, OpenSearch mapping (nested entities and topics), Redis key patterns with TTLs.

**Section 10** — Feedback and training loop: all five feedback action types and what each triggers in the system.

**Section 11** — Open source tool registry: 22 tools with version, license, stage, and purpose. Includes the REBEL license warning.

**Section 12** — Build phases: V1 component-by-component breakdown with estimated 6–8 week delivery, V2 upgrade table.

**Sections 13–14** — Non-functional requirements (performance, accuracy baselines, reliability, security, observability) and 8 open issues that need decisions before development starts.

Honest take: **yes, Python is the better choice for this specific system.** Here's why the question isn't even close:

---

### The core problem with TypeScript here

Every meaningful component in your pipeline — GLiNER, spaCy, SetFit, REBEL, sentence-transformers, FAISS, REL, OpenTapioca — is a Python library. There is no JavaScript equivalent for any of them. Not a thin wrapper, not a port, not an approximation. They simply don't exist in the Node.js ecosystem.

So with TypeScript you're forced into the sidecar architecture I started writing in v1.1: every NLP call crosses an HTTP boundary, every model runs in a separate process, and your "TypeScript application" is essentially an expensive proxy layer around Python services. You get the operational complexity of a microservices architecture without any of the benefits.

With Python, you call GLiNER in-process:

```python
entities = model.predict_entities(text, labels=ENTITY_LABELS)
```

With TypeScript, that same call is:

```typescript
const response = await fetch("http://localhost:8001/ner", {
  method: "POST",
  body: JSON.stringify({ text, labels: ENTITY_LABELS })
});
const entities = await response.json();
```

Every stage adds a serialization round-trip, a network hop (even on localhost), error handling across a process boundary, and a service to keep alive, monitor, and deploy. For a pipeline with 6–8 NLP stages, that compounds badly.

---

### Where TypeScript genuinely wins

TypeScript is the right choice when you need:
- A high-concurrency API serving many lightweight requests (e.g. a news feed, a search frontend)
- A frontend or full-stack application
- A system where the ecosystem (npm) has mature libraries for your domain

None of those apply to an NLP enrichment pipeline.

### Where Python is unambiguously better

- **The entire ML/NLP ecosystem lives here.** HuggingFace, spaCy, GLiNER, FAISS, sentence-transformers — all Python-native, all actively maintained, all with zero-friction integration.
- **FastAPI is production-grade.** It's async, fast, has automatic OpenAPI docs, Pydantic validation, and is used at scale by many companies. It is not meaningfully inferior to Fastify or Hono for this use case.
- **Celery + BullMQ are comparable.** BullMQ (Node) is excellent, but Celery with Redis is equally solid and is what the Python ecosystem expects.
- **The data science feedback loop** — retraining SetFit models, evaluating NER accuracy, building alias priors — all happens in Python notebooks and scripts. Having your application layer in the same language means no translation layer.

---

### Recommendation

Stick with Python across the board — the v1.0 SDS stack is the right call:

| Layer | Python choice |
|---|---|
| API | FastAPI + Pydantic v2 |
| Async tasks | Celery + Redis |
| ORM | SQLAlchemy 2.x (async) |
| Migrations | Alembic |
| NER | GLiNER (in-process) |
| NLP | spaCy (in-process) |
| Topics | SetFit / bart-large-mnli (in-process) |
| Relations | REBEL + GLiNER multitask (in-process) |
| Storage | Postgres + OpenSearch + Redis |

The only scenario where TypeScript makes sense here is if you're building a separate **consumer-facing web application** on top of this pipeline — a dashboard, a search UI, an annotation tool. That layer should absolutely be TypeScript/React. But the enrichment pipeline itself should be Python.

Should I revert the SDS back to the v1.0 Python stack and close that as the confirmed architecture?

Good question, and the answer varies a lot depending on which components you activate. Let me break it down by category.

---

## NLP Models

| Model | Size | Notes |
|---|---|---|
| GLiNER medium news | ~0.5 GB | DeBERTa-based, main NER model |
| GLiNER multitask large | ~1.5 GB | If used for relation extraction |
| spaCy en_core_web_trf | ~0.4 GB | Transformer pipeline |
| facebook/bart-large-mnli | ~1.6 GB | Zero-shot topic classifier |
| SetFit (all-MiniLM-L6-v2) | ~0.09 GB | Sentence transformer base |
| REBEL large | ~1.4 GB | Relation extraction |
| sentence-transformers bi-encoder | ~0.4 GB | For candidate generation embeddings |
| **Model subtotal** | **~6 GB** | All models loaded at once |

These are one-time downloads cached by HuggingFace. In production you'd bake them into Docker images or mount a shared model volume.

---

## Knowledge Base

This is where disk gets serious.

| Asset | Size | Notes |
|---|---|---|
| Wikidata full JSON dump (compressed) | ~100 GB | Raw download, `.json.gz` |
| Wikidata dump (decompressed for processing) | ~1.2 TB | Temporary, only needed during import |
| Filtered entity subset (companies, people, places, products) | ~15–25 GB | What you actually keep in Postgres after filtering |
| Alias table (Postgres) | ~8–12 GB | Surface forms + prior probs, heavily indexed |
| Entity descriptions (Postgres) | ~3–5 GB | Short text descriptions per entity |
| OpenSearch entity index | ~10–15 GB | BM25 index over entity names + aliases |
| Wikipedia mention index for REL | ~50 GB | Required if you use REL for linking |
| FAISS vector index (entity embeddings) | ~5–8 GB | If you activate ANN search in v2 |

The **Wikidata decompressed dump is the biggest spike** — you need ~1.3 TB free temporarily during the import process, then it drops back down after you've loaded the filtered subset into Postgres and deleted the raw dump. If that's too heavy, the `wikid` spaCy project lets you filter during import so you never need to decompress the full dump.

If you skip REL and use OpenTapioca instead, you lose the 50 GB Wikipedia mention index but OpenTapioca's Solr index is still ~8–12 GB.

---

## Application Data

Depends entirely on your document volume. A rough model:

| Asset | Size per 1M docs | Notes |
|---|---|---|
| Raw document storage (Postgres body column) | ~5–15 GB | Average news article ~5–15 KB |
| Enrichment output (entities, topics, relations) | ~2–4 GB | JSONB columns, normalized schema |
| OpenSearch document index | ~8–20 GB | Full-text + nested entity/topic fields |
| Redis (queue + cache) | ~1–4 GB | In-memory; cache evicts automatically |
| Feedback records | ~0.1 GB | Typically low volume |

For **1 million documents**, expect roughly **15–40 GB** of application data depending on average article length and how much enrichment output you store.

---

## Practical Totals

### V1 — Minimal (no REL, no FAISS, OpenTapioca only, 1M docs)

| Component | Disk |
|---|---|
| Models | ~4 GB |
| KB (filtered Postgres + aliases + OpenSearch index) | ~35 GB |
| Application data (1M docs) | ~25 GB |
| OS + dependencies + Docker images | ~15 GB |
| **Total** | **~80 GB** |

**Recommended server:** 150–200 GB SSD minimum for comfortable headroom.

### V2 — Full (REL, FAISS, multilingual, 5M docs)

| Component | Disk |
|---|---|
| Models (all, multilingual) | ~10 GB |
| KB (Postgres + aliases + REL Wikipedia index + FAISS) | ~90 GB |
| Application data (5M docs) | ~120 GB |
| OS + dependencies + Docker images | ~20 GB |
| **Total** | **~240 GB** |

**Recommended server:** 400–500 GB SSD, or split storage: fast NVMe for Postgres + OpenSearch, cheaper SSD for model storage.

---

## The One Decision That Changes Everything

Whether you use the **full Wikidata dump or a pre-filtered subset** is the biggest lever:

- **Full dump import:** needs ~1.5 TB temporarily, ~40 GB settled. Better coverage.
- **Pre-filtered Wikidata subset** (e.g. only entities with English labels and instance-of = company/person/place): ~5 GB download, ~20 GB settled. Faster to set up, lower recall on obscure entities.

For the BDYA Media Group use case (financial news, companies, people, instruments), a filtered subset covering ~10–15M entities is probably sufficient and avoids the 1.5 TB headache entirely. I'd recommend starting there and only going to the full dump if you hit coverage gaps.

Great question — and yes, this tool has natural integration points across several of your projects. Let me go through each one honestly, including where the fit is strong and where it's marginal.

---

## Docdex

**Fit: Very strong.**

Docdex is a local code indexer using Tantivy and AST-based indexing. Right now it indexes code structure — symbols, types, function signatures. The enrichment pipeline adds a semantic layer on top of that:

- **Entity extraction over code comments and docstrings.** When a developer writes "this function calls the Provider Alpha API to process refunds," the tagger can extract `Provider Alpha` as a company entity, `refunds` as a financial concept, and link them to canonical knowledge. That makes docstrings searchable by concept, not just keyword.
- **Relation extraction over architecture docs.** If a repo has markdown docs describing service dependencies ("OrderService calls PaymentService via gRPC"), the relation extractor can surface those as typed edges — useful for dependency graphs.
- **Topic classification over files and modules.** Tag each file or module with a domain label (authentication, data persistence, billing logic). This gives you a semantic map of the codebase that goes beyond folder structure.
- **Cross-repo entity linking.** If multiple repos mention "the auth service" or "the Kafka cluster," entity linking can resolve those to the same canonical concept across codebases, making cross-repo search genuinely useful.

The most immediate win is probably enriching markdown and docstring content during indexing, then exposing topic and entity facets in search. This is low-integration-cost since the tagger is a REST call — Docdex's Rust daemon just fires `POST /v1/tag` on documentation content at index time.

---

## BDYA Media Group / The Neural Ledger

**Fit: This is the primary use case — built for this.**

The enrichment pipeline is essentially a financial news intelligence layer:

- Every article ingested gets entity tags (companies, people, instruments, regulators) with relevance scores
- Topic classification maps articles to financial taxonomies (earnings, M&A, central bank, IPO)
- Relation extraction surfaces named events: "Org Alpha acquired Org Beta," "Regulator Alpha raised rates by Y bps," "CEO appointed at Org Gamma"
- Entity linking connects mentions to canonical records — so "Person Alpha," "Org Alpha's CEO," and "Alpha" in the same article all resolve to `ent:person-alpha`

This enables the product features that matter for a financial media platform: entity-centric news feeds ("show me everything about `ent:org-alpha` this week"), event alerts ("notify when Acquisition relation detected for watchlist companies"), and cross-article entity timelines.

---

## BDYA Capital (Algorithmic Trading)

**Fit: Strong for signal generation, indirect.**

The trading system doesn't consume enriched articles directly, but the enrichment pipeline is a natural upstream data source:

- **Sentiment + entity extraction as trading signals.** An article tagged with `Org Alpha Holdings` (relevance: 0.91) and topic `earnings report` (score: 0.88) is a structured event that the trading system can act on, rather than raw text it has to parse itself.
- **Event detection as order triggers.** Relation extraction surfacing `Acquisition` or `RevenueReport` events can feed directly into event-driven strategy logic.
- **Named entity frequency as market sentiment proxy.** How often is a company mentioned in negative vs. neutral vs. positive topic contexts this week compared to last week?

The clean interface here is: enrichment pipeline emits structured events to Kafka, trading system consumes them as typed signals alongside price data. The SDS already has Redis/Celery for async — extending to Kafka for the trading system's consumption is straightforward.

---

## Crawlermaster

**Fit: Strong — natural upstream producer.**

Crawlermaster crawls the web. Right now it presumably stores raw HTML or extracted text. The enrichment pipeline sits directly downstream:

- Crawlermaster fetches a page → passes it to the tagger → stores enriched output
- The tagger's Trafilatura stage already handles HTML extraction, so Crawlermaster could pass raw HTML directly to `POST /v1/tag` without its own extraction step
- Enriched output from Crawlermaster feeds The Neural Ledger and BDYA Capital

This makes Crawlermaster the **ingestion layer** and the enrichment pipeline the **processing layer** — clean separation of concerns. Crawlermaster doesn't need to understand NLP; it just produces documents and the pipeline enriches them.

---

## MSwarm + Docdex/Mcoda Agents

**Fit: Excellent, and this is the most interesting integration.**

If MSwarm is an agent orchestration layer (based on the name), the enrichment pipeline becomes a **shared tool that any agent can call**. This is a genuinely strong architecture:

```
MSwarm orchestrator
├── Docdex agent     → calls /v1/tag on documentation content
├── Mcoda agent      → calls /v1/tag on research papers or data descriptions
├── News agent       → calls /v1/tag on articles for The Neural Ledger
└── Trading agent    → consumes entity/event output as signals
```

Each agent gets the same structured output format — entities, topics, relations, relevance scores — which means agents can share context about the same real-world entities across different domains. If the Docdex agent and the News agent both tag content mentioning `Org Alpha Holdings (ent:org-alpha)`, they're using the same canonical identifier, so MSwarm can reason across them without any entity resolution at the orchestration level.

Concretely for each agent:

**Docdex agent** — tag code documentation, surface entity-linked concepts, enable semantic code search queries like "find all code related to Provider Alpha payment processing."

**Mcoda agent** — if Mcoda deals with data or research content, entity and topic tagging makes that content queryable by concept rather than keyword. Relation extraction over research text can surface methodology connections or citation-worthy claims.

The enrichment pipeline effectively gives every agent in MSwarm a shared **semantic memory layer** — all agents speak the same entity vocabulary (Wikidata QIDs), which makes cross-agent reasoning much more tractable than each agent maintaining its own entity model.

---

## Summary

| Project | Integration value | Integration effort | Priority |
|---|---|---|---|
| The Neural Ledger | Core product feature | Low — built for this | Ship together |
| BDYA Capital | Signal generation upstream | Medium — Kafka bridge | V2 |
| Crawlermaster | Natural upstream producer | Low — HTTP call at crawl time | V1 |
| Docdex | Semantic layer on docs/code | Low — index-time HTTP call | After V1 stable |
| MSwarm agents | Shared entity vocabulary | Medium — tool registration | After V1 stable |
| Mcoda | Content enrichment | Low if content is text | Evaluate after Mcoda scope clear |

The highest-leverage move is probably making this a **shared internal service** that Crawlermaster, The Neural Ledger, BDYA Capital, and MSwarm all consume via the same REST API — rather than embedding it into each project separately. One deployed instance, one KB, one entity vocabulary across all your tools. That's essentially what OpenCalais was for media companies, except you own it.

Great question. Yes, it absolutely can be a local tool — and this is actually a well-trodden path. Think of it like Ollama but for semantic tagging instead of LLM chat. Let me break down what "local" realistically looks like at different tiers.

---

## The Core Challenge

The full system as designed in the SDS is a server-grade deployment. But you don't need all of it to run locally. The question is which components are essential vs. optional, and what you can swap for lighter alternatives.

---

## Tier 1 — Minimal Local (Developer Machine)

**Target:** MacBook Pro M-series, 16GB RAM, 50GB free disk  
**Use case:** Single developer, moderate document volume, no persistence required between restarts

### What you keep

| Component | Local alternative | Size |
|---|---|---|
| GLiNER NER | Same model, runs on CPU/MPS | ~0.5 GB |
| spaCy NLP | `en_core_web_sm` instead of trf | ~0.012 GB |
| bart-large-mnli topics | Same, CPU inference (~2s/doc) | ~1.6 GB |
| Alias table | SQLite instead of Postgres | ~4 GB |
| Entity KB | SQLite (filtered Wikidata subset) | ~8 GB |
| API | Same FastAPI, localhost only | — |
| Queue | In-process (no Celery/Redis) | — |
| Search | Tantivy or simple SQLite FTS | ~0.5 GB index |

### What you drop

- Postgres → SQLite
- Redis + Celery → synchronous in-process pipeline
- OpenSearch → SQLite FTS or Tantivy
- REL Wikipedia index (50 GB) → OpenTapioca or spaCy EL with filtered KB
- REBEL relation extraction → dep rules only (no 1.4 GB model)
- SetFit → zero-shot NLI only (already included in bart-large-mnli)

### Download size

| Asset | Size |
|---|---|
| Python runtime + dependencies | ~1.5 GB |
| Models (GLiNER + spaCy sm + bart-mnli) | ~2.2 GB |
| Filtered Wikidata KB (SQLite) | ~8 GB |
| Alias table (SQLite) | ~4 GB |
| Application code | ~0.05 GB |
| **Total** | **~16 GB** |

**RAM at runtime:** ~6–8 GB (bart-large-mnli is the heaviest at ~3 GB loaded)

This is very achievable on a developer machine. The bottleneck is bart-large-mnli — if you swap that for a smaller NLI model like `cross-encoder/nli-MiniLM2-L6-H768` (~0.09 GB), the whole thing drops to under 5 GB download and ~3 GB RAM.

---

## Tier 2 — Full Local (Workstation / Home Server)

**Target:** Linux workstation or home server, 32GB RAM, 200GB disk, optional GPU  
**Use case:** Power user, persistent storage, full relation extraction, reasonable throughput

### What you keep vs. the SDS

Everything from the SDS except:
- REL Wikipedia mention index → use filtered version (~15 GB instead of 50 GB)
- OpenSearch → keep it (runs fine locally, ~2 GB RAM)
- Postgres → keep it
- Redis + Celery → keep it

### Download size

| Asset | Size |
|---|---|
| Python runtime + dependencies | ~2 GB |
| All models (GLiNER + spaCy trf + bart-mnli + REBEL + sentence-transformers) | ~6 GB |
| Filtered Wikidata KB (Postgres dump) | ~20 GB |
| Alias table | ~10 GB |
| REL filtered Wikipedia index | ~15 GB |
| OpenSearch + Postgres + Redis (Docker images) | ~2 GB |
| Application code | ~0.05 GB |
| **Total** | **~55 GB** |

**RAM at runtime:** ~14–18 GB (all models loaded simultaneously)  
**With GPU (RTX 3090):** inference drops from ~2s/doc to ~200ms/doc for the transformer models

---

## Tier 3 — Ultra-Lightweight Local (Laptop, Air-gap)

**Target:** Any machine with 8GB RAM, 10GB free disk  
**Use case:** Offline use, embedded in another tool (e.g. Docdex), minimal dependencies

This is where you make aggressive trade-offs:

| Full stack component | Ultra-light replacement |
|---|---|
| GLiNER medium | GLiNER small or NuNER_Zero (MIT, ~0.2 GB) |
| bart-large-mnli | `cross-encoder/nli-MiniLM2-L6-H768` (~0.09 GB) |
| spaCy trf | `en_core_web_sm` (~12 MB) |
| Wikidata KB | Pre-filtered domain subset (e.g. finance only, ~500 MB) |
| Alias table | In-memory hash map from filtered CSV |
| Entity linking | Alias-only matching, no neural disambiguation |
| Relation extraction | Dep rules only, no REBEL |
| Storage | SQLite single file |
| API | Optional — can run as a library, no HTTP server |

### Download size

| Asset | Size |
|---|---|
| Python + deps (minimal) | ~0.8 GB |
| Models | ~0.8 GB |
| Domain-filtered KB + alias table | ~0.5–2 GB |
| **Total** | **~2–4 GB** |

**RAM at runtime:** ~2 GB  
This is realistic for embedding inside Docdex as a library call, or shipping as a CLI tool similar to how you'd ship a local Ollama model.

---

## Packaging It as a Local Tool (like Ollama)

The right model for this is exactly how Ollama works:

```
ades pull finance-en       # downloads the finance/English pack
ades pull medical-en       # downloads the medical/English pack
ades serve                 # starts local HTTP server on localhost:8734
ades tag "Org Alpha CEO Person Alpha announced..."   # CLI usage
```

The key engineering decisions for this to work well:

**1. Tiered model packs** — ship different bundles for different use cases. A `finance-en-lite` pack (2 GB) vs. a `general-en-full` pack (16 GB). Users download what they need.

**2. Pre-filtered KB snapshots** — don't make users process a Wikidata dump. Ship pre-built SQLite KB snapshots by domain (finance, tech, biomedical, general). Host them on S3 or a CDN. Monthly rebuild on your end, one-line download for the user.

**3. Lazy model loading** — don't load REBEL (1.4 GB) if the user hasn't requested relation extraction. Load models on first use, cache in memory.

**4. Library mode** — expose a Python package API alongside the HTTP server so tools like Docdex can import it directly without the HTTP hop:

```python
from ades import Tagger

tagger = Tagger(profile="finance-en-lite")
result = tagger.tag("Org Alpha CEO Person Alpha announced quarterly earnings...")
```

**5. Quantized models** — ONNX export + INT8 quantization cuts model sizes roughly in half and speeds up CPU inference significantly. GLiNER already supports ONNX export out of the box.

---

## Realistic Size Comparison

| Tier | Download | RAM | GPU needed | Throughput |
|---|---|---|---|---|
| Ultra-light (embedded/CLI) | 2–4 GB | 2 GB | No | ~5 docs/sec CPU |
| Developer (laptop) | ~16 GB | 6–8 GB | No (MPS helps) | ~1 doc/sec CPU |
| Full local (workstation) | ~55 GB | 14–18 GB | Optional | ~5 docs/sec GPU |
| Server (SDS as designed) | ~150 GB | 32 GB | Recommended | ~50+ docs/sec |

---

## Recommendation for Your Use Case

Given that Docdex already runs locally and you're thinking about MSwarm agents, I'd build two distribution modes:

**Mode 1 — Embedded library** (`pip install ades-tool`) for Docdex and Mcoda integration. 2–4 GB, runs in-process, no HTTP server, domain-filtered KB. This is what Docdex agents call directly.

**Mode 2 — Local server** (`ades serve`) for MSwarm and BDYA Capital where multiple agents need to share one running instance. 16 GB full download, localhost REST API, persistent SQLite storage.

The server SDS design is then just this local server mode deployed on a machine with more RAM and a GPU — same codebase, different config.

This is one of the most interesting angles on the project, and the answer is: **very useful, in ways that are fundamentally different from what RAG alone gives you.**

Let me explain the distinct value layers.

---

## The Core Problem This Solves for LLMs

LLMs have three structural weaknesses when dealing with documents and knowledge:

1. **They don't know what they don't know.** An LLM reading "Alpha announced Q3 results" doesn't automatically know this Alpha is `ent:org-alpha`, that Person Alpha is its CEO (`ent:person-alpha`), or that this event belongs to the `earnings report` topic class. It guesses from training data, which may be stale or wrong.

2. **They can't reliably distinguish entities.** "Alpha" the organization, "Alpha" the product line, and "Alpha Labs" the research group can all appear in the same corpus. An LLM in a zero-shot prompt has no reliable mechanism to resolve this. It pattern-matches from context, which fails on ambiguous or niche cases.

3. **Their context window is flat.** An LLM sees a chunk of text. It doesn't see a structured graph of which entities are most central, how they relate, or how this document connects to 10,000 others mentioning the same entities.

The enrichment pipeline solves all three structurally, before the LLM ever sees the content.

---

## Use Case 1 — Grounded, Structured RAG

Standard RAG gives an LLM chunks of text retrieved by vector similarity. The enrichment pipeline makes that retrieval dramatically more precise.

Instead of:
```
Query: "What did Org Alpha say about margins?"
→ retrieve top-5 chunks by cosine similarity
→ LLM reads chunks and answers
```

You get:
```
Query: "What did Org Alpha say about margins?"
→ resolve "Org Alpha" to ent:org-alpha (Org Alpha Holdings)
→ retrieve chunks WHERE entity_id = ent:org-alpha
     AND topic = "earnings report"
     AND relevance > 0.7
→ sort by relevance score descending
→ LLM reads pre-filtered, entity-anchored chunks and answers
```

The LLM gets higher signal content. Hallucination risk drops because the retrieval is entity-anchored rather than embedding-approximate. This matters enormously for financial or legal content where "Org Alpha" vs "Alpha Labs" is not a minor distinction.

---

## Use Case 2 — Structured Context Injection

Instead of passing raw text to the LLM, you pass enriched structured context alongside it. The LLM gets to reason over facts, not just text.

```
System prompt:
You are analyzing a financial news article. Here is structured metadata
extracted from it before you read the full text:

Entities (by relevance):
- Org Alpha Holdings (ent:org-alpha, company) — relevance: 0.91
- Person Alpha (ent:person-alpha, person, CEO of Org Alpha Holdings) — relevance: 0.78
- Metro Alpha (ent:metro-alpha, city) — relevance: 0.22

Topics: earnings report (0.88), technology sector (0.76)

Relations detected:
- Person Alpha [CEO_of] Org Alpha Holdings
- Org Alpha Holdings [reported_revenue] $94.9B [for_period] Q3 2026

Now read the article and answer the user's question.
---
[full article text]
```

This gives the LLM a pre-computed semantic scaffold. It doesn't need to extract entities itself — it can focus on reasoning. For smaller, cheaper models (Qwen, Mistral, Llama) this is especially powerful because it compensates for their weaker entity extraction capability.

---

## Use Case 3 — Entity-Centric Memory for Agents

This is the most powerful use case for MSwarm specifically.

An agent that processes 1,000 documents over its lifetime has no persistent memory of which entities appeared where, how often, or in what context — unless you give it one. The enrichment pipeline builds that memory automatically.

```
Agent query: "What has been said about Org Delta across all documents I've processed?"

Without enrichment:
→ vector search over embeddings
→ returns approximate chunks
→ agent re-reads and re-extracts

With enrichment:
→ SELECT * FROM linked_entity
     JOIN document ON ...
     WHERE entity_id = 'ent:org-delta'  -- Org Delta
     ORDER BY relevance DESC
→ returns structured timeline: dates, topics, relations, relevance scores
→ agent gets a pre-organized dossier on Org Delta
```

The agent doesn't burn context window re-extracting what the pipeline already extracted. It gets structured facts and uses the context window for reasoning, not extraction.

---

## Use Case 4 — Reducing Hallucination on Named Entities

This is a well-documented failure mode: LLMs confabulate entity details, especially for less famous entities, private companies, recent appointments, or niche domain concepts.

The enrichment pipeline gives you a verification layer:

```
LLM output: "Person Alpha became Org Alpha CEO in 2012."
Enrichment KB: Person Alpha (ent:person-alpha) became Org Alpha CEO in 2011.
→ flag discrepancy, correct before returning to user
```

Or more proactively — before the LLM answers a question about an entity, inject the KB record as ground truth:

```
User: "Who is the CEO of Org Alpha?"
→ resolve Org Alpha → ent:org-alpha
→ lookup current CEO relation → ent:person-alpha (Person Alpha), as of KB last updated
→ inject: "According to KB: Person Alpha (ent:person-alpha) is CEO of Org Alpha Holdings (ent:org-alpha)"
→ LLM answers with grounded context
```

This is particularly valuable for BDYA Capital where stale or wrong entity facts in an LLM response could have real consequences.

---

## Use Case 5 — Cheap Model Augmentation

This is directly relevant to your existing infrastructure thinking around Qwen, Mistral, and Llama on local hardware.

Smaller models are weak at:
- Zero-shot entity extraction on niche domains
- Disambiguation (Org Alpha the company vs. Alpha Labs the research group)
- Relation extraction from complex sentences
- Maintaining entity consistency across a long document

Your enrichment pipeline handles all of these before the LLM sees the document. So instead of:

```
Expensive model: GPT-4 reads raw article → extracts entities → reasons
Cost: ~$0.01–0.05 per document
```

You get:

```
Enrichment pipeline: extracts + links + classifies (one-time, local, fast)
Cheap local model: reads pre-structured context → reasons only
Cost: ~$0.00 per document after fixed infrastructure
```

The pipeline essentially acts as a **structured pre-processor that elevates a 7B model to perform entity-aware reasoning at near-70B quality** for document understanding tasks. This is a real, measurable effect — several research papers have shown that structured entity context injection closes most of the gap between small and large models on NLP benchmarks.

---

## Use Case 6 — Tool Calling Grounding

When an LLM uses tool calls (function calling), it often hallucinates tool parameters — especially entity identifiers. If the LLM decides to call `get_company_filings(company="Org Alpha")`, which Org Alpha? Which identifier format does the API expect?

The enrichment pipeline gives the LLM pre-resolved canonical IDs to use in tool calls:

```json
{
  "tool": "get_company_filings",
  "parameters": {
    "entity_id": "ent:org-alpha",
    "canonical_name": "Org Alpha Holdings",
    "external_ref": "REF-ALPHA-001"
  }
}
```

No ambiguity, no hallucinated identifiers, no API call failures from malformed entity references. This is especially valuable in MSwarm where agents are making tool calls across multiple systems that each expect different identifier formats.

---

## Use Case 7 — Cross-Document Synthesis

An LLM asked to synthesize information across 50 documents about a company has no efficient way to do that without the enrichment layer. Context windows fill fast, retrieval is imprecise, and the model re-extracts the same entities repeatedly.

With enrichment:

```
"Summarize everything relevant about Org Delta's funding history"

→ query: linked_entity WHERE entity_id = 'ent:org-delta'
         AND relation_type IN ('FundingRound', 'Acquisition', 'IPO')
         ORDER BY document.published_at ASC

→ returns structured timeline of 12 events across 50 documents
→ LLM receives a pre-organized chronological fact list
→ generates synthesis from structured facts, not raw text
```

The LLM's job becomes writing and reasoning, not extraction and organization. That's where LLMs are actually good.

---

## Summary: What the Pipeline Gives LLMs

| LLM weakness | What the pipeline provides |
|---|---|
| Stale or wrong entity knowledge | Grounded KB records injected at inference time |
| Entity ambiguity | Disambiguated canonical IDs before LLM sees content |
| Flat context window | Relevance-ranked, structured, pre-filtered content |
| Re-extraction waste | Pre-computed entities/relations/topics from storage |
| Hallucinated tool parameters | Canonical IDs ready for tool calls |
| Weak relation extraction (small models) | Relations pre-extracted, injected as structured facts |
| Cross-document synthesis difficulty | Entity-anchored query returns structured timelines |
| High cost per document (large models) | Small models + structured context = large model quality |

The way to think about it: **the enrichment pipeline does the left-brain structured extraction work so the LLM can focus entirely on right-brain reasoning and generation.** That division of labor is where you get the best quality-to-cost ratio, and it's the architecture that scales — because the pipeline runs once per document and the results are reused by every LLM call that touches that document forever.

For MSwarm specifically, this means every agent in the swarm shares a structured, entity-anchored world model that was built once and maintained continuously — rather than each agent re-deriving its own understanding of the same documents from scratch.
