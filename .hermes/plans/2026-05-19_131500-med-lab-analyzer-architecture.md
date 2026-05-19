# Medical Laboratory Parameter Analyzer — Architectural Blueprint

**Date**: 2026-05-19
**Status**: Design Phase
**Goal**: Architecture & ingestion pipeline for a system that ingests structured lab parameters (web) and unstructured clinical knowledge (PDFs), then evaluates patient lab results.

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PATIENT INPUT                                │
│   Lab results (CSV / FHIR / manual)  →  Analysis / Report           │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                    API LAYER  (FastAPI + Pydantic)                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────────────────────────┐   ┌──────────────────────────┐   │
│   │  LAB PARAMETER ENGINE       │   │  CLINICAL RAG ENGINE     │   │
│   │                             │   │                          │   │
│   │  • Lookup reference range   │   │  • Semantic search       │   │
│   │  • Flag abnormal values     │   │  • Retrieve disease      │   │
│   │  • Unit conversion          │   │    associations          │   │
│   │  • Critical value alert     │   │  • Augment LLM prompt    │   │
│   └──────────┬──────────────────┘   └───────────┬──────────────┘   │
│              │                                  │                   │
└──────────────┼──────────────────────────────────┼───────────────────┘
               │                                  │
    ┌──────────▼──────────┐            ┌──────────▼──────────┐
    │   POSTGRESQL        │            │   QDRANT            │
    │                     │            │   (Vector DB)       │
    │  • lab_parameters   │            │                     │
    │  • reference_ranges │            │  • clinical_chunks  │
    │  • units            │            │   (768-d vectors)   │
    │  • categories       │            │  • disease_links    │
    │  • audit_log        │            │  • source_metadata  │
    └──────────┬──────────┘            └──────────┬──────────┘
               │                                  │
               │                    ┌─────────────▼─────────────┐
               │                    │   REDIS                   │
               │                    │   • LLM response cache    │
               │                    │   • Frequent query cache  │
               │                    │   • Rate limiting         │
               │                    └───────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────────────┐
│                     INGESTION PIPELINE                               │
│                                                                     │
│   Pipeline A: Structured          Pipeline B: Unstructured           │
│   ┌─────────────────────┐        ┌─────────────────────────┐       │
│   │ Web → Scraper       │        │ PDF → Text Extraction   │       │
│   │  ↓                  │        │  ↓                      │       │
│   │ Parse / Normalize   │        │ Semantic Chunking       │       │
│   │  ↓                  │        │  ↓                      │       │
│   │ Conflict Resolution │        │ Embedding (PubMedBERT)  │       │
│   │  ↓                  │        │  ↓                      │       │
│   │ Upsert → PostgreSQL │        │ Upsert → Qdrant         │       │
│   └─────────────────────┘        └─────────────────────────┘       │
│                                                                     │
│   Orchestrator: Prefect (scheduled + event-driven)                  │
│   Monitoring: Prometheus metrics + structured logging               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Sources

### Pipeline A — Structured Data (Core Database)

**Target Sources**:
| Source | Data | Access |
|--------|------|--------|
| LOINC | Universal lab test codes & names | REST API / CSV download |
| Lab Tests Online | Reference ranges, clinical info | Web scraping |
| Mayo Clinic Laboratories | Test catalog, reference ranges | Web scraping |
| ARUP Consult | Lab test algorithms | Web scraping |
| NIH / NLM DailyMed | Drug-lab interactions | REST API |
| PubMed E-utilities | Lab parameter research metadata | REST API |

**Schema** (PostgreSQL):

```sql
-- Canonical lab parameter
CREATE TABLE lab_parameters (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    loinc_code      VARCHAR(20) UNIQUE,
    name            VARCHAR(300) NOT NULL,
    display_name    VARCHAR(500),
    category        VARCHAR(100),         -- e.g., 'Hematology', 'Chemistry', 'Endocrinology'
    subcategory     VARCHAR(100),
    description     TEXT,
    methodology     VARCHAR(500),
    specimen_type   VARCHAR(200),         -- e.g., 'Serum', 'Whole Blood', 'Urine'
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- Reference ranges (age/sex/method-specific)
CREATE TABLE reference_ranges (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parameter_id    UUID REFERENCES lab_parameters(id) ON DELETE CASCADE,
    source          VARCHAR(200),         -- 'Mayo Clinic', 'Lab Tests Online', etc.
    source_url      TEXT,
    low             DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    unit            VARCHAR(50),
    unit_ucum       VARCHAR(50),          -- UCUM standard code
    age_min_years   DOUBLE PRECISION,
    age_max_years   DOUBLE PRECISION,
    sex             VARCHAR(10),          -- 'male', 'female', 'any'
    pregnancy       BOOLEAN,
    condition       VARCHAR(500),         -- e.g., 'fasting', 'post-prandial'
    confidence      DOUBLE PRECISION,     -- 0.0-1.0 source reliability score
    is_primary      BOOLEAN DEFAULT false,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_ref_ranges_param ON reference_ranges(parameter_id);
CREATE INDEX idx_ref_ranges_demo ON reference_ranges(age_min_years, age_max_years, sex);
```

### Pipeline B — Unstructured Data (Clinical Knowledge Base)

**Target Sources**:
| Source | Format | Content |
|--------|--------|---------|
| Clinical practice guidelines (NICE, ESC, ADA, AACE) | PDF | Disease–lab linkage |
| Harrison's / Cecil textbook chapters | PDF | Pathophysiology |
| PubMed Central full-text articles | PDF/XML | Research findings |
| UpToDate / DynaMed exports | PDF | Clinical decision support |
| FDA drug labels | PDF | Drug-lab interactions |

**Schema** (Qdrant + metadata in PostgreSQL):

```sql
-- Metadata in PostgreSQL (lightweight pointer to vector chunks)
CREATE TABLE clinical_documents (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title           VARCHAR(1000),
    source          VARCHAR(500),
    source_type     VARCHAR(50),          -- 'guideline', 'textbook', 'research', 'drug_label'
    publication_year INT,
    authors         TEXT,
    file_path       TEXT,
    page_count      INT,
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    chunk_count     INT
);

CREATE TABLE clinical_chunks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id     UUID REFERENCES clinical_documents(id) ON DELETE CASCADE,
    chunk_index     INT,
    chunk_text      TEXT NOT NULL,
    qdrant_point_id VARCHAR(100) UNIQUE,   -- ID in Qdrant
    token_count     INT,
    section_heading VARCHAR(500),
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_chunks_doc ON clinical_chunks(document_id);
```

**Qdrant Collection** (`clinical_knowledge`):
- Vector dimension: 768 (PubMedBERT / BioBERT-large)
- Distance metric: Cosine
- Payload per point: `{chunk_id, document_id, section_heading, source_type, publication_year}`
- Full-text index enabled on payload for hybrid search

---

## 3. Ingestion Pipeline — Detailed

### Pipeline A: Structured Web Data

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐    ┌───────────┐
│ Scheduler│───▶│  Discovery   │───▶│  Extraction  │───▶│ Normalize │───▶│  Upsert   │
│ (Prefect)│    │  (Spider)    │    │  (Scraper)   │    │ (Resolver)│    │ (Postgres)│
└──────────┘    └──────────────┘    └──────────────┘    └───────────┘    └───────────┘
     │                │                   │                   │               │
     ▼                ▼                   ▼                   ▼               ▼
 Cron every      URL discovery       httpx + BS4/       Unit mapping,     INSERT ON
 24h / manual    + dedup check       selectolax         LOINC mapping     CONFLICT
 trigger         against DB          parse tables       age/sex tags      UPDATE
```

**Stage 1 — Discovery**:
- Maintain a `source_registry` table of known URLs to scrape
- For each source, fetch index/toc page, extract links to individual test pages
- Deduplicate against already-ingested URLs

**Stage 2 — Extraction**:
- Use `httpx` + `selectolax` (faster than BeautifulSoup for table-heavy pages)
- Extract structured tables containing: test name, reference range, units, specimen, methodology
- Fallback: `marker-pdf` for sources that provide PDF catalogs

**Stage 3 — Normalization**:
- Map local parameter names to LOINC codes via `loinc.org` API
- Convert units to UCUM standard (e.g., `mg/dL` → `mg/dL` UCUM code `mg/dL`)
- Parse reference ranges: extract numeric low/high from text like "3.5-5.0 mmol/L"
- Tag demographics: age range, sex, pregnancy status from surrounding text

**Stage 4 — Conflict Resolution**:
When multiple sources disagree on reference ranges:
1. Prioritize source with higher `confidence` score
2. Flag conflict in `audit_log` for manual review
3. Store all ranges; mark one as `is_primary = true`
4. Enrich with metadata about resolving algorithm version

**Stage 5 — Upsert**:
- `INSERT ... ON CONFLICT (loinc_code) DO UPDATE` for `lab_parameters`
- Batch insert for `reference_ranges` within a transaction

**Key Libraries**:
```
httpx>=0.27           # Async HTTP
selectolax>=0.3       # Fast HTML parsing (Modest engine)
pydantic>=2.5         # Data validation
sqlalchemy[asyncio]   # Async ORM
alembic               # Migrations
tenacity              # Retry logic
```

### Pipeline B: Unstructured PDF Data

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐
│ Ingest   │───▶│  PDF Parse   │───▶│  Chunking    │───▶│  Embedding   │───▶│  Upsert   │
│ Trigger  │    │  (pymupdf)   │    │  (Semantic)  │    │  (BioBERT)   │    │ (Qdrant)  │
└──────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └───────────┘
     │                │                   │                  │                  │
  Watch folder     Extract text +     Split on section    Transformers       Batch insert
  or API call      tables + meta     boundaries, not     or sentence-       with payload
                                     token count          transformers
```

**Stage 1 — PDF Parsing**:
- **pymupdf** (PyMuPDF) for text-based PDFs: fast, lightweight, handles tables
- **marker-pdf** as fallback for scanned/image-heavy PDFs (OCR needed)
- Extract: full text, table data, section headings, metadata (title, authors, year)

**Stage 2 — Semantic Chunking**:
- Split on **section boundaries** (heading detection) rather than fixed token counts
- Preserve context: include parent heading in chunk metadata
- Target chunk size: 300-500 tokens (optimal for medical concept retrieval)
- Overlap: 50 tokens between adjacent chunks (prevent mid-concept splits)

**Stage 3 — Embedding**:
- **Primary**: `pritamdeka/S-PubMedBert-MS-MARCO` — fine-tuned on PubMed for semantic search
- **Alternative**: `BAAI/bge-base-en-v1.5` — general-purpose, strong performance
- **Production**: Run via `sentence-transformers` or ONNX Runtime for speed
- Batch size: 32 chunks per embedding call

**Stage 4 — Upsert to Qdrant**:
- Batch insert with `upsert` (idempotent)
- Store full payload per point for hybrid search capability
- Create payload indices on `source_type`, `publication_year`, `section_heading`

**Key Libraries**:
```
pymupdf>=1.24          # PDF text extraction
marker-pdf             # OCR fallback (heavy, use sparingly)
sentence-transformers  # Embedding models
qdrant-client>=1.9     # Vector DB client
langchain-text-splitters  # Semantic chunking
nltk                   # Medical text tokenization
```

---

## 4. Query / Analysis Layer

### Lab Result Evaluation Flow

```
User submits lab results
        │
        ▼
┌───────────────────┐
│ 1. Parse input    │  JSON/CSV → [(parameter_name, value, unit, patient_demo)]
└───────┬───────────┘
        ▼
┌───────────────────┐
│ 2. Parameter      │  Map name → LOINC → lookup in lab_parameters
│    Resolution     │  + fetch matching reference_range (age/sex/condition)
└───────┬───────────┘
        ▼
┌───────────────────┐
│ 3. Flag Abnormal  │  value < low  →  "LOW"
│    Values         │  value > high →  "HIGH"
│                   │  critical     →  "CRITICAL" (outside panic ranges)
└───────┬───────────┘
        ▼
┌───────────────────┐
│ 4. RAG Query      │  For each abnormal parameter:
│    Construction   │  "What diseases are associated with elevated {param}?"
│                   │  → embed query → search Qdrant (top_k=5)
└───────┬───────────┘
        ▼
┌───────────────────┐
│ 5. LLM Synthesis  │  Context: reference ranges + flagged values + RAG chunks
│                   │  Prompt: "Given these lab results and clinical context,
│                   │           provide differential diagnosis with confidence"
│                   │  Model: GPT-4o / Claude Sonnet / local medical LLM
└───────┬───────────┘
        ▼
┌───────────────────┐
│ 6. Report Output  │  Structured JSON + human-readable summary
│                   │  Includes: parameter, value, flag, reference range,
│                   │            associated conditions, citations
└───────────────────┘
```

---

## 5. Infrastructure & Deployment

### Development

```
docker compose up
```

`docker-compose.yml` services:
- `postgres:16-alpine` — port 5432
- `qdrant/qdrant:latest` — port 6333
- `redis:7-alpine` — port 6379
- `api` — FastAPI app with hot-reload (uvicorn)
- `prefect-server` — orchestration UI (port 4200)

### Production Considerations

| Layer | Recommendation |
|-------|---------------|
| API | FastAPI behind nginx/Caddy, Gunicorn + Uvicorn workers |
| PostgreSQL | RDS / Cloud SQL with read replicas for query-heavy workloads |
| Qdrant | Qdrant Cloud or self-hosted on GPU instance (embedding generation) |
| Redis | ElastiCache / Memorystore |
| Embeddings | GPU instance (T4/L4) with ONNX-optimized models for throughput |
| File Storage | S3 / GCS for raw PDFs, processed chunks |
| CI/CD | GitHub Actions: lint → test → build Docker → deploy |
| Monitoring | Prometheus + Grafana; structured JSON logging to Loki |

### Scaling

- **Structured ingestion**: Parallelize by source (each scraper is independent); Prefect DAG with concurrency limits
- **PDF ingestion**: Queue-based (Redis/SQS); worker pool processes PDFs in parallel; GPU workers for embedding
- **Query layer**: Cache frequent parameter lookups in Redis (TTL: 24h); cache LLM responses for identical lab patterns (TTL: 7d)

---

## 6. Project Structure

```
med-lab-analyzer/
├── docker-compose.yml
├── pyproject.toml
├── alembic.ini
├── alembic/
│   └── versions/
├── src/
│   ├── api/                    # FastAPI application
│   │   ├── main.py
│   │   ├── routes/
│   │   │   ├── analysis.py     # POST /analyze — main endpoint
│   │   │   ├── parameters.py   # CRUD for lab parameters
│   │   │   └── documents.py    # PDF upload / ingestion trigger
│   │   └── schemas.py          # Pydantic models
│   ├── db/
│   │   ├── models.py           # SQLAlchemy ORM models
│   │   ├── session.py          # DB connection setup
│   │   └── queries.py          # Complex queries
│   ├── ingestion/
│   │   ├── structured/
│   │   │   ├── spiders/        # Per-source scrapers
│   │   │   │   ├── base.py     # Abstract spider
│   │   │   │   ├── labtests_online.py
│   │   │   │   ├── mayo_clinic.py
│   │   │   │   └── arup.py
│   │   │   ├── normalizer.py   # Unit/LOINC normalization
│   │   │   ├── resolver.py     # Conflict resolution
│   │   │   └── pipeline.py     # Prefect flow definition
│   │   └── unstructured/
│   │       ├── pdf_parser.py   # pymupdf + marker wrapper
│   │       ├── chunker.py      # Semantic chunking
│   │       ├── embedder.py     # BioBERT embedding
│   │       ├── qdrant_store.py # Qdrant upsert
│   │       └── pipeline.py     # Prefect flow definition
│   ├── engine/
│   │   ├── analyzer.py         # Core analysis logic
│   │   ├── rag.py              # RAG retrieval + prompt construction
│   │   └── llm.py              # LLM client (OpenAI/Anthropic/local)
│   └── config.py               # Settings (pydantic-settings)
├── tests/
│   ├── test_ingestion/
│   ├── test_engine/
│   └── test_api/
├── data/                       # Git-ignored
│   ├── raw_pdfs/
│   └── processed/
├── .pi/                        # Pi config (committed — team-shared)
│   └── agent/
│       └── models.json         # Local + cloud model registry
└── .hermes/                    # Hermes config (git-ignored)
    └── plans/                  # Architecture plans
```

---

## 7. Implementation Roadmap

| Phase | Scope | Tools | Models | Estimate |
|-------|-------|-------|--------|----------|
| **Phase 0: Environment** | Install Ollama + models (~40 GB), Pi (npm), Docker Desktop, project skeleton | Pi + terminal | — | 0.5 day |
| **Phase 1: Foundation** | PostgreSQL schema, FastAPI skeleton, pydantic models, Docker Compose | Pi / Hermes | L1 Qwen2.5-Coder 7B | 2-3 days |
| **Phase 2: Structured Ingestion** | LOINC importer, 2-3 web scrapers, normalizer, Prefect pipeline | Pi | L1 + L2 + L3 (BioMistral for LOINC) | 3-4 days |
| **Phase 3: Unstructured Ingestion** | PDF parser, semantic chunker, BioBERT embedder, Qdrant store | Pi / Hermes | L1 + L2 + L3 (BioMistral for medical extraction) | 3-4 days |
| **Phase 4: Analysis Engine** | Reference range lookup, flagging, RAG retrieval, LLM synthesis | Pi + Hermes | L1 + L2 + L5 (Claude Opus for clinical synthesis) | 3-4 days |
| **Phase 5: API & Polish** | Full REST API, validation, error handling, caching, monitoring | Pi | L1 + L2 | 2-3 days |
| **Total** | | | **77% local models, 23% cloud** | **14-19 days** |

---

## 8. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Reference range conflicts between sources | Multi-source store + confidence scoring + manual review queue |
| PDF quality varies (scanned, multi-column, tables) | pymupdf first, marker-pdf fallback; flag failures for manual review |
| Medical terminology ambiguity | LOINC mapping as canonical key; MeSH synonym expansion during RAG retrieval |
| LLM hallucination in clinical context | Always cite source chunks; confidence thresholds; human-in-the-loop for critical flags |
| Regulatory compliance (HIPAA, GDPR) | De-identified storage; audit trail on all queries; configurable data retention |
| Source websites change structure | Monitor spider success rate; alert on selector failure; version scrapers separately |
| Embedding model quality for medical domain | Use PubMedBERT (trained on biomedical literature); benchmark retrieval precision regularly |

---

## 9. Key Design Decisions

1. **PostgreSQL (not MongoDB)** for structured data: ACID compliance, JSONB for flexible fields, mature ORM support, excellent for range queries
2. **Qdrant (not Pinecone/Weaviate)** for vector DB: open-source, fast HNSW indexing, payload filtering, hybrid search, self-hostable, lower cost
3. **pymupdf (not PyPDF2/pdfplumber)** for PDF: 10x faster, handles tables natively, battle-tested
4. **PubMedBERT (not OpenAI embeddings)** for medical text: domain-specific, offline-capable, no API costs, 768-d vectors are efficient
5. **Prefect (not Airflow)** for orchestration: Python-native, lighter weight for small-to-medium pipelines, excellent local dev experience
6. **Semantic chunking (not fixed-size)**: Medical text has natural section boundaries; splitting on headings preserves clinical concept integrity

---

## 10. LLM Model Selection Matrix

**Core principle**: Prefer local models where feasible (privacy, zero API cost, offline capability). Escalate to cloud/API models only when the task demands capabilities unavailable locally. The matrix below maps every pipeline subtask to the optimal model, with a local-first fallback chain.

### Model Tier Summary

| Tier | Description | Examples | When to use |
|------|-------------|----------|-------------|
| **L1 — Local Small** | 7B–14B params, runs on consumer GPU / CPU | Qwen2.5-Coder 7B, Llama 3.1 8B, Phi-4 14B | Boilerplate code, simple parsing, schema generation |
| **L2 — Local Large** | 20B–32B params, needs 24GB+ VRAM | Qwen2.5-Coder 32B, DeepSeek-Coder-V2 Lite, Mistral Small 3.1 24B | Complex code, refactoring, test generation |
| **L3 — Local Medical** | Domain fine-tuned, 7B–13B | BioMistral 7B, MedLlama3 8B, OpenBioLLM 8B | Medical text extraction, concept normalization |
| **L4 — Cloud Coding** | API-based coding-specialized | Claude Sonnet 4, GPT-4.3-Codex, DeepSeek-V4 | Architecture design, complex debugging, PR review |
| **L5 — Cloud Reasoning** | API-based general reasoning | Claude Opus 4, GPT-5.3, Gemini 2.5 Pro | Clinical synthesis, differential diagnosis |

### Detailed Task → Model Mapping

#### Phase 1: Foundation

| Task | Primary Model | Fallback | Rationale |
|------|--------------|----------|-----------|
| FastAPI app skeleton | **L1** Qwen2.5-Coder 7B | L4 Claude Sonnet | Boilerplate with low complexity |
| SQLAlchemy ORM models | **L1** Qwen2.5-Coder 7B | L4 GPT-4.3-Codex | Schema from spec is pattern-matching |
| Pydantic schemas | **L1** Qwen2.5-Coder 7B | L4 Claude Sonnet | Data models are straightforward |
| Alembic migrations | **L1** Qwen2.5-Coder 7B | L2 DeepSeek-Coder-V2 | Autogenerate + minor edits |
| Docker Compose | **L1** Qwen2.5-Coder 7B | — | Static infra-as-code, fully local |

#### Phase 2: Structured Ingestion

| Task | Primary Model | Fallback | Rationale |
|------|--------------|----------|-----------|
| Web scraper spiders | **L1** Qwen2.5-Coder 7B | L4 Claude Sonnet | Pattern-based HTML parsing |
| selectolax selectors | **L1** Qwen2.5-Coder 7B | — | CSS selector logic is simple |
| Unit normalizer (UCUM) | **L2** DeepSeek-Coder-V2 | L4 GPT-4.3-Codex | Unit conversion has edge cases |
| LOINC mapper | **L3** BioMistral 7B | L4 Claude Sonnet | Medical terminology benefits from domain model |
| Conflict resolver | **L2** Qwen2.5-Coder 32B | L4 GPT-4.3-Codex | Logic-heavy with multiple resolution strategies |
| Prefect flow definitions | **L1** Qwen2.5-Coder 7B | — | Decorator-based, well-documented API |

#### Phase 3: Unstructured Ingestion

| Task | Primary Model | Fallback | Rationale |
|------|--------------|----------|-----------|
| PDF parser wrapper | **L1** Qwen2.5-Coder 7B | L4 Claude Sonnet | Library wrapper, low complexity |
| Semantic chunker | **L2** Qwen2.5-Coder 32B | L4 GPT-4.3-Codex | NLP pipeline with heading detection logic |
| Section boundary detection | **L3** BioMistral 7B | L2 Qwen2.5-Coder 32B | Medical document structure recognition |
| Qdrant client + upsert logic | **L1** Qwen2.5-Coder 7B | — | Straightforward SDK usage |
| Medical concept extraction | **L3** BioMistral 7B | L5 Claude Opus | MeSH/SNOMED concept recognition |

#### Phase 4: Analysis Engine

| Task | Primary Model | Fallback | Rationale |
|------|--------------|----------|-----------|
| Reference range lookup logic | **L1** Qwen2.5-Coder 7B | — | Pure SQL + Python, no AI needed |
| Abnormal value flagging | **L1** Qwen2.5-Coder 7B | — | Deterministic comparison logic |
| RAG retrieval (query embedding) | **Embedding model** PubMedBERT | — | Embedding, not generation |
| RAG query construction | **L2** Qwen2.5-Coder 32B | L4 Claude Sonnet | Prompt engineering + retrieval logic |
| **Clinical synthesis / Differential diagnosis** | **L5** Claude Opus 4 | GPT-5.3 | ⚠️ HIGH STAKES — requires maximum reasoning ability; local models insufficient for clinical reasoning |
| Report formatting | **L1** Qwen2.5-Coder 7B | L4 Claude Sonnet | Template-based formatting |

#### Phase 5: API & Polish

| Task | Primary Model | Fallback | Rationale |
|------|--------------|----------|-----------|
| REST endpoint implementation | **L1** Qwen2.5-Coder 7B | L4 Claude Sonnet | FastAPI is pattern-heavy |
| Input validation | **L1** Qwen2.5-Coder 7B | — | Pydantic validators |
| Error handling middleware | **L2** Qwen2.5-Coder 32B | L4 GPT-4.3-Codex | Edge cases matter |
| Test suite | **L2** DeepSeek-Coder-V2 | L4 Claude Sonnet | High coverage demands reasoning |
| Monitoring / Structured logging | **L1** Qwen2.5-Coder 7B | — | Standard patterns |

#### Ongoing: Code Review & Debugging

| Task | Primary Model | Fallback | Rationale |
|------|--------------|----------|-----------|
| PR review (first pass) | **L2** DeepSeek-Coder-V2 | — | Catch obvious issues locally |
| PR review (clinical safety) | **L5** Claude Opus 4 | GPT-5.3 | Safety-critical code paths |
| Bug diagnosis (simple) | **L2** Qwen2.5-Coder 32B | L4 Claude Sonnet | Stack traces + context |
| Bug diagnosis (complex / race conditions) | **L4** Claude Sonnet 4 | GPT-4.3-Codex | Multi-file reasoning needed |
| Architecture decisions | **L4** Claude Sonnet 4 | L5 Claude Opus 4 | High-level design reasoning |

### Model Allocation Summary

```
╔══════════════════════════════════════════════════════════════════╗
║                    MODEL TIER USAGE BY PHASE                     ║
╠══════════╦═══════╦═══════╦═══════╦═══════╦═══════╦══════════════╣
║          │ L1    │ L2    │ L3    │ L4    │ L5    │ Total tasks  ║
║          │Local  │Local  │Local  │Cloud  │Cloud  │              ║
║          │Small  │Large  │Medical│Coding │Reason │              ║
╠══════════╬═══════╬═══════╬═══════╬═══════╬═══════╬══════════════╣
║ Phase 1  │   5   │   0   │   0   │   0   │   0   │      5       ║
║ Phase 2  │   3   │   1   │   1   │   1   │   0   │      6       ║
║ Phase 3  │   1   │   1   │   2   │   1   │   0   │      5       ║
║ Phase 4  │   3   │   1   │   0   │   0   │   1   │      5       ║
║ Phase 5  │   3   │   1   │   0   │   1   │   0   │      5       ║
║ Ongoing  │   0   │   2   │   0   │   2   │   1   │      5       ║
╠══════════╬═══════╬═══════╬═══════╬═══════╬═══════╬══════════════╣
║ TOTAL    │  15   │   6   │   3   │   5   │   2   │     31       ║
║   %      │ 48%   │ 19%   │ 10%   │ 16%   │  6%   │    100%      ║
╚══════════╩═══════╩═══════╩═══════╩═══════╩═══════╩══════════════╝

77% of tasks can use LOCAL models (L1 + L2 + L3).
Only clinical synthesis (L5) and complex debugging/architecture (L4) require cloud.
```

### Local Model Setup (Ollama)

```bash
# Install Ollama (one-time)
curl -fsSL https://ollama.com/install.sh | sh

# Pull models (one-time, ~40 GB total)
ollama pull qwen2.5-coder:7b          # L1 — 4.7 GB, primary coding model
ollama pull qwen2.5-coder:32b         # L2 — 19 GB, complex code + reasoning
ollama pull deepseek-coder-v2:16b     # L2 — 8.9 GB, alternative coding
ollama pull biomistral:7b             # L3 — 4.1 GB, medical text processing
ollama pull llama3.1:8b               # L1 — 4.7 GB, fallback general model
```

### 🔴 Clinical Safety Boundary

```
─────────────────────────────────────────────────────────────────
  CRITICAL: The clinical synthesis step (differential diagnosis)
  MUST use a cloud reasoning model (L5: Claude Opus 4 or GPT-5.3).
  
  Local models (even 32B+) lack the medical reasoning depth,
  safety guardrails, and factual accuracy required for clinical
  decision support. This is a HARD boundary — no exceptions.
─────────────────────────────────────────────────────────────────
```

---

## 11. Pi Development Environment

**[Pi](https://pi.dev)** is a minimal TypeScript terminal coding harness by Earendil Inc. It serves as the primary AI-assisted development environment for this project — orchestrating code generation, review, and refactoring across the Python codebase.

### Why Pi (alongside Hermes)

| Concern | Hermes | Pi |
|---------|--------|-----|
| **Role** | System architect, pipeline orchestrator, RAG design, deployment planning | Code-level assistant: write, edit, review, debug individual files |
| **Strengths** | Cross-session memory, skills, cron, multi-agent delegation, gateway | Minimal, fast, TypeScript-native, excellent code-editing ergonomics |
| **Interaction** | Long-running sessions with context persistence | Quick file-level edits via terminal |
| **Model routing** | Uses Nous Portal / cloud providers | Can directly use local Ollama models |

**Workflow**: Hermes owns the architecture + plan. Pi executes the code-level implementation, especially for boilerplate-heavy Python files where local models (L1/L2) excel.

### Pi Installation

```bash
# Option A: npm (recommended for Windows)
npm install -g @earendil-works/pi-coding-agent

# Option B: curl (Linux/macOS)
curl -fsSL https://pi.dev/install.sh | sh

# Launch in project directory
cd med-lab-analyzer/
pi
```

### Pi Configuration for Local Models

Create `~/.pi/agent/models.json` to register local Ollama models:

```json
{
  "providers": {
    "ollama": {
      "baseUrl": "http://localhost:11434/v1",
      "api": "openai-completions",
      "apiKey": "ollama",
      "compat": {
        "supportsDeveloperRole": false,
        "supportsReasoningEffort": false
      },
      "models": [
        {
          "id": "qwen2.5-coder:7b",
          "name": "Qwen 2.5 Coder 7B (Local)",
          "reasoning": false,
          "input": ["text"],
          "contextWindow": 32768,
          "maxTokens": 8192,
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 }
        },
        {
          "id": "qwen2.5-coder:32b",
          "name": "Qwen 2.5 Coder 32B (Local)",
          "reasoning": true,
          "input": ["text"],
          "contextWindow": 32768,
          "maxTokens": 8192,
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 }
        },
        {
          "id": "deepseek-coder-v2:16b",
          "name": "DeepSeek Coder V2 16B (Local)",
          "reasoning": true,
          "input": ["text"],
          "contextWindow": 131072,
          "maxTokens": 32768,
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 }
        },
        {
          "id": "biomistral:7b",
          "name": "BioMistral 7B (Local Medical)",
          "reasoning": false,
          "input": ["text"],
          "contextWindow": 32768,
          "maxTokens": 8192,
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 }
        },
        {
          "id": "llama3.1:8b",
          "name": "Llama 3.1 8B (Local Fallback)",
          "reasoning": false,
          "input": ["text"],
          "contextWindow": 131072,
          "maxTokens": 32768,
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 }
        }
      ]
    },
    "openai": {
      "apiKey_env": "OPENAI_API_KEY",
      "models": [
        {
          "id": "gpt-4.3-codex",
          "name": "GPT-4.3 Codex (Cloud)",
          "reasoning": true,
          "input": ["text"],
          "contextWindow": 131072
        }
      ]
    },
    "anthropic": {
      "apiKey_env": "ANTHROPIC_API_KEY",
      "models": [
        {
          "id": "claude-sonnet-4-20250514",
          "name": "Claude Sonnet 4 (Cloud)",
          "reasoning": true,
          "input": ["text", "image"],
          "contextWindow": 200000,
          "maxTokens": 32000
        }
      ]
    }
  }
}
```

### Pi + Hermes Daily Workflow

```
┌──────────────────────────────────────────────────────────────────┐
│ START OF SESSION                                                  │
│                                                                   │
│  $ cd med-lab-analyzer/                                           │
│  $ hermes -c "med-lab"     ← Hermes: resume architecture session  │
│                                                                   │
│  Hermes assigns tasks:                                            │
│    "Phase 2 task: implement labtests_online.py spider"            │
│                                                                   │
│  (Hermes delegates boilerplate to Pi)                             │
│                                                                   │
│  $ pi                        ← Pi: code-level implementation      │
│  > /model → Qwen 2.5 Coder 7B (Local)                             │
│  > "Implement the LabTestsOnlineSpider class following the        │
│     base.py abstract class. Extract: test name, reference range,  │
│     units, specimen type from each test detail page."             │
│                                                                   │
│  Pi generates spider code → user reviews → commit                 │
│                                                                   │
│  For complex debugging:                                           │
│  > /model → Claude Sonnet 4 (Cloud)                                │
│  > "Debug: the normalizer crashes on range 'Negatív (<5)' ..."   │
│                                                                   │
│  Back to Hermes:                                                  │
│    "Phase 2 spider implemented. What's next?"                     │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Pi Key Commands

| Command | Action |
|---------|--------|
| `/model` | Switch between local/cloud models (reloads models.json automatically) |
| `/login` | Authenticate with Anthropic, OpenAI, GitHub Copilot for cloud models |
| `/skills` | Load Pi skills for specialized workflows |
| `/theme` | Switch editor theme |
| `/help` | Show all commands |
| `Esc` | Exit insert mode, enter command mode |

### When to Use Pi vs Hermes for This Project

| Scenario | Tool | Model | Why |
|----------|------|-------|-----|
| Write a new FastAPI endpoint | **Pi** | Qwen2.5-Coder 7B (L1) | Boilerplate, local, fast |
| Implement a web scraper spider | **Pi** | Qwen2.5-Coder 7B (L1) | Pattern-matching code |
| Debug a SQLAlchemy relationship error | **Pi** | DeepSeek-Coder-V2 (L2) | Code-level debugging |
| Design the RAG retrieval strategy | **Hermes** | Cloud (any) | Architecture reasoning |
| Write unit tests for a module | **Pi** | Qwen2.5-Coder 32B (L2) | Test generation needs reasoning |
| Review a PR for clinical safety | **Pi** | Claude Sonnet 4 (L4) | Safety-critical code paths |
| Plan Phase 3 implementation | **Hermes** | Cloud (any) | Cross-file, multi-step planning |
| Merge PDF parser with chunker | **Pi** | Qwen2.5-Coder 7B (L1) | Simple integration code |
| Evaluate if a lab value is critical | **Pi + code** | None (deterministic) | Pure comparison logic, no AI needed |
