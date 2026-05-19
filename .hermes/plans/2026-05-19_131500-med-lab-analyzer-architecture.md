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
|| Evaluate if a lab value is critical | **Pi + code** | None (deterministic) | Pure comparison logic, no AI needed |

---

## 12. API Specification

### 12.1 Design Principles

- **JSON-first**: minden request és response JSON, kivéve a PDF feltöltést (multipart/form-data)
- **Pagination**: minden listázó endpoint támogatja az offset/limit paginációt
- **Idempotency**: az ingestion trigger endpointok idempotensek (idempotency key header támogatás)
- **Versioning**: URL-prefix alapú verziózás (`/api/v1/...`)
- **Error shape**: minden hiba egységes struktúrában érkezik
- **OpenAPI**: automatikusan generált OpenAPI 3.1 séma a `/docs` végponton

### 12.2 Common Types

```python
# src/api/schemas.py — shared Pydantic models

from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID
from datetime import datetime
from enum import Enum
from typing import Optional, Annotated
from pydantic.functional_validators import AfterValidator

# ── Enums ──────────────────────────────────────────────────────────

class Flag(str, Enum):
    NORMAL   = "NORMAL"
    LOW      = "LOW"
    HIGH     = "HIGH"
    CRITICAL = "CRITICAL"
    UNKNOWN  = "UNKNOWN"   # when reference range not available

class Sex(str, Enum):
    MALE   = "male"
    FEMALE = "female"
    ANY    = "any"

class ParameterCategory(str, Enum):
    HEMATOLOGY       = "Hematology"
    CHEMISTRY        = "Chemistry"
    ENDOCRINOLOGY    = "Endocrinology"
    IMMUNOLOGY       = "Immunology"
    MICROBIOLOGY     = "Microbiology"
    COAGULATION      = "Coagulation"
    URINALYSIS       = "Urinalysis"
    TOXICOLOGY       = "Toxicology"
    CARDIAC          = "Cardiac"
    TUMOR_MARKERS    = "Tumor Markers"
    GENETICS         = "Genetics"
    OTHER            = "Other"

class SourceType(str, Enum):
    GUIDELINE  = "guideline"
    TEXTBOOK   = "textbook"
    RESEARCH   = "research"
    DRUG_LABEL = "drug_label"
    OTHER      = "other"

# ── Pagination ─────────────────────────────────────────────────────

class PaginationParams(BaseModel):
    offset: int = Field(default=0, ge=0)
    limit:  int = Field(default=20, ge=1, le=100)

class PaginatedResponse(BaseModel):
    total:   int
    offset:  int
    limit:   int
    has_more: bool

# ── Error Response ──────────────────────────────────────────────────

class ErrorDetail(BaseModel):
    loc:   list[str] = []     # pl. ["body", "results", 0, "value"]
    msg:   str
    type:  str                # pl. "value_error.missing", "not_found"

class ErrorResponse(BaseModel):
    error:   str              # "ValidationError", "NotFound", "InternalError"
    message: str
    details: list[ErrorDetail] = []
    request_id: str | None = None  # UUID a traceability-hez

# ── Patient Demographics (de-identified) ────────────────────────────

class PatientDemographics(BaseModel):
    age_years:    float | None = Field(default=None, ge=0, le=150, description="Patient age in years")
    sex:          Sex | None = None
    is_pregnant:  bool = False
    condition:    str | None = Field(default=None, max_length=500, description="e.g., 'fasting', 'post-prandial', 'pregnant 3rd trimester'")

# ── Lab Result (input) ──────────────────────────────────────────────

class LabResultInput(BaseModel):
    """Egyetlen laborparameter eredmeny."""
    parameter_name: str  = Field(min_length=1, max_length=300)
    value:          float
    unit:           str  = Field(min_length=1, max_length=50)
    loinc_code:     str | None = Field(default=None, pattern=r'^\d{1,6}-\d$', description="Optional explicit LOINC code")

class AnalysisRequest(BaseModel):
    """POST /analyze request body."""
    results:      list[LabResultInput]  = Field(min_length=1, max_length=200, description="1-200 lab results per request")
    patient:      PatientDemographics | None = None
    include_rag:  bool = True           # whether to include RAG context in response
    language:     str  = "en"           # output language hint ("en" or "hu")

# ── Lab Result (output) ─────────────────────────────────────────────

class ReferenceRange(BaseModel):
    low:          float | None
    high:         float | None
    unit:         str
    source:       str
    age_min:      float | None
    age_max:      float | None
    sex:          Sex = Sex.ANY

class AnalyzedResult(BaseModel):
    """Egyetlen kiertekelt laborparameter."""
    parameter_name:   str
    loinc_code:       str | None
    value:            float
    unit:             str
    flag:             Flag
    reference_range:  ReferenceRange | None
    interpretation:   str | None = None  # short human-readable note, e.g. "Mildly elevated, consider retest"

class RAGCitation(BaseModel):
    chunk_id:       UUID
    document_title: str
    source:         str
    source_type:    SourceType
    publication_year: int | None
    section_heading: str | None
    excerpt:        str   # max 500 chars from chunk

class ClinicalContext(BaseModel):
    abnormal_params:  list[str]                # names of flagged parameters
    rag_citations:    list[RAGCitation]         # top RAG results per abnormal param
    synthesized_note: str | None = None         # L5-generated clinical synthesis

class AnalysisResponse(BaseModel):
    """POST /analyze response body."""
    request_id:        UUID
    analyzed_results:  list[AnalyzedResult]
    clinical_context:  ClinicalContext | None = None  # None when include_rag=False
    evaluated_at:      datetime
    model_used:        str | None = None       # which LLM generated the synthesis
    disclaimer:        str = (
        "This is an AI-generated analysis for research and educational purposes only. "
        "It is NOT a medical diagnosis. Always consult a qualified healthcare professional."
    )

# ── Batch Analysis ─────────────────────────────────────────────────

class BatchAnalysisRequest(BaseModel):
    """POST /analyze/batch request body."""
    patients: list[AnalysisRequest] = Field(min_length=1, max_length=50, description="1-50 patients per batch")

class BatchPatientResult(BaseModel):
    index:     int
    status:    str   # "success" | "error"
    response:  AnalysisResponse | None = None
    error:     ErrorResponse | None = None

class BatchAnalysisResponse(BaseModel):
    request_id: UUID
    results:    list[BatchPatientResult]
    summary:    dict = {}   # {"total": 50, "success": 48, "error": 2}

# ── Parameter CRUD ─────────────────────────────────────────────────

class LabParameterCreate(BaseModel):
    loinc_code:    str = Field(pattern=r'^\d{1,6}-\d$')
    name:          str = Field(min_length=1, max_length=300)
    display_name:  str | None = Field(default=None, max_length=500)
    category:      ParameterCategory = ParameterCategory.OTHER
    subcategory:   str | None = Field(default=None, max_length=100)
    description:   str | None = None
    methodology:   str | None = Field(default=None, max_length=500)
    specimen_type: str | None = Field(default=None, max_length=200)

class LabParameterResponse(LabParameterCreate):
    id:         UUID
    created_at: datetime
    updated_at: datetime
    reference_range_count: int = 0
    model_config = ConfigDict(from_attributes=True)

class LabParameterFilter(BaseModel):
    category:    ParameterCategory | None = None
    search:      str | None = Field(default=None, min_length=1, description="Search in name, display_name, loinc_code")
    specimen:    str | None = None

class ReferenceRangeCreate(BaseModel):
    parameter_id:  UUID
    source:        str = Field(max_length=200)
    source_url:    str | None = None
    low:           float
    high:          float
    unit:          str = Field(max_length=50)
    unit_ucum:     str | None = Field(default=None, max_length=50)
    age_min_years: float | None = None
    age_max_years: float | None = None
    sex:           Sex = Sex.ANY
    pregnancy:     bool = False
    condition:     str | None = Field(default=None, max_length=500)
    confidence:    float = Field(default=0.5, ge=0.0, le=1.0)

class ReferenceRangeResponse(ReferenceRangeCreate):
    id:         UUID
    is_primary: bool
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)

# ── Document Management ─────────────────────────────────────────────

class DocumentUploadResponse(BaseModel):
    document_id: UUID
    title:       str
    page_count:  int
    status:      str   # "queued", "processing", "completed", "failed"
    queued_at:   datetime

class DocumentStatusResponse(BaseModel):
    document_id: UUID
    title:       str
    status:      str
    page_count:  int
    chunk_count: int | None
    error:       str | None
    ingested_at: datetime | None

class IngestTriggerResponse(BaseModel):
    pipeline:  str   # "structured" | "unstructured"
    status:    str
    flow_run_id: str | None
    message:   str

# ── Health ──────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status:    str   # "healthy" | "degraded" | "unhealthy"
    version:   str
    uptime:    float # seconds
    components: dict[str, str]  # {"postgres": "healthy", "qdrant": "healthy", "redis": "healthy", "ollama": "healthy"}
```

### 12.3 Endpoint Reference

| Method | Path | Summary | Auth | Rate Limit |
|--------|------|---------|------|------------|
| `POST`   | `/api/v1/analyze`             | Single patient lab analysis  | API key (opt) | 30/min |
| `POST`   | `/api/v1/analyze/batch`       | Batch patient analysis       | API key (opt) | 5/min  |
| `GET`    | `/api/v1/parameters`          | List/search lab parameters   | API key (opt) | 60/min |
| `GET`    | `/api/v1/parameters/{id}`     | Get single parameter         | API key (opt) | 60/min |
| `POST`   | `/api/v1/parameters`          | Create lab parameter         | API key       | 30/min |
| `PUT`    | `/api/v1/parameters/{id}`     | Update lab parameter         | API key       | 30/min |
| `DELETE` | `/api/v1/parameters/{id}`     | Delete lab parameter         | API key       | 10/min |
| `GET`    | `/api/v1/parameters/{id}/ranges` | List reference ranges     | API key (opt) | 60/min |
| `POST`   | `/api/v1/parameters/{id}/ranges` | Add reference range       | API key       | 30/min |
| `POST`   | `/api/v1/documents/upload`    | Upload PDF for ingestion      | API key       | 10/min |
| `GET`    | `/api/v1/documents/{id}`      | Get document status           | API key (opt) | 60/min |
| `GET`    | `/api/v1/documents`           | List documents                | API key (opt) | 60/min |
| `POST`   | `/api/v1/ingest/structured`   | Trigger structured ingestion  | API key       | 5/min  |
| `POST`   | `/api/v1/ingest/unstructured` | Trigger unstructured ingestion| API key       | 5/min  |
| `GET`    | `/api/v1/health`              | Health check                  | None         | 120/min |

### 12.4 Endpoint: POST /analyze

A rendszer fo elemzesi endpointja. Laboreredmenyeket fogad, referenciatartomanyokat keres, abnormalis ertekeket jelez, RAG kontextust gyujt, es opcionalisan klinikai szintezist general L5 modellel.

**Request**:
```json
{
  "results": [
    {"parameter_name": "Hemoglobin", "value": 10.2, "unit": "g/dL"},
    {"parameter_name": "WBC", "value": 14.5, "unit": "x10^9/L", "loinc_code": "6690-2"},
    {"parameter_name": "ALT", "value": 120, "unit": "U/L"},
    {"parameter_name": "TSH", "value": 0.15, "unit": "mIU/L"},
    {"parameter_name": "Glucose (fasting)", "value": 6.8, "unit": "mmol/L"}
  ],
  "patient": {
    "age_years": 45,
    "sex": "female",
    "is_pregnant": false,
    "condition": "fasting"
  },
  "include_rag": true,
  "language": "hu"
}
```

**Response (200)**:
```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "analyzed_results": [
    {
      "parameter_name": "Hemoglobin",
      "loinc_code": "718-7",
      "value": 10.2,
      "unit": "g/dL",
      "flag": "LOW",
      "reference_range": {
        "low": 12.0,
        "high": 16.0,
        "unit": "g/dL",
        "source": "Mayo Clinic Laboratories",
        "age_min": 18,
        "age_max": 60,
        "sex": "female"
      },
      "interpretation": "Enyhén csökkent. Vas-, B12- vagy folsavhiány lehetősége. Terhesség kizárva."
    },
    {
      "parameter_name": "TSH",
      "loinc_code": "3016-3",
      "value": 0.15,
      "unit": "mIU/L",
      "flag": "LOW",
      "reference_range": {
        "low": 0.4,
        "high": 4.0,
        "unit": "mIU/L",
        "source": "AACE Guidelines 2022",
        "sex": "any"
      },
      "interpretation": "Szupprimált TSH — hyperthyreosis gyanúja. Szabad T4, T3 meghatározása javasolt."
    }
  ],
  "clinical_context": {
    "abnormal_params": ["Hemoglobin", "WBC", "ALT", "TSH", "Glucose (fasting)"],
    "rag_citations": [
      {
        "chunk_id": "abc-def-123",
        "document_title": "Evaluation of Anemia — AAFP Guidelines",
        "source": "American Family Physician",
        "source_type": "guideline",
        "publication_year": 2024,
        "section_heading": "Iron Deficiency Anemia — Laboratory Findings",
        "excerpt": "Serum ferritin <30 ng/mL is diagnostic for iron deficiency in non-inflammatory states..."
      }
    ],
    "synthesized_note": "A laborlelet alapján több irányú kivizsgálás szükséges. Az enyhe anaemia..."
  },
  "evaluated_at": "2026-05-19T14:30:00Z",
  "model_used": "claude-opus-4",
  "disclaimer": "This is an AI-generated analysis for research and educational purposes only. It is NOT a medical diagnosis. Always consult a qualified healthcare professional."
}
```

**Error responses**:

| Status | `error` | Scenario |
|--------|---------|----------|
| 400 | `ValidationError` | Invalid input (missing required field, out-of-range value) |
| 400 | `TooManyResults` | `results` array has >200 items |
| 404 | `ParameterNotFound` | A `parameter_name` couldn't be matched to any LOINC code |
| 422 | `NoReferenceRange` | Parameter found but no reference range matches patient demographics |
| 429 | `RateLimitExceeded` | Too many requests (30/min for /analyze) |
| 500 | `InternalError` | Unexpected server error (DB down, embedding model crash, etc.) |
| 503 | `ServiceUnavailable` | Critical dependency unavailable (PostgreSQL, Qdrant) |

### 12.5 Endpoint: POST /analyze/batch

Tobbes beteg egyideju elemzese. Az egyes betegek fuggetlenul kerulnek feldolgozasra; ha egy beteg feldolgozasa hibara fut, a tobbi folytatodik.

**Request**:
```json
{
  "patients": [
    {
      "results": [{"parameter_name": "Hemoglobin", "value": 10.2, "unit": "g/dL"}],
      "patient": {"age_years": 45, "sex": "female"},
      "language": "hu"
    },
    {
      "results": [{"parameter_name": "ALT", "value": 120, "unit": "U/L"}],
      "patient": {"age_years": 32, "sex": "male"},
      "include_rag": false
    }
  ]
}
```

**Response (200)**:
```json
{
  "request_id": "batch-abc-123",
  "results": [
    {
      "index": 0,
      "status": "success",
      "response": { /* AnalysisResponse */ }
    },
    {
      "index": 1,
      "status": "error",
      "error": {
        "error": "ParameterNotFound",
        "message": "No LOINC mapping found for 'ALT' in the knowledge base",
        "details": [],
        "request_id": null
      }
    }
  ],
  "summary": {"total": 2, "success": 1, "error": 1}
}
```

### 12.6 Endpoints: Parameter CRUD

**GET /parameters** — Search and list
```
GET /api/v1/parameters?category=Hematology&search=hemoglobin&limit=20&offset=0
GET /api/v1/parameters?search=glucose    # full-text search across name, display_name, loinc_code
GET /api/v1/parameters?specimen=Serum    # filter by specimen type
```

Response: `PaginatedResponse` wrapper + `list[LabParameterResponse]`

**GET /parameters/{id}** — Single parameter with reference range count
```
GET /api/v1/parameters/550e8400-e29b-41d4-a716-446655440000
```

**POST /parameters** — Create new parameter
```json
{
  "loinc_code": "718-7",
  "name": "Hemoglobin",
  "display_name": "Hemoglobin [Mass/volume] in Blood",
  "category": "Hematology",
  "specimen_type": "Whole Blood"
}
```

**GET /parameters/{id}/ranges** — All reference ranges for a parameter
```
GET /api/v1/parameters/550e8400.../ranges?sex=female&age=45
```

Optional query params `sex` and `age` filter to matching ranges only.

**POST /parameters/{id}/ranges** — Add a reference range
```json
{
  "source": "Mayo Clinic Laboratories",
  "low": 12.0,
  "high": 16.0,
  "unit": "g/dL",
  "sex": "female",
  "age_min_years": 18,
  "age_max_years": 60,
  "confidence": 0.9
}
```

### 12.7 Endpoints: Document Management

**POST /documents/upload** — Upload PDF for clinical knowledge ingestion
```
Content-Type: multipart/form-data

Fields:
  file:        (binary)    PDF file, max 100 MB
  title:       (string)    Human-readable title (optional, extracted from PDF metadata if omitted)
  source:      (string)    Source identifier, e.g. "NICE Guidelines NG28"
  source_type: (string)    One of: guideline, textbook, research, drug_label
  authors:     (string)    Optional, semicolon-separated
  year:        (int)       Optional publication year
```

Response (202 Accepted):
```json
{
  "document_id": "uuid",
  "title": "NICE NG28 — Type 2 Diabetes in Adults: Management",
  "page_count": 84,
  "status": "queued",
  "queued_at": "2026-05-19T14:35:00Z"
}
```

Note: The upload only queues the document for background processing. Use `GET /documents/{id}` to check status.

**GET /documents/{id}** — Document processing status
```json
{
  "document_id": "uuid",
  "title": "NICE NG28",
  "status": "completed",
  "page_count": 84,
  "chunk_count": 312,
  "error": null,
  "ingested_at": "2026-05-19T14:38:00Z"
}
```

Statuses: `queued` → `processing` → `completed` | `failed`

**GET /documents** — List all documents
```
GET /api/v1/documents?source_type=guideline&limit=20&offset=0
GET /api/v1/documents?status=failed    # find failed ingestions for retry
```

### 12.8 Endpoints: Ingestion Triggers

**POST /ingest/structured** — Trigger structured data scrape
```json
{
  "sources": ["mayo_clinic", "labtests_online"],  // omit for all sources
  "full_rescrape": false                           // true = re-scrape everything, false = only new/updated
}
```

**POST /ingest/unstructured** — Trigger document processing
```json
{
  "document_ids": ["uuid1", "uuid2"],  // process specific documents; omit to process all queued
  "force_reembed": false               // true = re-embed even if already completed
}
```

Both return:
```json
{
  "pipeline": "structured",
  "status": "triggered",
  "flow_run_id": "prefect-flow-run-uuid",
  "message": "Prefect flow dispatched. Monitor at http://localhost:4200"
}
```

### 12.9 Endpoint: Health Check

**GET /health** — No auth. Used by Docker healthcheck, load balancers, monitoring.

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "uptime": 86400.5,
  "components": {
    "postgres": "healthy",
    "qdrant": "healthy",
    "redis": "healthy",
    "ollama": "healthy",
    "prefect": "unavailable"
  }
}
```

Component checks:
- **postgres**: `SELECT 1` ping
- **qdrant**: `/health` endpoint on Qdrant REST API
- **redis**: `PING` command
- **ollama**: `GET /api/tags` on Ollama
- **prefect**: Prefect server `/api/health` (marked "unavailable" if not running, not a critical dependency for analysis)

Aggregate status: "healthy" (all critical healthy), "degraded" (1+ non-critical unhealthy, no critical unhealthy), "unhealthy" (any critical unhealthy). Critical: postgres, qdrant. Non-critical: redis, ollama, prefect.

### 12.10 API Key Authentication (Optional)

Az API kulcs authentikacio opcionalis — lokalis fejlesztesnel es szemelyes hasznalatnal kikapcsolhato.

```python
# src/api/auth.py

from fastapi import Security, HTTPException
from fastapi.security import APIKeyHeader
from src.config import settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def require_api_key(api_key: str | None = Security(api_key_header)) -> str:
    """Require valid API key. Passes through if auth is disabled."""
    if not settings.API_AUTH_ENABLED:
        return "anonymous"
    if api_key is None:
        raise HTTPException(status_code=401, detail="X-API-Key header required")
    if api_key not in settings.API_KEYS:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return api_key

async def optional_api_key(api_key: str | None = Security(api_key_header)) -> str:
    """Optionally validate API key. Always returns 'anonymous' if auth disabled."""
    if not settings.API_AUTH_ENABLED:
        return "anonymous"
    if api_key is None:
        return "anonymous"
    if api_key not in settings.API_KEYS:
        return "anonymous"  # Don't block read endpoints, just don't elevate
    return api_key
```

Configuration in `src/config.py`:
```python
class Settings(BaseSettings):
    API_AUTH_ENABLED: bool = False           # Set True in production
    API_KEYS: set[str] = set()              # Valid API keys from .env

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
```

### 12.11 Rate Limiting

Redis-alapu sliding window rate limit, slowapi middleware-en keresztul:

```python
# src/api/main.py
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request
from fastapi.responses import JSONResponse

limiter = Limiter(key_func=get_remote_address)

app = FastAPI()
app.state.limiter = limiter

@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={
            "error": "RateLimitExceeded",
            "message": f"Too many requests. Retry after {exc.retry_after} seconds.",
            "details": [],
            "request_id": request.state.request_id if hasattr(request.state, "request_id") else None
        },
        headers={"Retry-After": str(exc.retry_after)}
    )
```

Rate limits (configurable in settings):
- `/analyze`: 30/minute per IP
- `/analyze/batch`: 5/minute per IP (heavier, involves multiple LLM calls)
- `/parameters` (GET): 60/minute per IP
- `/parameters` (POST/PUT/DELETE): 30/minute per IP, requires API key
- `/documents/upload`: 10/minute per IP, requires API key
- `/ingest/*`: 5/minute per IP, requires API key
- `/health`: 120/minute (no auth, used by probes)

---

## 13. Test Strategy

### 13.1 Test Pyramid & Philosophy

```
         ╱──────╲
        ╱  E2E   ╲          ~10 tests — full pipeline from upload to analysis
       ╱──────────╲
      ╱ Integration╲        ~40 tests — DB queries, Qdrant ops, API endpoints
     ╱──────────────╲
    ╱   Unit Tests    ╲     ~200+ tests — individual functions, models, validators
   ╱────────────────────╲
```

**Alapelv**: 
- A teszteles L1/L2 lokalis modellekkel tortenik a Pi-ben, nem draga cloud modellekkel
- Minden teszt determinisztikus kell legyen — nincs LLM hivas, nincs valodi web scraping, nincs kulsos API
- Mock/monkeypatch strategia: ami kimegy a folyamatbol, azt mockoljuk
- Teszt adatok: valos laborleletek anonimizalt valtozatai + szintetikus edge case-ek
- Minden teszt <100ms futasi ido (unit), <2s (integration), <30s (e2e)

### 13.2 Test Infrastructure

```python
# conftest.py — shared fixtures

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from src.db.models import Base

TEST_DB_URL = "postgresql+asyncpg://test:test@localhost:5432/test_med_lab"

@pytest.fixture(scope="session")
def event_loop():
    """Session-scoped event loop for async tests."""
    import asyncio
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="function")
async def db_session():
    """Fresh DB with schema per test function. Auto-rollback after each test."""
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        yield session
        await session.rollback()
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()

@pytest.fixture
def sample_lab_parameters():
    """Battery of known lab parameters with reference ranges."""
    return [
        {"loinc_code": "718-7", "name": "Hemoglobin", "category": "Hematology", "specimen_type": "Whole Blood"},
        {"loinc_code": "6690-2", "name": "WBC", "category": "Hematology", "specimen_type": "Whole Blood"},
        {"loinc_code": "1742-6", "name": "ALT", "category": "Chemistry", "specimen_type": "Serum"},
        {"loinc_code": "3016-3", "name": "TSH", "category": "Endocrinology", "specimen_type": "Serum"},
        {"loinc_code": "2345-7", "name": "Glucose", "category": "Chemistry", "specimen_type": "Serum"},
        {"loinc_code": "20565-8", "name": "Fasting Glucose", "category": "Chemistry", "specimen_type": "Plasma"},
    ]

@pytest.fixture
def sample_reference_ranges():
    """Reference ranges matched to the above parameters."""
    return [
        {"parameter_loinc": "718-7", "low": 12.0, "high": 16.0, "unit": "g/dL", "sex": "female", "age_min": 18, "age_max": 60},
        {"parameter_loinc": "718-7", "low": 13.5, "high": 17.5, "unit": "g/dL", "sex": "male", "age_min": 18, "age_max": 60},
        {"parameter_loinc": "6690-2", "low": 4.0, "high": 11.0, "unit": "x10^9/L", "sex": "any", "age_min": 0, "age_max": 120},
        {"parameter_loinc": "1742-6", "low": 7.0, "high": 56.0, "unit": "U/L", "sex": "any", "age_min": 0, "age_max": 120},
        {"parameter_loinc": "3016-3", "low": 0.4, "high": 4.0, "unit": "mIU/L", "sex": "any", "age_min": 0, "age_max": 120},
        {"parameter_loinc": "20565-8", "low": 3.9, "high": 5.6, "unit": "mmol/L", "sex": "any", "age_min": 0, "age_max": 120, "condition": "fasting"},
    ]

@pytest.fixture
def mock_embedder(monkeypatch):
    """Replace the real embedder with a deterministic mock returning fixed 768-d vectors."""
    import numpy as np
    
    def fake_embed(texts: list[str]) -> list[list[float]]:
        rng = np.random.RandomState(hash(tuple(texts)) % 2**31)
        return [
            (rng.randn(768) / 10).tolist()
            for _ in texts
        ]
    
    monkeypatch.setattr("src.ingestion.unstructured.embedder.embed_batch", fake_embed)
    return fake_embed

@pytest.fixture
def mock_qdrant(monkeypatch):
    """Replace Qdrant client with in-memory fake."""
    from tests.mocks import FakeQdrantClient
    client = FakeQdrantClient()
    monkeypatch.setattr("src.ingestion.unstructured.qdrant_store.get_client", lambda: client)
    monkeypatch.setattr("src.engine.rag.get_qdrant_client", lambda: client)
    return client

@pytest.fixture
def mock_llm(monkeypatch):
    """Replace clinical synthesis LLM with deterministic fake."""
    async def fake_synthesize(prompt: str, model: str = "claude-opus-4") -> str:
        return "TEST: This is a mock clinical synthesis response."
    
    monkeypatch.setattr("src.engine.llm.synthesize", fake_synthesize)
    return fake_synthesize

@pytest.fixture
def mock_loinc_api(monkeypatch):
    """Replace LOINC API calls with local lookup table."""
    LOINC_LOOKUP = {
        "Hemoglobin": "718-7",
        "WBC": "6690-2",
        "ALT": "1742-6",
        "TSH": "3016-3",
        "Glucose": "2345-7",
    }
    async def fake_loinc_lookup(name: str) -> str | None:
        return LOINC_LOOKUP.get(name)
    
    monkeypatch.setattr("src.ingestion.structured.normalizer.lookup_loinc", fake_loinc_lookup)
    return fake_loinc_lookup
```

### 13.3 Fakie Mock Implementations

`tests/mocks.py` — lightweight in-memory fakes for external dependencies:

```python
# tests/mocks.py

import uuid
from collections import defaultdict

class FakeQdrantClient:
    """In-memory Qdrant that mirrors the real client's search/upsert API."""
    
    def __init__(self):
        self.collections: dict[str, list[dict]] = defaultdict(list)
    
    def search(self, collection_name: str, query_vector: list[float], limit: int = 5, 
               query_filter: dict | None = None, with_payload: bool = True):
        """Fake cosine similarity search. Returns deterministic ordering."""
        points = self.collections.get(collection_name, [])
        # Sort by naive "similarity" — for tests we just return first N
        results = []
        for i, pt in enumerate(points[:limit]):
            results.append(type('ScoredPoint', (), {
                'id': pt['id'],
                'score': 0.95 - i * 0.05,
                'payload': pt.get('payload', {})
            })())
        return results
    
    def upsert(self, collection_name: str, points: list[dict]):
        existing_ids = {p['id'] for p in self.collections[collection_name]}
        for pt in points:
            if pt['id'] in existing_ids:
                for i, existing in enumerate(self.collections[collection_name]):
                    if existing['id'] == pt['id']:
                        self.collections[collection_name][i] = pt
                        break
            else:
                self.collections[collection_name].append(pt)
    
    def create_collection(self, collection_name: str, vectors_config: dict):
        pass  # auto-created on first upsert
    
    def collection_exists(self, collection_name: str) -> bool:
        return collection_name in self.collections

class FakeRedisClient:
    """In-memory Redis for rate limiting and caching tests."""
    
    def __init__(self):
        self.store: dict[str, str] = {}
        self.expiry: dict[str, float] = {}
    
    def get(self, key: str) -> bytes | None:
        import time
        if key in self.expiry and time.time() > self.expiry[key]:
            del self.store[key]
            del self.expiry[key]
            return None
        val = self.store.get(key)
        return val.encode() if val else None
    
    def set(self, key: str, value: str, ex: int | None = None):
        import time
        self.store[key] = value
        if ex:
            self.expiry[key] = time.time() + ex
    
    def incr(self, key: str) -> int:
        val = int(self.store.get(key, 0)) + 1
        self.store[key] = str(val)
        return val
    
    def expire(self, key: str, seconds: int):
        import time
        self.expiry[key] = time.time() + seconds
    
    def ping(self) -> bool:
        return True
```

### 13.4 Unit Test Matrix

`tests/` konyvtar struktura es tesztelt komponensek:

```
tests/
├── conftest.py                    # Shared fixtures
├── mocks.py                       # Fake clients (Qdrant, Redis, LLM, Embedder)
├── test_models/                   # SQLAlchemy + Pydantic model tests
│   ├── test_lab_parameters.py     # LabParameter model constraints
│   ├── test_reference_ranges.py   # ReferenceRange validation rules
│   ├── test_clinical_documents.py # ClinicalDocument + ClinicalChunk
│   └── test_schemas.py            # Pydantic request/response validation
├── test_ingestion/
│   ├── structured/
│   │   ├── test_normalizer.py     # Unit conversion, LOINC mapping
│   │   ├── test_resolver.py       # Conflict resolution logic
│   │   ├── test_spiders/          # Spider selector extraction
│   │   │   ├── test_base.py       # AbstractSpider framework
│   │   │   ├── test_labtests_online.py
│   │   │   └── test_mayo_clinic.py
│   │   └── test_pipeline.py       # Prefect flow DAG structure
│   └── unstructured/
│       ├── test_pdf_parser.py     # Text extraction from PDF fixtures
│       ├── test_chunker.py        # Semantic boundary detection
│       ├── test_embedder.py       # Embedding vector shape + determinism
│       └── test_qdrant_store.py   # Upsert/search operations
├── test_engine/
│   ├── test_analyzer.py           # Flag detection: NORMAL/LOW/HIGH/CRITICAL
│   ├── test_rag.py                # Query construction, citation dedup
│   └── test_llm.py                # LLM client routing (local vs cloud)
├── test_api/
│   ├── test_analyze.py            # POST /analyze full request-response
│   ├── test_analyze_batch.py      # Batch processing + partial failure
│   ├── test_parameters.py         # CRUD endpoints
│   ├── test_documents.py          # Upload + status tracking
│   ├── test_ingest_triggers.py    # Ingestion trigger endpoints
│   ├── test_health.py             # Health check + component status
│   ├── test_auth.py               # API key validation
│   └── test_rate_limit.py         # Rate limit enforcement
└── test_e2e/
    ├── test_structured_pipeline.py # Full scrape → normalize → upsert
    ├── test_unstructured_pipeline.py # PDF → chunk → embed → upsert
    └── test_analysis_flow.py       # Upload → analyze → verify output
```

### 13.5 Unit Test Priorities & Examples

#### L1 — Determinisztikus logika (nincs kulso fuggoseg)

```python
# tests/test_engine/test_analyzer.py

import pytest
from src.engine.analyzer import flag_value, Flag

class TestFlagValue:
    """A flag_value() tisztan determinisztikus — nincs DB, nincs halozat."""

    @pytest.mark.parametrize("value, low, high, expected", [
        # NORMAL — value inside range
        (14.0, 12.0, 16.0, Flag.NORMAL),
        (12.0, 12.0, 16.0, Flag.NORMAL),  # inclusive lower bound
        (16.0, 12.0, 16.0, Flag.NORMAL),  # inclusive upper bound
        
        # LOW
        (11.9, 12.0, 16.0, Flag.LOW),
        (5.0, 12.0, 16.0, Flag.LOW),
        (0.0, 12.0, 16.0, Flag.LOW),
        
        # HIGH
        (16.1, 12.0, 16.0, Flag.HIGH),
        (50.0, 12.0, 16.0, Flag.HIGH),
        
        # CRITICAL — outside critical thresholds (configurable panic ranges)
        # Hemoglobin: critical_low=7.0, critical_high=20.0
        (6.5, 12.0, 16.0, Flag.CRITICAL),   # critical_low
        (25.0, 12.0, 16.0, Flag.CRITICAL),  # critical_high
        
        # UNKNOWN — no reference range available
        (14.0, None, None, Flag.UNKNOWN),
        (14.0, 12.0, None, Flag.UNKNOWN),
    
        # Negative values (should be flagged, some labs report <0.01 as negative)
        (-1.0, 0.0, 10.0, Flag.LOW),
    ])
    def test_flag_cases(self, value, low, high, expected):
        assert flag_value(value, low, high) == expected

    def test_flag_with_critical_thresholds(self):
        """Custom critical thresholds override defaults."""
        thresholds = {"critical_low": 5.0, "critical_high": 50.0}
        assert flag_value(5.5, 10.0, 40.0, thresholds) == Flag.LOW    # not critical
        assert flag_value(4.5, 10.0, 40.0, thresholds) == Flag.CRITICAL
```

#### L2 — Unit konverzio logika (edge case heavy)

```python
# tests/test_ingestion/structured/test_normalizer.py

class TestUnitNormalizer:
    """Unit conversion requires handling edge cases: missing UCUM, non-standard units, rounding."""

    @pytest.mark.parametrize("value, from_unit, to_unit, expected, tolerance", [
        # Standard conversions
        (100, "mg/dL", "g/L", 1.0, 0.01),
        (1.0, "g/L", "mg/dL", 100, 0.01),
        (1.0, "mmol/L", "mg/dL", 18.018, 0.01),     # glucose
        (5.0, "mg/dL", "mmol/L", 0.277, 0.001),      # glucose reverse
        (1.0, "mg/dL", "µmol/L", 88.4, 0.1),         # creatinine
        
        # Same unit — no conversion
        (14.0, "g/dL", "g/dL", 14.0, 0),
        (100, "U/L", "U/L", 100, 0),
        
        # SI prefixes
        (1.0, "g/L", "mg/dL", 100, 0.01),
        (1000, "µg/L", "mg/L", 1.0, 0.01),
        (500, "pg/mL", "ng/L", 500, 0.01),
    ])
    def test_standard_conversions(self, value, from_unit, to_unit, expected, tolerance):
        result = convert_unit(value, from_unit, to_unit)
        assert abs(result - expected) <= tolerance
    
    def test_unknown_unit_raises(self):
        """Unrecognized unit string should raise clear error."""
        with pytest.raises(UnitConversionError, match="Unknown unit: 'femtoblob'"):
            convert_unit(10.0, "femtoblob", "mg/dL")
    
    def test_incompatible_units_raises(self):
        """Mass vs activity units can't be converted."""
        with pytest.raises(UnitConversionError, match="Incompatible dimensions"):
            convert_unit(10.0, "g/dL", "U/L")  # mass vs enzyme activity
    
    def test_percentage_units(self):
        """Percentage units (%): treat as dimensionless, pass through."""
        result = convert_unit(45.0, "%", "%")
        assert result == 45.0
    
    def test_rounding_precision(self):
        """Results should maintain reasonable precision (4 decimal places)."""
        result = convert_unit(1.0, "mg/dL", "mmol/L")  # creatinine: /88.4
        assert result == pytest.approx(0.0113, abs=0.0001)
```

### 13.6 Integration Test Examples

```python
# tests/test_api/test_analyze.py

import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import app

@pytest.mark.asyncio
class TestAnalyzeEndpoint:
    """Integration test: real FastAPI app with real DB, mock'd external services."""

    @pytest.fixture
    async def client(self, db_session, mock_llm, mock_embedder, mock_qdrant):
        """Override DB session dependency with test session."""
        from src.db.session import get_session
        async def override_get_session():
            yield db_session
        app.dependency_overrides[get_session] = override_get_session
        
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac
        app.dependency_overrides.clear()

    async def test_analyze_normal_results(self, client, db_session):
        """All results within normal range → flag=NORMAL, no RAG context if all normal."""
        # Seed DB with parameters and ranges
        await seed_parameters(db_session, sample_lab_parameters)
        await seed_reference_ranges(db_session, sample_reference_ranges)
        
        response = await client.post("/api/v1/analyze", json={
            "results": [
                {"parameter_name": "Hemoglobin", "value": 14.0, "unit": "g/dL"},
                {"parameter_name": "WBC", "value": 7.0, "unit": "x10^9/L"},
            ],
            "patient": {"age_years": 30, "sex": "female"},
            "include_rag": True,
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["analyzed_results"][0]["flag"] == "NORMAL"
        assert data["analyzed_results"][1]["flag"] == "NORMAL"
        assert data["clinical_context"] is not None
        assert data["clinical_context"]["abnormal_params"] == []

    async def test_analyze_abnormal_with_rag(self, client, db_session):
        """Abnormal results trigger RAG retrieval and clinical synthesis."""
        await seed_parameters(db_session, sample_lab_parameters)
        await seed_reference_ranges(db_session, sample_reference_ranges)
        
        response = await client.post("/api/v1/analyze", json={
            "results": [
                {"parameter_name": "Hemoglobin", "value": 7.0, "unit": "g/dL"},
                {"parameter_name": "ALT", "value": 200, "unit": "U/L"},
            ],
            "patient": {"age_years": 45, "sex": "female"},
            "include_rag": True,
        })
        
        assert response.status_code == 200
        data = response.json()
        flags = {r["parameter_name"]: r["flag"] for r in data["analyzed_results"]}
        assert flags["Hemoglobin"] == "CRITICAL"
        assert flags["ALT"] == "HIGH"
        assert len(data["clinical_context"]["abnormal_params"]) == 2
        assert data["clinical_context"]["synthesized_note"] is not None
    
    async def test_analyze_no_rag_flag(self, client, db_session):
        """include_rag=False → clinical_context is null."""
        await seed_parameters(db_session, sample_lab_parameters)
        await seed_reference_ranges(db_session, sample_reference_ranges)
        
        response = await client.post("/api/v1/analyze", json={
            "results": [{"parameter_name": "ALT", "value": 200, "unit": "U/L"}],
            "include_rag": False,
        })
        
        assert response.status_code == 200
        assert response.json()["clinical_context"] is None
    
    async def test_analyze_parameter_not_found(self, client, db_session):
        """Unknown parameter → 404 with ParameterNotFound."""
        response = await client.post("/api/v1/analyze", json={
            "results": [{"parameter_name": "MadeUpTest", "value": 100, "unit": "U/L"}],
        })
        
        assert response.status_code == 404
        assert response.json()["error"] == "ParameterNotFound"
    
    async def test_analyze_empty_results(self, client):
        """Zero results → validation error."""
        response = await client.post("/api/v1/analyze", json={
            "results": [],
        })
        assert response.status_code == 422 or response.status_code == 400
    
    async def test_analyze_too_many_results(self, client):
        """>200 results → TooManyResults error."""
        results = [{"parameter_name": "ALT", "value": 50, "unit": "U/L"}] * 201
        response = await client.post("/api/v1/analyze", json={"results": results})
        assert response.status_code == 400
        assert response.json()["error"] == "TooManyResults"
    
    async def test_analyze_language_hint(self, client, db_session):
        """language='hu' produces Hungarian output text."""
        await seed_parameters(db_session, sample_lab_parameters)
        await seed_reference_ranges(db_session, sample_reference_ranges)
        
        response = await client.post("/api/v1/analyze", json={
            "results": [{"parameter_name": "Hemoglobin", "value": 7.0, "unit": "g/dL"}],
            "patient": {"age_years": 30, "sex": "female"},
            "language": "hu",
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["disclaimer"]  # should still be present
        # interpretation text should be in Hungarian (depends on mock_llm setup)
```

### 13.7 E2E Test — Full Pipeline

```python
# tests/test_e2e/test_analysis_flow.py

@pytest.mark.e2e
@pytest.mark.asyncio
class TestEndToEndAnalysisFlow:
    """Full flow: seed parameters → upload PDF → trigger ingestion → analyze (requires real services)."""
    
    async def test_full_cycle(self, client, db_session, test_pdf_path):
        """Complete cycle from parameter seeding through analysis."""
        # 1. Seed lab parameters via API
        for param in sample_lab_parameters:
            resp = await client.post("/api/v1/parameters", json=param)
            assert resp.status_code == 201
        
        # 2. Upload a clinical PDF
        with open(test_pdf_path, "rb") as f:
            files = {"file": ("guideline.pdf", f, "application/pdf")}
            data = {"source": "NICE Guidelines", "source_type": "guideline", "year": 2024}
            resp = await client.post("/api/v1/documents/upload", files=files, data=data)
        assert resp.status_code == 202
        doc_id = resp.json()["document_id"]
        
        # 3. Trigger unstructured ingestion
        resp = await client.post("/api/v1/ingest/unstructured", json={"document_ids": [doc_id]})
        assert resp.status_code == 200
        
        # 4. Wait for ingestion (poll status)
        import asyncio
        for _ in range(30):  # max 30 seconds
            resp = await client.get(f"/api/v1/documents/{doc_id}")
            if resp.json()["status"] == "completed":
                break
            await asyncio.sleep(1)
        assert resp.json()["status"] == "completed"
        
        # 5. Analyze lab results
        resp = await client.post("/api/v1/analyze", json={
            "results": [
                {"parameter_name": "Hemoglobin", "value": 7.0, "unit": "g/dL"},
                {"parameter_name": "TSH", "value": 0.15, "unit": "mIU/L"},
            ],
            "patient": {"age_years": 45, "sex": "female"},
            "include_rag": True,
            "language": "en",
        })
        
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["analyzed_results"]) == 2
        assert data["clinical_context"]["rag_citations"]  # should have citations from the PDF
```

### 13.8 Coverage Targets

| Layer | Coverage Target | Enforcement |
|-------|----------------|-------------|
| `src/engine/` | 95%+ | CI gate, fail below 90% |
| `src/ingestion/` | 85%+ | CI gate, fail below 80% |
| `src/api/` | 90%+ | CI gate, fail below 85% |
| `src/db/` | 80%+ | CI warning below 80% |
| `src/config.py` | 100% | CI gate |

Medical-critical paths (`engine/analyzer.py`, `engine/rag.py` clinical safety boundary) — 100% coverage mandatory.

### 13.9 Fixture Data Strategy

**Test PDFs**: `tests/fixtures/pdfs/`

| File | Description | Pages | Content |
|------|-------------|-------|---------|
| `anemia_guidelines.pdf` | AAFP Iron Deficiency Anemia, 2024 | 12 | Structured headings, tables |
| `diabetes_nice_ng28.pdf` | NICE NG28 Type 2 Diabetes, 2023 | 84 | Multi-section, complex tables |
| `thyroid_aace.pdf` | AACE Thyroid Guidelines, 2022 | 35 | Algorithm flowcharts, lab values |
| `scanned_report.pdf` | Old scanned lab manual (OCR test) | 3 | Image-based, no selectable text |
| `minimal.pdf` | Single page, no metadata | 1 | Edge case: missing title/author |

**Test HTML fixtures**: `tests/fixtures/html/`

| File | Source | Content |
|------|--------|---------|
| `labtests_online_hemoglobin.html` | labtestsonline.org | Hemoglobin test page snapshot |
| `mayo_clinic_alt.html` | mayocliniclabs.com | ALT test catalog entry |
| `arup_tsh.html` | aruplab.com | TSH test page |
| `error_page.html` | — | 404/503 test cases |
| `empty_table.html` | — | Page with no data tables |

**Test JSON data**: `tests/fixtures/data/`

| File | Content |
|------|---------|
| `patient_lab_results.json` | 50 anonymized patient result sets |
| `loinc_mapping.json` | Local LOINC lookup table (200 entries) |
| `bad_inputs.json` | Malformed JSON, SQL injection attempts, XSS |
| `batch_requests.json` | Batch request payloads (1, 5, 50 patients) |

### 13.10 CI Integration

```yaml
# .github/workflows/test.yml — unit + integration
- name: Run tests
  run: |
    python -m pytest tests/ \
      -v --tb=short \
      --cov=src --cov-report=term --cov-report=html \
      --cov-fail-under=80 \
      -m "not e2e" \
      -n auto
```

E2E tests run separately (require real services):
```yaml
# .github/workflows/test-e2e.yml
- name: Start services
  run: docker compose -f docker-compose.test.yml up -d
- name: Run E2E tests
  run: python -m pytest tests/test_e2e/ -v -m e2e
```

### 13.11 Medical-Specific Testing Concerns

| Concern | Test Approach |
|---------|--------------|
| **Numerical precision** | Test float comparisons with `pytest.approx(rel=1e-4)`; lab values must not lose precision in unit conversion |
| **Reference range aged-based interpolation** | Test boundary ages (0, 1 day, 1 month, 1 year, 18 years, 65 years) |
| **Sex-specific ranges** | Test male, female, any — verify correct range is selected |
| **Pregnancy ranges** | Test non-pregnant vs pregnant for hCG, TSH, iron panels |
| **Critical value thresholds** | Verify panic-range alerts fire at exactly the right boundaries |
| **LOINC code collisions** | Test that same name with different LOINC codes (e.g., Glucose vs Fasting Glucose) routes correctly |
| **PDF text extraction accuracy** | Assert extracted text for a known PDF fixture matches golden file; catch regressions in pymupdf upgrades |
| **RAG hallucination detection** | Test that synthesized note always cites source chunks, never fabricates citations |
| **Clinical disclaimer** | Assert every AnalysisResponse includes the disclaimer field with the exact mandatory text |
| **De-identification** | Test that PHI (names, DOBs, MRNs) is stripped from input before storage |

---

## 14. Error Handling Strategy

### 14.1 Error Taxonomy

Minden hibat egyseges strukturaban kerul rogzitesre — legyen szo API response-rol, pipeline hibarol, vagy belso exceptionrol.

```
ErrorCategory
├── VALIDATION        # Input data invalid, fixable by caller
├── NOT_FOUND         # Resource doesn't exist
├── CONFLICT          # State conflict (duplicate, stale)
├── UNAVAILABLE       # Dependent service down
├── TIMEOUT           # Operation exceeded deadline
├── RATE_LIMITED      # Too many requests
├── QUOTA_EXCEEDED    # LLM API quota exhausted
├── DATA_QUALITY      # Data in unexpected format (not invalid, just surprising)
└── INTERNAL          # Unexpected system failure
```

### 14.2 Exception Hierarchy

```python
# src/errors.py

class MedLabError(Exception):
    """Base exception for all application errors."""
    def __init__(self, message: str, category: str, *, details: dict | None = None):
        self.message = message
        self.category = category
        self.details = details or {}
        super().__init__(message)

class ValidationError(MedLabError):
    def __init__(self, message: str, **kwargs):
        super().__init__(message, "VALIDATION", **kwargs)

class ParameterNotFoundError(MedLabError):
    def __init__(self, parameter_name: str, **kwargs):
        super().__init__(f"Parameter '{parameter_name}' not found", "NOT_FOUND", 
                        details={"parameter_name": parameter_name}, **kwargs)

class NoReferenceRangeError(MedLabError):
    def __init__(self, parameter_name: str, demographics: dict, **kwargs):
        super().__init__(
            f"No reference range for '{parameter_name}' matching demographics",
            "NOT_FOUND",
            details={"parameter_name": parameter_name, "demographics": demographics},
            **kwargs
        )

class LoincMappingError(MedLabError):
    def __init__(self, parameter_name: str, **kwargs):
        super().__init__(f"Cannot map '{parameter_name}' to LOINC code", "NOT_FOUND",
                        details={"parameter_name": parameter_name}, **kwargs)

class UnitConversionError(MedLabError):
    def __init__(self, value: float, from_unit: str, to_unit: str, reason: str = "", **kwargs):
        super().__init__(
            f"Cannot convert {value} from '{from_unit}' to '{to_unit}': {reason}",
            "VALIDATION",
            details={"value": value, "from_unit": from_unit, "to_unit": to_unit, "reason": reason},
            **kwargs
        )

class ServiceUnavailableError(MedLabError):
    def __init__(self, service: str, **kwargs):
        super().__init__(f"Service '{service}' is unavailable", "UNAVAILABLE",
                        details={"service": service}, **kwargs)

class TooManyResultsError(MedLabError):
    def __init__(self, count: int, max_allowed: int, **kwargs):
        super().__init__(f"Too many results: {count} (max {max_allowed})", "VALIDATION",
                        details={"count": count, "max_allowed": max_allowed})

class ScrapingError(MedLabError):
    def __init__(self, source: str, url: str, reason: str = "", **kwargs):
        super().__init__(f"Scraping '{source}' at {url} failed: {reason}", "UNAVAILABLE",
                        details={"source": source, "url": url, "reason": reason})

class PDFParseError(MedLabError):
    def __init__(self, file_path: str, reason: str = "", **kwargs):
        super().__init__(f"PDF parse failed for '{file_path}': {reason}", "DATA_QUALITY",
                        details={"file_path": file_path, "reason": reason})

class EmbeddingError(MedLabError):
    def __init__(self, reason: str = "", **kwargs):
        super().__init__(f"Embedding generation failed: {reason}", "INTERNAL",
                        details={"reason": reason})

class LLMError(MedLabError):
    def __init__(self, model: str, reason: str = "", **kwargs):
        super().__init__(f"LLM call to '{model}' failed: {reason}", "UNAVAILABLE",
                        details={"model": model, "reason": reason})

class QuotaExceededError(MedLabError):
    def __init__(self, model: str, provider: str, **kwargs):
        super().__init__(f"Quota exceeded for {provider}/{model}", "QUOTA_EXCEEDED",
                        details={"model": model, "provider": provider})
```

### 14.3 FastAPI Exception Handlers

```python
# src/api/error_handlers.py

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from src.errors import (
    MedLabError, ValidationError, ParameterNotFoundError,
    NoReferenceRangeError, ServiceUnavailableError, TooManyResultsError
)

def register_error_handlers(app: FastAPI):
    
    @app.exception_handler(MedLabError)
    async def medlab_error_handler(request: Request, exc: MedLabError):
        status_map = {
            "VALIDATION": 400,
            "NOT_FOUND": 404,
            "CONFLICT": 409,
            "UNAVAILABLE": 503,
            "TIMEOUT": 504,
            "RATE_LIMITED": 429,
            "QUOTA_EXCEEDED": 429,
            "DATA_QUALITY": 422,
            "INTERNAL": 500,
        }
        status = status_map.get(exc.category, 500)
        return JSONResponse(
            status_code=status,
            content={
                "error": type(exc).__name__,
                "message": exc.message,
                "details": exc.details,
                "request_id": getattr(request.state, "request_id", None),
            }
        )
    
    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        """Catch-all for unexpected errors. Log full traceback; return sanitized message."""
        import traceback, logging
        logger = logging.getLogger("medlab.api")
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        
        return JSONResponse(
            status_code=500,
            content={
                "error": "InternalError",
                "message": "An unexpected error occurred. The error has been logged.",
                "details": [],
                "request_id": getattr(request.state, "request_id", None),
            }
        )
```

### 14.4 Pipeline Error Recovery

A Prefect pipeline-oknak reszletes retry es recovery strategiaja van:

```python
# src/ingestion/structured/pipeline.py

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.retries import exponential_backoff
from datetime import timedelta

@task(
    retries=5,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    retry_jitter_factor=0.5,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
async def scrape_source(source_name: str, url: str) -> list[dict]:
    """Scrape a single source with 5 retries, exponential backoff, 1h result cache."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
        return parse_page(response.text, source_name)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            retry_after = int(e.response.headers.get("Retry-After", 60))
            raise ScrapingError(source_name, url, f"Rate limited, retry after {retry_after}s")
        elif e.response.status_code in (403, 404):
            # Don't retry — permanent failure
            raise ScrapingError(source_name, url, f"HTTP {e.response.status_code} — skipping")
        raise
    except httpx.TimeoutException:
        raise ScrapingError(source_name, url, "Timeout after 30s")

@task(
    retries=3,
    retry_delay_seconds=[10, 60, 300],  # Fixed delays: 10s, 1m, 5m
)
async def normalize_parameter(raw: dict) -> dict:
    """Normalize with retry — LOINC API might be temporarily unavailable."""
    try:
        loinc_code = await lookup_loinc(raw["name"])
        return {**raw, "loinc_code": loinc_code}
    except LoincMappingError:
        # Don't retry — permanent unknown parameter
        return {**raw, "loinc_code": None, "_warning": "No LOINC mapping"}

@task(retries=2, retry_delay_seconds=30)
async def upsert_batch(parameters: list[dict]) -> int:
    """Batch upsert with retry for transient DB issues."""
    try:
        async with get_session() as session:
            count = await bulk_upsert_parameters(session, parameters)
            await session.commit()
            return count
    except AsyncPGError:
        raise  # Retry on DB connection issues

@flow(name="structured-ingestion", log_prints=True)
async def structured_ingestion_flow(
    sources: list[str] | None = None,
    full_rescrape: bool = False,
) -> dict:
    """Main structured ingestion flow with per-source isolation."""
    sources = sources or ["mayo_clinic", "labtests_online", "arup"]
    results = {"total_scraped": 0, "total_upserted": 0, "skipped": [], "errors": []}
    
    for source in sources:
        try:
            raw_data = await scrape_source(source, get_source_url(source))
            normalized = [await normalize_parameter(r) for r in raw_data]
            count = await upsert_batch(normalized)
            results["total_scraped"] += len(raw_data)
            results["total_upserted"] += count
        except ScrapingError as e:
            results["skipped"].append({"source": source, "reason": str(e)})
        except Exception as e:
            results["errors"].append({"source": source, "error": str(e)})
    
    return results
```

### 14.5 LLM Fallback Chain

A klinikai szintezishez tobb fallback szint van definialva — ha egy modell elerhetetlen, automatikusan a kovetkezore esunk vissza:

```python
# src/engine/llm.py

from enum import Enum
from dataclasses import dataclass

class LLMTier(int, Enum):
    L5_PRIMARY   = 0  # Claude Opus 4
    L5_FALLBACK  = 1  # GPT-5.3
    L4_FALLBACK  = 2  # Claude Sonnet 4 (degraded — less reasoning, still safe)
    L2_LOCAL     = 3  # Local model (NOT for clinical — only for formatting/non-clinical)

FALLBACK_CHAIN: list[tuple[LLMTier, str, str]] = [
    (LLMTier.L5_PRIMARY,  "anthropic", "claude-opus-4"),
    (LLMTier.L5_FALLBACK, "openai",    "gpt-5.3"),
    (LLMTier.L4_FALLBACK, "anthropic", "claude-sonnet-4"),   # Silently degraded
    (LLMTier.L2_LOCAL,    "ollama",    "qwen2.5-coder:32b"),  # Last resort — WITH WARNING
]

@dataclass
class LLMResponse:
    text: str
    model: str
    tier: LLMTier
    fallback_used: bool = False

async def synthesize_with_fallback(
    prompt: str,
    is_clinical: bool = False,
    max_tokens: int = 4096,
) -> LLMResponse:
    """Call LLM with fallback chain. Clinical calls stop at L4 (never use L2 for clinical)."""
    
    last_error = None
    for tier, provider, model in FALLBACK_CHAIN:
        # 🔴 SAFETY BOUNDARY: never use local models for clinical decisions
        if is_clinical and tier == LLMTier.L2_LOCAL:
            raise MedLabError(
                "All cloud clinical models unavailable. Cannot proceed with clinical synthesis using local model.",
                "UNAVAILABLE",
                details={"last_error": str(last_error), "attempted_models": len(FALLBACK_CHAIN) - 1}
            )
        
        try:
            response = await call_llm(provider, model, prompt, max_tokens)
            return LLMResponse(
                text=response,
                model=model,
                tier=tier,
                fallback_used=(tier != LLMTier.L5_PRIMARY),
            )
        except (QuotaExceededError, ServiceUnavailableError) as e:
            last_error = e
            logger.warning(f"LLM {provider}/{model} unavailable: {e}. Falling back...")
            continue
        except Exception as e:
            last_error = e
            logger.error(f"LLM {provider}/{model} unexpected error: {e}")
            continue
    
    raise MedLabError("All LLM models exhausted", "UNAVAILABLE", 
                      details={"last_error": str(last_error)})
```

### 14.6 Graceful Degradation Matrix

| Component | Primary | Degraded Mode | Offline Mode |
|-----------|---------|---------------|-------------|
| PostgreSQL | Required | — | App fails to start |
| Qdrant | Required | — | App fails to start |
| Redis | Caching + Rate limit | No cache, no rate limit (permissive) | App starts, warns |
| Ollama | Local LLM calls | Fall through to cloud L4/L5 | Only cloud models used |
| Cloud LLMs | Clinical synthesis (L5) | Fallback chain: Opus4→GPT-5.3→Sonnet4 | ⚠️ Clinical synthesis blocked |
| Prefect Server | Pipeline orchestration | Manual trigger via API only | Pipelines runnable only via direct API |
| LOINC API | Real-time mapping | Local LOINC cache (stale OK) | Only cached mappings work |
| Source websites | Fresh data | Last known good data in DB | Stale data, log warning |

```python
# src/api/health.py — reflects degradation in health endpoint

async def get_component_status() -> dict[str, str]:
    components = {}
    
    # Critical
    components["postgres"] = "healthy" if await check_postgres() else "unhealthy"
    components["qdrant"] = "healthy" if await check_qdrant() else "unhealthy"
    
    # Non-critical
    components["redis"] = "healthy" if await check_redis() else "degraded"
    components["ollama"] = "healthy" if await check_ollama() else "unavailable"
    components["prefect"] = "healthy" if await check_prefect() else "unavailable"
    
    return components

def aggregate_status(components: dict[str, str]) -> str:
    critical = {"postgres", "qdrant"}
    if any(components[c] == "unhealthy" for c in critical):
        return "unhealthy"
    if any(v == "degraded" for k, v in components.items() if k not in critical):
        return "degraded"
    return "healthy"
```

### 14.7 Structured Logging

Minden hibat strukturalt JSON formaban loggolunk, kontextus infokkal:

```python
# src/logging_config.py

import logging
import json
import uuid
from datetime import datetime, timezone

class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Attach error details if present
        if record.exc_info and record.exc_info[1]:
            exc = record.exc_info[1]
            log_entry["error"] = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
            if isinstance(exc, MedLabError):
                log_entry["error"]["category"] = exc.category
                log_entry["error"]["details"] = exc.details
        
        # Attach extra context (request_id, user, etc.)
        if hasattr(record, "request_id"):
            log_entry["request_id"] = record.request_id
        if hasattr(record, "pipeline"):
            log_entry["pipeline"] = record.pipeline
        
        return json.dumps(log_entry, default=str)
```

### 14.8 Retry Policy Summary

| Operation | Max Retries | Strategy | Backoff |
|-----------|------------|----------|---------|
| Web scraping (single page) | 5 | Exponential | 2s → 4s → 8s → 16s → 32s |
| Web scraping (rate limited) | 3 | Fixed (respect Retry-After) | Retry-After header |
| LOINC API lookup | 3 | Fixed | 10s, 60s, 300s |
| DB upsert (transient error) | 2 | Fixed | 30s |
| PDF text extraction | 1 | Single retry | 60s |
| Embedding batch | 2 | Fixed | 30s |
| Qdrant upsert | 3 | Exponential | 1s, 5s, 25s |
| LLM API call | 3 | Fallback chain (different models) | N/A |
| Health check ping | 3 | Fixed | 1s, 2s, 3s |

### 14.9 Dead Letter Queue

A pipeline hibak, amiket nem sikerult feloldani, Dead Letter Queue-ba (DLQ) kerulnek kesobbi manualis felulvizsgalatra:

```python
# src/ingestion/dlq.py

@dataclass
class DeadLetter:
    id: str                    # UUID
    source: str                # pipeline or component name
    error: str                 # error message
    payload: dict              # the original data that failed
    error_type: str            # exception class name
    timestamp: datetime
    retry_count: int = 0
    resolved: bool = False

class DeadLetterQueue:
    """Stores failed pipeline items for manual review and retry."""
    
    def __init__(self, db_session_factory):
        self.db = db_session_factory
    
    async def enqueue(self, source: str, error: Exception, payload: dict):
        async with self.db() as session:
            dl = DeadLetter(
                id=str(uuid.uuid4()),
                source=source,
                error=str(error),
                payload=payload,
                error_type=type(error).__name__,
                timestamp=datetime.now(timezone.utc),
            )
            session.add(dl)
            await session.commit()
    
    async def list_pending(self, source: str | None = None, limit: int = 50):
        """List unresolved DLQ entries for manual triage."""
        ...
    
    async def retry(self, dlq_id: str):
        """Re-attempt a failed item manually."""
        ...
```

SQL schema for DLQ:
```sql
CREATE TABLE dead_letters (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source      VARCHAR(200) NOT NULL,
    error       TEXT NOT NULL,
    payload     JSONB NOT NULL,
    error_type  VARCHAR(200),
    retry_count INT DEFAULT 0,
    resolved    BOOLEAN DEFAULT false,
    created_at  TIMESTAMPTZ DEFAULT now(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX idx_dlq_source ON dead_letters(source, resolved, created_at);
```

---

## 15. Data Validation Rules

### 15.1 Input Validation Layers

A validacio harom retegen keresztul tortenik:

```
Layer 1: Pydantic (schema szint)  →  type checks, required fields, min/max constraints
Layer 2: Domain validators        →  business rules, cross-field validation
Layer 3: DB constraints           →  UNIQUE, NOT NULL, CHECK (last line of defense)
```

### 15.2 Per-Field Validation Rules

```python
# src/api/validators.py — custom Pydantic validators

from pydantic import field_validator, model_validator
from typing import Any

# ── LabResultInput validators ──────────────────────────────────────

def validate_parameter_name(v: str) -> str:
    """Parameter names: alphanumeric + spaces + hyphens + parentheses. No special chars."""
    import re
    v = v.strip()
    if not v:
        raise ValueError("parameter_name cannot be empty")
    if len(v) > 300:
        raise ValueError(f"parameter_name too long: {len(v)} chars (max 300)")
    if re.search(r'[<>"\';&|`$\\]', v):
        raise ValueError(f"parameter_name contains invalid characters: {v!r}")
    return v

def validate_lab_value(v: float) -> float:
    """Lab values must be finite and within a sane range."""
    import math
    if math.isnan(v):
        raise ValueError("Lab value cannot be NaN")
    if math.isinf(v):
        raise ValueError("Lab value cannot be infinite")
    if v < -1e6 or v > 1e6:
        raise ValueError(f"Lab value out of plausible range: {v}")
    return v

def validate_unit(v: str) -> str:
    """Unit strings: alphanumeric + / * ^ % ( ). No arbitrary strings."""
    import re
    v = v.strip()
    if not v:
        raise ValueError("unit cannot be empty")
    if len(v) > 50:
        raise ValueError(f"unit too long: {len(v)} chars (max 50)")
    if not re.match(r'^[a-zA-Z0-9μµ/%^*()\-.× ]+$', v):
        raise ValueError(f"unit contains invalid characters: {v!r}. Use standard notation (e.g., mg/dL, µmol/L)")
    return v

def validate_loinc_code(v: str | None) -> str | None:
    """LOINC codes: NNNNN-N format (e.g., 718-7)."""
    import re
    if v is None:
        return None
    if not re.match(r'^\d{1,6}-\d$', v):
        raise ValueError(f"Invalid LOINC code format: {v!r}. Must be NNNNN-N")
    return v

# ── Patient demographics validators ────────────────────────────────

def validate_age(v: float | None) -> float | None:
    if v is None:
        return None
    if v < 0:
        raise ValueError("Age cannot be negative")
    if v > 150:
        raise ValueError(f"Age {v} exceeds maximum plausible age (150)")
    return v

# ── Reference range validators ──────────────────────────────────────

def validate_range_bounds(low: float, high: float) -> None:
    """Low must be strictly less than high. Both must be finite."""
    import math
    if math.isnan(low) or math.isnan(high):
        raise ValueError("Reference range bounds cannot be NaN")
    if low >= high:
        raise ValueError(f"Invalid range: low ({low}) must be < high ({high})")
    if low < 0 and high < 0:
        raise ValueError("Both range bounds cannot be negative — check units")

def validate_age_range(age_min: float | None, age_max: float | None, sex: str) -> None:
    """Age range sanity: min < max. Pediatric ranges (<18) flagged."""
    if age_min is not None and age_max is not None:
        if age_min >= age_max:
            raise ValueError(f"age_min ({age_min}) must be < age_max ({age_max})")
    if age_min is not None and age_max is not None:
        if age_max - age_min < 0.1:
            raise ValueError(f"Age range too narrow: [{age_min}, {age_max}]")

def validate_confidence(v: float) -> float:
    if not (0.0 <= v <= 1.0):
        raise ValueError(f"confidence must be in [0.0, 1.0], got {v}")
    return v

# ── Batch request validators ────────────────────────────────────────

def validate_batch_size(v: list[Any]) -> list[Any]:
    if len(v) == 0:
        raise ValueError("Batch cannot be empty")
    if len(v) > 50:
        raise ValueError(f"Batch size {len(v)} exceeds maximum (50)")
    return v

def validate_not_empty(v: list[Any]) -> list[Any]:
    if len(v) == 0:
        raise ValueError("results list cannot be empty")
    if len(v) > 200:
        raise ValueError(f"results list too large: {len(v)} items (max 200)")
    return v
```

### 15.3 Domain Validation Rules

```python
# src/validation/domain_rules.py

class DomainValidator:
    """Business-rule validators that run AFTER schema validation."""

    @staticmethod
    def validate_lab_result_context(result, patient) -> list[str]:
        """Cross-field validation between lab results and patient context."""
        warnings = []

        # Rule: Fasting glucose requires fasting condition
        if "fasting" in result.parameter_name.lower() and "glucose" in result.parameter_name.lower():
            if not patient or not patient.condition or "fasting" not in patient.condition.lower():
                warnings.append(
                    f"'{result.parameter_name}' typically requires fasting. "
                    "No fasting condition specified for patient."
                )

        # Rule: Pregnancy-specific tests require pregnancy flag
        pregnancy_tests = {"hcg", "beta-hcg", "afp", "estriol", "inhibin"}
        if any(t in result.parameter_name.lower() for t in pregnancy_tests):
            if patient and not patient.is_pregnant:
                warnings.append(
                    f"'{result.parameter_name}' is typically a pregnancy-related test, "
                    "but pregnancy flag is not set."
                )

        # Rule: Pediatric ranges for neonates (age < 1 month)
        if patient and patient.age_years is not None:
            if patient.age_years < 0.083:  # < 1 month
                warnings.append(
                    "Patient is a neonate (<1 month). Neonatal reference ranges "
                    "differ significantly from adult ranges."
                )

        # Rule: Tanner-stage dependent parameters for adolescents
        adolescent_hormones = {"lh", "fsh", "estradiol", "testosterone"}
        if patient and patient.age_years:
            if 9 <= patient.age_years <= 17:
                if any(h in result.parameter_name.lower() for h in adolescent_hormones):
                    warnings.append(
                        f"'{result.parameter_name}' in adolescent patients is "
                        "Tanner-stage dependent. Reference ranges may vary widely."
                    )

        # Rule: Diurnal variation
        diurnal_params = {"cortisol", "acth", "prolactin", "growth hormone"}
        if any(p in result.parameter_name.lower() for p in diurnal_params):
            warnings.append(
                f"'{result.parameter_name}' exhibits diurnal variation. "
                "Time of collection affects reference range interpretation."
            )

        return warnings

    @staticmethod
    def validate_unit_consistency(loinc_code: str, unit: str) -> list[str]:
        """Check that the submitted unit matches the LOINC-preferred unit."""
        warnings = []

        # Known UCUM unit mappings for common parameters
        PREFERRED_UNITS = {
            "718-7": "g/dL",      # Hemoglobin
            "6690-2": "x10*9/L",  # WBC
            "1742-6": "U/L",      # ALT
            "3016-3": "m[IU]/L",  # TSH
            "2345-7": "mg/dL",    # Glucose
            "20565-8": "mmol/L",  # Fasting Glucose (SI)
        }

        preferred = PREFERRED_UNITS.get(loinc_code)
        if preferred:
            # Normalize both for comparison (UCUM symbols)
            norm_unit = unit.replace("^", "").replace("10^9", "10*9")
            norm_pref = preferred.replace("[", "").replace("]", "")
            if norm_unit.lower() != norm_pref.lower():
                warnings.append(
                    f"Unit '{unit}' for LOINC {loinc_code} differs from preferred unit "
                    f"'{preferred}'. Conversion will be applied if possible."
                )

        return warnings
```

### 15.4 Database-Level Constraints

```sql
-- Additional validation constraints on reference_ranges
ALTER TABLE reference_ranges ADD CONSTRAINT chk_range_bounds 
    CHECK (low < high);

ALTER TABLE reference_ranges ADD CONSTRAINT chk_age_range 
    CHECK (age_min_years IS NULL OR age_max_years IS NULL OR age_min_years <= age_max_years);

ALTER TABLE reference_ranges ADD CONSTRAINT chk_confidence_range 
    CHECK (confidence >= 0.0 AND confidence <= 1.0);

ALTER TABLE reference_ranges ADD CONSTRAINT chk_sex_valid 
    CHECK (sex IN ('male', 'female', 'any'));

-- LOINC code uniqueness + format
ALTER TABLE lab_parameters ADD CONSTRAINT chk_loinc_format
    CHECK (loinc_code ~ '^\d{1,6}-\d$');
```

### 15.5 Edge Case Catalog

| Category | Edge Case | Expected Behavior |
|----------|-----------|-------------------|
| **Zero values** | `value: 0.0` for any parameter | Valid — flag against reference range. Some tests (troponin) have 0 ref range |
| **Negative results** | `value: -0.01` | Valid if reference range allows. Flag as LOW/CRITICAL |
| **Extremely small** | `value: 1e-10` | Valid. Floating point safe. Do not round to zero |
| **Extremely large** | `value: 999999` | Reject at Pydantic level (>1e6) or flag CRITICAL |
| **NaN/Inf** | `value: NaN` | Reject at Pydantic level |
| **Very long names** | `name: "Immunoglobulin G Subclass 4..."` repeated to 500 chars | Truncate to 300 in DB, warn |
| **Unicode units** | `unit: "µmol/L"` (mu character) | Accept. Normalize µ ↔ mu internally |
| **Mixed-case units** | `unit: "Mg/Dl"` | Accept. Case-insensitive comparison internally |
| **Age=0** | Neonatal (<1 day) | Flag for neonatal ranges. Critical — wrong range could miss severe abnormality |
| **Age at boundary** | `age_years: 18.0` for pediatric→adult transition | Apply adult range (≥18). Warn that transition is arbitrary |
| **Pregnant + sex=male** | `is_pregnant: true, sex: male` | Reject with validation error |
| **Fasting + non-fasting test** | `condition: "fasting"` but tests don't require it | No error, just non-applicable condition |
| **Duplicate parameter** | Two Hemoglobin results in one request | Accept. Process both independently. Flag if values differ >5% |
| **LOINC code in name** | `name: "718-7"` | Treat as explicit LOINC code. Skip name-based lookup |
| **HTML in name** | `name: "<script>alert(1)</script>"` | Strip HTML at Pydantic level. Reject if suspicious |
| **SQL injection** | `name: "'; DROP TABLE--"` | Parameterized queries prevent injection. Pass through as literal string |
|| **BOM in upload** | UTF-8 BOM in PDF metadata | Strip BOM before parsing title/author |

---

## 16. Security Architecture

### 16.1 Threat Model Summary

| Threat | Mitigation |
|--------|------------|
| SQL Injection | Parameterized queries via SQLAlchemy ORM. No raw SQL with string formatting. |
| XSS (client-facing) | OpenAPI /docs uses Swagger UI (auto-escaped). If we add a web UI, CSP headers + Jinja2 autoescaping |
| CSRF | Not applicable for API-only service (no cookie sessions). If we add a web UI, use CSRF tokens + SameSite cookies |
| Rate limiting abuse | Slowapi per-IP rate limiting; stricter limits for write endpoints |
| Data exfiltration | API key auth (opt-in), audit log on all parameter reads and document uploads |
| PHI leakage | De-identification at input validation; no patient names/IDs stored |
| Dependency supply chain | `pip-audit` in CI; `uv` lockfile; pinned versions |
| Secret exposure | `.env` in `.gitignore`; `python-dotenv` auto-loading; no hardcoded keys |
| Container escape | Non-root user in Docker (`USER appuser`, UID 1000); read-only root filesystem |

### 16.2 CORS Policy

```python
# src/api/main.py

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,   # From .env, default: ["http://localhost:3000"] for dev
    allow_credentials=False,                # No session cookies
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "X-API-Key"],
    expose_headers=["X-Request-Id"],        # Expose request ID for client-side tracing
    max_age=3600,                          # Cache preflight for 1 hour
)
```

Production default: `CORS_ORIGINS=[""]` — no origins allowed unless explicitly configured.

### 16.3 Security Headers

```python
# src/api/middleware.py

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Content-Security-Policy"] = "default-src 'self'; frame-ancestors 'none'; form-action 'self'"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response
```

### 16.4 Input Sanitization

```python
# src/security/sanitize.py

import bleach
import re
import html

# HTML escape for any field that might carry user input into logs or titles
def sanitize_log_input(text: str, max_length: int = 500) -> str:
    """Sanitize for log injection (remove newlines, null bytes)."""
    text = str(text).replace("\n", "\\n").replace("\r", "\\r").replace("\x00", "\\x00")
    return text[:max_length]

# Full HTML sanitization for document titles, parameter names
ALLOWED_TAGS = []   # No HTML allowed in any field
ALLOWED_ATTRS = {}

def sanitize_html(value: str) -> str:
    """Strip all HTML tags and entities. Use for user-submitted text fields."""
    stripped = bleach.clean(value, tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRS)
    return html.unescape(stripped)
```

### 16.5 PHI De-identification Pipeline

A RAG és audit log szempontjabol ne adjunk meg nevet, szuletesnapot, MRN-t, telefonszamot.

```python
# src/security/deidentify.py

import re
from datetime import datetime

PHI_PATTERNS = {
    "email":      re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
    "phone":      re.compile(r'\b(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})\b'),
    "ssn":        re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
    "mrn":        re.compile(r'\b(MRN|Medical Record)[\s:#]*\d+\b', re.IGNORECASE),
    "dob":        re.compile(r'\b(DOB|Date of Birth)[\s:#]*\d{1,2}/\d{1,2}/\d{2,4}\b', re.IGNORECASE),
    "name":       re.compile(r'\b(Patient Name|Name)[\s:#]*[A-Z][a-z]+ [A-Z][a-z]+\b'),  # naive
    "address":    re.compile(r'\b\d+\s+[\w\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln)\b', re.IGNORECASE),
}

PHI_REPLACEMENT = "[REDACTED]"

def deidentify_text(text: str) -> str:
    """Replace PHI patterns with redacted token."""
    for pattern in PHI_PATTERNS.values():
        text = pattern.sub(PHI_REPLACEMENT, text)
    return text

def is_deidentified(text: str) -> bool:
    """Check if text has already been deidentified (no PHI patterns remain)."""
    for pattern in PHI_PATTERNS.values():
        if pattern.search(text):
            return False
    return True
```

### 16.6 Audit Log

Minden kliens interakcio (analízis, paraméter módosítás, dokumentum feltöltés, beágyazási események) naplózása írásvédett naplóba:

```python
# src/security/audit_log.py

import uuid
from datetime import datetime, timezone
from enum import Enum
from sqlalchemy.dialects.postgresql import JSONB

class AuditAction(str, Enum):
    ANALYSIS_CREATE       = "analysis.create"
    ANALYSIS_BATCH_CREATE = "analysis.batch_create"
    PARAMETER_CREATE      = "parameter.create"
    PARAMETER_UPDATE      = "parameter.update"
    PARAMETER_DELETE      = "parameter.delete"
    RANGE_ADD             = "range.add"
    DOCUMENT_UPLOAD       = "document.upload"
    DOCUMENT_STATUS_READ  = "document.status_read"
    INGEST_STRUCTURED     = "ingest.structured"
    INGEST_UNSTRUCTURED   = "ingest.unstructured"
    API_KEY_AUTH          = "auth.api_key"

class AuditLog(Base):
    __tablename__ = "audit_log"

    id          = Column(UUID, primary_key=True, default=gen_random_uuid())
    timestamp   = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), index=True)
    action      = Column(String(100), index=True)  # AuditAction string value
    actor       = Column(String(200))              # API key ID, "anonymous", "system"
    request_id  = Column(UUID, index=True)         # matches AnalysisRequest.request_id
    ip_address  = Column(INET)                    # PostgreSQL INET type
    user_agent  = Column(Text)

    # Payload — vary by action:
    # analysis.create: {request_id, results_count, abnormal_count, model_used}
    # parameter.*: {parameter_id, loinc_code, name}
    # document.*: {document_id, filename, size_kb}
    payload     = Column(JSONB)

    # Cross-reference for linking related audit events
    parent_request_id = Column(UUID, nullable=True, index=True)

    model_config = ConfigDict(arbitrary_types_allowed=True)

async def log_audit(
    db: AsyncSession,
    action: AuditAction,
    actor: str,
    request_id: uuid.UUID | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
    payload: dict | None = None,
    parent_request_id: uuid.UUID | None = None,
):
    entry = AuditLog(
        action=action.value,
        actor=actor,
        request_id=request_id,
        ip_address=ip_address,
        user_agent=user_agent,
        payload=payload or {},
        parent_request_id=parent_request_id,
    )
    db.add(entry)
    await db.commit()

# Audit route decorator
def audit(action: AuditAction):
    """FastAPI dependency that logs audit info after request completes."""
    async def audit_dependency(request: Request, response: Response):
        actor = request.state.actor  # from require_api_key
        return {"audit_action": action, "audit_actor": actor}
    return audit_dependency
```

Audit log schema index:
```sql
CREATE INDEX idx_audit_timestamp ON audit_log(timestamp DESC);
CREATE INDEX idx_audit_action ON audit_log(action, timestamp DESC);
CREATE INDEX idx_audit_actor ON audit_log(actor, timestamp DESC);
CREATE INDEX idx_audit_request_id ON audit_log(request_id);
CREATE INDEX idx_audit_parent_id ON audit_log(parent_request_id);
```

### 16.7 GDPR / HIPAA Compliance

Ez egy forras- és a dokumentum adatgyűjtemény. Az alábbiakat kell implementálni:

| Requirement | Implementation |
|-------------|----------------|
| **Lawful basis** | Legitimate interest for structured data (LOINC, public sources). Explicit consent for any patient-submitted data. |
| **Data minimization** | Store only de-identified lab values (no names, MRNs, DOBs in the database). |
| **Retention limits** | `analysis_results` table: auto-purge after 90 days. Audit log: 180 days. Configurable via `DATA_RETENTION_DAYS`. |
| **Right to erasure** | `DELETE /api/v1/patients/{patient_hash}` — cascading delete all lab results for a hashed patient ID. |
| **Breach notification** | Audit log provides full audit trail within 72-hour disclosure window. |
| **Audit trail** | Immutable audit log (no UPDATE/DELETE on audit_log). Sign with HMAC if tampering is a concern. |
| **HIPAA Business Associate Agreement** | Structured data from public sources (LOINC, Mayo Clinic) is considered "de-identified" under HIPAA Safe Harbor (no PHI stored). Patient-submitted data must have BAA-covered hosting. |
| **PHI identifier list** | Names, geographic subdivisions smaller than state, all dates except year, phone numbers, fax numbers, email addresses, SSN, MRN, health plan beneficiary number, account numbers, certificates/license numbers, device identifiers, URLs, IP addresses, biometric identifiers, full-face photos — none of these are stored. |

### 16.8 Authentication

```python
# src/api/auth.py — API key management

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

class AuthConfig(BaseSettings):
    API_AUTH_ENABLED: bool = False
    ADMIN_API_KEY: str | None = None   # for mutation endpoints (POST/PUT/DELETE)

    @validator("API_KEYS", pre=True)
    def parse_api_keys(cls, v):
        if isinstance(v, str):
            return {k.strip() for k in v.split(",") if k.strip()}
        return v or set()

async def require_api_key(api_key: str | None = Depends(API_KEY_HEADER)) -> str:
    if not settings.API_AUTH_ENABLED:
        return "anonymous"
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="X-API-Key header required")
    if api_key not in settings.API_KEYS:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")
    return api_key

async def require_admin(api_key: str | None = Depends(API_KEY_HEADER)) -> str:
    """Require admin API key for mutation endpoints."""
    if not settings.API_AUTH_ENABLED:
        return "anonymous"
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="X-API-Key header required")
    if api_key != settings.ADMIN_API_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin API key required")
    return api_key
```

### 16.9 Secrets Management

| Secret | Storage | Rotation |
|--------|---------|---------|
| DB password | `.env` (local), Vault/KMS (prod) | 90 days |
| Cloud API keys (Claude, OpenAI) | `.env` (local), Vault (prod) | On demand |
| Docker secrets | `docker secret` swarm mode | Revoke on compromise |
```

### 16.10 Rate Limits (Summary)

| Endpoint | Limit | Scope |
|----------|-------|-------|
| `POST /analyze` | 30/min | Per IP |
| `POST /analyze/batch` | 5/min | Per IP |
| `GET /parameters` | 60/min | Per IP |
| `POST /parameters` | 30/min | Per IP + API key |
| `DELETE /parameters/{id}` | 10/min | Per IP + API key |
| `POST /documents/upload` | 10/min | Per IP + API key |
| `POST /ingest/*` | 5/min | Per IP + API key |
| `GET /health` | 120/min | Per IP |

---

## 17. Monitoring & Observability

### 17.1 Runbook — Prometheus Metrics

Minden endpoint és pipeline szinten metrikákat gyűjtünk, Prometheus scraping-el érhetők el (`/metrics`).

```python
# src/api/metrics.py

from prometheus_client import Counter, Histogram, Gauge, Summary, generate_latest, REGISTRY
from starlette.responses import Response

# Request metrics
REQUEST_COUNT = Counter(
    "medlab_requests_total",
    "Total API requests",
    ["method", "path", "status_code", "auth_type"],  # auth_type: authenticated/anonymous
)
REQUEST_DURATION = Histogram(
    "medlab_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path", "status_code"],
)
REQUEST_IN_PROGRESS = Gauge(
    "medlab_requests_in_progress",
    "Requests currently being processed",
    ["method", "path"],
)

# LLM usage
LLM_CALL_COUNT = Counter(
    "medlab_llm_calls_total",
    "Total LLM API calls",
    ["tier", "model", "provider", "status"],  # status: success/failure/fallback
)
LLM_CALL_DURATION = Histogram(
    "medlab_llm_call_duration_seconds",
    "LLM call duration in seconds",
    ["tier", "model"],
)
LLM_FALLBACK_COUNT = Counter(
    "medlab_llm_fallback_count_total",
    "Number of times fallback was used",
    ["from_model", "to_model"],
)

# Analysis pipeline
ANALYSIS_COUNT = Counter(
    "medlab_analysis_total",
    "Total analyses performed",
    ["status"],  # success/partial/error
)
ABNORMAL_PARAMETERS = Histogram(
    "medlab_abnormal_params_per_analysis",
    "Number of abnormal parameters per analysis",
    buckets=[0, 1, 2, 3, 5, 10, 15, 20],
)
CRITICAL_FINDINGS = Counter(
    "medlab_critical_findings_total",
    "Total critical findings across all analyses",
)
RAG_CITATIONS_RETRIEVED = Histogram(
    "medlab_rag_citations_retrieved",
    "Number of RAG citations returned per analysis",
    ["source_type"],  # guideline/textbook/research/drug_label
)

# Database
DB_CONNECTIONS = Gauge(
    "medlab_db_connections_active",
    "Active DB connections in pool",
    ["pool_name"],
)
DB_QUERY_DURATION = Histogram(
    "medlab_db_query_duration_seconds",
    "Database query duration in seconds",
    ["operation"],  # select/insert/update/delete
)

# Qdrant
QDRANT_UPSERT_COUNT = Counter(
    "medlab_qdrant_upsert_total",
    "Total points upserted to Qdrant",
    ["collection"],
)
QDRANT_SEARCH_COUNT = Counter(
    "medlab_qdrant_search_total",
    "Total Qdrant searches",
    ["collection"],
)
QDRANT_SEARCH_DURATION = Histogram(
    "medlab_qdrant_search_duration_seconds",
    "Qdrant search duration in seconds",
    ["collection"],
)

# Cache
CACHE_HIT = Counter(
    "medlab_cache_hits_total",
    "Cache hits",
    ["cache_type"],  # llm_response/parameter_lookup/frequent_query
)
CACHE_MISS = Counter(
    "medlab_cache_misses_total",
    "Cache misses",
    ["cache_type"],
)

# Pipeline
PIPELINE_RUNS = Counter(
    "medlab_pipeline_runs_total",
    "Total Prefect pipeline runs",
    ["pipeline", "status"],  # completed/failed/crashed
)
PIPELINE_DURATION = Summary(
    "medlab_pipeline_duration_seconds",
    "Pipeline duration in seconds per run",
    ["pipeline"],
)
DLQ_ENTRIES = Gauge(
    "medlab_dlq_entries",
    "Current dead letter queue size",
    ["source"],
)

# Resource metrics (collected via process metrics)
PROCESS_CPU_USAGE = Gauge(
    "medlab_process_cpu_usage",
    "Process CPU usage percent",
)
PROCESS_MEMORY_USAGE = Gauge(
    "medlab_process_memory_bytes",
    "Process memory usage in bytes",
)

# Expose metrics
@app.get("/metrics", include_in_schema=False)
async def metrics():
    return Response(generate_latest(REGISTRY), media_type="text/plain")
```

### 17.2 Exporters and Scraping

```
 med-lab-analyzer  /metrics (HTTP)  ←  prometheus (scrape every 15s)
       │
       ├── App custom metrics
       ├── process metrics (uvicorn workers)
       └── client-python metrics (httpx, psycopg, redis)

External services:
       ├── PostgreSQL    postgres_exporter (pg_stat_statements, pg_stat_replication)
       ├── Qdrant        qdrant_prometheus_exporter (vector count, memory usage)
       ├── Redis         redis_exporter (hit rate, evictions, connections)
       └── Ollama        ollama_prometheus_exporter (GPU VRAM, model load time)
```

### 17.3 Grafana Dashboard Panels

A dashboard a következő paneleket tartalmazza:

1. **Request rate** (`rate(medlab_requests_total[5m])` by `path`)
2. **Error rate** (5xx / total, 90s window, 5% threshold for alert)
3. **Analysis latency** (p99 `medlab_analysis_duration_seconds` < 10s)
4. **LLM fallback rate** (fallback / total < 2%)
5. **RAG latency** (search + embed < 2s p99)
6. **Cache hit rate** (`medlab_cache_hits / (medlab_cache_hits + medlab_cache_misses)` > 80%)
7. **DB connections** (pool utilization < 80%)
8. **Qdrant memory** (RAM / GPU VRAM < 90% per watermark)
9. **DLQ size** (grow/shrink trend, alert if > 100 items)
10. **Pipeline success rate** (completed / (completed + failed) > 95%)

### 17.4 Logging — Structured Aggregation

Minden log esemény JSON formátumban van, indexelve a következő mezőkkel:

```python
# Common log fields (appear in every log entry)
{
    "timestamp": "2026-05-19T14:30:00Z",
    "level": "ERROR" | "WARNING" | "INFO" | "DEBUG",
    "logger": "medlab.pipeline.structured",
    "service": "med-lab-analyzer",
    "version": "0.1.0",
    "environment": "production" | "staging" | "development",
    
    # Request context (when available)
    "request_id": "uuid",
    "trace_id": "uuid",  # cross-service trace (OpenTelemetry)
    "actor": "api-key-id" | "anonymous",
    "ip_address": "203.0.113.1",
    
    # Error-specific
    "error": {
        "type": "ValidationError",
        "category": "VALIDATION",
        "message": "parameter_name cannot be empty",
        "details": {"field": "parameter_name"},
    },
    
    # Pipeline context
    "pipeline": "structured" | "unstructured",
    "flow_run_id": "prefect-flow-run-uuid",
    "task_name": "scrape_source",
    
    # Performance
    "duration_ms": 1250,
}
```

Logging configuration (`src/logging_config.py`):

```python
import logging
import structlog
import sys

def configure_logging(env: str = "production"):


