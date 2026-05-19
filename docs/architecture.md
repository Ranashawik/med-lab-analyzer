# Architecture Overview

**Medical Laboratory Parameter Analyzer** — architectural documentation.

- [System Design](#system-design)
- [Data Schema](#data-schema)
- [Pipeline A: Structured Ingestion](#pipeline-a-structured-ingestion)
- [Pipeline B: Unstructured Ingestion](#pipeline-b-unstructured-ingestion)
- [Analysis Flow](#analysis-flow)
- [Key Design Decisions](#key-design-decisions)

---

## System Design

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PATIENT INPUT                                │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                    API LAYER  (FastAPI + Pydantic)                   │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────┐   ┌──────────────────────────┐     │
│  │  LAB PARAMETER ENGINE       │   │  CLINICAL RAG ENGINE     │     │
│  │  • Lookup reference range   │   │  • Semantic search       │     │
│  │  • Flag abnormal values     │   │  • Retrieve disease      │     │
│  │  • Unit conversion          │   │    associations          │     │
│  └──────────┬──────────────────┘   └───────────┬──────────────┘     │
└──────────────┼──────────────────────────────────┼───────────────────┘
               │                                  │
    ┌──────────▼──────────┐            ┌──────────▼──────────┐
    │   POSTGRESQL        │            │   QDRANT            │
    │  • lab_parameters   │            │  • clinical_chunks  │
    │  • reference_ranges │            │   (768-d vectors)   │
    │  • units            │            │  • source_metadata  │
    │  • audit_log        │            └──────────┬──────────┘
    └──────────┬──────────┘                       │
               │                    ┌─────────────▼─────────────┐
               │                    │   REDIS                   │
               │                    │  • LLM response cache     │
               │                    │  • Frequent query cache   │
               │                    │  • Rate limiting          │
               │                    └───────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────────────┐
│                     INGESTION PIPELINE                               │
│                                                                     │
│  Pipeline A: Structured          Pipeline B: Unstructured            │
│   Web → Scraper                    PDF → Text Extraction             │
│     ↓                                ↓                               │
│   Parse / Normalize               Semantic Chunking                  │
│     ↓                                ↓                               │
│   Conflict Resolution             Embedding (PubMedBERT)             │
│     ↓                                ↓                               │
│   Upsert → PostgreSQL             Upsert → Qdrant                    │
│                                                                     │
│  Orchestrator: Prefect (scheduled + event-driven)                   │
└─────────────────────────────────────────────────────────────────────┘
```

### Core Principles

1. **Deterministic first** — reference range lookup, flagging, and critical value alerts are pure logic, not AI
2. **Domain-specific RAG** — clinical knowledge is embedded with PubMedBERT, a model fine-tuned on biomedical literature
3. **Tiered LLM** — local models for routine tasks, cloud L5 for clinical safety
4. **Idempotent pipelines** — ingestion can be re-run without duplicates

---

## Data Schema

### PostgreSQL — Lab Parameters

```sql
CREATE TABLE lab_parameters (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    loinc_code      VARCHAR(20) UNIQUE,
    name            VARCHAR(300) NOT NULL,
    display_name    VARCHAR(500),
    category        VARCHAR(100),         -- e.g., 'Hematology', 'Chemistry'
    subcategory     VARCHAR(100),
    description     TEXT,
    methodology     VARCHAR(500),
    specimen_type   VARCHAR(200),         -- e.g., 'Serum', 'Whole Blood'
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
```

### PostgreSQL — Reference Ranges

```sql
CREATE TABLE reference_ranges (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parameter_id    UUID REFERENCES lab_parameters(id) ON DELETE CASCADE,
    source          VARCHAR(200),         -- 'Mayo Clinic', 'Lab Tests Online'
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
    confidence      DOUBLE PRECISION,     -- 0.0–1.0 source reliability
    is_primary      BOOLEAN DEFAULT false,
    created_at      TIMESTAMPTZ DEFAULT now()
);
```

### PostgreSQL — Clinical Documents (Metadata)

```sql
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
    qdrant_point_id VARCHAR(100) UNIQUE,
    token_count     INT,
    section_heading VARCHAR(500),
    created_at      TIMESTAMPTZ DEFAULT now()
);
```

### Qdrant — Vector Collection

| Property | Value |
|----------|-------|
| Collection name | `clinical_knowledge` |
| Vector dimension | 768 (PubMedBERT / BioBERT-large) |
| Distance metric | Cosine |
| Payload fields | `chunk_id`, `document_id`, `section_heading`, `source_type`, `publication_year` |

---

## Pipeline A: Structured Ingestion

### Source Registry

| Source | Data | Access Method |
|--------|------|---------------|
| LOINC | Universal lab test codes & names | REST API / CSV download |
| Lab Tests Online | Reference ranges, clinical info | Web scraping (`httpx` + `selectolax`) |
| Mayo Clinic Laboratories | Test catalog, reference ranges | Web scraping |
| ARUP Consult | Lab test algorithms | Web scraping |
| NIH / NLM DailyMed | Drug-lab interactions | REST API |
| PubMed E-utilities | Lab parameter research metadata | REST API |

### Stage Flow

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

1. **Discovery** — index pages are fetched, individual test page URLs extracted, deduplicated against existing records
2. **Extraction** — structured data (test name, reference range, units, specimen, methodology) parsed from HTML tables
3. **Normalization** — parameter names mapped to LOINC codes, units converted to UCUM, ranges parsed from text
4. **Conflict Resolution** — when sources disagree, confidence scoring picks the primary range while storing all alternatives
5. **Upsert** — idempotent insert with conflict handling

### Key Libraries

```
httpx>=0.27           # Async HTTP
selectolax>=0.3       # Fast HTML parsing
pydantic>=2.5         # Data validation
sqlalchemy[asyncio]   # Async ORM
alembic               # Migrations
tenacity              # Retry logic
```

---

## Pipeline B: Unstructured Ingestion

### Source Examples

| Source | Format | Content |
|--------|--------|---------|
| Clinical practice guidelines (NICE, ESC, ADA, AACE) | PDF | Disease–lab linkage |
| Harrison's / Cecil textbook chapters | PDF | Pathophysiology |
| PubMed Central full-text | PDF/XML | Research findings |
| FDA drug labels | PDF | Drug-lab interactions |

### Stage Flow

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐
│ Ingest   │───▶│  PDF Parse   │───▶│  Chunking    │───▶│  Embedding   │───▶│  Upsert   │
│ Trigger  │    │  (pymupdf)   │    │  (Semantic)  │    │  (BioBERT)   │    │ (Qdrant)  │
└──────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └───────────┘
```

1. **PDF Parsing** — `pymupdf` (primary) for text-based PDFs, `marker-pdf` (fallback) for scanned/OCR
2. **Semantic Chunking** — split on section boundaries (headings), not fixed token counts. Target: 300–500 tokens with 50-token overlap
3. **Embedding** — `pritamdeka/S-PubMedBert-MS-MARCO` via `sentence-transformers` or ONNX Runtime. Batch size: 32
4. **Upsert** — batch insert to Qdrant with full payload for hybrid search

### Key Libraries

```
pymupdf>=1.24              # PDF text extraction
marker-pdf                 # OCR fallback (heavy)
sentence-transformers       # Embedding models
qdrant-client>=1.9         # Vector DB client
langchain-text-splitters   # Semantic chunking
nltk                       # Medical text tokenization
```

---

## Analysis Flow

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
│                   │  critical     →  "CRITICAL"
└───────┬───────────┘
        ▼
┌───────────────────┐
│ 4. RAG Query      │  For each abnormal parameter:
│    Construction   │  embed query → search Qdrant (top_k=5)
└───────┬───────────┘
        ▼
┌───────────────────┐
│ 5. LLM Synthesis  │  Context: reference ranges + flagged values + RAG chunks
│                   │  Model: L5 (Claude Opus 4 or GPT-5.3) — clinical safety
└───────┬───────────┘
        ▼
┌───────────────────┐
│ 6. Report Output  │  Structured JSON with: parameter, value, flag,
│                   │  reference range, associated conditions, citations
└───────────────────┘
```

### Safety Notes

- Steps 1–4 are **deterministic** — no AI involved, pure logic
- Step 5 (clinical synthesis) is the **only** AI-dependent step
- L5 cloud model is **mandatory** for clinical synthesis — local models insufficient
- Human-in-the-loop for critical flags (configurable)

---

## Key Design Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | **PostgreSQL** (not MongoDB) for structured data | ACID compliance, JSONB for flexible fields, mature ORM, excellent range queries |
| 2 | **Qdrant** (not Pinecone/Weaviate) for vector DB | Open-source, fast HNSW indexing, payload filtering, hybrid search, self-hostable |
| 3 | **pymupdf** (not PyPDF2/pdfplumber) for PDF | 10× faster, native table handling, battle-tested |
| 4 | **PubMedBERT** (not OpenAI embeddings) | Domain-specific, offline, no API costs, 768-d vectors are efficient |
| 5 | **Prefect** (not Airflow) for orchestration | Python-native, lighter weight, excellent local dev experience |
| 6 | **Semantic chunking** (not fixed-size) | Medical text has natural section boundaries; splitting on headings preserves clinical concept integrity |

---

*Full architectural blueprint (3000+ lines): `.hermes/plans/2026-05-19_131500-med-lab-analyzer-architecture.md`*
