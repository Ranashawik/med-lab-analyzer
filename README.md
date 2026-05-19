# Medical Laboratory Parameter Analyzer

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)](https://postgresql.org)
[![Qdrant](https://img.shields.io/badge/Qdrant-latest-red)](https://qdrant.tech)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

Automated system for ingesting, structuring, and analyzing medical laboratory data.

Given a set of lab results, the system:
- **Maps** each parameter to canonical LOINC codes and retrieves age/sex-specific reference ranges
- **Flags** abnormal and critical values with deterministic logic
- **Augments** findings with relevant clinical knowledge via semantic RAG (PubMedBERT + Qdrant)
- **Synthesizes** differential diagnosis suggestions using LLMs (local for routine, cloud L5 for clinical safety)

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [System Components](#system-components)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [API Summary](#api-summary)
- [Project Structure](#project-structure)
- [LLM Model Strategy](#llm-model-strategy)
- [Development](#development)
- [Testing](#testing)
- [Deployment](#deployment)
- [License](#license)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PATIENT INPUT                                │
│   Lab results (CSV / JSON / manual)  →  Analysis / Report           │
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
    └─────────────────────┘            └──────────┬──────────┘
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

The system has two major data pipelines feeding into a unified analysis engine:

1. **Pipeline A (Structured)** — scrapes web sources for lab parameter definitions, reference ranges, units, and LOINC codes. Data lands in PostgreSQL.
2. **Pipeline B (Unstructured)** — ingests PDF clinical guidelines, textbooks, and research papers. Text is semantically chunked, embedded with PubMedBERT, and stored in Qdrant for RAG retrieval.

During analysis, both stores are queried: PostgreSQL for reference ranges and parameter metadata, Qdrant for relevant clinical context. Results are synthesized through a tiered LLM pipeline.

---

## System Components

### Data Stores

| Component | Purpose | Technology |
|-----------|---------|------------|
| **PostgreSQL** | Lab parameters, reference ranges, units, audit log | PostgreSQL 16 (async via `asyncpg`) |
| **Qdrant** | Clinical knowledge vector store (768-d PubMedBERT embeddings) | Qdrant with HNSW index + cosine distance |
| **Redis** | LLM response cache, rate limiting, frequent query cache | Redis 7 |

### Ingestion Pipelines

#### Pipeline A — Structured Web Data

| Source | Data | Method |
|--------|------|--------|
| LOINC | Universal lab test codes & names | REST API / CSV download |
| Lab Tests Online | Reference ranges, clinical info | Web scraping (`httpx` + `selectolax`) |
| Mayo Clinic Laboratories | Test catalog, reference ranges | Web scraping |
| ARUP Consult | Lab test algorithms | Web scraping |
| NIH / NLM DailyMed | Drug-lab interactions | REST API |
| PubMed E-utilities | Lab parameter research metadata | REST API |

#### Pipeline B — Unstructured Clinical Knowledge

| Source | Format | Content |
|--------|--------|---------|
| Clinical practice guidelines (NICE, ESC, ADA, AACE) | PDF | Disease–lab linkage |
| Harrison's / Cecil textbook chapters | PDF | Pathophysiology |
| PubMed Central full-text articles | PDF/XML | Research findings |
| UpToDate / DynaMed exports | PDF | Clinical decision support |
| FDA drug labels | PDF | Drug-lab interactions |

### Analysis Engine

The core analysis follows a deterministic + AI hybrid pipeline:

1. **Parse** — lab results from JSON/CSV input
2. **Resolve** — map parameter names to LOINC codes, fetch reference ranges matching patient demographics
3. **Flag** — deterministic comparison: LOW / HIGH / CRITICAL / NORMAL
4. **Retrieve** — embed flagged parameters and query Qdrant for relevant clinical context
5. **Synthesize** — LLM generates clinical interpretation from reference ranges + RAG context
6. **Report** — structured JSON response with full traceability

---

## Quick Start

### Prerequisites

- Python 3.11+
- Docker Desktop (for PostgreSQL, Qdrant, Redis)
- [Ollama](https://ollama.com) (optional — for local LLM models)
- 8+ GB RAM recommended

### 1. Clone & setup

```bash
git clone https://github.com/Ranashawik/med-lab-analyzer.git
cd med-lab-analyzer
python -m venv .venv
source .venv/bin/activate       # Linux/macOS
.venv\Scripts\activate          # Windows

pip install -e ".[dev]"
```

### 2. Start infrastructure

```bash
docker compose up -d
```

This starts:
- PostgreSQL 16 on port 5432
- Qdrant on port 6333
- Redis 7 on port 6379

### 3. Run database migrations

```bash
alembic upgrade head
```

### 4. Start the API

```bash
uvicorn src.api.main:app --reload --port 8000
```

Open http://localhost:8000/docs for interactive Swagger UI.

### 5. (Optional) Pull local LLM models

```bash
ollama pull qwen2.5-coder:7b       # L1 — coding tasks
ollama pull deepseek-coder-v2:16b  # L2 — complex code
ollama pull biomistral:7b          # L3 — medical text
```

---

## Usage Examples

### Analyze a single patient's lab results

```bash
curl -X POST http://localhost:8000/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "results": [
      {"parameter_name": "Hemoglobin", "value": 10.2, "unit": "g/dL"},
      {"parameter_name": "WBC", "value": 14.5, "unit": "x10^9/L"},
      {"parameter_name": "ALT", "value": 120, "unit": "U/L"}
    ],
    "patient": {"age_years": 45, "sex": "female"},
    "language": "hu"
  }'
```

### Upload a clinical guideline PDF

```bash
curl -X POST http://localhost:8000/api/v1/documents/upload \
  -H "X-API-Key: your-key" \
  -F "file=@nice_diabetes_guideline.pdf" \
  -F "source=NICE Guidelines NG28" \
  -F "source_type=guideline"
```

### Check ingestion status

```bash
curl -s http://localhost:8000/api/v1/health | jq .
```

---

## API Summary

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/analyze` | Single patient lab analysis |
| `POST` | `/api/v1/analyze/batch` | Batch patient analysis (1–50 patients) |
| `GET` | `/api/v1/parameters` | List / search lab parameters |
| `GET` | `/api/v1/parameters/{id}` | Get single parameter |
| `POST` | `/api/v1/parameters` | Create new parameter |
| `PUT` | `/api/v1/parameters/{id}` | Update parameter |
| `DELETE` | `/api/v1/parameters/{id}` | Delete parameter |
| `GET` | `/api/v1/parameters/{id}/ranges` | List reference ranges |
| `POST` | `/api/v1/parameters/{id}/ranges` | Add reference range |
| `POST` | `/api/v1/documents/upload` | Upload PDF for ingestion |
| `GET` | `/api/v1/documents/{id}` | Document processing status |
| `GET` | `/api/v1/documents` | List all documents |
| `POST` | `/api/v1/ingest/structured` | Trigger structured data scrape |
| `POST` | `/api/v1/ingest/unstructured` | Trigger document processing |
| `GET` | `/api/v1/health` | Health check |

Full API specification with request/response schemas is in [docs/api.md](docs/api.md).

---

## Project Structure

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
├── docs/                       # Documentation
│   ├── architecture.md
│   ├── api.md
│   └── development.md
├── .hermes/plans/              # Architecture plans
└── README.md
```

---

## LLM Model Strategy

The system uses a **tiered model allocation** strategy: local models for routine tasks, cloud models for clinical safety.

| Tier | Description | Models | When to use |
|------|-------------|--------|-------------|
| **L1 — Local Small** | 7B–14B, consumer GPU/CPU | Qwen2.5-Coder 7B, Llama 3.1 8B, Phi-4 14B | Boilerplate code, simple parsing, schema generation |
| **L2 — Local Large** | 20B–32B, needs 24GB+ VRAM | Qwen2.5-Coder 32B, DeepSeek-Coder-V2, Mistral Small 3.1 24B | Complex code, refactoring, test generation |
| **L3 — Local Medical** | Domain fine-tuned, 7B–13B | BioMistral 7B, MedLlama3 8B, OpenBioLLM 8B | Medical text extraction, concept normalization |
| **L4 — Cloud Coding** | API-based reasoning | Claude Sonnet 4, GPT-4.3-Codex | Architecture design, complex debugging, PR review |
| **L5 — Cloud Reasoning** | API-based clinical | Claude Opus 4, GPT-5.3, Gemini 2.5 Pro | ⚠️ **Clinical synthesis & differential diagnosis** |

**Local model share**: 77% of development tasks (L1–L3). Cloud models used only where necessary.

> ⚠️ **Clinical Safety Boundary**: The differential diagnosis step **MUST** use an L5 cloud reasoning model. Local models lack the medical reasoning depth and safety guardrails required for clinical decision support. This is a hard boundary.

---

## Development

See [docs/development.md](docs/development.md) for:
- Environment setup (Ollama, Pi, Docker)
- LLM model configuration
- Development workflow with Hermes + Pi
- Contribution guidelines

---

## Testing

The test strategy follows a standard pyramid:

| Layer | Count | Scope |
|-------|-------|-------|
| **Unit** | 200+ | Individual functions, validators, models (<100ms each) |
| **Integration** | ~40 | DB queries, Qdrant ops, API endpoints (<2s each) |
| **E2E** | ~10 | Full pipeline from upload to analysis (<30s each) |

Run tests:
```bash
pytest                    # all tests
pytest tests/test_engine/ # specific module
pytest -k "analyze"       # specific test name
```

All tests are deterministic — no LLM calls, no real web scraping, no external APIs. Mock fixtures handle external dependencies.

---

## Deployment

### Production stack

| Layer | Recommendation |
|-------|---------------|
| API | FastAPI behind nginx/Caddy, Gunicorn + Uvicorn workers |
| PostgreSQL | RDS / Cloud SQL with read replicas |
| Qdrant | Qdrant Cloud or self-hosted (GPU for embedding generation) |
| Redis | ElastiCache / Memorystore |
| Embeddings | GPU instance (T4/L4) with ONNX-optimized models |
| File Storage | S3 / GCS for raw PDFs |
| Monitoring | Prometheus + Grafana + structured JSON logging to Loki |

### Docker Compose (development)

```bash
docker compose up -d     # PostgreSQL, Qdrant, Redis, API, Prefect
docker compose logs -f   # Follow all logs
```

---

## License

MIT — see [LICENSE](LICENSE).

---

## Architecture Plan

The full architectural blueprint is available at:
- `.hermes/plans/2026-05-19_131500-med-lab-analyzer-architecture.md` (detailed, 3000+ lines)
- [docs/architecture.md](docs/architecture.md) (condensed overview)
