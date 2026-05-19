# Implementation Plan — Task Breakdown

This plan breaks each development phase into **30–90 minute tasks**, each producing exactly 1–2 files that a single LLM call (at the specified tier) can complete independently.

Each task specifies:
- **Files** to create or modify
- **Model tier** (L1–L5) to use
- **Estimated time**
- **Dependencies** (must be done first)
- **Acceptance criteria** — how to verify it's done

---

## Phase 0: Environment Setup

**Duration**: ~4 hours  |  **Model**: Manual + scripts  |  **AI needed**: No (one-time infra)

| # | Task | Files | Time | Deps | Notes |
|---|------|-------|------|------|-------|
| 0.1 | Install Ollama + pull models | — | 30 min | — | `ollama pull qwen2.5-coder:7b` etc. (~22 GB) |
| 0.2 | Docker Desktop + infra containers | `docker-compose.yml` | 20 min | — | PostgreSQL, Qdrant, Redis up & reachable |
| 0.3 | Python venv + project install | `.venv/`, `pip install -e ".[dev]"` | 15 min | — | Dependencies from `pyproject.toml` |
| 0.4 | Alembic scaffold + first migration | `alembic.ini`, `alembic/env.py`, `alembic/versions/001_initial.py` | 20 min | 0.3 | Autogenerate from models once Phase 1.2 is done |
| 0.5 | Git + GitHub sync | — | 10 min | — | `git pull`, credential check |
| 0.6 | Pi install + models.json | `~/.pi/agent/models.json` | 15 min | 0.1 | Register all 5 Ollama models + cloud fallbacks |

---

## Phase 1: Foundation

**Duration**: ~12 hours  |  **Local model share**: 100%  |  **All tasks L1 unless noted**

### 1.1 — Project Configuration (L1, ~30 min)
**Depends on**: Phase 0.3

**Files**:
- `pyproject.toml` — complete with dependencies, dev extras, tool config (ruff, mypy, pytest)
- `src/__init__.py` — empty package marker

**Acceptance**: `pip install -e ".[dev]"` installs without error. `ruff check src/` passes.

### 1.2 — Application Settings (L1, ~45 min)
**Depends on**: 1.1

**Files**:
- `src/config.py` — `pydantic-settings` `Settings` class with all env vars:
  - `DATABASE_URL`, `QDRANT_URL`, `REDIS_URL`, `OLLAMA_URL`
  - `API_AUTH_ENABLED`, `API_KEYS`
  - `LOG_LEVEL`, `VERSION`
  - `CLINICAL_LLM_MODEL` (default: `claude-opus-4`)
  - `CRITICAL_THRESHOLDS` (per-parameter panic ranges)

**Acceptance**: `from src.config import settings; print(settings.VERSION)` works.

### 1.3 — Database Base + Models (L1, ~60 min)
**Depends on**: 1.2

**Files**:
- `src/db/__init__.py`
- `src/db/base.py` — `DeclarativeBase`
- `src/db/models.py` — all 4 tables:
  - `LabParameter` — loinc_code, name, display_name, category, subcategory, description, methodology, specimen_type
  - `ReferenceRange` — parameter_id (FK), source, source_url, low, high, unit, unit_ucum, age_min/max_years, sex, pregnancy, condition, confidence, is_primary
  - `ClinicalDocument` — title, source, source_type, publication_year, authors, file_path, page_count, chunk_count
  - `ClinicalChunk` — document_id (FK), chunk_index, chunk_text, qdrant_point_id, token_count, section_heading

**Acceptance**: `python -c "from src.db.models import LabParameter; print('OK')"` works.

### 1.4 — Database Session (L1, ~30 min)
**Depends on**: 1.3

**Files**:
- `src/db/session.py` — async engine + session factory + `get_session` dependency
- Tests session lifecycle: create tables → yield → drop

**Acceptance**: `async with get_session() as session: await session.execute(text("SELECT 1"))` works against live PostgreSQL.

### 1.5 — Pydantic Schemas (L1, ~60 min)
**Depends on**: 1.2

**Files**:
- `src/api/__init__.py`
- `src/api/schemas.py` — all Pydantic models from the API spec:
  - Enums: `Flag`, `Sex`, `ParameterCategory`, `SourceType`
  - I/O: `LabResultInput`, `AnalysisRequest`, `AnalysisResponse`, `AnalyzedResult`, `ReferenceRange`
  - Batch: `BatchAnalysisRequest`, `BatchAnalysisResponse`, `BatchPatientResult`
  - CRUD: `LabParameterCreate`, `LabParameterResponse`, `ReferenceRangeCreate`, `ReferenceRangeResponse`
  - Documents: `DocumentUploadResponse`, `DocumentStatusResponse`
  - Ingestion: `IngestTriggerResponse`
  - Shared: `PatientDemographics`, `RAGCitation`, `ClinicalContext`
  - Error: `ErrorDetail`, `ErrorResponse`
  - Pagination: `PaginationParams`, `PaginatedResponse`
  - Health: `HealthResponse`

**Acceptance**: `from src.api.schemas import AnalysisRequest; AnalysisRequest(results=[...])` validates correctly.

### 1.6 — FastAPI Application Skeleton (L1, ~45 min)
**Depends on**: 1.4, 1.5

**Files**:
- `src/api/main.py` — FastAPI app with:
  - CORS middleware
  - Request ID middleware (UUID per request)
  - `startup` event: verify DB connection, Qdrant health, Redis ping
  - `shutdown` event: close connections
  - Root router mounting (health, analysis, parameters, documents, ingestion)
  - Exception handlers (ValidationError, RateLimitExceeded, generic 500)

**Acceptance**: `uvicorn src.api.main:app --port 8000` starts without error.

### 1.7 — Health Check Route (L1, ~30 min)
**Depends on**: 1.6

**Files**:
- `src/api/routes/__init__.py` — package + router aggregation
- `src/api/routes/health.py` — `GET /api/v1/health` with component checks:
  - PostgreSQL: `SELECT 1`
  - Qdrant: `/health` REST call
  - Redis: `PING`
  - Ollama: `GET /api/tags`
  - Prefect: optional, marked unavailable if not running

**Acceptance**: `curl localhost:8000/api/v1/health` returns `{"status": "healthy", ...}`.

### 1.8 — Docker Compose (L1, ~30 min)
**Depends on**: —

**Files**:
- `docker-compose.yml` — services:
  - `postgres:16-alpine` (port 5432)
  - `qdrant/qdrant:latest` (port 6333)
  - `redis:7-alpine` (port 6379)
  - `api` (build from Dockerfile, port 8000, hot-reload)
  - `prefect-server` (port 4200, optional)

**Acceptance**: `docker compose up -d` starts all services; `docker compose ps` shows all healthy.

### 1.9 — Alembic Initial Migration (L1, ~20 min)
**Depends on**: 1.3, 1.4

**Files**:
- `alembic/env.py` — async Alembic config
- `alembic/versions/001_initial_schema.py` — autogenerated from models

**Acceptance**: `alembic upgrade head` creates all 4 tables. `alembic downgrade -1` drops them.

---

## Phase 2: Structured Ingestion

**Duration**: ~18 hours  |  **Local model share**: 83%  |  **L1: 4, L2: 1, L3: 1, L4: 1**

### 2.1 — Source Registry + Abstract Spider Base (L1, ~45 min)
**Depends on**: 1.5

**Files**:
- `src/ingestion/__init__.py`
- `src/ingestion/structured/__init__.py`
- `src/ingestion/structured/spiders/__init__.py`
- `src/ingestion/structured/spiders/base.py` — `AbstractSpider` class:
  - `source_name: str` class attribute
  - `base_url: str`
  - `async def discover(self) -> list[str]` — discover test page URLs
  - `async def extract(self, url: str) -> dict | None` — extract structured data
  - `async def run(self) -> list[dict]` — discover → extract loop
  - Retry decorator (`tenacity`)
  - Rate limiting (1 request/sec base)

**Acceptance**: Subclassing `AbstractSpider` and calling `run()` works in a test.

### 2.2 — Lab Tests Online Spider (L1, ~60 min)
**Depends on**: 2.1

**Files**:
- `src/ingestion/structured/spiders/labtests_online.py` — `LabTestsOnlineSpider(AbstractSpider)`:
  - `discover()`: fetch index page → extract test detail links
  - `extract()`: parse HTML tables → test name, reference range, units, specimen, methodology
  - Uses `httpx` + `selectolax` for fast parsing
  - Returns dict matching `ReferenceRangeCreate` schema

**Acceptance**: Unit test with fixture HTML returns valid structured data.

### 2.3 — Mayo Clinic Spider (L1, ~60 min)
**Depends on**: 2.1

**Files**:
- `src/ingestion/structured/spiders/mayo_clinic.py` — `MayoClinicSpider(AbstractSpider)`:
  - Same interface as 2.2
  - Handles Mayo's specific HTML structure (different table layout)
  - Extracts: test ID, name, aliases, reference ranges by age/sex, CPT codes

**Acceptance**: Unit test with Mayo Clinic fixture HTML returns valid data.

### 2.4 — ARUP Spider (L1, ~45 min)
**Depends on**: 2.1

**Files**:
- `src/ingestion/structured/spiders/arup.py` — `ARUPSpider(AbstractSpider)`:
  - Different HTML structure from Mayo/LabTestsOnline
  - Algorithm-based test descriptions (harder to parse)
  - Extract: test name, methodology, reference ranges, turnaround time

**Acceptance**: Unit test with ARUP fixture HTML returns valid data.

### 2.5 — LOINC Normalizer (L3 BioMistral, ~90 min)
**Depends on**: 1.5

**Files**:
- `src/ingestion/structured/normalizer.py`:
  - `normalize_parameter(raw_name: str, source: str) -> NormalizedParam`
  - LOINC lookup via REST API + local cache
  - `lookup_loinc(name: str) -> str | None` — fuzzy match parameter name to LOINC code
  - `convert_unit(value: float, from_unit: str, to_unit: str) -> float` — UCUM conversions
  - `parse_range(text: str) -> tuple[float, float]` — parse "3.5-5.0 mmol/L" → (3.5, 5.0)
  - Source priority scoring for conflict resolution input
  - **L3 model**: medical terminology benefits from domain-specific model for edge case handling

**Acceptance**: 
- `convert_unit(100, "mg/dL", "g/L")` → `1.0`
- `lookup_loinc("Hemoglobin")` → `"718-7"`
- `parse_range("3.5-5.0 mmol/L")` → `(3.5, 5.0)`

### 2.6 — Conflict Resolver (L2 Qwen2.5-Coder 32B, ~90 min)
**Depends on**: 2.5

**Files**:
- `src/ingestion/structured/resolver.py`:
  - `resolve_ranges(ranges: list[dict], strategy: str = "confidence") -> dict`
  - Strategies: confidence (default), newest source, most specific demographics
  - `SourceReliability` scoring: peer-reviewed > official lab > crowdsourced
  - Audit log entry generation for conflicts
  - `flag_for_review(conflict: dict) -> None` — mark for manual review
  - **L2 model**: logic-heavy with multiple resolution strategies and edge cases

**Acceptance**: Given 3 conflicting ranges, returns the highest-confidence one as `is_primary=True`, logs the conflict.

### 2.7 — Structured Pipeline Prefect Flow (L1, ~60 min)
**Depends on**: 2.2, 2.3, 2.4, 2.5, 2.6

**Files**:
- `src/ingestion/structured/pipeline.py`:
  - `structured_ingestion_flow(sources: list[str] | None = None)` — Prefect flow
  - Parallel spider execution (run each spider concurrently)
  - Normalize → resolve → upsert chain per extracted record
  - Error handling: per-source failure doesn't abort others
  - Logging: record counts per source, conflict counts, error counts
  - Schedules: `cron: 0 6 * * *` (daily at 6 AM)

**Acceptance**: Flow dispatches via `from prefect import serve; serve(structured_ingestion_flow)`.

### 2.8 — Parameter CRUD Routes (L1, ~60 min)
**Depends on**: 1.6, 1.5, 1.4

**Files**:
- `src/api/routes/parameters.py`:
  - `GET /api/v1/parameters` — list with search, category filter, pagination
  - `GET /api/v1/parameters/{id}` — single parameter with range count
  - `POST /api/v1/parameters` — create (via `LabParameterCreate`)
  - `PUT /api/v1/parameters/{id}` — update
  - `DELETE /api/v1/parameters/{id}` — delete (cascades to ranges)
  - `GET /api/v1/parameters/{id}/ranges` — list ranges, optional demographic filter
  - `POST /api/v1/parameters/{id}/ranges` — add range (via `ReferenceRangeCreate`)

**Acceptance**: `curl localhost:8000/api/v1/parameters?search=glucose` returns paginated results.

---

## Phase 3: Unstructured Ingestion

**Duration**: ~16 hours  |  **Local model share**: 80%  |  **L1: 3, L2: 1, L3: 1, L4: 1**

### 3.1 — PDF Parser (L1, ~60 min)
**Depends on**: 1.5

**Files**:
- `src/ingestion/unstructured/__init__.py`
- `src/ingestion/unstructured/pdf_parser.py`:
  - `parse_pdf(file_path: str) -> PdfResult`
  - Uses `pymupdf` (primary) for text-based PDFs
  - Falls back to `marker-pdf` for scanned/image-only PDFs (OCR)
  - Extracts: full text, tables (as markdown), section headings, metadata (title, authors, page count)
  - `get_metadata(file_path: str) -> dict` — PDF info dict
  - `extract_tables(page) -> list[str]` — table → markdown conversion

**Acceptance**: `parse_pdf("tests/fixtures/pdfs/anemia_guidelines.pdf")` returns text + tables + headings.

### 3.2 — Semantic Chunker (L2 Qwen2.5-Coder 32B, ~90 min)
**Depends on**: 3.1

**Files**:
- `src/ingestion/unstructured/chunker.py`:
  - `chunk_document(text: str, headings: list[str]) -> list[Chunk]`
  - Section boundary detection: split on heading levels (H1, H2, H3)
  - Target chunk size: 300-500 tokens
  - Overlap: 50 tokens between adjacent chunks
  - Preserve parent heading in chunk metadata
  - `Chunk` dataclass: `text`, `section_heading`, `chunk_index`, `token_count`, `parent_headings: list[str]`
  - **L2 model**: NLP pipeline with heading detection logic, edge cases in medical doc structure

**Acceptance**: A 50-page PDF produces ~100 chunks, each with correct section heading and no split in the middle of a sentence.

### 3.3 — Section Boundary Detector (L3 BioMistral, ~45 min)
**Depends on**: 3.1

**Files**:
- (Adds to `chunker.py` or separate module):
  - `detect_sections(text: str) -> list[Section]`
  - Regex-based heading detection: `^([A-Z][A-Za-z\s]+):`, `^\d+\.\s+[A-Z]`, common medical headings
  - L3 enhances accuracy on edge cases: "Introduction", "Methods", "Results" in medical text
  - Handles nested sections (1.1, 1.2, etc.)
  - Fallback: paragraph-level split when no headings found

**Acceptance**: Correctly identifies sections in all 5 test PDF fixtures.

### 3.4 — Embedder (L1, ~60 min)
**Depends on**: 1.2

**Files**:
- `src/ingestion/unstructured/embedder.py`:
  - `EmbeddingModel` class wrapping `sentence-transformers`
  - Model: `pritamdeka/S-PubMedBert-MS-MARCO` (768-d)
  - `embed(texts: list[str]) -> list[list[float]]` — batch embedding
  - Batch size: 32 (configurable)
  - ONNX Runtime optional acceleration
  - Caching: identical text returns cached vector
  - L1 because the model loading is boilerplate; model selection is already decided

**Acceptance**: `embed(["Hemoglobin is low"])` returns a 768-d float vector.

### 3.5 — Qdrant Store (L1, ~45 min)
**Depends on**: 1.2, 3.4

**Files**:
- `src/ingestion/unstructured/qdrant_store.py`:
  - `QdrantStore` class:
  - `create_collection(name="clinical_knowledge", dim=768, distance="Cosine")`
  - `upsert_chunks(chunks: list[Chunk], vectors: list[list[float]])`
  - `search(query_vector: list[float], top_k: int = 5, filters: dict | None = None)`
  - Payload per point: `{chunk_id, document_id, section_heading, source_type, publication_year}`
  - Payload index on `source_type`, `publication_year`

**Acceptance**: `store.upsert_chunks(...)` then `store.search(...)` returns matching chunks.

### 3.6 — Unstructured Pipeline Prefect Flow (L1, ~60 min)
**Depends on**: 3.1, 3.2, 3.4, 3.5

**Files**:
- `src/ingestion/unstructured/pipeline.py`:
  - `unstructured_ingestion_flow(document_ids: list[str] | None = None)`
  - Stages: parse → chunk → embed → upsert (sequential, each stage depends on previous)
  - Document status tracking: queued → processing → completed/failed
  - Error handling: per-document failure doesn't stop the flow
  - Logging: pages processed, chunks created, embedding time

**Acceptance**: Flow processes a PDF end-to-end: file → Qdrant points.

### 3.7 — Document Routes (L1, ~45 min)
**Depends on**: 1.6, 3.6

**Files**:
- `src/api/routes/documents.py`:
  - `POST /api/v1/documents/upload` — multipart PDF upload
    - Saves to `data/raw_pdfs/`
    - Queues document in DB (status=queued)
    - Returns 202 with document_id
  - `GET /api/v1/documents/{id}` — document processing status
  - `GET /api/v1/documents` — list documents (filter by source_type, status)

**Acceptance**: Upload a PDF → 202 response → poll status → eventually "completed".

### 3.8 — Ingestion Trigger Routes (L4 Claude Sonnet, ~45 min)
**Depends on**: 1.6, 2.7, 3.6

**Files**:
- `src/api/routes/ingest.py`:
  - `POST /api/v1/ingest/structured` — trigger structured scrape
  - `POST /api/v1/ingest/unstructured` — trigger document processing
  - Both validate input, dispatch Prefect flow, return flow_run_id
  - Centralized error handling for flow dispatch failures
  - **L4 model**: cross-module orchestration — needs to understand both pipeline modules to wire them together

**Acceptance**: `POST /ingest/structured` triggers a Prefect flow run.

---

## Phase 4: Analysis Engine

**Duration**: ~16 hours  |  **Local model share**: 60%  |  **L1: 3, L2: 1, L5: 1 (mandatory cloud)**

### 4.1 — Core Analyzer (L1, ~60 min)
**Depends on**: 1.5

**Files**:
- `src/engine/__init__.py`
- `src/engine/analyzer.py`:
  - `flag_value(value: float, low: float | None, high: float | None, critical_thresholds: dict | None = None) -> Flag`
  - Pure deterministic logic (no AI)
  - Critical thresholds override: `{"critical_low": 7.0, "critical_high": 20.0}` for Hemoglobin
  - `resolve_reference_range(parameter_name: str, patient: PatientDemographics, db_session) -> ReferenceRange | None`
  - Finds best matching range by: age match → sex match → condition match → confidence score
  - `analyze_single(result: LabResultInput, patient: PatientDemographics | None, db_session) -> AnalyzedResult`
  - Orchestrates: resolve parameter → fetch range → flag → generate interpretation

**Acceptance**: 
- `flag_value(10.2, 12.0, 16.0)` → `Flag.LOW`
- `flag_value(6.5, 12.0, 16.0)` → `Flag.CRITICAL`
- Integration test with seeded DB returns correct flags per patient demographics.

### 4.2 — RAG Engine (L2 Qwen2.5-Coder 32B, ~90 min)
**Depends on**: 3.4, 3.5, 4.1

**Files**:
- `src/engine/rag.py`:
  - `build_rag_query(abnormal_params: list[AnalyzedResult]) -> list[str]`
  - One query per abnormal parameter, e.g. "What diseases are associated with elevated ALT?"
  - `retrieve_context(queries: list[str], top_k: int = 5) -> list[RAGCitation]`
  - Embed each query → search Qdrant → deduplicate citations
  - `build_llm_context(analyzed_results: list[AnalyzedResult], rag_citations: list[RAGCitation], language: str) -> str`
  - Construct prompt with: reference ranges + flagged values + RAG chunks
  - Prompt template with clinical safety instructions
  - **L2 model**: prompt engineering + retrieval logic with dedup and ranking

**Acceptance**: Given flagged parameters, returns context + citations ready for L5 synthesis.

### 4.3 — LLM Client (L1, ~45 min)
**Depends on**: 1.2

**Files**:
- `src/engine/llm.py`:
  - `ClinicalLLM` class:
  - `synthesize(context: str, language: str = "en", model: str | None = None) -> str`
  - Routing: uses configured `CLINICAL_LLM_MODEL` (default: `claude-opus-4`)
  - Async HTTP client for OpenAI-compatible API (works with Ollama, OpenAI, Anthropic via proxy)
  - Response caching via Redis (TTL: 7 days for identical lab patterns)
  - Fallback chain: primary → first fallback → error
  - L1 because pattern is standard (HTTP client + cache wrapper); model choice is config.

**Acceptance**: Returns synthesis text for a well-formed prompt. Falls back gracefully when primary model unavailable.

### 4.4 — Full Analysis Route (L5 Claude Opus 4, ~90 min)
**Depends on**: 4.1, 4.2, 4.3, 1.6

**Files**:
- `src/api/routes/analysis.py`:
  - `POST /api/v1/analyze` — full analysis endpoint:
    1. Validate request → create `AnalysisRequest`
    2. Resolve each parameter → fetch reference range → flag value
    3. If `include_rag=True`: build queries → retrieve context
    4. If abnormal params found: call L5 `synthesize()`
    5. Return `AnalysisResponse`
  - `POST /api/v1/analyze/batch` — batch mode:
    1. Validate 1-50 patients
    2. Process independently (partial failure allowed)
    3. Return `BatchAnalysisResponse` with summary
  - `POST /api/v1/ingest/structured` + `/unstructured` — trigger pipelines
  - **L5 mandatory**: Clinical synthesis requires maximum reasoning ability. Local models are insufficient for differential diagnosis. This is a hard safety boundary.
  - Rate limiting: 30/min single, 5/min batch

**Acceptance**: 
- `curl -X POST /analyze` with 5 lab results returns 200 with full analysis + RAG citations + clinical note.
- Batch endpoint processes 3 patients, returns 3 results even if 1 fails.
- Clinical synthesis uses L5 model; error if L5 unavailable.

---

## Phase 5: API & Polish

**Duration**: ~14 hours  |  **Local model share**: 80%  |  **L1: 4, L2: 1, L4: 1**

### 5.1 — Error Handling Middleware (L2 DeepSeek-Coder-V2, ~60 min)
**Depends on**: 1.6, 1.5

**Files**:
- `src/api/middleware.py`:
  - `RequestIDMiddleware` — UUID per request, inject into all log entries and error responses
  - `ProcessTimeHeader` — `X-Process-Time` header on responses
  - Exception handlers:
    - `RequestValidationError` → 400 `ValidationError`
    - `HTTPException` → passthrough
    - `IntegrityError` → 409 Conflict
    - Unhandled → 500 `InternalError` with log + masked traceback
  - **L2 model**: edge cases matter — middleware must handle all FastAPI exception paths without leaking internals

**Acceptance**: Invalid input returns 400 with `ErrorResponse` shape. Internal error returns 500 without stack trace in body.

### 5.2 — Rate Limiting (L1, ~45 min)
**Depends on**: 5.1, 1.2

**Files**:
- (Adds to `src/api/middleware.py` or separate):
  - Redis-based sliding window rate limiter using `slowapi`
  - Per-endpoint limits from API spec:
    - `/analyze`: 30/min
    - `/analyze/batch`: 5/min
    - `/parameters` GET: 60/min, POST/PUT/DELETE: 30/min
    - `/documents/upload`: 10/min
    - `/ingest/*`: 5/min
    - `/health`: 120/min
  - Rate limit exceeded → 429 with `Retry-After` header
  - Graceful degradation: if Redis unavailable, allow requests (no rate limiting)

**Acceptance**: 31 rapid requests to `/analyze` → 30 succeed, 1 returns 429.

### 5.3 — API Key Authentication (L1, ~45 min)
**Depends on**: 5.1, 1.2

**Files**:
- `src/api/auth.py`:
  - `api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)`
  - `require_api_key()` — mandatory for write endpoints
  - `optional_api_key()` — pass-through for read endpoints
  - Config toggled by `API_AUTH_ENABLED` env var
  - Local dev: disabled by default

**Acceptance**: With `API_AUTH_ENABLED=true`, POST without key returns 401. With `false`, all requests pass.

### 5.4 — Test Suite — Unit Tests (L1, ~90 min)
**Depends on**: Phase 1-4 completion

**Files**:
- `tests/conftest.py` — shared fixtures (db_session, mock_embedder, mock_qdrant, mock_llm, mock_loinc_api)
- `tests/mocks.py` — `FakeQdrantClient`, `FakeRedisClient`
- `tests/fixtures/pdfs/` — 5 test PDFs
- `tests/fixtures/html/` — 5 test HTML snapshots
- `tests/fixtures/data/` — 50 patient result sets, LOINC mapping, bad inputs
- `tests/test_models/test_lab_parameters.py` — model constraints
- `tests/test_models/test_schemas.py` — Pydantic validation
- `tests/test_ingestion/structured/test_normalizer.py` — unit conversion + LOINC
- `tests/test_ingestion/structured/test_resolver.py` — conflict resolution
- `tests/test_ingestion/structured/test_spiders/test_base.py` — spider framework
- `tests/test_ingestion/unstructured/test_pdf_parser.py`
- `tests/test_ingestion/unstructured/test_chunker.py`
- `tests/test_ingestion/unstructured/test_embedder.py`
- `tests/test_ingestion/unstructured/test_qdrant_store.py`
- `tests/test_engine/test_analyzer.py` — flag detection (all edge cases)
- `tests/test_engine/test_rag.py` — query construction, citation dedup
- `tests/test_engine/test_llm.py` — model routing, caching

**Acceptance**: `pytest tests/` — all unit tests pass in <30s.

### 5.5 — Test Suite — Integration + E2E (L1, ~90 min)
**Depends on**: 5.4

**Files**:
- `tests/test_api/test_analyze.py` — 8+ test cases for POST /analyze
- `tests/test_api/test_analyze_batch.py` — batch + partial failure
- `tests/test_api/test_parameters.py` — CRUD
- `tests/test_api/test_documents.py` — upload + status
- `tests/test_api/test_ingest_triggers.py`
- `tests/test_api/test_health.py`
- `tests/test_api/test_auth.py` — API key validation
- `tests/test_api/test_rate_limit.py`
- `tests/test_e2e/test_structured_pipeline.py`
- `tests/test_e2e/test_unstructured_pipeline.py`
- `tests/test_e2e/test_analysis_flow.py`

**Acceptance**: `pytest tests/test_api/` — integration tests pass with live Docker services.

### 5.6 — CI/CD (L4 Claude Sonnet, ~60 min)
**Depends on**: 5.5

**Files**:
- `.github/workflows/test.yml` — CI pipeline:
  - lint (ruff, mypy) → unit tests → integration tests (with service containers) → coverage report
- `.github/workflows/deploy.yml` — CD pipeline:
  - Build Docker image → push to registry → deploy
- **L4 model**: GitHub Actions YAML needs multi-file reasoning (matrix builds, service containers, caching)

**Acceptance**: PR triggers lint + test GitHub Action. Push to main triggers deploy.

### 5.7 — Structured Logging + Monitoring (L1, ~45 min)
**Depends on**: 5.1

**Files**:
- `src/logging_config.py`:
  - Structured JSON logging (loguru or structlog)
  - Request ID in every log line
  - Log levels: per-module configuration
- Prometheus metrics:
  - `lab_analysis_requests_total` (counter)
  - `lab_analysis_duration_seconds` (histogram)
  - `lab_ingestion_chunks_created` (counter)
  - `lab_llm_tokens_used` (counter, by model)

**Acceptance**: Log output is valid JSON with `request_id`, `module`, `level`, `message` fields.

---

## Summary

| Phase | Tasks | Total Time | L1 | L2 | L3 | L4 | L5 | Manual |
|-------|-------|-----------|----|----|----|----|----|--------|
| 0. Environment | 6 | ~4 h | — | — | — | — | — | 6 |
| 1. Foundation | 9 | ~12 h | 9 | — | — | — | — | — |
| 2. Structured Ingestion | 8 | ~18 h | 4 | 1 | 1 | 1 | — | — |
| 3. Unstructured Ingestion | 8 | ~16 h | 3 | 1 | 1 | 1 | — | — |
| 4. Analysis Engine | 4 | ~16 h | 3 | 1 | — | — | 1 | — |
| 5. API & Polish | 7 | ~14 h | 4 | 1 | — | 1 | — | — |
| **Total** | **42** | **~80 h** | **23** | **4** | **2** | **3** | **1** | **6** |

### Model Allocation

| Tier | Tasks | % | When |
|------|-------|---|------|
| L1 — Local Small | 23 | 55% | Boilerplate, parsers, schemas, tests, CRUD |
| L2 — Local Large | 4 | 10% | Complex logic (chunker, resolver, rate limit) |
| L3 — Local Medical | 2 | 5% | Medical text (LOINC mapping, section detection) |
| L4 — Cloud Coding | 3 | 7% | Cross-module orchestration, CI/CD |
| L5 — Cloud Reasoning | 1 | 2% | **Clinical synthesis — safety critical** |
| Manual | 6 | 14% | Env setup (one-time) |

**Local model total: 69% of tasks. Cloud model total: 10%. Manual setup: 14%.**

### Development Order

```
Phase 0 ──────────────────────────────────────────────────
  │
  ▼
Phase 1.1 → 1.2 → 1.3 → 1.4 → 1.5 → 1.6 → 1.7 → 1.9
  │                              │
  │                              ▼
  │                     Phase 2.1 → 2.2 → 2.3 → 2.4
  │                              │
  │                     Phase 2.5 ←─────────────────────────
  │                              │
  │                     Phase 2.6 ←──── 2.7 ←── 2.8
  │
  │                     Phase 3.1 → 3.2 → 3.3
  │                              │
  │                     Phase 3.4 ─→ 3.5 ─→ 3.6 ─→ 3.7
  │                              │
  │                              └── 3.8
  │
  │                     Phase 4.1 → 4.2 → 4.3 → 4.4
  │
  │                     Phase 5.1 → 5.2 → 5.3 → 5.4 → 5.5 → 5.6 → 5.7
  │
  ▼
Done
```

Tasks within a phase should be done sequentially (each depends on the previous). Phases can partially overlap: Phase 2 can start once **Phase 1.5 (schemas)** is done; Phase 3 can start once **Phase 1.2 (config)** is done.
