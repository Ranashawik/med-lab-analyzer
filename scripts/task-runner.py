#!/usr/bin/env python3
"""
Task Runner — automatikus modellválasztás + kódgenerálás a med-lab-analyzerhez.

Használat:
  python scripts/task-runner.py --task phase2.2 --output src/ingestion/structured/spiders/labtests_online.py

Opciók:
  --task ID         Task azonosító a TASK_MODEL_MAP-ból (pl. phase2.2)
  --output PATH     Kimeneti fájl elérési útja
  --prompt TEXT     (opcionális) Egyedi prompt, ha nincs a task registry-ben
  --model NAME      (opcionális) Modell override
  --provider NAME   (opcionális) Provider override: ollama, openai, anthropic
  --list-tasks      Kilistázza az összes task-ot és modelljét

Példák:
  # Task futtatás a registry alapján
  python scripts/task-runner.py --task phase1.2 --output src/config.py

  # Egyedi prompt + modell override
  python scripts/task-runner.py --task phase2.5 --output src/ingestion/structured/normalizer.py --model biomistral:7b

  # Task-ek listázása
  python scripts/task-runner.py --list-tasks
"""

import argparse
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Optional


# ═══════════════════════════════════════════════════════════════
# TASK → MODELL MAPPING (az implementation-plan.md alapján)
# ═══════════════════════════════════════════════════════════════

TASK_MODEL_MAP: dict[str, dict] = {
    # ── Phase 1: Foundation ──
    "phase1.1": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "30 min",
        "desc": "pyproject.toml — project config, dependencies, tool settings",
    },
    "phase1.2": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "src/config.py — pydantic-settings Settings class with all env vars",
    },
    "phase1.3": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "src/db/base.py + models.py — SQLAlchemy ORM models (4 tables)",
    },
    "phase1.4": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "30 min",
        "desc": "src/db/session.py — async engine + session factory + get_session",
    },
    "phase1.5": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "src/api/schemas.py — all Pydantic models (enums, I/O, CRUD, batch)",
    },
    "phase1.6": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "src/api/main.py — FastAPI app skeleton with middleware, routers, handlers",
    },
    "phase1.7": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "30 min",
        "desc": "src/api/routes/health.py — GET /api/v1/health with component checks",
    },
    "phase1.8": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "30 min",
        "desc": "docker-compose.yml — PostgreSQL, Qdrant, Redis, API, Prefect",
    },
    "phase1.9": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "20 min",
        "desc": "alembic/versions/001_initial_schema.py — async Alembic migration",
    },
    # ── Phase 2: Structured Ingestion ──
    "phase2.1": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "AbstractSpider base class — discover, extract, run with retry + rate limit",
    },
    "phase2.2": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "LabTestsOnlineSpider — HTML table parser with selectolax",
    },
    "phase2.3": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "MayoClinicSpider — Mayo-specific HTML structure parser",
    },
    "phase2.4": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "ARUPSpider — ARUP-specific HTML + algorithm description parser",
    },
    "phase2.5": {
        "model": "biomistral:7b",
        "provider": "ollama",
        "tier": "L3",
        "time": "90 min",
        "desc": "normalizer.py — LOINC lookup, UCUM unit conversion, range parsing",
    },
    "phase2.6": {
        "model": "qwen2.5-coder:32b",
        "provider": "ollama",
        "tier": "L2",
        "time": "90 min",
        "desc": "resolver.py — conflict resolution (confidence, source priority, audit)",
    },
    "phase2.7": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "structured/pipeline.py — Prefect flow for structured ingestion",
    },
    "phase2.8": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "routes/parameters.py — CRUD endpoints for lab parameters",
    },
    # ── Phase 3: Unstructured Ingestion ──
    "phase3.1": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "pdf_parser.py — pymupdf wrapper, text/table/heading extraction",
    },
    "phase3.2": {
        "model": "qwen2.5-coder:32b",
        "provider": "ollama",
        "tier": "L2",
        "time": "90 min",
        "desc": "chunker.py — semantic chunking at section boundaries, 300-500 tokens",
    },
    "phase3.3": {
        "model": "biomistral:7b",
        "provider": "ollama",
        "tier": "L3",
        "time": "45 min",
        "desc": "section boundary detector — regex + medical heading patterns",
    },
    "phase3.4": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "embedder.py — sentence-transformers PubMedBERT embedding wrapper",
    },
    "phase3.5": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "qdrant_store.py — Qdrant client, collection mgmt, upsert, search",
    },
    "phase3.6": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "unstructured/pipeline.py — Prefect flow for PDF ingestion pipeline",
    },
    "phase3.7": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "routes/documents.py — PDF upload + status tracking endpoints",
    },
    "phase3.8": {
        "model": "claude-sonnet-4-20250514",
        "provider": "anthropic",
        "tier": "L4",
        "time": "45 min",
        "desc": "routes/ingest.py — ingestion trigger endpoints (cross-module orchestration)",
    },
    # ── Phase 4: Analysis Engine ──
    "phase4.1": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "60 min",
        "desc": "analyzer.py — deterministic flag_value + reference range resolution",
    },
    "phase4.2": {
        "model": "qwen2.5-coder:32b",
        "provider": "ollama",
        "tier": "L2",
        "time": "90 min",
        "desc": "rag.py — RAG query construction, Qdrant retrieval, prompt building",
    },
    "phase4.3": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "llm.py — ClinicalLLM client with routing + caching + fallback",
    },
    "phase4.4": {
        "model": "claude-opus-4-20250514",
        "provider": "anthropic",
        "tier": "L5",
        "time": "90 min",
        "desc": "⚠️ CLINICAL SAFETY — routes/analysis.py — POST /analyze + /analyze/batch (L5 mandatory)",
    },
    # ── Phase 5: API & Polish ──
    "phase5.1": {
        "model": "qwen2.5-coder:32b",
        "provider": "ollama",
        "tier": "L2",
        "time": "60 min",
        "desc": "middleware.py — error handling, request ID, process time headers",
    },
    "phase5.2": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "rate limiting — Redis-based sliding window with slowapi",
    },
    "phase5.3": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "auth.py — API key authentication (optional, toggle via env var)",
    },
    "phase5.4": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "90 min",
        "desc": "unit tests — conftest.py, mocks.py, model/ingestion/engine tests",
    },
    "phase5.5": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "90 min",
        "desc": "integration + E2E tests — API tests, full pipeline tests",
    },
    "phase5.6": {
        "model": "claude-sonnet-4-20250514",
        "provider": "anthropic",
        "tier": "L4",
        "time": "60 min",
        "desc": "CI/CD — GitHub Actions (lint → test → build → deploy)",
    },
    "phase5.7": {
        "model": "qwen2.5-coder:7b",
        "provider": "ollama",
        "tier": "L1",
        "time": "45 min",
        "desc": "logging_config.py — structured JSON logging + Prometheus metrics",
    },
}


# ═══════════════════════════════════════════════════════════════
# PROMPT TEMPLATE
# ═══════════════════════════════════════════════════════════════

CODING_PROMPT_TEMPLATE = """You are implementing a module for the Medical Laboratory Parameter Analyzer project.

PROJECT CONTEXT:
This system ingests structured lab parameter data (web scraping) and unstructured clinical knowledge (PDFs), stores them in PostgreSQL and Qdrant, then evaluates patient lab results through a deterministic + RAG + LLM pipeline. FastAPI backend, SQLAlchemy async ORM, Pydantic v2.

CODING STANDARDS:
- Python 3.11+, async/await throughout
- Type hints on all functions (mypy strict)
- Pydantic v2 models (BaseModel, ConfigDict(from_attributes=True))
- SQLAlchemy 2.0 style (DeclarativeBase, mapped_column)
- Docstrings on all public functions (Google style)
- No print() — use structlog or logging
- Ruff-compatible formatting

MODEL TIER: {tier}
TASK: {task_id}
ESTIMATED TIME: {time}

FILE TO CREATE/MODIFY: {output_file}

TASK DESCRIPTION:
{task_description}

TASK SPECIFICATION FROM ARCHITECTURE PLAN:
{task_spec}

Generate ONLY the file content. No explanations, no markdown formatting around the code.
"""


def get_task_spec(task_id: str) -> str:
    """Visszaadja a task specifikációját az implementation-plan-ból."""
    specs = {
        "phase1.1": textwrap.dedent("""\
            Create pyproject.toml with:
            - Project metadata (name=med-lab-analyzer, version=0.1.0)
            - Dependencies: fastapi>=0.115, uvicorn[standard], sqlalchemy[asyncio]>=2.0, asyncpg,
              alembic, pydantic>=2.5, pydantic-settings, qdrant-client>=1.9, redis[hiredis],
              httpx>=0.27, selectolax>=0.3, pymupdf>=1.24, sentence-transformers, prefect>=3.0,
              tenacity, slowapi, structlog, prometheus-client
            - Dev deps: pytest>=8, pytest-asyncio, pytest-cov, ruff>=0.3, mypy
            - [tool.ruff] and [tool.mypy] sections
            
            Also create src/__init__.py (empty)."""),
        "phase1.2": textwrap.dedent("""\
            Create src/config.py with pydantic-settings Settings class.
            Env vars: DATABASE_URL, QDRANT_URL, REDIS_URL, OLLAMA_URL,
            OPENROUTER_API_KEY, ANTHROPIC_API_KEY, OPENAI_API_KEY,
            API_AUTH_ENABLED, API_KEYS, LOG_LEVEL, VERSION,
            CLINICAL_LLM_MODEL (default: claude-opus-4),
            CRITICAL_THRESHOLDS (JSON dict with per-parameter panic ranges).
            Use SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')."""),
        "phase1.3": textwrap.dedent("""\
            Create src/db/__init__.py, src/db/base.py, src/db/models.py.
            Tables: LabParameter, ReferenceRange, ClinicalDocument, ClinicalChunk.
            SQLAlchemy 2.0 style (DeclarativeBase, mapped_column).
            All columns from the architecture plan (see schema section).
            UUID primary keys with gen_random_uuid().
            ForeignKey relationships with cascade delete.
            Indexes on: reference_ranges.parameter_id, reference_ranges(age, sex),
            clinical_chunks.document_id.""")
        .strip(),
        "phase1.4": textwrap.dedent("""\
            Create src/db/session.py.
            create_async_engine from settings.DATABASE_URL.
            async_sessionmaker with expire_on_commit=False.
            get_session() async generator for FastAPI dependency injection.
            Proper dispose on shutdown.""")
        .strip(),
        "phase1.5": textwrap.dedent("""\
            Create src/api/__init__.py and src/api/schemas.py.
            All Pydantic models from the API specification:
            Enums: Flag, Sex, ParameterCategory, SourceType
            I/O: LabResultInput, AnalysisRequest, AnalysisResponse, AnalyzedResult, ReferenceRange, PatientDemographics
            Batch: BatchAnalysisRequest, BatchAnalysisResponse, BatchPatientResult
            CRUD: LabParameterCreate/LabParameterResponse, ReferenceRangeCreate/ReferenceRangeResponse
            Documents: DocumentUploadResponse, DocumentStatusResponse
            Ingestion: IngestTriggerResponse
            Error: ErrorDetail, ErrorResponse
            Pagination: PaginationParams, PaginatedResponse
            Health: HealthResponse
            Use Field validators (min_length, max_length, ge, le, pattern).
            ConfigDict(from_attributes=True) on response models.""")
        .strip(),
        "phase1.6": textwrap.dedent("""\
            Create src/api/main.py.
            FastAPI app with title="Medical Lab Analyzer", version="0.1.0".
            CORS middleware (allow all origins for dev).
            Request ID middleware (UUID per request in state + header).
            lifespan handler: verify DB, Qdrant, Redis on startup; close on shutdown.
            Mount routers: health, analysis, parameters, documents, ingest.
            Exception handlers: RequestValidationError, RateLimitExceeded, generic Exception.
            Root redirect / → /docs.""")
        .strip(),
        "phase1.7": textwrap.dedent("""\
            Create src/api/routes/__init__.py (router aggregation) and src/api/routes/health.py.
            GET /api/v1/health returns HealthResponse with component checks:
            postgres: SELECT 1 ping, qdrant: REST /health call, redis: PING,
            ollama: GET /api/tags, prefect: optional.
            Aggregate status: healthy if all critical OK, degraded if non-critical fail, unhealthy if critical fail.
            Critical components: postgres, qdrant.""")
        .strip(),
        "phase1.8": textwrap.dedent("""\
            Create docker-compose.yml with services:
            postgres:16-alpine (port 5432, volume for data, env POSTGRES_DB=med_lab)
            qdrant/qdrant:latest (port 6333, volume for storage)
            redis:7-alpine (port 6379)
            api: build from ., port 8000, hot-reload with uvicorn --reload
            prefect-server: prefecthq/prefect:3-latest (port 4200, optional).""")
        .strip(),
        "phase1.9": textwrap.dedent("""\
            Create alembic.ini and alembic/env.py for async Alembic.
            env.py: async engine from config, run_async() for migrations.
            Create initial migration revision importing all models from src.db.models.
            Migration creates all 4 tables with proper constraints and indexes.
            Also create alembic/versions/__init__.py (empty).""")
        .strip(),
        "phase2.1": textwrap.dedent("""\
            Create spider framework files: src/ingestion/__init__.py,
            src/ingestion/structured/__init__.py, src/ingestion/structured/spiders/__init__.py,
            and src/ingestion/structured/spiders/base.py.
            AbstractSpider class with:
            - source_name, base_url class attributes
            - discover() -> list[str]: discover test page URLs
            - extract(url) -> dict|None: parse HTML to structured data
            - run() -> list[dict]: discover → extract loop
            - retry with tenacity (stop=stop_after_attempt(3), wait=wait_exponential)
            - rate limiting (1 req/sec with asyncio.sleep)
            - httpx.AsyncClient for HTTP
            - Pydantic model for extracted data (SpiderResult)""")
        .strip(),
        "phase2.2": textwrap.dedent("""\
            Create src/ingestion/structured/spiders/labtests_online.py.
            Extends AbstractSpider with source_name="labtests_online".
            discover(): fetch index page, extract test detail page URLs.
            extract(): parse HTML tables with selectolax to get:
            test name, reference range text, units, specimen type, methodology.
            Returns dict matching ReferenceRangeCreate schema.
            Include error handling for missing tables or changed page structure.""")
        .strip(),
        "phase2.3": textwrap.dedent("""\
            Create src/ingestion/structured/spiders/mayo_clinic.py.
            Extends AbstractSpider with source_name="mayo_clinic".
            Handles Mayo's specific HTML structure (different table layout from LabTestsOnline).
            Extracts: test ID, name, aliases, reference ranges by age/sex, CPT codes.
            Mayo pages have a different DOM structure — use selectolax with Mayo-specific selectors.""")
        .strip(),
        "phase2.4": textwrap.dedent("""\
            Create src/ingestion/structured/spiders/arup.py.
            Extends AbstractSpider with source_name="arup".
            ARUP has algorithm-based test descriptions (harder to parse than simple tables).
            Extracts: test name, methodology, reference ranges, turnaround time.
            May need fallback to regex-based extraction for algorithm descriptions.""")
        .strip(),
        "phase2.5": textwrap.dedent("""\
            Create src/ingestion/structured/normalizer.py.
            Functions:
            - normalize_parameter(raw_name, source) -> NormalizedParam
            - lookup_loinc(name) -> str|None: fuzzy match to LOINC via REST + local cache
            - convert_unit(value, from_unit, to_unit) -> float: UCUM conversion with factor table
            - parse_range(text) -> tuple[float, float]: parse "3.5-5.0 mmol/L" -> (3.5, 5.0)
            - validate_normalized(data: dict) -> bool: Pydantic validation
            Edge cases: negative values, percentages, non-standard units, missing LOINC codes.
            Unit conversion table: mg/dL↔g/L, mmol/L↔mg/dL (glucose), mg/dL↔umol/L (creatinine).
            L3 model: use BioMistral for improved medical term matching.""")
        .strip(),
        "phase2.6": textwrap.dedent("""\
            Create src/ingestion/structured/resolver.py.
            Classes/functions:
            - resolve_ranges(ranges: list[dict], strategy='confidence') -> dict
            - SourceReliability: enum with score per source
            - flag_for_review(conflict) -> None: write to audit_log
            - resolve_demographics(ranges, patient) -> list[dict]: filter by age/sex/condition
            Strategies: confidence_score (default), newest_source, most_specific_demographics.
            Audit log entry: parameter_id, sources, values, resolving_strategy, timestamp.
            L2 model: logic-heavy with edge cases in conflict resolution.""")
        .strip(),
        "phase2.8": textwrap.dedent("""\
            Create src/api/routes/parameters.py.
            Endpoints:
            - GET /api/v1/parameters — list with search, category filter, pagination
            - GET /api/v1/parameters/{id} — single parameter with reference range count
            - POST /api/v1/parameters — create LabParameter
            - PUT /api/v1/parameters/{id} — update
            - DELETE /api/v1/parameters/{id} — delete with cascade
            - GET /api/v1/parameters/{id}/ranges — list ranges with optional demographic filter
            - POST /api/v1/parameters/{id}/ranges — add ReferenceRange
            All async, use db session from get_session dependency.""")
        .strip(),
        "phase3.1": textwrap.dedent("""\
            Create src/ingestion/unstructured/__init__.py and src/ingestion/unstructured/pdf_parser.py.
            Classes:
            - PdfResult: dataclass with text, tables, headings, metadata
            Functions:
            - parse_pdf(file_path) -> PdfResult: pymupdf wrapper
            - extract_tables(page) -> list[str]: table-to-markdown
            - get_metadata(file_path) -> dict: title, authors, page count
            - is_scanned_pdf(file_path) -> bool: detect image-only PDFs
            For scanned PDFs, raise clear error suggesting marker-pdf fallback.
            Handle: missing metadata, encrypted PDFs, 0-page PDFs.""")
        .strip(),
        "phase3.4": textwrap.dedent("""\
            Create src/ingestion/unstructured/embedder.py.
            Classes:
            - EmbeddingModel: wraps sentence-transformers
            - Uses pritamdeka/S-PubMedBert-MS-MARCO (768-d vectors)
            - Auto-downloads model on first use
            Methods:
            - embed(texts: list[str]) -> list[list[float]]: batch embedding
            - embed_single(text: str) -> list[float]: convenience
            Configurable batch_size (default: 32).
            Caching: identical text returns cached vector (LRU cache with maxsize=10000).
            ONNX optional: try onnxruntime for 2-3x speedup.""")
        .strip(),
        "phase3.5": textwrap.dedent("""\
            Create src/ingestion/unstructured/qdrant_store.py.
            Classes:
            - QdrantStore: wraps qdrant-client
            Methods:
            - create_collection(name='clinical_knowledge', dim=768, distance='Cosine')
            - upsert_chunks(chunks, vectors): batch insert with payload
            - search(query_vector, top_k=5, filters=None): search with filters
            - delete_collection(): cleanup
            - collection_exists(name): bool
            Payload: {chunk_id, document_id, section_heading, source_type, publication_year}
            Create payload indexes on source_type, publication_year.
            Connection: from settings.QDRANT_URL, gRPC optional.""")
        .strip(),
        "phase4.1": textwrap.dedent("""\
            Create src/engine/__init__.py and src/engine/analyzer.py.
            Functions:
            - flag_value(value, low, high, critical_thresholds=None) -> Flag enum
            - resolve_reference_range(parameter_name, patient, db_session) -> ReferenceRange | None
            - find_best_range(ranges, patient) -> dict: age match > sex match > condition > confidence
            - analyze_single(result, patient, db_session) -> AnalyzedResult
            Pure deterministic logic — no AI calls. Must handle:
            - Missing reference ranges (return UNKNOWN)
            - Exact boundary values (inclusive)
            - Multiple matching ranges (pick best)
            - Critical thresholds per parameter (configurable)""")
        .strip(),
        "phase4.3": textwrap.dedent("""\
            Create src/engine/llm.py.
            Classes:
            - ClinicalLLM: async HTTP client for OpenAI-compatible API
            Methods:
            - synthesize(context, language='en', model=None) -> str
            Router: uses CLINICAL_LLM_MODEL from settings
            Supports: Ollama (local), OpenAI, Anthropic via their respective API formats
            Fallback chain: primary -> first fallback -> error
            Redis caching: cache key = hash(context + model), TTL = 7 days
            Config: OLLAMA_URL, OPENAI_API_KEY, ANTHROPIC_API_KEY env vars
            Error handling: timeout, rate limit, auth error -> log + fallback""")
        .strip(),
    }
    return specs.get(task_id, "No detailed specification available. Use generic coding standards.")


# ═══════════════════════════════════════════════════════════════
# API HÍVÁS
# ═══════════════════════════════════════════════════════════════

def call_ollama(model: str, prompt: str, temperature: float = 0.3) -> Optional[str]:
    """Meghívja az Ollama API-t az OpenAI kompatibilis endpointon."""
    import httpx

    ollama_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")
    url = f"{ollama_url.rstrip('/')}/v1/chat/completions"

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a senior Python developer implementing a medical lab analyzer system. Generate clean, type-hinted, async Python code."},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "max_tokens": 8192,
    }

    try:
        resp = httpx.post(url, json=payload, timeout=300)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except httpx.TimeoutException:
        print(json.dumps({"status": "error", "error": "Ollama request timed out after 300s"}), file=sys.stderr)
        return None
    except httpx.HTTPStatusError as e:
        print(json.dumps({"status": "error", "error": f"Ollama HTTP {e.response.status_code}: {e.response.text}"}), file=sys.stderr)
        return None
    except Exception as e:
        print(json.dumps({"status": "error", "error": f"Ollama error: {str(e)}"}), file=sys.stderr)
        return None


def call_cloud_api(model: str, provider: str, prompt: str) -> Optional[str]:
    """Meghívja a cloud API-t (OpenRouter, Anthropic vagy OpenAI).
    
    OpenRouter az ajánlott — egy API kulcs, minden modell elérhető.
    Környezeti változók (prioritási sorrendben):
      1. OPENROUTER_API_KEY — ajánlott, minden modellhez
      2. ANTHROPIC_API_KEY — csak Anthropic modellekhez
      3. OPENAI_API_KEY — csak OpenAI modellekhez
    """
    import httpx

    # ── 1. OpenRouter (ajánlott — egy API kulcs minden modellhez) ──
    openrouter_key = os.environ.get("OPENROUTER_API_KEY")
    if openrouter_key:
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {openrouter_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/Ranashawik/med-lab-analyzer",
            "X-Title": "med-lab-analyzer-task-runner",
        }
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a senior Python developer implementing a medical lab analyzer system. Generate clean, type-hinted, async Python code."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.3,
            "max_tokens": 8192,
        }

        try:
            resp = httpx.post(url, json=payload, headers=headers, timeout=300)
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]
        except httpx.TimeoutException:
            print(json.dumps({"status": "error", "error": "OpenRouter request timed out after 300s"}), file=sys.stderr)
            # Fallback to provider-specific
        except Exception as e:
            print(json.dumps({"status": "warn", "error": f"OpenRouter error, trying provider fallback: {str(e)}"}), file=sys.stderr)
            # Fallback to provider-specific

    # ── 2. Anthropic direkt ──
    if provider == "anthropic":
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if api_key:
            url = "https://api.anthropic.com/v1/messages"
            headers = {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }
            payload = {
                "model": model,
                "max_tokens": 8192,
                "messages": [{"role": "user", "content": prompt}],
            }

            try:
                resp = httpx.post(url, json=payload, headers=headers, timeout=300)
                resp.raise_for_status()
                data = resp.json()
                return data["content"][0]["text"]
            except Exception as e:
                print(json.dumps({"status": "error", "error": f"Anthropic API error: {str(e)}"}), file=sys.stderr)
                return None

    # ── 3. OpenAI direkt ──
    if provider == "openai":
        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key:
            url = "https://api.openai.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": "You are a senior Python developer implementing a medical lab analyzer system. Generate clean, type-hinted, async Python code."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.3,
                "max_tokens": 8192,
            }

            try:
                resp = httpx.post(url, json=payload, headers=headers, timeout=300)
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            except Exception as e:
                print(json.dumps({"status": "error", "error": f"OpenAI API error: {str(e)}"}), file=sys.stderr)
                return None

    # ── Nincs egyik API kulcs sem ──
    print(json.dumps({
        "status": "error",
        "error": f"No API key available for {provider} model '{model}'. "
                 f"Set OPENROUTER_API_KEY (recommended), or {provider.upper()}_API_KEY."
    }), file=sys.stderr)
    return None


# ═══════════════════════════════════════════════════════════════
# PI INTEGRÁCIÓ
# ═══════════════════════════════════════════════════════════════

def is_pi_available() -> bool:
    """Ellenőrzi, hogy a Pi CLI elérhető-e."""
    try:
        result = subprocess.run(["pi", "--version"], capture_output=True, text=True, timeout=5)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def call_pi_via_pty(model: str, prompt: str) -> Optional[str]:
    """Elindítja a Pi-t PTY módban, beállítja a modellt, elküldi a prompt-ot, visszakapja a kódot.
    
    Ez a függvény csak akkor működik, ha a task-runner-t egy Hermes session-ben futtatjuk,
    mert a PTY kezeléshez terminál emuláció kell.
    
    Használat helyett: a Hermes delegate_task-al vagy terminal(pty=true)-vel kezeli a Pi-t.
    """
    print(json.dumps({
        "status": "info",
        "message": f"Pi available. Use Hermes terminal(pty=true) to interact:\n"
                   f"  $ pi\n"
                   f"  > /model {model}\n"
                   f"  > {prompt[:100]}..."
    }))
    return None


# ═══════════════════════════════════════════════════════════════
# FŐ LOGIKA
# ═══════════════════════════════════════════════════════════════

def build_prompt(task_id: str, task_info: dict, output_file: str, custom_prompt: str | None = None) -> str:
    """Felépíti a prompt-ot a task specifikáció alapján."""
    task_spec = get_task_spec(task_id) if custom_prompt is None else custom_prompt

    return CODING_PROMPT_TEMPLATE.format(
        tier=task_info["tier"],
        task_id=task_id,
        time=task_info["time"],
        output_file=output_file,
        task_description=task_info["desc"],
        task_spec=task_spec,
    )


def write_output(file_path: str, content: str) -> bool:
    """Fájlba írja a generált kódot. Létrehozza a szülő könyvtárakat."""
    try:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return True
    except Exception as e:
        print(json.dumps({"status": "error", "error": f"Failed to write {file_path}: {str(e)}"}), file=sys.stderr)
        return False


def list_tasks() -> None:
    """Kilistázza az összes task-ot."""
    print(f"{'Task ID':<12} {'Tier':<6} {'Provider':<12} {'Model':<30} {'Time':<8} Description")
    print("-" * 120)
    for task_id in sorted(TASK_MODEL_MAP.keys()):
        info = TASK_MODEL_MAP[task_id]
        short_desc = info["desc"][:55] + ("..." if len(info["desc"]) > 55 else "")
        print(f"{task_id:<12} {info['tier']:<6} {info['provider']:<12} {info['model']:<30} {info['time']:<8} {short_desc}")
    print()
    print(f"Total: {len(TASK_MODEL_MAP)} tasks")
    print(f"Local (Ollama): {sum(1 for t in TASK_MODEL_MAP.values() if t['provider'] == 'ollama')} tasks")
    print(f"Cloud (API): {sum(1 for t in TASK_MODEL_MAP.values() if t['provider'] != 'ollama')} tasks")


def main():
    parser = argparse.ArgumentParser(description="Task Runner for med-lab-analyzer")
    parser.add_argument("--task", type=str, help="Task ID (pl. phase2.2)")
    parser.add_argument("--output", type=str, help="Kimeneti fájl elérési útja")
    parser.add_argument("--prompt", type=str, help="(Opcionális) Egyedi prompt override")
    parser.add_argument("--model", type=str, help="(Opcionális) Modell override")
    parser.add_argument("--provider", type=str, choices=["ollama", "openai", "anthropic"], help="(Opcionális) Provider override")
    parser.add_argument("--list-tasks", action="store_true", help="Kilistázza az összes task-ot")
    parser.add_argument("--dry-run", action="store_true", help="Csak kiírja, hogy mi történne, nem hívja meg az API-t")
    parser.add_argument("--temperature", type=float, default=0.3, help="Modell temperature (default: 0.3)")

    args = parser.parse_args()

    if args.list_tasks:
        list_tasks()
        return

    if not args.task or not args.output:
        parser.print_help()
        sys.exit(1)

    task_id = args.task.lower()
    if task_id not in TASK_MODEL_MAP:
        print(json.dumps({"status": "error", "error": f"Unknown task: {task_id}. Use --list-tasks to see available tasks."}), file=sys.stderr)
        sys.exit(1)

    task_info = dict(TASK_MODEL_MAP[task_id])

    # Override-ok
    if args.model:
        task_info["model"] = args.model
    if args.provider:
        task_info["provider"] = args.provider

    model = task_info["model"]
    provider = task_info["provider"]
    tier = task_info["tier"]

    # Prompt felépítése
    prompt_text = build_prompt(task_id, task_info, args.output, args.prompt)
    output_path = args.output

    # Ellenőrzés: cloud modellekhez API kulcs
    if provider != "ollama":
        # OpenRouter az ajánlott — egy API kulcs minden modellhez
        has_openrouter = bool(os.environ.get("OPENROUTER_API_KEY"))
        has_provider_key = bool(os.environ.get(f"{provider.upper()}_API_KEY"))
        if not has_openrouter and not has_provider_key:
            result = {
                "status": "error",
                "error": f"No API key available for {provider} model '{model}' (tier {tier}). "
                         f"Set OPENROUTER_API_KEY (recommended) or {provider.upper()}_API_KEY.",
                "task": task_id,
                "model": model,
                "tier": tier,
            }
            print(json.dumps(result))
            sys.exit(1)

    # DRY RUN
    if args.dry_run:
        result = {
            "status": "dry_run",
            "task": task_id,
            "model": model,
            "provider": provider,
            "tier": tier,
            "output": output_path,
            "prompt_length": len(prompt_text),
            "prompt_preview": prompt_text[:500] + "...",
        }
        print(json.dumps(result, indent=2))
        return

    # API hívás
    print(json.dumps({
        "status": "running",
        "task": task_id,
        "model": model,
        "provider": provider,
        "tier": tier,
        "output": output_path,
    }))

    # Ellenőrizzük a Pi elérhetőségét
    pi_available = is_pi_available()
    generated_code = None

    if provider == "ollama":
        if pi_available:
            # Pi elérhető — használjuk
            call_pi_via_pty(model, prompt_text)
            print(json.dumps({
                "status": "info",
                "message": f"Pi is available! Run this in Hermes with terminal(pty=true):\n"
                          f"  pi\n"
                          f"  > /model {model}\n"
                          f"  > [paste prompt or describe task]\n\n"
                          f"Or use direct Ollama API (--provider ollama without Pi override).",
            }))

        # Minden esetben hívjuk az Ollama API-t direkt
        generated_code = call_ollama(model, prompt_text, temperature=args.temperature)

    elif provider in ("anthropic", "openai"):
        generated_code = call_cloud_api(model, provider, prompt_text)

    if generated_code is None:
        result = {
            "status": "error",
            "error": f"Model {model} ({provider}) returned no output.",
            "task": task_id,
        }
        print(json.dumps(result))
        sys.exit(1)

    # Kimenet tisztítása (code block wrapper eltávolítása)
    cleaned = generated_code.strip()
    if cleaned.startswith("```"):
        # Többféle formátum: ```python\n...```, ```\n...```
        lines = cleaned.split("\n")
        # Első sor: ```python vagy ```
        start = 1
        if lines[0].startswith("```"):
            start = 1
        # Utolsó sor: ```
        end = -1 if lines[-1].strip() == "```" else len(lines)
        cleaned = "\n".join(lines[start:end]).strip()

    # Fájlba írás
    if write_output(output_path, cleaned):
        result = {
            "status": "success",
            "task": task_id,
            "model": model,
            "provider": provider,
            "tier": tier,
            "output": output_path,
            "bytes_written": len(cleaned.encode("utf-8")),
            "lines": cleaned.count("\n") + 1,
        }
        print(json.dumps(result))
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
