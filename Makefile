# med-lab-analyzer — Task Runner Makefile
# =========================================
# Használat: make <task_id>
# Pl.:       make phase2.2
# Lista:     make list

RUNNER = python scripts/task-runner.py

# ════════════════════════════════════════════════════════════════════
# PHASE 1: Foundation
# ════════════════════════════════════════════════════════════════════

phase1.1:  ## pyproject.toml — project config, dependencies (L1, 30 min)
	$(RUNNER) --task phase1.1 --output pyproject.toml

phase1.2:  ## src/config.py — Settings class (L1, 45 min)
	$(RUNNER) --task phase1.2 --output src/config.py

phase1.3:  ## Database models — 4 SQLAlchemy ORM tables (L1, 60 min)
	$(RUNNER) --task phase1.3 --output src/db/models.py

phase1.4:  ## src/db/session.py — async engine + session (L1, 30 min)
	$(RUNNER) --task phase1.4 --output src/db/session.py

phase1.5:  ## src/api/schemas.py — all Pydantic models (L1, 60 min)
	$(RUNNER) --task phase1.5 --output src/api/schemas.py

phase1.6:  ## src/api/main.py — FastAPI app skeleton (L1, 45 min)
	$(RUNNER) --task phase1.6 --output src/api/main.py

phase1.7:  ## Health check route (L1, 30 min)
	$(RUNNER) --task phase1.7 --output src/api/routes/health.py

phase1.8:  ## docker-compose.yml (L1, 30 min)
	$(RUNNER) --task phase1.8 --output docker-compose.yml

phase1.9:  ## Alembic initial migration (L1, 20 min)
	$(RUNNER) --task phase1.9 --output alembic/env.py

# ════════════════════════════════════════════════════════════════════
# PHASE 2: Structured Ingestion
# ════════════════════════════════════════════════════════════════════

phase2.1:  ## AbstractSpider base class (L1, 45 min)
	$(RUNNER) --task phase2.1 --output src/ingestion/structured/spiders/base.py

phase2.2:  ## LabTestsOnline spider (L1, 60 min)
	$(RUNNER) --task phase2.2 --output src/ingestion/structured/spiders/labtests_online.py

phase2.3:  ## MayoClinic spider (L1, 60 min)
	$(RUNNER) --task phase2.3 --output src/ingestion/structured/spiders/mayo_clinic.py

phase2.4:  ## ARUP spider (L1, 45 min)
	$(RUNNER) --task phase2.4 --output src/ingestion/structured/spiders/arup.py

phase2.5:  ## normalizer.py — LOINC + UCUM (L3 BioMistral, 90 min)
	$(RUNNER) --task phase2.5 --output src/ingestion/structured/normalizer.py

phase2.6:  ## resolver.py — conflict resolution (L2 Qwen 32B, 90 min)
	$(RUNNER) --task phase2.6 --output src/ingestion/structured/resolver.py

phase2.7:  ## Structured Prefect pipeline (L1, 60 min)
	$(RUNNER) --task phase2.7 --output src/ingestion/structured/pipeline.py

phase2.8:  ## Parameter CRUD routes (L1, 60 min)
	$(RUNNER) --task phase2.8 --output src/api/routes/parameters.py

# ════════════════════════════════════════════════════════════════════
# PHASE 3: Unstructured Ingestion
# ════════════════════════════════════════════════════════════════════

phase3.1:  ## PDF parser — pymupdf wrapper (L1, 60 min)
	$(RUNNER) --task phase3.1 --output src/ingestion/unstructured/pdf_parser.py

phase3.2:  ## Semantic chunker (L2 Qwen 32B, 90 min)
	$(RUNNER) --task phase3.2 --output src/ingestion/unstructured/chunker.py

phase3.3:  ## Section boundary detector (L3 BioMistral, 45 min)
	$(RUNNER) --task phase3.3 --output src/ingestion/unstructured/chunker.py

phase3.4:  ## Embedder — PubMedBERT (L1, 60 min)
	$(RUNNER) --task phase3.4 --output src/ingestion/unstructured/embedder.py

phase3.5:  ## Qdrant store client (L1, 45 min)
	$(RUNNER) --task phase3.5 --output src/ingestion/unstructured/qdrant_store.py

phase3.6:  ## Unstructured Prefect pipeline (L1, 60 min)
	$(RUNNER) --task phase3.6 --output src/ingestion/unstructured/pipeline.py

phase3.7:  ## Document routes — upload + status (L1, 45 min)
	$(RUNNER) --task phase3.7 --output src/api/routes/documents.py

phase3.8:  ## Ingestion trigger routes (L4 Claude Sonnet — cloud, 45 min)
	$(RUNNER) --task phase3.8 --output src/api/routes/ingest.py

# ════════════════════════════════════════════════════════════════════
# PHASE 4: Analysis Engine
# ════════════════════════════════════════════════════════════════════

phase4.1:  ## Core analyzer — flag_value + range resolution (L1, 60 min)
	$(RUNNER) --task phase4.1 --output src/engine/analyzer.py

phase4.2:  ## RAG engine — query + retrieval + prompt building (L2 Qwen 32B, 90 min)
	$(RUNNER) --task phase4.2 --output src/engine/rag.py

phase4.3:  ## LLM client — routing + caching + fallback (L1, 45 min)
	$(RUNNER) --task phase4.3 --output src/engine/llm.py

phase4.4:  ## ⚠️ CLINICAL SAFETY — analysis routes (L5 Claude Opus — cloud, 90 min)
	$(RUNNER) --task phase4.4 --output src/api/routes/analysis.py

# ════════════════════════════════════════════════════════════════════
# PHASE 5: API & Polish
# ════════════════════════════════════════════════════════════════════

phase5.1:  ## Error handling middleware (L2 Qwen 32B, 60 min)
	$(RUNNER) --task phase5.1 --output src/api/middleware.py

phase5.2:  ## Rate limiting — Redis sliding window (L1, 45 min)
	$(RUNNER) --task phase5.2 --output src/api/middleware.py

phase5.3:  ## API key authentication (L1, 45 min)
	$(RUNNER) --task phase5.3 --output src/api/auth.py

phase5.4:  ## Unit tests — conftest, mocks, model tests (L1, 90 min)
	$(RUNNER) --task phase5.4 --output tests/conftest.py

phase5.5:  ## Integration + E2E tests (L1, 90 min)
	$(RUNNER) --task phase5.5 --output tests/test_api/test_analyze.py

phase5.6:  ## CI/CD — GitHub Actions (L4 Claude Sonnet — cloud, 60 min)
	$(RUNNER) --task phase5.6 --output .github/workflows/test.yml

phase5.7:  ## Logging + Prometheus metrics (L1, 45 min)
	$(RUNNER) --task phase5.7 --output src/logging_config.py

# ════════════════════════════════════════════════════════════════════
# HASZNOS PARANCSOK
# ════════════════════════════════════════════════════════════════════

list:  ## List all available tasks with model info
	$(RUNNER) --list-tasks

dry-run:
	@echo "=== DRY RUN — all tasks ==="
	@for task in phase1.1 phase1.2 phase1.3 phase1.4 phase1.5 phase1.6 phase1.7 phase1.8 phase1.9 \
	             phase2.1 phase2.2 phase2.3 phase2.4 phase2.5 phase2.6 phase2.7 phase2.8 \
	             phase3.1 phase3.2 phase3.3 phase3.4 phase3.5 phase3.6 phase3.7 phase3.8 \
	             phase4.1 phase4.2 phase4.3 phase4.4 \
	             phase5.1 phase5.2 phase5.3 phase5.4 phase5.5 phase5.6 phase5.7; do \
		echo "--- $$task ---"; \
		$(RUNNER) --task $$task --output /dev/null --dry-run 2>&1 | python -c "import sys,json; d=json.load(sys.stdin); print(f'  Model: {d[\"model\"]} ({d[\"tier\"]})'); print(f'  Output: {d[\"output\"]}'); print(f'  Prompt: {d[\"prompt_length\"]} chars')"; \
	done

clean:  ## Remove generated files (git-ignored)
	rm -rf src/__pycache__ src/*/__pycache__
	rm -rf .pytest_cache
	rm -rf *.egg-info

.PHONY: list dry-run clean $(shell awk -F':' '/^[a-z][a-z0-9.]+:/ {print $$1}' $(MAKEFILE_LIST))
