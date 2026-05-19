# Development Guide

Guide for setting up the development environment and workflows for the Medical Laboratory Parameter Analyzer.

- [Environment Setup](#environment-setup)
- [LLM Model Strategy](#llm-model-strategy)
- [Local Model Setup (Ollama)](#local-model-setup-ollama)
- [Pi Development Environment](#pi-development-environment)
- [Development Workflow](#development-workflow)
- [Contribution Guidelines](#contribution-guidelines)
- [Code Quality](#code-quality)

---

## Environment Setup

### Prerequisites

- **Python** 3.11+
- **Docker Desktop** (PostgreSQL 16, Qdrant, Redis 7)
- **Ollama** (for local LLM models — optional but recommended)
- **Git** with your GitHub SSH key configured
- **8+ GB RAM** recommended (16+ GB if running local LLMs)

### Step 1: Clone the repository

```bash
git clone https://github.com/Ranashawik/med-lab-analyzer.git
cd med-lab-analyzer
```

### Step 2: Create virtual environment

```bash
# Linux / macOS
python -m venv .venv
source .venv/bin/activate

# Windows
python -m venv .venv
.venv\Scripts\activate
```

### Step 3: Install dependencies

```bash
# Development install (includes dev dependencies)
pip install -e ".[dev]"

# Production only
pip install -e .
```

### Step 4: Start infrastructure

```bash
docker compose up -d
```

This starts:
- **PostgreSQL 16** on `localhost:5432`
- **Qdrant** on `localhost:6333`
- **Redis 7** on `localhost:6379`
- **Prefect Server** on `localhost:4200`

### Step 5: Run migrations

```bash
alembic upgrade head
```

### Step 6: (Optional) Pull local LLM models

```bash
ollama pull qwen2.5-coder:7b          # L1 — primary coding model (4.7 GB)
ollama pull deepseek-coder-v2:16b     # L2 — complex coding (8.9 GB)
ollama pull biomistral:7b             # L3 — medical text (4.1 GB)
ollama pull llama3.1:8b               # L1 — fallback general (4.7 GB)
```

### Step 7: Start the API

```bash
uvicorn src.api.main:app --reload --port 8000
```

Open http://localhost:8000/docs for the interactive API documentation.

---

## LLM Model Strategy

The project uses a **tiered model allocation** strategy. Most development tasks use local models (privacy, zero API cost, offline capability). Cloud models are reserved for tasks requiring clinical reasoning or complex debugging.

### Model Tiers

| Tier | Description | Parameters | Examples | When to Use |
|------|-------------|-----------|----------|-------------|
| **L1 — Local Small** | Consumer GPU / CPU | 7B–14B | Qwen2.5-Coder 7B, Llama 3.1 8B, Phi-4 14B | Boilerplate code, simple parsing, schema generation, scrapers |
| **L2 — Local Large** | Needs 24GB+ VRAM | 20B–32B | Qwen2.5-Coder 32B, DeepSeek-Coder-V2 Lite, Mistral Small 3.1 24B | Complex code, refactoring, test generation, conflict resolution |
| **L3 — Local Medical** | Domain fine-tuned | 7B–13B | BioMistral 7B, MedLlama3 8B, OpenBioLLM 8B | Medical text extraction, LOINC mapping, section boundary detection |
| **L4 — Cloud Coding** | API-based reasoning | — | Claude Sonnet 4, GPT-4.3-Codex, DeepSeek-V4 | Architecture design, complex debugging, PR review |
| **L5 — Cloud Reasoning** | API-based clinical | — | Claude Opus 4, GPT-5.3, Gemini 2.5 Pro | ⚠️ **Clinical synthesis, differential diagnosis — safety critical** |

### Model Allocation by Phase

| Phase | L1 | L2 | L3 | L4 | L5 | Total |
|-------|----|----|----|----|----|-------|
| Phase 1: Foundation | 5 | 0 | 0 | 0 | 0 | 5 |
| Phase 2: Structured | 3 | 1 | 1 | 1 | 0 | 6 |
| Phase 3: Unstructured | 1 | 1 | 2 | 1 | 0 | 5 |
| Phase 4: Analysis | 3 | 1 | 0 | 0 | 1 | 5 |
| Phase 5: API & Polish | 3 | 1 | 0 | 1 | 0 | 5 |
| Ongoing (review, debug) | 0 | 2 | 0 | 2 | 1 | 5 |
| **Total** | **15** | **6** | **3** | **5** | **2** | **31** |
| **%** | **48%** | **19%** | **10%** | **16%** | **6%** | **100%** |

**77% of tasks use local models. Only clinical synthesis and complex debugging require cloud.**

### Clinical Safety Boundary

```
─────────────────────────────────────────────────────────────────
  CRITICAL: The clinical synthesis step (differential diagnosis)
  MUST use a cloud reasoning model (L5: Claude Opus 4 or GPT-5.3).
  
  Local models (even 32B+) lack the medical reasoning depth,
  safety guardrails, and factual accuracy required for clinical
  decision support. This is a HARD boundary — no exceptions.
─────────────────────────────────────────────────────────────────
```

### Detailed Task → Model Mapping

#### Phase 1: Foundation

| Task | Primary Model | Fallback |
|------|--------------|----------|
| FastAPI app skeleton | L1 Qwen2.5-Coder 7B | L4 Claude Sonnet |
| SQLAlchemy ORM models | L1 Qwen2.5-Coder 7B | L4 GPT-4.3-Codex |
| Pydantic schemas | L1 Qwen2.5-Coder 7B | L4 Claude Sonnet |
| Alembic migrations | L1 Qwen2.5-Coder 7B | L2 DeepSeek-Coder-V2 |
| Docker Compose | L1 Qwen2.5-Coder 7B | — |

#### Phase 2: Structured Ingestion

| Task | Primary Model | Fallback |
|------|--------------|----------|
| Web scraper spiders | L1 Qwen2.5-Coder 7B | L4 Claude Sonnet |
| selectolax selectors | L1 Qwen2.5-Coder 7B | — |
| Unit normalizer (UCUM) | L2 DeepSeek-Coder-V2 | L4 GPT-4.3-Codex |
| LOINC mapper | L3 BioMistral 7B | L4 Claude Sonnet |
| Conflict resolver | L2 Qwen2.5-Coder 32B | L4 GPT-4.3-Codex |
| Prefect flow definitions | L1 Qwen2.5-Coder 7B | — |

#### Phase 3: Unstructured Ingestion

| Task | Primary Model | Fallback |
|------|--------------|----------|
| PDF parser wrapper | L1 Qwen2.5-Coder 7B | L4 Claude Sonnet |
| Semantic chunker | L2 Qwen2.5-Coder 32B | L4 GPT-4.3-Codex |
| Section boundary detection | L3 BioMistral 7B | L2 Qwen2.5-Coder 32B |
| Qdrant client + upsert | L1 Qwen2.5-Coder 7B | — |
| Medical concept extraction | L3 BioMistral 7B | L5 Claude Opus |

#### Phase 4: Analysis Engine

| Task | Primary Model | Fallback |
|------|--------------|----------|
| Reference range lookup | L1 Qwen2.5-Coder 7B | — (pure SQL/Python) |
| Abnormal value flagging | L1 Qwen2.5-Coder 7B | — (deterministic logic) |
| RAG query construction | L2 Qwen2.5-Coder 32B | L4 Claude Sonnet |
| **Clinical synthesis** | **L5 Claude Opus 4** | GPT-5.3 |
| Report formatting | L1 Qwen2.5-Coder 7B | L4 Claude Sonnet |

#### Phase 5: API & Polish

| Task | Primary Model | Fallback |
|------|--------------|----------|
| REST endpoints | L1 Qwen2.5-Coder 7B | L4 Claude Sonnet |
| Input validation | L1 Qwen2.5-Coder 7B | — |
| Error handling middleware | L2 Qwen2.5-Coder 32B | L4 GPT-4.3-Codex |
| Test suite | L2 DeepSeek-Coder-V2 | L4 Claude Sonnet |
| Monitoring / logging | L1 Qwen2.5-Coder 7B | — |

---

## Local Model Setup (Ollama)

### Installation

```bash
# Linux
curl -fsSL https://ollama.com/install.sh | sh

# macOS
brew install ollama

# Windows
# Download from https://ollama.com/download
```

### Pull required models

```bash
# L1 — Primary coding (4.7 GB)
ollama pull qwen2.5-coder:7b

# L2 — Complex code + reasoning (8.9–19 GB)
ollama pull deepseek-coder-v2:16b
# or
ollama pull qwen2.5-coder:32b

# L3 — Medical text processing (4.1 GB)
ollama pull biomistral:7b

# L1 — General fallback (4.7 GB)
ollama pull llama3.1:8b
```

Total: ~22–40 GB depending on which L2 model you choose.

### Verify models

```bash
ollama list
# NAME                       ID              SIZE
# qwen2.5-coder:7b           ...            4.7 GB
# deepseek-coder-v2:16b      ...            8.9 GB
# biomistral:7b              ...            4.1 GB
# llama3.1:8b                ...            4.7 GB
```

---

## Pi Development Environment

[Pi](https://pi.dev) is a TypeScript terminal coding harness used as the primary code-level assistant alongside Hermes.

### Tool Roles

| Concern | Hermes | Pi |
|---------|--------|-----|
| **Role** | System architect, pipeline orchestrator, RAG design | Code-level assistant: write, edit, review, debug files |
| **Strengths** | Cross-session memory, skills, cron, multi-agent delegation | Minimal, fast, excellent code-editing ergonomics |
| **Interaction** | Long-running sessions with context persistence | Quick file-level edits via terminal |

### Installation

```bash
# Option A: npm (recommended for Windows)
npm install -g @earendil-works/pi-coding-agent

# Option B: curl (Linux/macOS)
curl -fsSL https://pi.dev/install.sh | sh
```

### Pi Configuration for Local Models

Create `~/.pi/agent/models.json` with the following content:

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

### Pi Key Commands

| Command | Action |
|---------|--------|
| `/model` | Switch between local/cloud models |
| `/login` | Authenticate with Anthropic, OpenAI, GitHub Copilot |
| `/skills` | Load Pi skills for specialized workflows |
| `/theme` | Switch editor theme |
| `/help` | Show all commands |
| `Esc` | Exit insert mode, enter command mode |

---

## Development Workflow

### Daily Session

```
$ cd med-lab-analyzer/
$ hermes -c "med-lab"          ← Hermes: resume architecture session

  Hermes assigns tasks:
    "Phase 2 task: implement labtests_online.py spider"
    
  $ pi                          ← Pi: code-level implementation
  > /model → Qwen 2.5 Coder 7B (Local)
  > "Implement the LabTestsOnlineSpider class..."
```

### When to Use Pi vs Hermes

| Scenario | Tool | Model |
|----------|------|-------|
| Write a new FastAPI endpoint | Pi | Qwen2.5-Coder 7B (L1) |
| Implement a web scraper | Pi | Qwen2.5-Coder 7B (L1) |
| Debug SQLAlchemy relationship | Pi | DeepSeek-Coder-V2 (L2) |
| Design RAG retrieval strategy | **Hermes** | Cloud (any) |
| Write unit tests | Pi | Qwen2.5-Coder 32B (L2) |
| Review PR for clinical safety | Pi | Claude Sonnet 4 (L4) |
| Plan Phase implementation | **Hermes** | Cloud (any) |
| Merge PDF parser with chunker | Pi | Qwen2.5-Coder 7B (L1) |

### Git Workflow

```bash
# Feature branch
git checkout -b feature/phase2-structured-ingestion

# Make changes... then:
git add .
git commit -m "Phase 2: add Mayo Clinic spider and normalizer"

# Push to GitHub
git push -u origin feature/phase2-structured-ingestion

# Sync between computers
git pull origin main
```

---

## Contribution Guidelines

### Branching Strategy

- `main` — stable, deployable code
- `feature/<phase>-<description>` — feature branches
- `fix/<description>` — bug fixes
- `docs/<description>` — documentation updates

### Commit Messages

Use conventional commits:

```
feat: add Mayo Clinic spider
fix: correct LOINC mapping for fasting glucose
docs: update API endpoint documentation
test: add unit tests for normalizer
refactor: extract conflict resolution logic
chore: update dependencies
```

### Pull Request Process

1. Create feature branch from `main`
2. Implement changes with tests
3. Ensure all tests pass: `pytest`
4. Run linting: `ruff check src/`
5. Create PR with description of changes
6. PR review:
   - First pass: L2 model (DeepSeek-Coder-V2)
   - Clinical safety paths: L5 model (Claude Opus 4)
7. Squash-merge to `main`

---

## Code Quality

### Linting & Formatting

```bash
ruff check src/          # Lint
ruff format src/         # Format
mypy src/                # Type checking
```

### Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
```

Configured in `.pre-commit-config.yaml`:
- `ruff` — linting + formatting
- `mypy` — type checking
- `trailing-whitespace` — cleanup
- `check-yaml` — YAML validity

### Testing

```bash
pytest                           # All tests
pytest -v                        # Verbose
pytest tests/test_engine/        # Specific module
pytest -k "analyze"              # Name filter
pytest --cov=src/                # Coverage report
```

### Environment Variables

Copy `.env.example` to `.env` and configure:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://user:pass@localhost:5432/med_lab` | PostgreSQL connection string |
| `QDRANT_URL` | `http://localhost:6333` | Qdrant server URL |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama server URL |
| `API_AUTH_ENABLED` | `false` | Enable API key authentication |
| `API_KEYS` | — | Comma-separated valid API keys |
| `LOG_LEVEL` | `INFO` | Logging level |
