# Medical Laboratory Parameter Analyzer

Automated system for ingesting, structuring, and analyzing medical laboratory data.

## Architecture

See [.hermes/plans/2026-05-19_131500-med-lab-analyzer-architecture.md](.hermes/plans/2026-05-19_131500-med-lab-analyzer-architecture.md)

## Quick Start

```bash
# Prerequisites
docker compose up -d    # PostgreSQL, Qdrant, Redis

# Setup
pip install -e ".[dev]"
alembic upgrade head

# Run API
uvicorn src.api.main:app --reload --port 8000
```
