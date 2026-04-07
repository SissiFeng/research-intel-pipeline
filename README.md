# Research Intelligence Pipeline

[![CI](https://github.com/your-org/research-intel-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/research-intel-pipeline/actions)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-311/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Real-time scientific research analytics pipeline for AI, ML, and Computational Biology. Ingests papers from **arXiv** and **OpenAlex**, transforms them with **Bruin** into a layered analytical model, and serves insights through an interactive **Streamlit** dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                 │
│   arXiv API              OpenAlex API                               │
│  (papers, abstracts)    (citations, concepts)                       │
└────────┬────────────────────┬────────────────────────────────────── ┘
         │                    │
         ▼                    ▼
┌────────────────────────────────────┐
│         PRODUCERS (Python)          │
│  arxiv_producer.py                  │
│  openalex_producer.py               │
│  Polls APIs every 15 min            │
│  Writes directly to Supabase        │
└────────────────┬───────────────────┘
                 │  psycopg2 (direct SQL upsert)
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SUPABASE (PostgreSQL)                              │
│  landing.raw_arxiv_papers     landing.raw_openalex_works             │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRUIN PIPELINE (SQL + Python)                      │
│                                                                       │
│  staging/                      intermediate/                          │
│  ┌──────────────────┐          ┌────────────────────────────────┐    │
│  │stg_papers        │─────────▶│int_paper_embeddings.py (TF-IDF)│    │
│  │stg_authors       │          │int_author_network              │    │
│  │stg_citations     │          │int_topic_clusters              │    │
│  └──────────────────┘          └───────────────┬────────────────┘    │
│                                                 │                     │
│                                     marts/      ▼                     │
│                           ┌─────────────────────────────────┐        │
│                           │mart_research_trends              │        │
│                           │mart_rising_papers                │        │
│                           │mart_author_influence             │        │
│                           │mart_topic_heatmap                │        │
│                           └──────────────┬──────────────────┘        │
└──────────────────────────────────────────┼────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      STREAMLIT DASHBOARD                              │
│  ┌────────────┐  ┌─────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ Overview   │  │ Trends  │  │Citation Graph│  │Author Network │  │
│  │ • KPIs     │  │ • Lines │  │ • pyvis      │  │ • pyvis       │  │
│  │ • Topics   │  │ • Heat  │  │ • D3-like    │  │ • Leaderboard │  │
│  │ • Rising   │  │   map   │  │   Force      │  │               │  │
│  └────────────┘  └─────────┘  └──────────────┘  └───────────────┘  │
│                                        ┌────────────────────┐        │
│                                        │      Search        │        │
│                                        │ • Full-text (PG)   │        │
│                                        │ • Topic filter     │        │
│                                        └────────────────────┘        │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Differentiators

| Feature | Description |
|---|---|
| **ML Component** | TF-IDF embeddings + topic clustering via `int_paper_embeddings.py` Python Bruin asset |
| **Citation Velocity** | Novel metric: citations per day since publication, benchmarked against category median |
| **Citation Graph** | Interactive force-directed paper-paper citation network (pyvis) |
| **Author Influence** | h-index proxy + collaboration degree + composite influence score |
| **Topic Co-occurrence** | Category pair co-occurrence matrix powering the heatmap |

---

## Tech Stack

| Component | Technology |
|---|---|
| Ingestion | Python 3.11, `urllib`, `xml.etree`, `psycopg2` |
| Landing DB | [Supabase](https://supabase.com) (PostgreSQL 15) |
| Transformation | [Bruin](https://bruin-data.github.io) |
| ML | scikit-learn (TF-IDF), scipy, numpy |
| Dashboard | [Streamlit](https://streamlit.io), Plotly, pyvis |
| IaC | Terraform (cyrilgdn/postgresql provider) |
| CI/CD | GitHub Actions |
| Containers | Docker + Docker Compose |
| Linting | ruff |
| Types | mypy --strict |

---

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Supabase project (free tier works)
- Terraform ≥ 1.6 (for schema provisioning)

### 1. Clone & Configure

```bash
git clone https://github.com/your-org/research-intel-pipeline.git
cd research-intel-pipeline
cp .env.example .env
# Edit .env with your Supabase credentials
```

### 2. Provision Supabase Schemas

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars with Supabase DB host + password

make tf-init
make tf-apply
```

### 3. Start the Pipeline (Docker)

```bash
make docker-build
make docker-up

# Tail logs
make docker-logs
```

### 4. Run Bruin Transformations

```bash
# Install bruin CLI: https://bruin-data.github.io/bruin/getting-started/installation
make bruin-run
```

### 5. Launch Dashboard

```bash
# Via Docker (already running after make docker-up)
open http://localhost:8501

# Or locally
make dashboard
```

---

## Project Structure

```
research-intel-pipeline/
├── CLAUDE.md                    # Project brief
├── README.md
├── Makefile                     # All common tasks
├── docker-compose.yml           # 3-service orchestration (producers + dashboard)
├── .env.example                 # Environment variable template
│
├── producers/                   # Ingestion scripts
│   ├── arxiv_producer.py        # arXiv API → Supabase (psycopg2)
│   ├── openalex_producer.py     # OpenAlex API → Supabase (psycopg2)
│   ├── Dockerfile
│   └── requirements.txt
│
├── bruin/                       # Bruin transformation pipeline
│   ├── .bruin.yml               # Project config
│   ├── landing/
│   │   ├── raw_arxiv_papers.sql
│   │   └── raw_openalex_works.sql
│   ├── staging/
│   │   ├── stg_papers.sql       # Clean + dedupe + enrich
│   │   ├── stg_authors.sql      # Normalized author entities
│   │   └── stg_citations.sql    # Citation edge table
│   ├── intermediate/
│   │   ├── int_paper_embeddings.py  # TF-IDF ML asset (Python)
│   │   ├── int_author_network.sql   # Co-authorship edges
│   │   └── int_topic_clusters.sql   # Topic assignments
│   └── marts/
│       ├── mart_research_trends.sql  # Weekly topic volume
│       ├── mart_rising_papers.sql    # Citation velocity outliers
│       ├── mart_author_influence.sql # h-index + influence score
│       └── mart_topic_heatmap.sql    # Topic co-occurrence matrix
│
├── dashboard/                   # Streamlit app
│   ├── app.py                   # Entry point + DB connection
│   ├── Dockerfile
│   ├── requirements.txt
│   └── pages/
│       ├── 1_Overview.py        # KPIs + rising papers + category pie
│       ├── 2_Trends.py          # Line chart + velocity heatmap + co-occurrence
│       ├── 3_Citation_Network.py # Interactive citation graph
│       ├── 4_Author_Network.py   # Co-authorship graph + leaderboard
│       └── 5_Search.py          # Full-text search with filters
│
├── terraform/
│   ├── main.tf                  # Supabase schema provisioning
│   └── terraform.tfvars.example
│
└── .github/
    └── workflows/
        └── ci.yml               # Lint + typecheck + docker build + tf validate
```

---

## Bruin Pipeline DAG

```
landing.raw_arxiv_papers ──┐
                            ├──▶ staging.stg_papers ──────────────────────┐
landing.raw_openalex_works ─┘         │                                   │
                                      ├──▶ staging.stg_authors            │
                                      │                                   │
                                      └──▶ staging.stg_citations          │
                                                                           │
                            ┌─────────────────────────────────────────────┘
                            │
                            ├──▶ intermediate.int_paper_embeddings (Python/TF-IDF)
                            │         │
                            ├──▶ intermediate.int_author_network
                            │         │
                            └──▶ intermediate.int_topic_clusters
                                      │
                          ┌───────────┴───────────────────────────┐
                          │           │            │               │
                          ▼           ▼            ▼               ▼
                    mart_research  mart_rising  mart_author  mart_topic
                    _trends       _papers      _influence   _heatmap
```

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `SUPABASE_DB_URL` | ✅ | Full PostgreSQL connection string |
| `SUPABASE_URL` | ✅ | Supabase project URL |
| `SUPABASE_KEY` | ✅ | Supabase anon/service key |
| `ARXIV_CATEGORIES` | | Comma-separated (default: `cs.AI,cs.LG,q-bio.NC`) |
| `ARXIV_MAX_RESULTS` | | Per poll (default: `100`) |
| `OPENALEX_MAX_RESULTS` | | Per poll (default: `200`) |
| `OPENALEX_MAILTO` | | Polite pool email for OpenAlex API |
| `POLL_INTERVAL_SECONDS` | | Producer sleep between polls (default: `3600`) |
| `SEMANTIC_SCHOLAR_API_KEY` | | Optional: higher rate limits |

---

## Development

```bash
# Install dev tools
make setup

# Lint
make lint

# Format
make format

# Type check
make typecheck

# Test
make test

# Full clean
make clean
```

---

## Dashboard Screenshots

> _(Add screenshots after first run)_

| Page | Description |
|---|---|
| Overview | KPI cards, top topics bar chart, rising papers feed |
| Trends | Weekly volume line chart, citation velocity heatmap, topic co-occurrence matrix |
| Citation Network | Force-directed graph of paper→paper citations, colored by category |
| Author Network | Co-authorship graph + influence leaderboard with h-index proxy |
| Search | PostgreSQL full-text search (`to_tsvector` + `plainto_tsquery`) with category/year filters |

---

## License

MIT
