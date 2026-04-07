.PHONY: help setup lint format typecheck test \
        producers bruin-run dashboard \
        docker-build docker-up docker-down \
        tf-init tf-plan tf-apply all clean

# ── Colours ───────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[0;32m
CYAN  := \033[0;36m

help: ## Show this help message
	@echo ""
	@echo "$(BOLD)Research Intelligence Pipeline$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ── Environment setup ─────────────────────────────────────────────────────────
setup: ## Create .env from .env.example and install all deps
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env — fill in your credentials"; fi
	pip install -r producers/requirements.txt
	pip install -r dashboard/requirements.txt
	pip install ruff mypy types-psycopg2 pytest pytest-cov
	@echo "$(GREEN)Setup complete$(RESET)"

# ── Code quality ──────────────────────────────────────────────────────────────
lint: ## Run ruff linter on all Python source
	ruff check producers/ dashboard/ bruin/

format: ## Auto-format with ruff
	ruff format producers/ dashboard/ bruin/

format-check: ## Check formatting without modifying files
	ruff format --check producers/ dashboard/ bruin/

typecheck: ## Run mypy type checker
	mypy producers/ --ignore-missing-imports --strict

test: ## Run unit tests with coverage
	pytest tests/ -v --cov=producers --cov-report=term-missing

# ── Running services ──────────────────────────────────────────────────────────
producers: ## Start both producers locally (requires .env)
	@set -a; source .env; set +a; \
	  python producers/arxiv_producer.py & \
	  python producers/openalex_producer.py & \
	  wait

dashboard: ## Launch Streamlit dashboard (requires .env)
	@set -a; source .env; set +a; \
	  streamlit run dashboard/app.py --server.port=8501

# ── Bruin pipeline ────────────────────────────────────────────────────────────
bruin-run: ## Run full Bruin pipeline (landing → staging → intermediate → marts)
	cd bruin && bruin run --full-refresh

bruin-run-staging: ## Run only staging layer
	cd bruin && bruin run --asset staging.*

bruin-run-marts: ## Run only mart layer
	cd bruin && bruin run --asset marts.*

bruin-validate: ## Validate all Bruin assets
	cd bruin && bruin validate

# ── Docker ────────────────────────────────────────────────────────────────────
docker-build: ## Build all Docker images
	docker compose build

docker-up: ## Start all services in Docker (detached)
	docker compose up -d

docker-down: ## Stop all Docker services
	docker compose down

docker-logs: ## Tail logs from all services
	docker compose logs -f

docker-ps: ## Show running containers
	docker compose ps

# ── Terraform ─────────────────────────────────────────────────────────────────
tf-init: ## Initialize Terraform
	cd terraform && terraform init

tf-plan: ## Show Terraform execution plan
	cd terraform && terraform plan

tf-apply: ## Apply Terraform to provision Supabase schemas
	cd terraform && terraform apply

tf-destroy: ## Destroy Terraform-managed resources (CAUTION)
	cd terraform && terraform destroy

# ── Full pipeline ─────────────────────────────────────────────────────────────
all: setup docker-build docker-up bruin-run ## Full setup: build images, start services, run pipeline
	@echo "$(GREEN)$(BOLD)Pipeline is live!$(RESET)"
	@echo "Dashboard: http://localhost:8501"

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean: ## Remove Python cache files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)Cleaned$(RESET)"
