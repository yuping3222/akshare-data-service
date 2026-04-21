.PHONY: help install test test-unit test-integration test-system test-all lint lint-fix format type-check clean build docker-run docker-build docker-stop docker-logs docker-clean config-lint prober batch-download quality-check dev version

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies in development mode
	pip install -e ".[all,dev]"

install-prod: ## Install production dependencies only
	pip install .

test: ## Run the test suite
	python -m pytest tests/ -v

test-cov: ## Run tests with coverage
	python -m pytest tests/ -v --cov=akshare_data --cov-report=term-missing

test-unit: ## Run unit tests only (excluding integration/ and system/)
	python -m pytest tests/ -v -m unit --ignore=tests/integration --ignore=tests/system

test-integration: ## Run integration tests
	python -m pytest tests/integration/ -v -m integration

test-system: ## Run system tests
	python -m pytest tests/system/ -v -m system

test-all: ## Run all tests with coverage (fails if <80%)
	python -m pytest tests/ -v --cov=akshare_data --cov-fail-under=80

lint: ## Run linter (ruff)
	ruff check src/ tests/

lint-fix: ## Run linter with auto-fix
	ruff check --fix src/ tests/

format: ## Format code with ruff
	ruff format src/ tests/

type-check: ## Run type checking (mypy)
	mypy src/akshare_data/

clean: ## Remove cache, logs, and build artifacts
	rm -rf cache/ logs/ __pycache__/ .pytest_cache/ .ruff_cache/
	rm -rf build/ dist/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete

build: ## Build the package
	pip install build
	python -m build

docker-build: ## Build Docker image
	docker build -t akshare-data-service:latest .

docker-run: ## Run Docker container (requires .env file)
	docker-compose up -d

docker-stop: ## Stop Docker container
	docker-compose down

docker-logs: ## Show container logs
	docker-compose logs -f

docker-clean: ## Stop container and remove volumes
	docker-compose down -v

# Config management
config-lint: ## Validate YAML config files
	python -c "import yaml, glob; [yaml.safe_load(open(f)) for f in glob.glob('config/**/*.yaml', recursive=True)]; print('All config files valid')"

prober: ## Run API prober to check interface health
	python -m akshare_data.offline prober

batch-download: ## Run batch downloader
	python -m akshare_data.offline batch-download

quality-check: ## Run data quality checks
	python -m akshare_data.offline quality

# Development
dev: ## Run interactive Python with service loaded
	python -c "import akshare_data; svc = akshare_data.get_service(); import code; code.interact(local=locals())"

version: ## Print installed package version
	python -c "import akshare_data; print(akshare_data.__version__)"
