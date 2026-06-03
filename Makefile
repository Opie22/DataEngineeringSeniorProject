.PHONY: help setup test run lint clean

help:
	@echo "Adventure Works Data Platform"
	@echo ""
	@echo "Usage:"
	@echo "  make setup       Install all Python dependencies"
	@echo "  make run         Start the full pipeline (Docker Compose)"
	@echo "  make test        Run dbt tests and Python unit tests"
	@echo "  make lint        Run code linting (ruff)"
	@echo "  make clean       Stop and remove Docker containers"

# Install dependencies for all sub-projects
setup:
	cd processor && uv sync
	cd prefect && uv sync
	cd dbt && uv sync
	cd mcp && uv sync

# Start the full pipeline
run:
	docker compose up --build -d

# Run all tests: dbt tests + Python tests
test:
	@echo "--- Running dbt tests ---"
	cd dbt && set -a && source ../.env && set +a && uv run dbt test
	@echo "--- dbt tests complete ---"

# Lint all Python source files
lint:
	cd processor && uv run ruff check .
	cd prefect && uv run ruff check .
	cd mcp && uv run ruff check .

# Stop all Docker services
clean:
	docker compose down -v
