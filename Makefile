.PHONY: help install install-dev test lint format clean docker-build docker-run pipeline report

help:
	@echo "Targets:"
	@echo "  install       Install dependencies"
	@echo "  install-dev   Install dev dependencies"
	@echo "  test          Run tests"
	@echo "  lint          Run linters"
	@echo "  format        Format code"
	@echo "  clean         Remove caches and build artifacts"
	@echo "  docker-build  Build Docker image"
	@echo "  docker-run    Run pipeline in Docker"
	@echo "  pipeline      Run the pipeline locally"
	@echo "  report        Generate a report"

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt
	pip install -e ".[dev]"

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/
	mypy src/

format:
	black src/ tests/
	ruff check --fix src/ tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	rm -rf build/ dist/ *.egg-info/

docker-build:
	docker build -t livestock-outbreak-detection:latest .

docker-run:
	docker compose up pipeline

pipeline:
	python run_pipeline.py

report:
	python report_tool.py sample --count 200