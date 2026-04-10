# Makefile for development tasks

.PHONY: help install dev-install test lint format type-check clean

help:
	@echo "Available commands:"
	@echo "  make install       - Install production dependencies"
	@echo "  make dev-install   - Install all dependencies including dev tools"
	@echo "  make test          - Run tests with coverage"
	@echo "  make lint          - Run linters (ruff)"
	@echo "  make format        - Format code with black and isort"
	@echo "  make type-check    - Run mypy type checking"
	@echo "  make clean         - Clean up build artifacts"

install:
	pip install -r requirements.txt

dev-install:
	pip install -r requirements-dev.txt

test:
	pytest -v --cov=src/confluence_client

lint:
	ruff check src tests

format:
	black src tests
	isort src tests

type-check:
	mypy src

clean:
	rm -rf build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache htmlcov
