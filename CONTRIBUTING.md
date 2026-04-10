# Contributing to async-confluence-client

Thank you for your interest in contributing! Please follow these guidelines:

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/async-confluence-client.git`
3. Create a virtual environment: `python -m venv venv`
4. Activate it: `source venv/bin/activate` (or `venv\Scripts\activate` on Windows)
5. Run setup: `bash setup.sh` or follow the manual steps in README.md

## Development Workflow

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make your changes
3. Format code: `make format`
4. Run linting: `make lint`
5. Run type checking: `make type-check`
6. Run tests: `make test`
7. Commit and push to your fork
8. Open a pull request

## Code Style

- Follow PEP 8
- Use type hints
- Format with `black` (line length: 100)
- Sort imports with `isort`
- Run `make format` before committing

## Testing

- Write tests for all new features
- Ensure tests pass: `make test`
- Aim for >80% coverage

## Pull Request Process

1. Update README.md with any new features
2. Add tests for new functionality
3. Ensure all CI checks pass
4. Request review from maintainers
