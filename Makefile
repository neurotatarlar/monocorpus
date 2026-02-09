PYTHON ?= .venv/bin/python

.PHONY: help lint lint-fix test compile check check-artifacts

help:
	@echo "Available targets:"
	@echo "  make lint            - run Ruff linter (critical rules)"
	@echo "  make lint-fix        - run Ruff with auto-fix enabled"
	@echo "  make test            - run unit tests"
	@echo "  make compile         - compile-check Python sources"
	@echo "  make check-artifacts - run artifact checks command"
	@echo "  make check           - run lint + compile + tests + artifact checks"

lint:
	$(PYTHON) -m ruff check src tests

lint-fix:
	$(PYTHON) -m ruff check src tests --fix

test:
	$(PYTHON) -m unittest discover -s tests -v

compile:
	$(PYTHON) -m compileall -q src tests

check-artifacts:
	$(PYTHON) src/main.py check-artifacts

check: lint compile test check-artifacts
