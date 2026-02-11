PYTHON ?= $(shell \
	if [ -x .venv/bin/python ]; then echo .venv/bin/python; \
	elif command -v python3 >/dev/null 2>&1; then echo python3; \
	else echo python; \
	fi)

.PHONY: help lint lint-fix arch-check test compile check check-artifacts clean

help:
	@echo "Available targets:"
	@echo "  make lint            - run Ruff linter (critical rules)"
	@echo "  make lint-fix        - run Ruff with auto-fix enabled"
	@echo "  make arch-check      - verify source import boundaries"
	@echo "  make test            - run unit tests"
	@echo "  make compile         - compile-check Python sources"
	@echo "  make check-artifacts - run artifact checks command"
	@echo "  make clean           - remove local Python cache artifacts"
	@echo "  make check           - run lint + compile + tests + artifact checks"

lint:
	$(PYTHON) -m ruff check src tests
	$(PYTHON) scripts/check_architecture.py

lint-fix:
	$(PYTHON) -m ruff check src tests --fix

arch-check:
	$(PYTHON) scripts/check_architecture.py

test:
	$(PYTHON) -m unittest discover -s tests -v

compile:
	$(PYTHON) -m compileall -q src tests

check-artifacts:
	$(PYTHON) src/main.py check-artifacts

check: lint compile test check-artifacts

clean:
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type f -name '*.py[co]' -delete
