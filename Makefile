.PHONY: check format test lint typecheck

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python)

check:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m basedpyright
	$(PYTHON) -m pytest

format:
	$(PYTHON) -m ruff check . --fix
	$(PYTHON) -m ruff format .

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

typecheck:
	$(PYTHON) -m basedpyright
