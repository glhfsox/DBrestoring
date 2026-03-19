.PHONY: bootstrap check format test lint typecheck

BOOTSTRAP_PYTHON ?= $(shell command -v python3 || command -v python)
VENV_PYTHON := .venv/bin/python
DEV_STAMP := .venv/.dev-installed

ifeq ($(strip $(BOOTSTRAP_PYTHON)),)
$(error Could not find python3 or python in PATH)
endif

$(VENV_PYTHON):
	$(BOOTSTRAP_PYTHON) -m venv .venv

$(DEV_STAMP): pyproject.toml | $(VENV_PYTHON)
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -e ".[dev]"
	@touch $(DEV_STAMP)

bootstrap: $(DEV_STAMP)

check: $(DEV_STAMP)
	$(VENV_PYTHON) -m ruff check .
	$(VENV_PYTHON) -m ruff format --check .
	$(VENV_PYTHON) -m basedpyright
	$(VENV_PYTHON) -m pytest

format: $(DEV_STAMP)
	$(VENV_PYTHON) -m ruff check . --fix
	$(VENV_PYTHON) -m ruff format .

test: $(DEV_STAMP)
	$(VENV_PYTHON) -m pytest

lint: $(DEV_STAMP)
	$(VENV_PYTHON) -m ruff check .
	$(VENV_PYTHON) -m ruff format --check .

typecheck: $(DEV_STAMP)
	$(VENV_PYTHON) -m basedpyright
