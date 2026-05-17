# Makefile — task runner for the Miller-Pera FF pipeline.
#
# Encodes the multi-step pipeline (otherwise documented only as prose in
# README.md) so the stage order and per-method flags can't drift. Targets
# are phony: each runs its scripts every time. Make is used here as a
# command runner, not an incremental build system.
#
# Assumes the conda env `p312` is active (`conda activate p312`). To run
# without activating it, override the interpreter:
#     make PYTHON="conda run -n p312 python" test
#
#   make            # list targets
#   make check      # ruff + mypy + pytest
#   make v2026b     # full v2026b pipeline, both methods

PYTHON ?= python
METHOD ?= assumed

.DEFAULT_GOAL := help

# The pipeline stages have a strict order (carbon-monitor -> ingest -> build
# -> compare). Forbid concurrent target execution so `make -j v2026b` can't
# race the stages and produce a silently-wrong output.
.NOTPARALLEL:

.PHONY: help
help:  ## List available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ── environment ──────────────────────────────────────────────────────────
.PHONY: env lock
env:  ## Create the conda env from environment.yml (first-time setup)
	conda env create -f environment.yml
lock:  ## Regenerate conda-lock.yml from environment.yml (needs conda-lock)
	conda-lock lock -f environment.yml -p osx-arm64 -p linux-64

# ── quality gates (mirror .github/workflows/ci.yml) ──────────────────────
.PHONY: lint typecheck test check
lint:  ## Lint with ruff
	$(PYTHON) -m ruff check
typecheck:  ## Type-check with mypy
	$(PYTHON) -m mypy .
test:  ## Run the pytest suite
	$(PYTHON) -m pytest
check: lint typecheck test  ## Run all three gates: ruff + mypy + pytest

# ── pipeline stages (see README.md for the full description) ─────────────
.PHONY: carbon-monitor edgar ingest
carbon-monitor:  ## Download / refresh CarbonMonitor NRT data
	$(PYTHON) download_carbon_monitor.py
edgar:  ## Extend EDGAR spatial fields if they lack the final year (Step 0)
	$(PYTHON) extrapolate_edgar.py
ingest:  ## Ingest & harmonise all raw inputs (Step 1)
	$(PYTHON) ingest.py

# ── per-method build: ff_country -> post_process -> split_ct (Steps 2-3) ──
.PHONY: assumed cm_yearly
assumed:  ## Build the 'assumed'-method outputs
	$(PYTHON) ff_country.py --method assumed
	$(PYTHON) post_process.py --method assumed
cm_yearly:  ## Build the 'cm_yearly'-method outputs
	$(PYTHON) ff_country.py --method cm_yearly
	$(PYTHON) post_process.py --method cm_yearly

# ── comparison & verification ────────────────────────────────────────────
.PHONY: compare verify
compare:  ## Generate the v2026b method-comparison report
	$(PYTHON) compare_methods.py
verify:  ## Execute the verify_nrt.ipynb quality-check notebook
	$(PYTHON) -m jupyter nbconvert --to notebook --execute --inplace verify_nrt.ipynb

# ── full pipeline ────────────────────────────────────────────────────────
.PHONY: v2026b
v2026b: carbon-monitor ingest assumed cm_yearly compare  ## Full v2026b pipeline, both methods
	@echo "v2026b pipeline complete. Run 'make verify' for the QA notebook."

# ── delivery bundle ──────────────────────────────────────────────────────
.PHONY: delivery
delivery:  ## Build the delivery/ bundle with outputs (METHOD=assumed|cm_yearly)
	$(PYTHON) package_delivery.py --with-outputs --method $(METHOD)

# ── housekeeping ─────────────────────────────────────────────────────────
.PHONY: clean
clean:  ## Remove tooling caches (pytest / mypy / ruff / __pycache__)
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
