SHELL := /bin/bash

VENV := .venv
# In CI (GitHub Actions sets CI=true), use system Python that already has
# packages installed via pip install -r requirements.txt.  Locally, use the
# project venv so system packages stay clean.
ifeq ($(CI),true)
PYTHON := python
PIP    := pip
else
PYTHON := $(VENV)/bin/python
PIP    := $(VENV)/bin/pip
endif

RUN_DATE ?= $(shell date +%F)
MODE ?= live
MARKET_MODE ?= auto
SCREENER_CSV ?= data/raw/fundamentals/screener/screener_export_$(RUN_DATE).csv
SCREENER_TEMPLATE ?= data/raw/fundamentals/screener/screener_export_TEMPLATE.csv
NSE_UNIVERSE_CSV ?= data/raw/universe/nse_symbols_$(RUN_DATE).csv
FUNDAMENTALS_CSV ?=
AUTO_FUNDAMENTALS_CSV ?= data/raw/fundamentals/screener/full_fundamentals_$(RUN_DATE).csv
UNIVERSE_REPORT ?= data/processed/universe/universe_prep_$(RUN_DATE).json
UNIVERSE_FETCH_REPORT ?= data/processed/universe/universe_fetch_$(RUN_DATE).json
MISSING_CLASSIFICATION_CSV ?= data/processed/universe/missing_classification_$(RUN_DATE).csv
CLASSIFICATION_CSV ?= data/raw/classification/nse_symbol_classification_master.csv
REQUIRE_CLASSIFICATION ?= false
BHAVCOPY_ZIP ?=
SCRAPER_DELAY ?= 1.5
SCRAPER_LIMIT ?= 0
SCRAPER_WORKERS ?= 1

.DEFAULT_GOAL := help

.PHONY: help venv setup bootstrap init fetch-universe fetch-price-history fetch-screener-data prepare-csv enrich-fundamentals fetch-shareholding fetch-delivery fetch-indices momentum-scoring institutional-tracking earnings-surprise forward-pe-peg stock-explainer data-freshness telegram-alerts prepare-universe daily-run auto-run ensure-csv run run-debug run-backtest backtest dashboard check check-config test clean clean-generated

help:
	@echo "NSE Screener Make Targets"
	@echo ""
	@echo "  make init                          Full first-time setup (setup + bootstrap)"
	@echo "  make setup                         Create venv + install requirements"
	@echo "  make bootstrap                     Create folders + generate CSV template"
	@echo "  make fetch-universe RUN_DATE=...   Fetch universe + apply classification master (default allows partial classification)"
	@echo "  make fetch-price-history RUN_DATE=... [SESSIONS=260]  Backfill bhavcopy history for raw price metrics"
	@echo "  make fetch-screener-data RUN_DATE=... [SCRAPER_DELAY=1.5] [SCRAPER_LIMIT=50] [SCRAPER_WORKERS=1]  Auto-scrape fundamentals from screener.in"
	@echo "  make prepare-csv RUN_DATE=...      Create dated Screener CSV from template if missing"
	@echo "  make enrich-fundamentals RUN_DATE=... [SCRAPE=true] [SCRAPE_LIMIT=N]  Compute missing metrics"
	@echo "  make fetch-shareholding RUN_DATE=... [SHP_LIMIT=N]  Fetch shareholding patterns from BSE"
	@echo "  make fetch-delivery RUN_DATE=... [DELIVERY_SESSIONS=60]  Fetch delivery position data from NSE"
	@echo "  make fetch-indices RUN_DATE=... [INDEX_SESSIONS=260]  Fetch Nifty index EOD data"
	@echo "  make momentum-scoring RUN_DATE=...  Compute advanced momentum metrics"
	@echo "  make institutional-tracking RUN_DATE=...  Track MF/FII holding changes"
	@echo "  make earnings-surprise RUN_DATE=...  Detect earnings surprises"
	@echo "  make forward-pe-peg RUN_DATE=...    Estimate forward PE/PEG/GARP"
	@echo "  make stock-explainer RUN_DATE=...   Generate per-stock investment theses"
	@echo "  make data-freshness RUN_DATE=...    Check data staleness & quality"
	@echo "  make telegram-alerts RUN_DATE=... [DRY_RUN=true]  Send picks via Telegram"
	@echo "  make backtest RUN_DATE=...          Backtest past recommendations"
	@echo "  make dashboard                      Launch Streamlit dashboard"
	@echo "  make prepare-universe RUN_DATE=... NSE_UNIVERSE_CSV=... [FUNDAMENTALS_CSV=...]"
	@echo "  make daily-run RUN_DATE=... [FUNDAMENTALS_CSV=...]  Auto: production if fundamentals file is valid, else debug"
	@echo "  make auto-run RUN_DATE=... [FUNDAMENTALS_CSV=...]   Alias of daily-run"
	@echo "  make run RUN_DATE=YYYY-MM-DD SCREENER_CSV=... [MODE=live|backtest] [MARKET_MODE=auto|bear|neutral|bull]"
	@echo "  make run-debug RUN_DATE=... SCREENER_CSV=...  (skip quality gate; debug only)"
	@echo "  make run-backtest RUN_DATE=... SCREENER_CSV=... [MARKET_MODE=auto|bear|neutral|bull]"
	@echo "  make check                         Python syntax check"
	@echo "  make check-config                  Validate config + loader/scorer compatibility"
	@echo "  make test                          Unit tests"
	@echo "  make clean                         Remove Python caches"
	@echo "  make clean-generated               Remove generated run/log artifacts"

venv:
ifeq ($(CI),true)
	@:  # CI uses system Python; no venv needed
else
	@test -d "$(VENV)" || python3 -m venv "$(VENV)"
endif

setup: venv
	@$(PYTHON) -m pip install --upgrade pip
	@$(PIP) install -r requirements.txt

bootstrap: venv
	@$(PYTHON) scripts/bootstrap.py --sample-row

init: setup bootstrap

fetch-universe: venv
	@REQUIRE_FLAG=""; \
	if [ "$(REQUIRE_CLASSIFICATION)" = "true" ]; then \
		REQUIRE_FLAG="--require-classification"; \
	fi; \
	if [ -n "$(BHAVCOPY_ZIP)" ]; then \
		$(PYTHON) scripts/fetch_nse_universe.py \
			--date "$(RUN_DATE)" \
			--bhavcopy-zip "$(BHAVCOPY_ZIP)" \
			--output-csv "$(NSE_UNIVERSE_CSV)" \
			--report-json "$(UNIVERSE_FETCH_REPORT)" \
			--classification-csv "$(CLASSIFICATION_CSV)" \
			--missing-classification-csv "$(MISSING_CLASSIFICATION_CSV)" \
			$$REQUIRE_FLAG \
			--force; \
	else \
		$(PYTHON) scripts/fetch_nse_universe.py \
			--date "$(RUN_DATE)" \
			--output-csv "$(NSE_UNIVERSE_CSV)" \
			--report-json "$(UNIVERSE_FETCH_REPORT)" \
			--classification-csv "$(CLASSIFICATION_CSV)" \
			--missing-classification-csv "$(MISSING_CLASSIFICATION_CSV)" \
			$$REQUIRE_FLAG \
			--force; \
		fi

SESSIONS ?= 260
MAX_CALENDAR_DAYS ?= 520
SCRAPE ?= false
SCRAPE_LIMIT ?= 0
SHP_LIMIT ?= 0
DELIVERY_SESSIONS ?= 60
INDEX_SESSIONS ?= 260

fetch-price-history: venv
	@$(PYTHON) scripts/fetch_price_history.py \
		--end-date "$(RUN_DATE)" \
		--sessions "$(SESSIONS)" \
		--max-calendar-days "$(MAX_CALENDAR_DAYS)"

fetch-screener-data: venv
	@$(PYTHON) scripts/fetch_fundamentals_screener.py \
		--universe "$(NSE_UNIVERSE_CSV)" \
		--output "$(SCREENER_CSV)" \
		--date "$(RUN_DATE)" \
		--delay "$(SCRAPER_DELAY)" \
		--limit "$(SCRAPER_LIMIT)" \
		--workers "$(SCRAPER_WORKERS)"

prepare-csv: bootstrap
	@if [ -f "$(SCREENER_CSV)" ]; then \
		echo "Screener CSV already exists: $(SCREENER_CSV)"; \
	elif [ ! -f "$(SCREENER_TEMPLATE)" ]; then \
		echo "Template not found: $(SCREENER_TEMPLATE)"; \
		echo "Run: make bootstrap"; \
		exit 1; \
	else \
		cp "$(SCREENER_TEMPLATE)" "$(SCREENER_CSV)"; \
		echo "Created: $(SCREENER_CSV)"; \
		echo "Next: fill this CSV with your Screener export values, then run make run."; \
	fi

enrich-fundamentals: venv
	@if [ -f "$(SCREENER_CSV)" ]; then \
		SCRAPE_FLAG=""; \
		if [ "$(SCRAPE)" = "true" ]; then \
			SCRAPE_FLAG="--scrape --scrape-limit $(SCRAPE_LIMIT)"; \
		fi; \
		$(PYTHON) scripts/enrich_fundamentals.py \
			--input "$(SCREENER_CSV)" \
			--output "$(SCREENER_CSV)" \
			--report "data/processed/enrichment_report_$(RUN_DATE).json" \
			$$SCRAPE_FLAG; \
	else \
		echo "Screener CSV not found: $(SCREENER_CSV). Run prepare-universe first."; \
		exit 1; \
	fi

fetch-shareholding: venv
	@$(PYTHON) scripts/fetch_shareholding.py \
		--date "$(RUN_DATE)" \
		--limit "$(SHP_LIMIT)" \
		--source bse

fetch-delivery: venv
	@$(PYTHON) scripts/fetch_delivery_data.py \
		--end-date "$(RUN_DATE)" \
		--sessions "$(DELIVERY_SESSIONS)" \
		--merge-csv "$(SCREENER_CSV)"

fetch-indices: venv
	@$(PYTHON) scripts/fetch_index_data.py \
		--end-date "$(RUN_DATE)" \
		--sessions "$(INDEX_SESSIONS)"

DRY_RUN ?= false

momentum-scoring: venv
	@$(PYTHON) scripts/momentum_scoring.py \
		--date "$(RUN_DATE)" \
		--merge-csv "$(SCREENER_CSV)"

institutional-tracking: venv
	@$(PYTHON) scripts/institutional_tracking.py \
		--date "$(RUN_DATE)" \
		--merge-csv "$(SCREENER_CSV)"

earnings-surprise: venv
	@$(PYTHON) scripts/earnings_surprise.py \
		--date "$(RUN_DATE)" \
		--merge-csv "$(SCREENER_CSV)"

forward-pe-peg: venv
	@$(PYTHON) scripts/forward_pe_peg.py \
		--date "$(RUN_DATE)" \
		--merge-csv "$(SCREENER_CSV)"

stock-explainer: venv
	@$(PYTHON) scripts/stock_explainer.py \
		--date "$(RUN_DATE)"

data-freshness: venv
	@$(PYTHON) scripts/data_freshness.py \
		--date "$(RUN_DATE)" \
		--output "runs/$(RUN_DATE)/data_freshness.json"

telegram-alerts: venv
	@TGDRY=""; \
	if [ "$(DRY_RUN)" = "true" ]; then TGDRY="--dry-run"; fi; \
	$(PYTHON) scripts/telegram_alerts.py \
		--date "$(RUN_DATE)" \
		$$TGDRY

backtest: venv
	@$(PYTHON) scripts/backtest.py \
		--date "$(RUN_DATE)"

dashboard: venv
	@$(PYTHON) -m streamlit run app.py

prepare-universe: venv
	@if [ -n "$(FUNDAMENTALS_CSV)" ]; then \
		$(PYTHON) scripts/prepare_universe.py \
			--date "$(RUN_DATE)" \
			--universe-csv "$(NSE_UNIVERSE_CSV)" \
			--fundamentals-csv "$(FUNDAMENTALS_CSV)" \
			--output-csv "$(SCREENER_CSV)" \
			--report-json "$(UNIVERSE_REPORT)" \
			--force; \
	else \
		$(PYTHON) scripts/prepare_universe.py \
			--date "$(RUN_DATE)" \
			--universe-csv "$(NSE_UNIVERSE_CSV)" \
			--output-csv "$(SCREENER_CSV)" \
			--report-json "$(UNIVERSE_REPORT)" \
			--force; \
	fi

daily-run: venv
	@echo "=== Data Freshness Check ==="
	@$(MAKE) data-freshness RUN_DATE="$(RUN_DATE)" || true
	@echo ""
	@echo "=== Fetch Universe ==="
	@$(MAKE) fetch-universe RUN_DATE="$(RUN_DATE)" NSE_UNIVERSE_CSV="$(NSE_UNIVERSE_CSV)" BHAVCOPY_ZIP="$(BHAVCOPY_ZIP)" CLASSIFICATION_CSV="$(CLASSIFICATION_CSV)" MISSING_CLASSIFICATION_CSV="$(MISSING_CLASSIFICATION_CSV)" REQUIRE_CLASSIFICATION="$(REQUIRE_CLASSIFICATION)"
	@echo "=== Fetch Fundamentals (screener.in scrape) ==="
	@if [ ! -f "$(SCREENER_CSV)" ]; then \
		$(MAKE) fetch-screener-data RUN_DATE="$(RUN_DATE)" NSE_UNIVERSE_CSV="$(NSE_UNIVERSE_CSV)" SCREENER_CSV="$(SCREENER_CSV)" SCRAPER_DELAY="$(SCRAPER_DELAY)" SCRAPER_WORKERS="$(SCRAPER_WORKERS)"; \
	else \
		echo "Screener CSV already exists: $(SCREENER_CSV)"; \
	fi
	@FUND_FILE="$(FUNDAMENTALS_CSV)"; \
	if [ -z "$$FUND_FILE" ]; then FUND_FILE="$(SCREENER_CSV)"; fi; \
	if [ -f "$$FUND_FILE" ] && [ -s "$$FUND_FILE" ] && [ "$$(wc -l < "$$FUND_FILE")" -gt 1 ] && head -n 1 "$$FUND_FILE" | grep -q ','; then \
		echo "=== Production Mode ==="; \
		echo "Using fundamentals file: $$FUND_FILE"; \
		$(MAKE) prepare-universe RUN_DATE="$(RUN_DATE)" NSE_UNIVERSE_CSV="$(NSE_UNIVERSE_CSV)" FUNDAMENTALS_CSV="$$FUND_FILE" SCREENER_CSV="$(SCREENER_CSV)"; \
		$(MAKE) enrich-fundamentals RUN_DATE="$(RUN_DATE)" SCREENER_CSV="$(SCREENER_CSV)" SCRAPE="$(SCRAPE)" SCRAPE_LIMIT="$(SCRAPE_LIMIT)"; \
		$(MAKE) run RUN_DATE="$(RUN_DATE)" MODE="$(MODE)" MARKET_MODE="$(MARKET_MODE)" SCREENER_CSV="$(SCREENER_CSV)"; \
	else \
		echo "=== Debug Mode (no fundamentals file) ==="; \
		$(MAKE) prepare-universe RUN_DATE="$(RUN_DATE)" NSE_UNIVERSE_CSV="$(NSE_UNIVERSE_CSV)" SCREENER_CSV="$(SCREENER_CSV)"; \
		$(MAKE) enrich-fundamentals RUN_DATE="$(RUN_DATE)" SCREENER_CSV="$(SCREENER_CSV)" SCRAPE="$(SCRAPE)" SCRAPE_LIMIT="$(SCRAPE_LIMIT)"; \
		$(MAKE) run-debug RUN_DATE="$(RUN_DATE)" MODE="$(MODE)" MARKET_MODE="$(MARKET_MODE)" SCREENER_CSV="$(SCREENER_CSV)"; \
	fi
	@echo ""
	@echo "=== Post-Engine Enrichment ==="
	-@$(MAKE) stock-explainer RUN_DATE="$(RUN_DATE)"

auto-run: daily-run

ensure-csv:
	@if [ ! -f "$(SCREENER_CSV)" ]; then \
		echo "Missing Screener CSV: $(SCREENER_CSV)"; \
		echo "Run: make prepare-universe RUN_DATE=$(RUN_DATE) NSE_UNIVERSE_CSV=... [FUNDAMENTALS_CSV=...]"; \
		exit 1; \
	fi

run: venv ensure-csv
	@$(PYTHON) scripts/run_engine.py \
		--date "$(RUN_DATE)" \
		--mode "$(MODE)" \
		--market-mode "$(MARKET_MODE)" \
		--screener-csv "$(SCREENER_CSV)"

run-debug: venv ensure-csv
	@$(PYTHON) scripts/run_engine.py \
		--date "$(RUN_DATE)" \
		--mode "$(MODE)" \
		--market-mode "$(MARKET_MODE)" \
		--screener-csv "$(SCREENER_CSV)" \
		--skip-quality-gate

run-backtest: MODE=backtest
run-backtest: run

check: venv
	@$(PYTHON) -m py_compile engine/*.py scripts/*.py tests/*.py run_test.py

check-config: venv
	@$(PYTHON) scripts/check_config.py

test: setup
	@$(PYTHON) -m unittest discover -s tests -p "test_*.py"

clean:
	@find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete

clean-generated:
	@find runs -mindepth 1 -maxdepth 1 -type d ! -name ".gitkeep" -exec rm -rf {} +
	@rm -f logs/recommendation_history.csv logs/errors.log
	@echo "Generated artifacts cleaned."
