.PHONY: setup db-up db-down run dashboard test lint clean

setup:            ## Create .env from template
	copy .env.example .env 2>nul || true
	@echo "Edit .env and set POSTGRES_PASSWORD / DATABASE_URL"

db-up:            ## Start PostgreSQL
	docker compose up -d postgres

db-down:          ## Stop all services
	docker compose down

run:              ## Run the full ETL pipeline locally
	python -m src.pipeline

dashboard:        ## Launch the Streamlit dashboard
	streamlit run src/dashboard/app.py

test:             ## Run the test suite
	pytest -v --cov=src

lint:             ## Run flake8
	flake8 src/ tests/

clean:            ## Remove generated artifacts
	python -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ('__pycache__','.pytest_cache','htmlcov')]"
	del /Q .coverage 2>nul || true
