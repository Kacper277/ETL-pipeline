.PHONY: setup db-up db-down kafka-up topics produce consume run dashboard test test-integration lint down clean

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

test:             ## Run the test suite (unit tests only, no broker)
	pytest -v --cov=src

test-integration: ## Run integration tests (requires a live broker)
	pytest -m integration -v

kafka-up:         ## Start Kafka + UI and wait for healthy
	docker compose up -d --wait kafka kafka-ui

topics:           ## Create the streaming topics (idempotent)
	python scripts/create_topics.py

produce:          ## Publish a demo batch of readings
	python -m src.streaming.produce_demo

consume:          ## Run the streaming consumer (Ctrl+C to stop)
	python -m src.streaming.consume

down:             ## Stop everything, including volumes
	docker compose down -v

lint:             ## Run flake8
	flake8 src/ tests/

clean:            ## Remove generated artifacts
	python -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ('__pycache__','.pytest_cache','htmlcov')]"
	del /Q .coverage 2>nul || true
