.PHONY: setup db-up db-down kafka-up topics produce consume run dashboard test test-integration lint down clean airflow-up airflow-down airflow-logs airflow-trigger airflow-reinit

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

# ---------------------------------------------------------------
# Airflow targets (require a POSIX shell - the targets above use
# Windows cmd syntax; these do not run in cmd.exe)
# ---------------------------------------------------------------
airflow-up:       ## Start the Airflow stack and wait for health
	docker compose --profile airflow up -d --wait postgres-airflow airflow-init airflow-webserver airflow-scheduler

airflow-down:     ## Stop the Airflow stack (metadata volume is KEPT)
	docker compose --profile airflow down --remove-orphans

airflow-logs:     ## Follow scheduler + webserver logs
	docker compose --profile airflow logs -f airflow-scheduler airflow-webserver

airflow-trigger:  ## Trigger the batch ETL DAG once
	docker compose --profile airflow exec airflow-scheduler airflow dags trigger weather_batch_etl_pipeline

airflow-reinit:   ## Drop the Airflow metadata volume and re-init (DELETES run history)
	docker compose --profile airflow down --remove-orphans
	docker volume rm $$(docker volume ls -q --filter name=airflow_db)
	docker compose --profile airflow up -d --wait postgres-airflow airflow-init airflow-webserver airflow-scheduler

lint:             ## Run flake8
	flake8 src/ tests/

clean:            ## Remove generated artifacts
	python -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ('__pycache__','.pytest_cache','htmlcov')]"
	del /Q .coverage 2>nul || true
