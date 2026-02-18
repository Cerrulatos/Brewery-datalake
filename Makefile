TODAY ?= $(shell date +%Y-%m-%d)
EMAIL ?= brewwerynyc@gmail.com

up:
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

test:
	docker exec -it airflow_scheduler bash -lc "pytest -q /opt/airflow/tests"

lint:
	ruff check src/

run-local:
	python src/ingestion/extract_breweries.py

logs:
	docker compose logs -f

restart:
	docker compose down
	docker compose up -d --build
	docker compose up airflow-init

dag:
	docker exec -it airflow_webserver airflow dags trigger brewery_datalake_pipeline

ls-dag:
	docker exec -it airflow_webserver ls /opt/airflow/dags

ls-task:
	docker exec -it airflow_webserver airflow tasks list brewery_datalake_pipeline

pipeline:
	docker exec -it airflow_webserver airflow dags test brewery_datalake_pipeline $(TODAY)

airflow:
	docker exec -it airflow_webserver bash

postgres:
	docker exec -it airflow_postgres psql -U airflow -d airflow

fernet:
	python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

alert-email:
	docker exec -it airflow_webserver airflow variables set ALERT_EMAIL $(EMAIL)

send-email:
	docker exec -it airflow_webserver airflow tasks test brewery_datalake_pipeline send_success_email $(TODAY)
