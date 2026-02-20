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

alert-email:
	docker exec -it airflow_webserver airflow variables set ALERT_EMAIL $(EMAIL)

send-email:
	docker exec -it airflow_webserver airflow tasks test brewery_datalake_pipeline send_success_email $(TODAY)

build:
	@> .env
	@SECRET_KEY=$$(python -c "import secrets; print(secrets.token_hex(32))" 2>/dev/null || py -3.8 -c "import secrets; print(secrets.token_hex(32))" 2>/dev/null); FERNET_KEY=$$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || py -3.8 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null); echo "Generated secret key: $$SECRET_KEY"; echo "Generated fernet key: $$FERNET_KEY"; if [ -f .env ]; then grep -q "^SECRET_KEY=" .env && sed -i "s/^SECRET_KEY=./SECRET_KEY=$$SECRET_KEY/" .env || echo "SECRET_KEY=$$SECRET_KEY" >> .env; grep -q "^FERNET_KEY=" .env && sed -i "s/^FERNET_KEY=./FERNET_KEY=$$FERNET_KEY/" .env || echo "FERNET_KEY=$$FERNET_KEY" >> .env; else echo "SECRET_KEY=$$SECRET_KEY" > .env; echo "FERNET_KEY=$$FERNET_KEY" >> .env; fi; echo "Saved to .env"
	@touch .env; printf "\n" >> .env; cat example_env.txt >> .env
	@yes | docker compose down --volumes --remove-orphans || true
	@docker compose up -d postgres
	@docker compose run --rm airflow-init
	@docker compose up -d --build airflow-webserver airflow-scheduler
	@docker compose up -d --build
	@docker ps
	@docker exec -i airflow_webserver airflow version
	@docker exec -i airflow_postgres psql -U airflow -d airflow < src/monitoring/sql/audit_init.sql && echo "Audit schema initialized."
	@docker exec -i airflow_webserver airflow variables set SMTP_USER "brewwerynyc@gmail.com"
	@docker exec -i airflow_webserver airflow variables set SMTP_PASSWORD "epkiewejkdnayuub"
	@docker exec -i airflow_webserver airflow connections add smtp_default --conn-type smtp --conn-host smtp.gmail.com --conn-login "brewwerynyc@gmail.com" --conn-password "epkiewejkdnayuub" --conn-port 587 || true
	@docker exec -i airflow_webserver airflow variables set QUALITY_THRESHOLDS "{\"min_silver_vs_bronze_ratio\":0.7,\"max_null_name_pct\":5,\"max_null_city_state_pct\":10,\"max_duplicate_id_pct\":1,\"max_duration_seconds\":180,\"min_duration_seconds\":2,\"max_invalid_brewery_type\":1,\"fail_on_schema_missing\":true,\"fail_on_schema_extra\":false}"
	@docker exec -i airflow_webserver airflow dags unpause brewery_datalake_pipeline