# Brewery Datalake Pipeline 🍺

Pipeline de dados completo utilizando Apache Airflow + Docker.

Arquitetura Bronze → Silver → Gold com:

- Ingestão via API OpenBrewery
- Transformações em Pandas
- Orquestração Airflow
- Auditoria Postgres
- Quality Checks
- Testes automatizados
- Makefile

---

## 🧱 Arquitetura


---

## 🚀 Como rodar

### Pré-requisitos

- Docker
- Docker Compose
- Make
---

### 1. Clone

```bash
git clone <repo>
cd brewery-datalake

### 2. Configure as variaveis editando o arquivo criando arquivo .env
cp example_env .env
cadastre o seu e-mail na variavel ALERT_EMAIL
Gere o código FERNET_KEY através dos comandos:
make fernet ou 	python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
Copie a chave e cole na variavel FERNET_KEY

### 3. Suba a Stack executando os comando
make up ou docker compose up -d --build

O Airflow ficará disponível em:
http://localhost:8080

user: airflow
pass: airflow

## 🧪 Rodar pipeline
make dag ou docker exec -it airflow_webserver airflow dags trigger brewery_datalake_pipeline
# Você também pode Listar as Dags e as tasks com os comandos:
make ls-dag ou docker exec -it airflow_webserver ls /opt/airflow/dags
make ls-task ou docker exec -it airflow_webserver airflow tasks list brewery_datalake_pipeline
# Caso queira definir uma data para o pipeline deve ser no formato 2026-02-17
make pipeline ou docker exec -it airflow_webserver airflow dags test brewery_datalake_pipeline $(TODAY)

📊 Rodar Testes
make test ou docker exec -it airflow_scheduler bash -lc "pytest -q /opt/airflow/tests"

📂 Estrutura do projeto
dags/        → DAG Airflow
src/         → lógica pipeline
tests/       → pytest
datalake/    → bronze/silver/gold

🛠 Stack
Apache Airflow
Docker
Postgres
Pandas
Pytest

👤 Autor
Marco Aurélio