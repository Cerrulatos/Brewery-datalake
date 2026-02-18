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
══════════════════════════════════════════════════════
                  ┌────────────────────────────┐
                  │        Developer            │
                  │  (feature/* branch)         │
                  └─────────────┬──────────────┘
                                ↓
                       Pull Request (to develop/main)
                                ↓
                  ┌────────────────────────────┐
                  │     GitHub Actions CI      │
                  │  - pytest                  │
                  │  - lint (future)           │
                  │  - quality checks          │
                  └─────────────┬──────────────┘
                                ↓
                        Merge Approved
                                ↓
══════════════════════════════════════════════════════
                 DATA PIPELINE (RUNTIME)
══════════════════════════════════════════════════════

                   Open Brewery API
               ↓ (paginated ingestion)
                  Python Extract Job
                          ↓
               Data Lake Local Storage
                 ├── Bronze (Raw)
                 ├── Silver (Clean)
                 └── Gold (Analytics)
                          ↓
               Airflow Orchestration
                          ↓
            Monitoring + Alerts + Metrics
            
══════════════════════════════════════════════════════

###📂 Estrutura do projeto
dags/        → DAG Airflow
src/         → lógica pipeline
tests/       → pytest
datalake/    → bronze/silver/gold

###🛠 Stack
Apache Airflow
Docker
Postgres
Pandas
Pytest
---

## 🚀 Como rodar

### Pré-requisitos
- GIT
- Docker
- Docker Compose
- Make

---

### 1. Clone

bash
`git clone <repo>`
`cd brewery-datalake`

### 2. Configure as variaveis editando o arquivo criando arquivo .env
`cp example_env .env`
cadastre o seu e-mail na variavel ALERT_EMAIL
Gere o código FERNET_KEY através dos comandos:
`make fernet` ou 	`python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
Copie a chave e cole na variavel FERNET_KEY

### 3. Suba a Stack executando os comando
`make up` ou `docker compose up -d --build`

O Airflow ficará disponível em:
http://localhost:8080
user: airflow
pass: airflow

## 🧪 4. Rodar pipeline
###Para executar um pipeline execute os comandos a seguir:
`make dag` ou `docker exec -it airflow_webserver airflow dags trigger brewery_datalake_pipeline`
### Você também pode Listar as Dags e as tasks com os comandos:
`make ls-dag` ou `docker exec -it airflow_webserver ls /opt/airflow/dags`
`make ls-task` ou `docker exec -it airflow_webserver airflow tasks list brewery_datalake_pipeline`
### Caso queira executar o pipeline para uma data especifica utilize o formato 2026-02-17 após os comandos abaixo, caso nenhuma data seja informada será utilizada a data do dia
`make pipeline` ou `docker exec -it airflow_webserver airflow dags test brewery_datalake_pipeline 2026-02-17`

##📊 5. Rodar Testes
`make test` ou `docker exec -it airflow_scheduler bash -lc "pytest -q /opt/airflow/tests"`

👤 Autor
Marco Aurélio
