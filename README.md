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

### 📂 Estrutura do projeto
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

### Instale os programas abaixo
- Instale o GIT for windows
- Instale o VScode
- Instale o Docker desktop
- Instale o Make
- Instale o Python 3.8

---
### 1. Abra o Docker Desktop
Espere aparecer: Docker is running no Docker desktop<br>

### 2. Abra o powershell como administrador<br>
Execute o comando `where.exe make` para consultar o caminho do make<br>
Copie o path e insira nas variaveis de ambiente do windows com o comando abaixo:<br>
`[Environment]::SetEnvironmentVariable(
  "Path",
  $env:Path + ";C:\Program Files (x86)\GnuWin32\bin",
  [EnvironmentVariableTarget]::Machine
)`<br>
O caminho padrão do make normalmente é: C:\Program Files (x86)\GnuWin32<br>
Você também pode inserir esta variavel manualmente<br>

### 3. Faça o Clone do projeto em uma pasta com o comando<br>
`git clone https://github.com/Cerrulatos/Brewery-datalake.git`<br>

### 4. Abra o VsCode<br>
Importe o projeto (File → Open Folder → selecionar o projeto)<br>
Abra um terminal no Vscode e teste o comando:<br>
`docker --version`<br>
`make --version`<br>
`python --version`<br>
É importante que esses 3 comandos funcionem, caso contrário o projeto não será executado com sucesso portando nesta situação revisite os passos de instalação do software que não funcionar corretamente.<br>

### 5. Crie o arquivo .env na raiz do projeto utilizando o VsCode<br>
Copie o conteúdo do arquivo example_env e cole no arquivo .env<br>
cadastre o seu e-mail na variavel ALERT_EMAIL para receber as mensagens do airflow<br>
cadastre na variavel SENDER_EMAIL o e-mail responsável por enviar as mensagens pelo airflow<br>
cadastre na variavel APP_PASS a senha de aplicativo fornecida pelo gerenciador do e-mail cadastrado na variavel SENDER_EMAIL<br>

### 6. Execute o comando abaixo no terminal do VsCode:<br>
`python -m pip install cryptography`<br>

### 7. Gere o código FERNET_KEY através dos comandos:<br>
`make fernet` ou 	`python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`<br>
Copie a chave e cole na variavel FERNET_KEY no arquivo .env dentro do VsCode<br>



### 8. Suba a Stack executando os comando<br>
`make up` ou `docker compose up -d --build`<br>

Acesse o Airflow em http://localhost:8080 com as credenciais abaixo:<br>

user: airflow<br>
pass: airflow<br>

## 🧪 4. Rodar pipeline
### Para executar um pipeline execute os comandos a seguir:
`make dag` ou `docker exec -it airflow_webserver airflow dags trigger brewery_datalake_pipeline`
### Você também pode Listar as Dags e as tasks com os comandos:
`make ls-dag` ou `docker exec -it airflow_webserver ls /opt/airflow/dags`
`make ls-task` ou `docker exec -it airflow_webserver airflow tasks list brewery_datalake_pipeline`
### Caso queira executar o pipeline para uma data especifica utilize o formato 2026-02-17 após os comandos abaixo, 
caso nenhuma data seja informada será utilizada a data do dia
`make pipeline` ou `docker exec -it airflow_webserver airflow dags test brewery_datalake_pipeline 2026-02-17`

##📊 5. Rodar Testes
`make test` ou `docker exec -it airflow_scheduler bash -lc "pytest -q /opt/airflow/tests"`

👤 Autor
Marco Aurélio
