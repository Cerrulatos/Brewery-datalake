# SCRIPTS
from src.utils_log import configurar_logger, print_log
from src.monitoring.metrics_store import init_db, save_metrics_dict, generate_run_id

# BIBLIOTECAS
import os
import json
import pandas as pd
import time
from datetime import timedelta, datetime
from dotenv import load_dotenv

# Função para validação de schema regression
def _normalize_schema(schema_iterable):
    """Normaliza schema para comparação (lower/strip) e retorna set."""
    if schema_iterable is None:
        return set()
    return {str(c).strip().lower() for c in list(schema_iterable) if str(c).strip()}

def _load_expected_schema_from_env_or_default():
    raw = os.getenv("EXPECTED_SILVER_SCHEMA")
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list) and parsed:
                return _normalize_schema(parsed)
        except Exception:
            # se vier inválido, cai no default
            pass

    # default (baseado no que normalmente vem na OpenBrewery)
    return _normalize_schema([
        "id", "name", "brewery_type", "address_1", "address_2", "address_3",
        "city", "state_province", "postal_code", "country",
        "longitude", "latitude", "phone", "website_url", "state", "street"
    ])

# FUNÇÃO PARA DEFINIR EXECUTION DATE
def _resolve_execution_date(execution_date: str = None) -> str:
    if execution_date:
        return execution_date
    return os.getenv("AIRFLOW_CTX_EXECUTION_DATE", datetime.now().strftime("%Y-%m-%d"))[:10]

# Função principal para leitura de todos os arquivos JSON da camada Bronze
# também consolida em um DataFrame limpo e salva na camada Silver
def transform_to_silver(execution_date: str = None):
    # CARREGA VARIAVEIS
    # Carrega .env somente quando a função roda (evita side-effects em testes/import)
    load_dotenv(".env", override=False)
    execution_date = _resolve_execution_date(execution_date)
    
    # Path das camadas
    datalake_root = os.getenv('DATALAKE_PATH', "/opt/airflow/datalake")
    if not datalake_root:
        return {"success": False, "error": "DATALAKE_PATH not set"}
    BRONZE_PATH = os.path.join(datalake_root, "raw")
    SILVER_PATH = os.path.join(datalake_root, "silver")

    # Variavel para quantificar tempo do processo
    start_time = time.time()

    # Variaveis do log
    log_root = os.getenv('LOG_FOLDER', "/opt/airflow/logs")
    logger = None
    if log_root and os.getenv("ENV") != "TEST":
        log_folder = os.path.join(log_root, "transform")
        os.makedirs(log_folder, exist_ok=True)
        logger = configurar_logger(log_folder, "_transform_silver.txt", "Silver")

    # Inicializa a variável global sucess para controlar fluxo
    global success
    success = False

    # Inicializações seguras
    all_records = []
    df = pd.DataFrame()
    null_name = null_brewery_type = invalid_brewery_type = null_city_state = duplicate_id = 0.0
    output_file = None
    run_id = None
    error_msg = None
    # Variaveis para monitora schema regression
    schema_missing_cols = []
    schema_extra_cols = []
    schema_changed = False

    try:
        print_log(logger, 'Iniciando transformação na camada silver.', 'info')
        print_log(logger, f"Salvando dados transformados em: {SILVER_PATH}", 'info')

        # Procura pasta na camada Bronze com a data da execução
        ingestion_folder = os.path.join(BRONZE_PATH, f"ingestion_date={execution_date}")

        if not os.path.exists(ingestion_folder):
            print_log(logger, f"Pasta Bronze não encontrada: {ingestion_folder}", "error")
            error_msg = "Bronze folder not found"
            success = False
            return {"success": False, "error": error_msg}

        # Lista de arquivos JSON
        json_files = [f for f in os.listdir(ingestion_folder) if f.endswith(".json")]

        if len(json_files) == 0:
            print_log(logger, f"Nenhum arquivo JSON encontrado na camada Bronze na data {execution_date}.", "error")
            error_msg = "No JSON files found"
            success = False
            return {"success": False, "error": "No JSON files found"}

        print_log(logger, f"Encontrados {len(json_files)} arquivos na camada Bronze", 'info')

        # Ler todos os JSON
        all_records = []

        for file in json_files:
            file_path = os.path.join(ingestion_folder, file)

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    all_records.extend(data)

            except json.JSONDecodeError:
                print_log(logger, f"Erro ao ler JSON inválido: {file}", "error")
                error_msg = f"Erro ao ler JSON inválido: {file}"
                success = False
                return {"success": False, "error": error_msg}

            except Exception as e:
                print_log(logger, f"Erro inesperado ao abrir {file}: {e}", "error")
                error_msg = f"Erro inesperado ao abrir {file}: {e}"
                success = False
                return {"success": False, "error": error_msg}

        if len(all_records) == 0:
            print_log(logger, "Nenhum registro válido foi carregado.", "error")
            error_msg = "No valid records loaded"
            success = False
            return {"success": False, "error": error_msg}

        print_log(logger, f"Total de registros carregados: {len(all_records)}", "info")

        # Criar DataFrame
        try:
            df = pd.DataFrame(all_records)
            if df.empty:
                print_log(logger, "DataFrame vazio após ingestão.", "error")
                error_msg = "DataFrame empty"
                success = False
                return {"success": False, "error": error_msg}
        
        except Exception as e:
            print_log(logger, f"Erro ao criar DataFrame: {e}", "error")
            error_msg = f"Erro ao criar DataFrame: {e}"
            success = False
            return {"success": False, "error": error_msg}

        print_log(logger, f"Dataframe criado com sucesso", "info")

        # Validação de schema
        expected_schema = _load_expected_schema_from_env_or_default()
        current_schema = _normalize_schema(df.columns)

        missing = sorted(list(expected_schema - current_schema))
        extra = sorted(list(current_schema - expected_schema))

        schema_missing_cols = missing
        schema_extra_cols = extra
        schema_changed = bool(missing or extra)

        if missing:
            # missing é mais crítico (provável quebra)
            print_log(logger, f"[SCHEMA REGRESSION] Colunas faltando: {missing}", "error")
        if extra:
            # extra geralmente é warning (API adicionou campo)
            print_log(logger, f"[SCHEMA CHANGE] Colunas novas detectadas: {extra}", "warning")

        # FAIL_ON_COLUMN_MISSING=true
        fail_on_missing = str(os.getenv("FAIL_ON_SCHEMA_MISSING", "false")).strip().lower() == "true"
        if fail_on_missing and missing:
            success = False
            error_msg = f"Schema regression: colunas faltando: {missing}"
            return {"success": False, "error": error_msg, "schema_missing_cols": missing, "schema_extra_cols": extra}

        # Padronização de Strings
        string_cols = ["name", "city", "state", "brewery_type"]

        for col in string_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.replace(r"\s+", " ", regex=True)
                )

                df[col] = df[col].replace(["none", "nan", ""], None)

        # Conversão de Tipos
        if "latitude" in df.columns:
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")

        if "longitude" in df.columns:
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

        # Monitorando a qualidade do dado
        print_log(logger, "Calculando indicadores de qualidade...", "info")

        null_name = df["name"].isnull().mean() * 100
        null_brewery_type = df["brewery_type"].isnull().mean() * 100
        null_city_state = (df["city"].isnull() | df["state"].isnull()).mean() * 100
        duplicate_id = df.duplicated(subset=["id"]).mean() * 100

        # Lista permitida (OpenBrewery "padrão" + fallback "closed")
        allowed_brewery_types = {
            "micro", "nano", "regional", "brewpub", "large",
            "planning", "bar", "contract", "proprietor", "closed"
        }

        # considera inválido: não nulo e fora da lista
        if "brewery_type" in df.columns:
            invalid_brewery_type = (
                df["brewery_type"].notnull() &
                (~df["brewery_type"].isin(allowed_brewery_types))
            ).mean() * 100
        else:
            invalid_brewery_type = 0.0

        print_log(logger, f"Qtd de registros sem nome: {null_name:.2f}%", "info")
        print_log(logger, f"Qtd de estabelecimentos sem tipo: {null_brewery_type:.2f}%", "info")
        print_log(logger, f"Qtd de registros sem estado / cidade: {null_city_state:.2f}%", "info")
        print_log(logger, f"Qtd de IDs duplicados: {duplicate_id:.2f}%", "info")
        print_log(logger, f"Qtd de brewery_type inválido: {invalid_brewery_type:.2f}%", "info")

        # Limpeza dos dados
        # Remover duplicados por ID
        if "id" in df.columns:
            df = df.drop_duplicates(subset=["id"])

        # Remover registros sem nome
        df = df[df["name"].notnull()]

        print_log(logger, f"Total de registros após tratamentos: {len(df)}", 'info')

        # Schema Final (colunas organizadas)
        final_cols = [
            "id", "name", "brewery_type",
            "city", "state", "country",
            "latitude", "longitude",
            "location", "has_geo",
            "processing_date"
        ]

        df = df[[c for c in final_cols if c in df.columns]]

        # Salvar Silver
        output_folder = os.path.join(SILVER_PATH, f"processing_date={execution_date}")
        output_file = os.path.join(output_folder, "breweries.parquet")

        try:
            os.makedirs(output_folder, exist_ok=True)
            df.to_parquet(output_file, index=False)
            print_log(logger, "Dados gravados na camada Silver com sucesso!", "success")
        except Exception as e:
            print_log(logger, f"Erro ao salvar arquivo Parquet: {e}", "error")
            error_msg = f"Erro ao salvar arquivo Parquet: {e}"
            success = False
            return {"success": False, "error": error_msg}    

        print_log(logger, "Camada Silver gerada com sucesso!", 'success')
        print_log(logger, f"Arquivo salvo em: {output_file} no formato parquet", 'info')
        success = True

    # Erro geral inesperado
    except Exception as e:
        print_log(logger, f"Erro fatal inesperado: {e}", "error")
        error_msg = f"Erro fatal inesperado: {e}"
        success = False
        return {"success": False, "error": error_msg}
    

    # Finalização do script
    finally:
        duration = str(timedelta(seconds=(time.time() - start_time)))

        if success:
            print_log(logger, f"Transformação finalizada com sucesso! Tempo: {duration}", "success")

            # Salvar métricas no SQLite 
            run_id = generate_run_id()
            init_db()

            quality_metrics = {
                "records_received_bronze": len(all_records),
                "records_transformed_silver": len(df),
                "null_name": null_name,
                "null_brewery_type": null_brewery_type,
                "null_city_state": null_city_state,
                "duplicate_id": duplicate_id,
                "invalid_brewery_type": invalid_brewery_type,
                "schema_changed": schema_changed,
                "schema_missing_cols": ",".join(schema_missing_cols),
                "schema_extra_cols": ",".join(schema_extra_cols)
            }

            save_metrics_dict(run_id, execution_date, "silver", quality_metrics)

        else:
            print_log(logger, f"Transformação finalizada com erro. Tempo: {duration}", "error")

        result = {
            "success": success,
            "records": len(df),
            "output_file": output_file,
            "null_name": round(null_name, 2),
            "null_brewery_type": round(null_brewery_type, 2),
            "null_city_state": round(null_city_state, 2),
            "duplicate_id": round(duplicate_id, 2),
            "invalid_brewery_type": round(invalid_brewery_type, 2),
            "schema_changed": bool(schema_changed),
            "schema_missing_cols": schema_missing_cols,
            "schema_extra_cols": schema_extra_cols,
            "transform_duration": duration,
            "transform_date": execution_date,
            "run_id": run_id
        }
    
        if not success:
            result["error"] = error_msg or "Unknown error"

        return result


if __name__ == "__main__":
    transform_to_silver()
