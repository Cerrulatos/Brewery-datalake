# Bibliotecas
import os
import socket
import requests
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.utils.log.logging_mixin import LoggingMixin
logger = LoggingMixin().log


# Imports dos scripts
from src.ingestion.extract_api import extract_breweries
from src.transformation.silver_transform import transform_to_silver
from src.transformation.gold_transform import transform_to_gold
from src.monitoring.audit_store import upsert_dag_run, insert_task_event

# Definição de variaveis
ALERT_EMAIL = Variable.get("ALERT_EMAIL", default_var="fallback@email.com")

# Função para converter um datetime para string ISO
def _iso(dt):
    return dt.isoformat() if dt else None

# FUNÇÃO PARA ENVIO DE EMAIL EM CASO DE FALHA
def notify_failure(context):
    ti = context["task_instance"]
    # Só alerta quando não houver mais retries
    if ti.try_number <= ti.max_tries:
        return
    subject = f"[AIRFLOW] FALHA - {ti.dag_id}.{ti.task_id}"
    body = f"""
    <h3>Falha detectada no Airflow</h3>
    <b>DAG:</b> {ti.dag_id}<br>
    <b>Task:</b> {ti.task_id}<br>
    <b>Run ID:</b> {context.get("run_id")}<br>
    <b>Execution date:</b> {context.get("ds")}<br>
    <b>Try number:</b> {ti.try_number}<br>
    <b>Log:</b> <a href="{ti.log_url}">Abrir log</a><br><br>
    <b>Exception:</b><br>
    <pre>{context.get("exception")}</pre>
    """
    send_email(ALERT_EMAIL, subject, body)

# Função para gravar dados na tabela audit.task_events
def _audit_task_event(context, status="started", message=None, metrics=None):
    ti = context["task_instance"]
    execution_date = context.get("logical_date") or context.get("execution_date")
    insert_task_event(
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        task_id=ti.task_id,
        status=status,
        message=message,
        metrics=metrics or {},
        try_number=ti.try_number,
        map_index=getattr(ti, "map_index", None),
        log_url=ti.log_url,
        execution_date=execution_date,
    )

# Função para gravar dados na tabela audit.dag_runs
def _audit_dag_upsert(context, status, metrics=None, error=None):
    dag_run = context.get("dag_run")
    if not dag_run:
        return

    duration = None
    if dag_run.start_date and dag_run.end_date:
        duration = (dag_run.end_date - dag_run.start_date).total_seconds()

    upsert_dag_run(
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        status=status,
        execution_date=context.get("ds"),                 # YYYY-MM-DD
        logical_date=_iso(context.get("logical_date")),  # tz-aware
        start_time=_iso(dag_run.start_date),
        end_time=_iso(dag_run.end_date),
        duration_seconds=duration,
        triggered_by="manual" if dag_run.external_trigger else "scheduled",
        host=socket.gethostname(),
        metrics=metrics or {},
        error=error,
    )

# Função para testar disponibilidade da API
def check_api_health(**context):
    _audit_task_event(context, status="started")

    api_url = os.getenv("URL_API")
    if not api_url:
        fail_metrics = {
            "success": False,
            "url": None,
            "attempt": 0,
            "retries": 0,
            "timeout": None,
            "error": "URL_API não definida no ambiente",
        }
        _audit_task_event(context, status="failed", message="URL_API não definida no ambiente.", metrics=fail_metrics)
        raise Exception("URL_API não definida no ambiente.")

    retries = int(Variable.get("API_HEALTHCHECK_RETRIES", default_var="3"))
    timeout = int(Variable.get("API_HEALTHCHECK_TIMEOUT", default_var="5"))
    sleep_seconds = int(Variable.get("API_HEALTHCHECK_SLEEP", default_var="2"))

    last_err = None
    last_status = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(api_url, params={"page": 1, "per_page": 1}, timeout=timeout)
            last_status = r.status_code
            # 429 ou 5xx = indisponível (ou momentaneamente)
            if r.status_code == 429:
                raise Exception("HTTP 429 (rate limit)")
            if r.status_code >= 500:
                raise Exception(f"HTTP {r.status_code} (server error)")
            r.raise_for_status()

            ok_metrics = {
                "success": True,
                "url": api_url,
                "attempt": attempt,
                "retries": retries,
                "timeout": timeout,
                "status_code": r.status_code,
            }

            _audit_task_event(context, status="success", metrics=ok_metrics)
            return ok_metrics

        except Exception as e:
            last_err = str(e)
            
            logger.warning(f"[API HEALTHCHECK] tentativa {attempt}/{retries} falhou: {last_err}")
            if attempt < retries:
                import time
                time.sleep(sleep_seconds)

    # aqui falhou de vez: registra failed + metrics finais
    final_metrics = {
        "success": False,
        "url": api_url,
        "attempt": retries,
        "retries": retries,
        "timeout": timeout,
        "status_code": last_status,
        "error": last_err,
    }

    _audit_task_event(context, status="failed", message=f"API indisponível após {retries} tentativas: {last_err}", metrics=final_metrics)
    raise Exception(f"API indisponível após {retries} tentativas: {last_err}")

# CONFIGURAÇÕES PADRÃO (retry, alertas, execução)
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,

    # Retry automático
    "retries": 3,
    "retry_delay": timedelta(minutes=2),

    # Alertas por e-mail
    "email_on_failure": False,
    "email_on_retry": False,

    # Callback de falha
    "on_failure_callback": notify_failure,
}

# QUALITY CHECKS
def _parse_duration_to_seconds(duration_str: str) -> float:
    if not duration_str:
        return 0.0
    parts = str(duration_str).split(":")
    if len(parts) != 3:
        return 0.0
    h = int(parts[0])
    m = int(parts[1])
    s = float(parts[2])
    return h * 3600 + m * 60 + s


def quality_checks(**context):
    thresholds = Variable.get("QUALITY_THRESHOLDS", default_var="{}", deserialize_json=True)
    ti = context["ti"]

    bronze = ti.xcom_pull(task_ids="bronze_ingestion")
    silver = ti.xcom_pull(task_ids="silver_transformation")
    gold = ti.xcom_pull(task_ids="gold_transformation")

    # Se algo anterior falhou, aqui a DAG nem deveria chegar,
    # mas mantemos proteção pra mensagens melhores.
    if not bronze or not bronze.get("success"):
        raise Exception("QualityCheck: Bronze não retornou sucesso.")
    if not silver or not silver.get("success"):
        raise Exception("QualityCheck: Silver não retornou sucesso.")
    if not gold or not gold.get("success"):
        raise Exception("QualityCheck: Gold não retornou sucesso.")

    records_bronze = int(bronze.get("total_records", 0))
    records_silver = int(silver.get("records", 0))

    null_name = float(silver.get("null_name", 0.0))
    null_city_state = float(silver.get("null_city_state", 0.0))
    duplicate_id = float(silver.get("duplicate_id", 0.0))
    invalid_brewery_type = float(silver.get("invalid_brewery_type", 0.0))
    silver_duration_sec = _parse_duration_to_seconds(silver.get("transform_duration"))

    # Schema flags
    schema_changed = bool(silver.get("schema_changed", False))
    schema_missing_cols = silver.get("schema_missing_cols") or []
    schema_extra_cols = silver.get("schema_extra_cols") or []

    violations = []

    # Queda brusca volume
    min_ratio = float(thresholds.get("min_silver_vs_bronze_ratio", 0.7))
    if records_bronze > 0:
        ratio = records_silver / records_bronze
        if ratio < min_ratio:
            violations.append(
                f"Queda brusca: silver({records_silver}) / bronze({records_bronze}) = {ratio:.2f} < {min_ratio}"
            )

    # Nulos
    max_null_name = float(thresholds.get("max_null_name_pct", 5.0))
    if null_name > max_null_name:
        violations.append(f"Null name alto: {null_name:.2f}% > {max_null_name:.2f}%")

    max_null_city_state = float(thresholds.get("max_null_city_state_pct", 10.0))
    if null_city_state > max_null_city_state:
        violations.append(f"Null city/state alto: {null_city_state:.2f}% > {max_null_city_state:.2f}%")

    # Duplicados
    max_dup = float(thresholds.get("max_duplicate_id_pct", 1.0))
    if duplicate_id > max_dup:
        violations.append(f"Duplicados por ID alto: {duplicate_id:.2f}% > {max_dup:.2f}%")
    
    # brewery_type inválido (fora do domínio esperado)
    max_invalid_brewery_type = float(thresholds.get("max_invalid_brewery_type", 1.0))
    if invalid_brewery_type > max_invalid_brewery_type:
        violations.append(f"brewery_type inválido alto: {invalid_brewery_type:.2f}% > {max_invalid_brewery_type:.2f}%")

    # Tempo execução
    max_dur = float(thresholds.get("max_duration_seconds", 180))
    if silver_duration_sec > max_dur:
        violations.append(f"Duração acima: {silver_duration_sec:.2f}s > {max_dur:.2f}s")

    min_dur = float(thresholds.get("min_duration_seconds", 0.1)) 
    if silver_duration_sec < min_dur: 
        logger.warning(f"Duração abaixo: {silver_duration_sec:.2f}s < {min_dur:.2f}s (suspeito)")

    # Schema regression/change (configurável)
    fail_on_schema_missing = bool(thresholds.get("fail_on_schema_missing", True))
    fail_on_schema_extra = bool(thresholds.get("fail_on_schema_extra", False))

    if schema_missing_cols and fail_on_schema_missing:
        violations.append(f"Schema regression (colunas faltando): {schema_missing_cols}")

    if schema_extra_cols and fail_on_schema_extra:
        violations.append(f"Schema change (colunas novas): {schema_extra_cols}")

    # Resultado
    if violations:
        msg = "QUALITY CHECK FAILED:\n- " + "\n- ".join(violations)
        raise Exception(msg)

    return {
        "success": True,
        "records_bronze": records_bronze,
        "records_silver": records_silver,
        "silver_duration_sec": round(silver_duration_sec, 2),
        "invalid_brewery_type": round(invalid_brewery_type, 2),
        "schema_changed": schema_changed,
        "schema_missing_cols": schema_missing_cols,
        "schema_extra_cols": schema_extra_cols,
        "violations": [],
    }

# DEFINIÇÃO DA DAG
with DAG(
    dag_id="brewery_datalake_pipeline",
    default_args=default_args,
    description="Pipeline completo Bronze → Silver → Gold + Métricas",
    schedule="0 15 * * *",  # todo dia às 15:00
    start_date=days_ago(1),
    catchup=False,
    tags=["brewery", "datalake", "bronze-silver-gold"],
) as dag:
    
    # TASK 0 — HEALTH CHECK API (antes da ingestão)
    api_health = PythonOperator(
        task_id="api_health_check",
        python_callable=check_api_health,
        provide_context=True,
    )

    # TASK 1 — INGESTÃO (BRONZE)
    def ingestion_task(**context):
        _audit_task_event(context, status="started")

        execution_date = context["ds"]
        result = extract_breweries(per_page=200, max_pages=500, execution_date=execution_date)

        # grava métricas retornadas pelo script
        _audit_task_event(context, status="metrics", metrics=result)

        if not result["success"]:
            _audit_task_event(context, status="failed", message=result.get("error", "Falha na ingestão Bronze"), metrics=result)
            raise Exception("Falha na ingestão Bronze")

        _audit_task_event(context, status="success")
        return result

    ingest = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=ingestion_task,
        provide_context=True,
    )


    # TASK 2 — TRANSFORMAÇÃO SILVER
    def silver_task(**context):
        _audit_task_event(context, status="started")

        execution_date = context["ds"]
        result = transform_to_silver(execution_date=execution_date)

        _audit_task_event(context, status="metrics", metrics=result)

        if not result.get("success"):
            _audit_task_event(context, status="failed", message=result.get("error", "Erro na Silver"), metrics=result)
            raise Exception(result.get("error", "Erro na Silver"))

        _audit_task_event(context, status="success")
        return result

    silver = PythonOperator(
        task_id="silver_transformation",
        python_callable=silver_task,
        provide_context=True,
    )


    # TASK 3 — TRANSFORMAÇÃO GOLD
    def gold_task(**context):
        _audit_task_event(context, status="started")

        execution_date = context["ds"]
        result = transform_to_gold(execution_date=execution_date)

        _audit_task_event(context, status="metrics", metrics=result)

        if not result.get("success"):
            _audit_task_event(context, status="failed", message=result.get("error", "Erro na Gold"), metrics=result)
            raise Exception(result.get("error", "Erro na Gold"))

        _audit_task_event(context, status="success")
        return result

    gold = PythonOperator(
        task_id="gold_transformation",
        python_callable=gold_task,
        provide_context=True,
    )

    # TASK 4 — QUALITY CHECKS
    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=quality_checks,
        provide_context=True,
    )


    # TASK 5 — TESTES AUTOMATIZADOS (pytest)
    run_tests = BashOperator(
        task_id="run_unit_tests",
        bash_command="pytest /opt/airflow/tests/ --maxfail=1 --disable-warnings",
    )


    # TASK 6 — AUDITORIA FINAL
    def audit_task(**context):
        ti = context["ti"]

        # puxa XComs dos tasks anteriores (retornos dict)
        bronze = ti.xcom_pull(task_ids="bronze_ingestion")
        silver_r = ti.xcom_pull(task_ids="silver_transformation")
        gold_r = ti.xcom_pull(task_ids="gold_transformation")

        consolidated = {
            "bronze": bronze or {},
            "silver": silver_r or {},
            "gold": gold_r or {},
        }

        # marca o dag_run como success + salva métricas consolidadas
        _audit_dag_upsert(context, status="success", metrics=consolidated)

        print("Pipeline executado com sucesso!")
        print("Auditoria consolidada no Postgres (audit.dag_runs.metrics)")

        return {"success": True}

    audit = PythonOperator(
        task_id="pipeline_audit",
        python_callable=audit_task,
        provide_context=True,
        on_failure_callback=lambda context: _audit_dag_upsert(context, status="failed", error=str(context.get("exception"))),
    )


    # TASK 7 — EMAIL DE SUCESSO
    success_email = EmailOperator(
        task_id="send_success_email",
        to=[ALERT_EMAIL],
        subject="Brewery Pipeline executado com sucesso!",
        html_content="""
        <h3>Pipeline Finalizado</h3>
        <p>As camadas Bronze → Silver → Gold foram geradas com sucesso.</p>
        <p>Métricas registradas no Postgres do ambiente Docker.</p>
        <br>
        <b>Status:</b> SUCCESS
        """,
        conn_id="smtp_default",
        trigger_rule="all_success",  # só envia se tudo antes tiver sucesso
    )


    # ORQUESTRAÇÃO FINAL
    api_health >> ingest >> silver >> gold >> quality >> run_tests >> audit >> success_email
