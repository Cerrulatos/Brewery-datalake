# Biliotecas
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional
import psycopg2
import psycopg2.extras

# Conecta no Postgres do docker-compose.
def _get_conn():

    host = os.getenv("AUDIT_DB_HOST", "airflow_postgres")
    port = int(os.getenv("AUDIT_DB_PORT", "5432"))
    db = os.getenv("AUDIT_DB_NAME", "airflow")
    user = os.getenv("AUDIT_DB_USER", "airflow")
    password = os.getenv("AUDIT_DB_PASSWORD", "airflow")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password,
    )

# Inserta os dados na tabela audit.dag_runs
def upsert_dag_run(
    dag_id: str,
    run_id: str,
    status: str,
    execution_date: Optional[str] = None,
    logical_date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    triggered_by: Optional[str] = None,
    host: Optional[str] = None,
    metrics: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    metrics = metrics or {}

    sql = """
    INSERT INTO audit.dag_runs (
      dag_id, run_id, status, execution_date, logical_date,
      start_time, end_time, duration_seconds, triggered_by, host,
      metrics, error, updated_at
    )
    VALUES (
      %(dag_id)s, %(run_id)s, %(status)s,
      %(execution_date)s::timestamptz, %(logical_date)s::timestamptz,
      %(start_time)s::timestamptz, %(end_time)s::timestamptz, %(duration_seconds)s,
      %(triggered_by)s, %(host)s,
      %(metrics)s::jsonb, %(error)s, now()
    )
    ON CONFLICT (dag_id, run_id)
    DO UPDATE SET
      status = EXCLUDED.status,
      execution_date = COALESCE(EXCLUDED.execution_date, audit.dag_runs.execution_date),
      logical_date = COALESCE(EXCLUDED.logical_date, audit.dag_runs.logical_date),
      start_time = COALESCE(EXCLUDED.start_time, audit.dag_runs.start_time),
      end_time = COALESCE(EXCLUDED.end_time, audit.dag_runs.end_time),
      duration_seconds = COALESCE(EXCLUDED.duration_seconds, audit.dag_runs.duration_seconds),
      triggered_by = COALESCE(EXCLUDED.triggered_by, audit.dag_runs.triggered_by),
      host = COALESCE(EXCLUDED.host, audit.dag_runs.host),
      metrics = audit.dag_runs.metrics || EXCLUDED.metrics,
      error = COALESCE(EXCLUDED.error, audit.dag_runs.error),
      updated_at = now();
    """

    payload = {
        "dag_id": dag_id,
        "run_id": run_id,
        "status": status,
        "execution_date": execution_date,
        "logical_date": logical_date,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration_seconds,
        "triggered_by": triggered_by,
        "host": host,
        "metrics": json.dumps(metrics),
        "error": error,
    }

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, payload)
    finally:
        conn.close()

# Função para garantir que a linha “pai” em dag_runs exista antes de inserir qualquer evento de task.
def ensure_dag_run_exists(
    dag_id: str,
    run_id: str,
    execution_date=None,
    status: str = "running",
    triggered_by: str = None,
    host: str = None,
):
    """
    Garante que exista uma linha em audit.dag_runs para (dag_id, run_id).
    Evita erro de FK quando task_events tenta inserir primeiro.
    """
    sql = """
    INSERT INTO audit.dag_runs (
        dag_id, run_id, status, execution_date, logical_date,
        start_time, triggered_by, host, metrics, error, updated_at, created_at
    )
    VALUES (
        %(dag_id)s, %(run_id)s, %(status)s, %(execution_date)s, %(logical_date)s,
        NOW(), %(triggered_by)s, %(host)s, '{}'::jsonb, NULL, NOW(), NOW()
    )
    ON CONFLICT (dag_id, run_id)
    DO UPDATE SET
        updated_at = NOW();
    """

    payload = {
        "dag_id": dag_id,
        "run_id": run_id,
        "status": status,
        "execution_date": execution_date,
        "logical_date": execution_date,
        "triggered_by": triggered_by,
        "host": host,
    }

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, payload)
    finally:
        conn.close()


# Inserta os dados na tabela audit.task_events
def insert_task_event(
    dag_id: str,
    run_id: str,
    task_id: str,
    status: str,
    message: str = None,
    metrics: dict = None,
    try_number: int = None,
    map_index: int = None,
    log_url: str = None,
    execution_date=None,
):
    if metrics is None:
        metrics = {}

    # ✅ GARANTE que o "pai" existe antes do evento da task (evita FK violation)
    ensure_dag_run_exists(
        dag_id=dag_id,
        run_id=run_id,
        execution_date=execution_date,
        status="running",
    )

    sql = """
    INSERT INTO audit.task_events (
        dag_id, run_id, task_id, status, message, metrics,
        try_number, map_index, log_url, event_time
    )
    VALUES (
        %(dag_id)s, %(run_id)s, %(task_id)s, %(status)s, %(message)s, %(metrics)s::jsonb,
        %(try_number)s, %(map_index)s, %(log_url)s, NOW()
    );
    """

    payload = {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "status": status,
        "message": message,
        "metrics": json.dumps(metrics),
        "try_number": try_number,
        "map_index": map_index,
        "log_url": log_url,
    }

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, payload)
    finally:
        conn.close()