import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv


# Caminho do banco
load_dotenv('cred.env')
DB_PATH = os.path.join(os.getenv('DATALAKE_PATH'), "metrics", "metrics.db")

# Cria o banco e a tabela caso não existam.
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            execution_date TEXT,
            layer TEXT,
            metric_name TEXT,
            metric_value REAL,
            created_at TEXT
        )
    """)

    conn.commit()
    conn.close()

# Cria um identificador único para cada execução. Exemplo: 20260214_230638
def generate_run_id():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# Insere uma métrica incrementalmente no banco.
def save_metric(run_id, execution_date, layer, metric_name, metric_value):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO pipeline_metrics (
            run_id,
            execution_date,
            layer,
            metric_name,
            metric_value,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        run_id,
        execution_date,
        layer,
        metric_name,
        metric_value,
        datetime.now().isoformat()
    ))

    conn.commit()
    conn.close()

# Salva várias métricas de uma vez.
def save_metrics_dict(run_id, execution_date, layer, metrics: dict):
    for name, value in metrics.items():
        save_metric(run_id, execution_date, layer, name, value)
