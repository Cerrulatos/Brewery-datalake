# Bibliotecas
import os
import json
import pandas as pd
from pathlib import Path
import pytest
from unittest.mock import patch
from datetime import date

# Script
from src.transformation.silver_transform import transform_to_silver

# Simula data de execução
@pytest.fixture
def execution_date():
    airflow_exec = os.getenv("AIRFLOW_CTX_EXECUTION_DATE")
    if airflow_exec:
        return airflow_exec[:10]  # YYYY-MM-DD
    return date.today().isoformat()

# Aponta env vars para datalake/logs temporários.
def _set_test_env(monkeypatch, isolated_env, execution_date: str):

    monkeypatch.setenv("DATALAKE_PATH", str(isolated_env["datalake"]))
    monkeypatch.setenv("LOG_FOLDER", str(isolated_env["logs"]))
    monkeypatch.setenv("ENV", "TEST")
    # força consistência com resolver do airflow
    monkeypatch.setenv("AIRFLOW_CTX_EXECUTION_DATE", f"{execution_date}T00:00:00+00:00")

# Função para criar camada bronze falsa
def _create_fake_bronze(datalake: Path, execution_date: str):
    bronze_dir = datalake / "raw" / f"ingestion_date={execution_date}"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    payload = [
        {
            "id": "1",
            "name": "Brew A",
            "city": "Los Angeles",
            "state": "CA",
            "brewery_type": "micro",
            "latitude": "34.0",
            "longitude": "-118.2",
        },
        {
            "id": "2",
            "name": "Brew B",
            "city": "New York",
            "state": "NY",
            "brewery_type": "brewpub",
            "latitude": "40.7",
            "longitude": "-74.0",
        },
        # duplicado
        {
            "id": "2",
            "name": "Brew B",
            "city": "New York",
            "state": "NY",
            "brewery_type": "brewpub",
            "latitude": "40.7",
            "longitude": "-74.0",
        },
    ]

    with open(bronze_dir / "page_001.json", "w", encoding="utf-8") as f:
        json.dump(payload, f)

    return bronze_dir

# Teste 1: Testa se a transformação foi bem-sucedida
@patch("src.transformation.silver_transform.init_db")
@patch("src.transformation.silver_transform.save_metrics_dict")
@patch("src.transformation.silver_transform.generate_run_id", return_value="run_test")
def test_transform_result(mock_run_id, mock_save_metrics, mock_init_db, isolated_env, monkeypatch, execution_date):
    _set_test_env(monkeypatch, isolated_env, execution_date)
    _create_fake_bronze(isolated_env["datalake"], execution_date)

    result = transform_to_silver(execution_date=execution_date)

    assert result["success"] is True
    assert result["records"] > 0
    assert result["output_file"] is not None
    assert result["output_file"].endswith(".parquet")

    # Não permite gravar dados no banco de métricas
    mock_init_db.assert_called_once()
    mock_save_metrics.assert_called_once()

# Teste 2: Verifica se não há erro e se a execução foi registrada corretamente
@patch("src.transformation.silver_transform.init_db")
@patch("src.transformation.silver_transform.save_metrics_dict")
def test_transform_register(mock_save_metrics, mock_init_db, isolated_env, monkeypatch, execution_date):
    _set_test_env(monkeypatch, isolated_env, execution_date)
    _create_fake_bronze(isolated_env["datalake"], execution_date)

    result = transform_to_silver(execution_date=execution_date)

    assert result["success"] is True
    assert "error" not in result

    # Não permite gravar dados no banco de métricas
    mock_init_db.assert_called_once()
    mock_save_metrics.assert_called_once()


# Teste 3: Valida se o arquivo parquet foi criado
    # e se o arquivo parquet contém registros
@patch("src.transformation.silver_transform.init_db")
@patch("src.transformation.silver_transform.save_metrics_dict")
def test_transform_parquet(mock_save_metrics, mock_init_db, isolated_env, monkeypatch, execution_date):
    _set_test_env(monkeypatch, isolated_env, execution_date)
    _create_fake_bronze(isolated_env["datalake"], execution_date)

    result = transform_to_silver(execution_date=execution_date)

    assert os.path.exists(result["output_file"])
    df = pd.read_parquet(result["output_file"])
    assert len(df) == result["records"]
    assert len(df) > 0


# Teste 4: Valida colunas essenciais
@patch("src.transformation.silver_transform.init_db")
@patch("src.transformation.silver_transform.save_metrics_dict")
def test_transform_colum(mock_save_metrics, mock_init_db, isolated_env, monkeypatch, execution_date):
    _set_test_env(monkeypatch, isolated_env, execution_date)
    _create_fake_bronze(isolated_env["datalake"], execution_date)

    result = transform_to_silver(execution_date=execution_date)

    assert os.path.exists(result["output_file"])
    df = pd.read_parquet(result["output_file"])
    assert "id" in df.columns
    assert "name" in df.columns
    assert "brewery_type" in df.columns
    assert "city" in df.columns
    assert "state" in df.columns

# Teste 5: Valida conversão numérica
@patch("src.transformation.silver_transform.init_db")
@patch("src.transformation.silver_transform.save_metrics_dict")
def test_transform_conversion(mock_save_metrics, mock_init_db, isolated_env, monkeypatch, execution_date):
    _set_test_env(monkeypatch, isolated_env, execution_date)
    _create_fake_bronze(isolated_env["datalake"], execution_date)

    result = transform_to_silver(execution_date=execution_date)

    assert os.path.exists(result["output_file"])
    df = pd.read_parquet(result["output_file"])
    assert df["latitude"].dtype != object
    assert df["longitude"].dtype != object

# Teste 6: Valida se existem IDs duplicados
@patch("src.transformation.silver_transform.init_db")
@patch("src.transformation.silver_transform.save_metrics_dict")
def test_transform_id(mock_save_metrics, mock_init_db, isolated_env, monkeypatch, execution_date):
    _set_test_env(monkeypatch, isolated_env, execution_date)
    _create_fake_bronze(isolated_env["datalake"], execution_date)

    result = transform_to_silver(execution_date=execution_date)
    
    assert os.path.exists(result["output_file"])
    df = pd.read_parquet(result["output_file"])
    assert df["id"].duplicated().sum() == 0

