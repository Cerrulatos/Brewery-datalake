# Bibliotecas
import os
import json
from datetime import date
from unittest.mock import patch
import pytest

# Script
from src.ingestion.extract_api import extract_breweries

# Simula resposta da API
def _mock_response(data, status_code=200):
    class MockResp:
        def raise_for_status(self):
            if status_code >= 400:
                raise Exception("HTTP error")
        def json(self):
            return data
    return MockResp()

# Simula data de execução
@pytest.fixture
def execution_date():
    airflow_exec = os.getenv("AIRFLOW_CTX_EXECUTION_DATE")
    if airflow_exec:
        return airflow_exec[:10]  # YYYY-MM-DD
    return date.today().isoformat()

# Aponta env vars para datalake/logs temporários.
def _set_test_env(monkeypatch, isolated_env):
    datalake = isolated_env["datalake"]
    logs = isolated_env["logs"]

    monkeypatch.setenv("DATALAKE_PATH", str(datalake))
    monkeypatch.setenv("LOG_FOLDER", str(logs))
    monkeypatch.setenv("URL_API", "https://fake-api")  # não chama internet real

    return datalake, logs


# Teste 1: Testa se a pasta da camada raw existe
@patch("src.ingestion.extract_api.requests.get")
@patch("src.ingestion.extract_api.init_db")
@patch("src.ingestion.extract_api.save_metrics_dict")
@patch("src.ingestion.extract_api.generate_run_id", return_value="run_test")
def test_ingestion_creates_raw_folder(mock_run_id, mock_save_metrics, mock_init_db, mock_get, isolated_env, monkeypatch, execution_date):
    _set_test_env(monkeypatch, isolated_env)

    mock_get.side_effect = [
        _mock_response([{"id": "1", "name": "A"}]),
        _mock_response([]),
    ]

    result = extract_breweries(per_page=5, max_pages=10, execution_date=execution_date)

    assert result["success"] is True
    assert os.path.exists(result["output_folder"])
    assert result["output_folder"].endswith(f"ingestion_date={execution_date}")

    # Não permite gravar dados no banco de métricas
    mock_init_db.assert_called_once()
    mock_save_metrics.assert_called_once()

# Teste 2: Testa se há arquivos salvos na pasta
# Valida se os arquivos são do tipo Json
@patch("src.ingestion.extract_api.requests.get")
@patch("src.ingestion.extract_api.init_db")
@patch("src.ingestion.extract_api.save_metrics_dict")
@patch("src.ingestion.extract_api.generate_run_id", return_value="run_test")
def test_ingestion_saves_json_file(mock_run_id, mock_save_metrics, mock_init_db, mock_get, isolated_env, monkeypatch, execution_date):
    _set_test_env(monkeypatch, isolated_env)

    mock_get.side_effect = [
        _mock_response([{"id": "1", "name": "A"}]),
        _mock_response([]),
    ]

    result = extract_breweries(per_page=5, max_pages=10, execution_date=execution_date)

    files = os.listdir(result["output_folder"])
    assert len(files) > 0
    assert any(f.endswith(".json") for f in files)

    json_file = next(f for f in files if f.endswith(".json"))
    with open(os.path.join(result["output_folder"], json_file), "r", encoding="utf-8") as f:
        payload = json.load(f)

    assert isinstance(payload, list)
    assert len(payload) > 0

# Teste 3: Testa se a API retornou dados reais
# Valida se resultado o resultado foi success is True
@patch("src.ingestion.extract_api.requests.get")
@patch("src.ingestion.extract_api.init_db")
@patch("src.ingestion.extract_api.save_metrics_dict")
@patch("src.ingestion.extract_api.generate_run_id", return_value="run_test")
def test_ingestion_returns_records(mock_run_id, mock_save_metrics, mock_init_db, mock_get, isolated_env, monkeypatch, execution_date):
    _set_test_env(monkeypatch, isolated_env)

    mock_get.side_effect = [
        _mock_response([{"id": "1", "name": "A"}, {"id": "2", "name": "B"}]),
        _mock_response([]),
    ]

    result = extract_breweries(per_page=5, max_pages=10, execution_date=execution_date)

    assert result["success"] is True
    assert result["total_records"] > 0
