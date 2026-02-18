# Bibliotecas
import os
import pandas as pd
from pathlib import Path
from datetime import date
import pytest
from unittest.mock import patch

# Script Gold
from src.transformation.gold_transform import transform_to_gold

# Cria a pasta e o arquivo Parquet esperado pela Gold. 
def _create_fake_silver(datalake: Path, execution_date: str):

    silver_dir = datalake / "silver" / f"processing_date={execution_date}"
    silver_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {"state": "CA", "brewery_type": "micro", "city": "Los Angeles"},
            {"state": "CA", "brewery_type": "micro", "city": "Los Angeles"},
            {"state": "NY", "brewery_type": "brewpub", "city": "New York"},
        ]
    )

    df.to_parquet(silver_dir / "breweries.parquet", index=False)
    return df

# Simula data de execução
@pytest.fixture
def execution_date():
    airflow_exec = os.getenv("AIRFLOW_CTX_EXECUTION_DATE")
    if airflow_exec:
        return airflow_exec[:10]  # YYYY-MM-DD
    return date.today().isoformat()

# TESTE 1: Gold executa corretamente e gera arquivo parquet (isolado)
@patch("src.transformation.gold_transform.init_db")
@patch("src.transformation.gold_transform.save_metrics_dict")
@patch("src.transformation.gold_transform.generate_run_id", return_value="run_test")
def test_gold_transform_creates_parquet(mock_run_id, mock_save_metrics, mock_init_db, isolated_env, execution_date):
    datalake: Path = isolated_env["datalake"]

    # Cria silver fake no datalake temporário
    _create_fake_silver(datalake, execution_date)
    result = transform_to_gold(execution_date=execution_date)

    # Valida retorno geral
    assert result["success"] is True
    assert result["output_file"] is not None
    assert result["output_file"].endswith(".parquet")

    # Valida se arquivo Gold foi criado
    assert os.path.exists(result["output_file"])

    # Valida conteúdo do parquet
    df_gold = pd.read_parquet(result["output_file"])
    assert not df_gold.empty

    # Valida colunas esperadas
    assert "state" in df_gold.columns
    assert "brewery_type" in df_gold.columns
    assert "breweries_per_state_type" in df_gold.columns

    # Valida se existe pelo menos 1 agregação
    assert len(df_gold) > 0

    # Não permite gravar dados no banco de métricas
    mock_init_db.assert_called_once()
    mock_save_metrics.assert_called_once()


# TESTE 2: Gold falha corretamente se Silver não existir
@patch("src.transformation.gold_transform.init_db")
@patch("src.transformation.gold_transform.save_metrics_dict")
def test_gold_fails_without_silver(mock_save_metrics, mock_init_db, isolated_env):
    result = transform_to_gold()


    # Deve falhar
    assert result["success"] is False
    assert "error" in result
    assert "Pasta Silver não encontrada" in result["error"]

    # Não deve salvar métricas
    mock_init_db.assert_not_called()
    mock_save_metrics.assert_not_called()


# TESTE 3: Valida que Gold recebe registros da Silver
@patch("src.transformation.gold_transform.init_db")
@patch("src.transformation.gold_transform.save_metrics_dict")
@patch("src.transformation.gold_transform.generate_run_id", return_value="run_test")
def test_gold_records_received_matches(mock_run_id, mock_save_metrics, mock_init_db, isolated_env, execution_date):
    datalake: Path = isolated_env["datalake"]
    df_silver = _create_fake_silver(datalake, execution_date)
    result = transform_to_gold(execution_date=execution_date)

    assert result["success"] is True

    # Total recebido deve bater com o tamanho do parquet silver fake
    assert result["records_received"] == len(df_silver)

    # Total de agregações Gold deve ser > 0
    assert result["records_gold"] > 0

    # Run_id deve existir (mockado)
    assert result["run_id"] == "run_test"
