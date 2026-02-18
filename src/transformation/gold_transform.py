# Scripts
from src.utils_log import configurar_logger, print_log
from src.monitoring.metrics_store import init_db, save_metrics_dict, generate_run_id

# Bibliotecas
import os
import time
import pandas as pd
from datetime import timedelta, datetime
from dotenv import load_dotenv

# FUNÇÃO PARA DEFINIR EXECUTION DATE
def _resolve_execution_date(execution_date: str = None) -> str:
    if execution_date:
        return execution_date
    return os.getenv("AIRFLOW_CTX_EXECUTION_DATE", datetime.now().strftime("%Y-%m-%d"))[:10]

# Função principal para agregações dos dados contidos da camada Silver
# também salva os dados na camada Gold
def transform_to_gold(execution_date: str = None):
     # CARREGA VARIAVEIS
    # Carrega .env somente quando a função roda (evita side-effects em testes/import)
    load_dotenv(".env", override=False)
    execution_date = _resolve_execution_date(execution_date)
    
    # Variavel para quantificar tempo do processo
    start_time = time.time()

    # Path das camadas
    datalake_root = os.getenv("DATALAKE_PATH")
    if not datalake_root:
        return {"success": False, "error": "DATALAKE_PATH not set"}
    SILVER_PATH = os.path.join(datalake_root, "silver")
    GOLD_PATH = os.path.join(datalake_root, "gold")

    # Variaveis do log
    log_root = os.getenv("LOG_FOLDER")
    logger = None
    if log_root and os.getenv("ENV") != "TEST":
        log_folder = os.path.join(log_root, "transform")
        os.makedirs(log_folder, exist_ok=True)
        logger = configurar_logger(log_folder, "_gold_transform.txt", "Gold")

    # Inicializa a variável global sucess para controlar fluxo e variaveis de controle
    global success
    success = False
    output_file = None
    output_folder = None
    output_files = {} 
    run_id = None
    df = pd.DataFrame()
    total_records = 0
    error_msg = None
    

    # Para o finally não quebrar
    gold_df = pd.DataFrame()
    df_state = pd.DataFrame()
    df_type = pd.DataFrame()
    df_city = pd.DataFrame()


    try:
        print_log(logger, "Iniciando transformação GOLD...", "info")

        # Localiza pasta Silver do dia
        silver_folder = os.path.join(SILVER_PATH, f"processing_date={execution_date}")

        if not os.path.exists(silver_folder):
            msg = f"Pasta Silver não encontrada: {silver_folder}"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}
        
        parquet_file = os.path.join(silver_folder, "breweries.parquet")

        if not os.path.exists(parquet_file):
            msg = f"Arquivo Parquet Silver não encontrado: {parquet_file}"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}
        
        print_log(logger, "Arquivo Silver encontrado, carregando dados...", "info")

        # Leitura do Parquet
        try:
            df = pd.read_parquet(parquet_file)
        except Exception as e:
            msg = f"Erro ao ler Parquet: {e}"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}
        
        if df.empty:
            msg = "Arquivo parquet foi carregado mas está vazio"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}
        
        total_records = len(df)
        print_log(logger, f"Total de registros recebidos: {total_records}", "info")

        # Transformação GOLD (agregações)
        print_log(logger, "Criando agregações Gold...", "info")

        try:
            # Cálculo das Métricas
            # Qual é a distribuição de tipos de cervejaria por estado?
            gold_df = (
                df.groupby(["state", "brewery_type"])
                .size()
                .reset_index(name="breweries_per_state_type")
                .sort_values("breweries_per_state_type", ascending=False)
            )

            # Quantas cervejarias existem por estado?
            df_state = df.groupby("state").size().reset_index(name="breweries_per_state").sort_values("breweries_per_state", ascending=False)

            # Quantas cervejarias existem de cada tipo?
            df_type = df.groupby("brewery_type").size().reset_index(name="brewery_per_type")

            # Qual é a concentração de cervejarias por cidade + estado?
            df_city = df.groupby(["city", "state"]).size().reset_index(name="brewery_per_city_state").sort_values("brewery_per_city_state", ascending=False)

        except Exception as e:
            msg = f"Erro ao gerar agregações: {e}"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}

        if gold_df.empty:
            msg = "Agregação state x brewery_type gerou resultado vazio"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}
        
        if df_state.empty:
            msg = "Agregação breweries_per_state gerou resultado vazio"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}

        if df_type.empty:
            msg = "Agregação brewery_type gerou resultado vazio"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}

        if df_city.empty:
            msg = "Agregação city x state gerou resultado vazio"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}

        print_log(logger, "Agregações criadas com sucesso", "success")

        # Save output
        output_folder = os.path.join(GOLD_PATH, f"processing_date={execution_date}")
        os.makedirs(output_folder, exist_ok=True)
        files_to_save  = {
            "breweries_by_state_type.parquet": gold_df,
            "breweries_by_state.parquet": df_state,
            "breweries_by_type.parquet": df_type,
            "breweries_by_city_state.parquet": df_city,
        }

        try:
            for filename, frame in files_to_save.items():
                out_path = os.path.join(output_folder, filename)
                frame.to_parquet(out_path, index=False)
                output_files[filename] = out_path

        except Exception as e:
            msg = f"Erro ao salvar Gold Parquet: {e}"
            error_msg = msg
            print_log(logger, msg, "error")
            return {"success": False, "error": msg}

        output_file = output_files.get("breweries_by_state_type.parquet")
        print_log(logger, "Camada GOLD gerada com sucesso!", "success")
        success = True
    
    # Erro inesperado geral
    except Exception as e:
        msg = f"Erro fatal inesperado: {e}"
        error_msg = msg
        print_log(logger, msg, "error")
        return {"success": False, "error": msg}
    
    # Finalização e métricas
    finally:
        duration = str(timedelta(seconds=time.time() - start_time))

        if success:
            print_log(logger, f"Transformação GOLD finalizada em {duration}", "success")

            run_id = generate_run_id()
            init_db()

            metrics = {
                "records_received_silver": total_records,
                "records_generated_gold": len(gold_df),
                "records_state": len(df_state),
                "records_type": len(df_type),
                "records_city": len(df_city)
            }

            save_metrics_dict(run_id, execution_date, "gold", metrics)

        else:
            print_log(logger, f"Transformação GOLD falhou. Tempo: {duration}", "error")

        result = {
            "success": success,
            "output_files": output_files,
            "output_file": output_file,
            "records_received": total_records,
            "records_gold": len(gold_df) if success else 0,
            "duration": duration,
            "run_id": run_id,
            "transform_date": execution_date
        }

        if error_msg:
            result["error"] = error_msg

        return result


if __name__ == "__main__":
    transform_to_gold()
