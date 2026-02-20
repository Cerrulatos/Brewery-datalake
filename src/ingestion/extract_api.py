# SCRIPTS
from src.utils_log import configurar_logger, print_log
from src.monitoring.metrics_store import init_db, save_metrics_dict, generate_run_id

# BIBLIOTECAS
import requests
import json
import os
import time
from datetime import timedelta, datetime
from dotenv import load_dotenv

# FUNÇÃO PARA DEFINIR EXECUTION DATE
def _resolve_execution_date(execution_date: str = None) -> str:
    if execution_date:
        return execution_date
    return os.getenv("AIRFLOW_CTX_EXECUTION_DATE", datetime.now().strftime("%Y-%m-%d"))[:10]


# Função para extrair todos os dados da API até retornar lista vazia
# Salva cada página como JSON bruto na camada Bronze
def extract_breweries(per_page=200, max_pages=500, execution_date: str = None):
    # CARREGA VARIAVEIS
    # Carrega .env somente quando a função roda (evita side-effects em testes/import)
    load_dotenv(".env", override=False)
    execution_date = _resolve_execution_date(execution_date)
    URL_API = os.getenv('URL_API', "https://api.openbrewerydb.org/v1/breweries?per_page=200")
    DATALAKE_PATH = os.getenv('DATALAKE_PATH', "/opt/airflow/datalake")
    LOG_FOLDER = os.getenv('LOG_FOLDER', "/opt/airflow/logs")

    # URL da API
    api_url = URL_API
    datalake_root = DATALAKE_PATH
    LOG_FOLDER = LOG_FOLDER

    if not api_url:
        return {"success": False, "error": "URL_API não definida no ambiente (.env)."}
    if not datalake_root:
        return {"success": False, "error": "DATALAKE_PATH não definida no ambiente (.env)."}
    if not LOG_FOLDER:
        return {"success": False, "error": "LOG_FOLDER não definida no ambiente (.env)."}

    output_path = os.path.join(datalake_root, "raw")

    # # Data da execução
    start_time = time.time() # Variavel para quantificar tempo do processo

    # Variaveis globais
    log_folder = os.path.join(os.getenv("LOG_FOLDER"), "extract")
    os.makedirs(log_folder, exist_ok=True)  # Garante que a pasta de log exista
    logger = configurar_logger(log_folder, "_ingestion_extract.txt", "Extract")
    success = False
    latencies = []
    total_records = 0
    page = 1
    latencies = []
    folder = os.path.join(output_path, f"ingestion_date={execution_date}")

    try:
        # Criando pasta para ingestão
        os.makedirs(folder, exist_ok=True)

        print_log(logger, 'Iniciando ingestão completa da API OpenBrewery.', 'info')
        print_log(logger, f"Salvando arquivos RAW em: {output_path}", 'info')


        # Loop de paginação
        while True:
            if page > max_pages:
                print_log(logger, "Limite máximo de páginas atingido.", 'warning')
                success = True
                break

            print_log(logger, f"Buscando página {page}...", 'info')
        
            try:
                # Request para API
                request_start = time.time() # Inicio da chamada
                response = requests.get(
                    api_url,
                    params={"page": page, "per_page": per_page},
                    timeout=10
                )
                request_end = time.time() # Fim da chamada

                # Calcula o tempo de latencia
                latency_ms = (request_end - request_start) * 1000
                latencies.append(latency_ms)
                print_log(logger, f"Latência API página {page}: {latency_ms:.2f} ms","info")

                # Valida status da chamada
                response.raise_for_status()
                # Converte JSON
                data = response.json()

            except requests.exceptions.Timeout:
                print_log(logger, f"Timeout na página {page}.", 'error')
                success = False
                break

            except requests.exceptions.HTTPError as e:
                print_log(logger, f"Erro HTTP na página {page}: {e}", 'error')
                success = False
                break

            except requests.exceptions.RequestException as e:
                print_log(logger, f"Erro de conexão na página {page}: {e}", 'error')
                success = False
                break

            except json.JSONDecodeError:
                print_log(logger, f"Erro ao interpretar JSON da página {page}", 'error')
                success = False
                break

            except Exception as e:
                print_log(logger, f"Erro inesperado na página {page}: {e}", 'error')
                success = False
                break


            # Condição de parada do loop
            if not data:
                print_log(logger, "API retornou vazio. Final da paginação.", 'success')
                success = True
                break

            # Salvar JSON
            file_name = f"page_{page:03}.json"
            file_path = os.path.join(folder, file_name)

            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)

                print_log(logger, f"Página {page} salva ({len(data)} registros)", 'success')
                success = True

            except Exception as e:
                print_log(logger, f"Erro ao salvar arquivo da página {page}: {e}", 'error')
                success = False
                break

            total_records += len(data)
            page += 1

    # Erro geral fatal
    except Exception as e:
        print_log(logger, f"Erro fatal inesperado: {e}", "error")
        success = False
        return {"success": False, "error": str(e)}

    # Finalização do script
    finally:
        duration = str(timedelta(seconds=(time.time() - start_time)))
        avg_latency = (sum(latencies) / len(latencies)) if latencies else None # KPI Final: média de latência

        #if latencies:
        #    avg_latency = sum(latencies) / len(latencies)
        #    print_log(logger, f"Tempo médio de resposta da API: {avg_latency:.2f} ms", "info")

        if success:
            print_log(logger, "Ingestão finalizada!", 'info')
            print_log(logger, f'Script concluido com sucesso! Tempo de Execucao: {duration}', 'success')
            print_log(logger, f"Total de registros extraídos: {total_records}", 'info')
            print_log(logger, f"Total de páginas processadas: {page - 1}", 'info')
            print_log(logger, f"Latência média da API: {round(avg_latency, 2) if avg_latency else None} ms", 'info')
        else:
            print_log(logger, f'Script finalizado com erro. Tempo de Execucao: {duration}', 'error')

        # Salvar métricas no SQLite
        run_id = generate_run_id()
        init_db()

        metrics = {
            "pages_processed": page - 1,
            "total_records": total_records,
            "ingestion_duration": duration,
            "api_latency_ms": avg_latency
        }

        save_metrics_dict(run_id, execution_date, "bronze", metrics)

            
        return {
            "success": success,
            "total_records": total_records,
            "pages_processed": page - 1,
            "output_folder": folder,
            "ingestion_duration": duration,
            "ingestion_date": execution_date,
            "api_latency_ms": round(avg_latency, 2) if avg_latency else None,
            "run_id": run_id
        }


if __name__ == "__main__":
    extract_breweries()
