import os
import logging
from datetime import datetime

def configurar_logger(log_folder, log_name, logger_name):

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # Evita adicionar múltiplos handlers ao chamar novamente
    if not logger.handlers:
        os.makedirs(log_folder, exist_ok=True)
        hoje = datetime.today().strftime('%Y%m%d')
        complete_log_name = f'{hoje}_{log_name}'
        log_file = os.path.join(log_folder, complete_log_name)

        # --- Define formato padrão ---
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        # --- Handler para arquivo ---
        file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # --- Handler opcional para console ---
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # --- Nível customizado SUCCESS ---
        SUCCESS_LEVEL_NUM = 25
        logging.addLevelName(SUCCESS_LEVEL_NUM, "SUCCESS")

        def success(self, message, *args, **kwargs):
            if self.isEnabledFor(SUCCESS_LEVEL_NUM):
                self._log(SUCCESS_LEVEL_NUM, message, args, **kwargs)
        logging.Logger.success = success

        logger.debug("Logger configurado com sucesso.")

    return logger

# Função auxiliar para padronizar logs com print + gravação no arquivo
def print_log(logger, msg, level='info'):
    # Se o logger estiver desligado (modo TEST), não faz nada
    if logger is None:
        return

    level = level.lower()
    if level == "success":
        logger.success(msg)
    elif level == "error":
        logger.error(msg)
    elif level == "warning":
        logger.warning(msg)
    else:
        logger.info(msg)
