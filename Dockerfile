# Imagem do Apache Airflow utilizada
FROM apache/airflow:2.8.0

# Copia o arquivo requirements.txt para dentro do container
COPY requirements.txt /

# Instala as bibliotecas Python necessárias para o pipeline
RUN pip install --no-cache-dir -r /requirements.txt
