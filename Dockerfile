# Use uma imagem base específica para consistência
FROM apache/spark:3.4.0-python3

# Define o usuário e o diretório de trabalho
USER root
WORKDIR /app

# Instala dependências do sistema, se necessário
# Adicione 'libsasl2-dev' para o pacote sasl e 'build-essential' para as ferramentas de compilação
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia as dependências primeiro para aproveitar o cache do Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código da aplicação
COPY ./src ./src
COPY ./soda_checks ./soda_checks
COPY ./data ./data

# Muda o dono dos arquivos para o usuário 'spark'
RUN chown -R spark:spark /app

# Muda para o usuário não-root para segurança
USER spark

# O comando de execução (ENTRYPOINT/CMD) será definido no docker-compose