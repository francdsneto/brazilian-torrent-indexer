FROM python:3.11-slim

WORKDIR /app

# Instala o curl para o healthcheck do docker-compose
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cria a pasta data onde o banco de dados SQLite vai ficar salvo
RUN mkdir -p /app/data

COPY app.py .

# Vamos usar a porta 7008 para não conflitar com o seu DFIndexer (7070)
EXPOSE 7008

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7008"]
