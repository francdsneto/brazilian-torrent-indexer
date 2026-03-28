FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instala o curl (para o healthcheck) e o tzdata (para o fuso horário America/Sao_Paulo)
RUN apt-get update && apt-get install -y curl tzdata && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 7070

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7070"]
