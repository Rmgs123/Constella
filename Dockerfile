FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=120

RUN apt-get update && apt-get install -y --no-install-recommends \
    libfreetype6 libpng16-16 fonts-dejavu-core \
    procps ca-certificates tzdata curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
# более устойчивый инсталл при сетевых багах
RUN pip install --retries 5 --timeout 120 -r requirements.txt

COPY app.py .
VOLUME ["/app/state"]

EXPOSE 4747
CMD ["python", "app.py"]
