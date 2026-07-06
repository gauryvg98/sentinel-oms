# Sentinel OMS — single always-on container (one writer per account).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HOST=0.0.0.0 \
    PORT=8000

WORKDIR /app

# Editable install keeps the package rooted at /app/src, so the static UI
# (src/sentinel/ui/static) and the SQL migrations (src/sentinel/ledger/schema)
# ship in the image and resolve via Path(__file__) at runtime.
COPY pyproject.toml ./
COPY src ./src
RUN pip install --upgrade pip && pip install -e ".[binance,ui]"

EXPOSE 8000

# Migrations run on boot inside main() (apply_migrations), before serving.
CMD ["python", "-m", "sentinel.ui"]
