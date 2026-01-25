FROM python:3.13.11-slim

RUN pip install --no-cache-dir uv

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY "pyproject.toml" "uv.lock" ".python-version" ./
RUN uv sync --locked

COPY ingest_nytaxi.py .

ENTRYPOINT ["uv", "run", "python", "ingest_nytaxi.py"]
