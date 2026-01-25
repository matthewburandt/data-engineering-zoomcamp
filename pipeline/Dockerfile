FROM python:3.13.11-slim

RUN pip install pandas pyarrow

WORKDIR /app

# COPY pipeline.py pipeline.py
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

COPY "pyproject.toml" "uv.lock" ".python-version" ./

ENV PATH="/app/.venv/bin:$PATH"

RUN uv sync --locked

COPY pipeline.py pipeline.py

ENTRYPOINT ["uv", "run", "python", "pipeline.py"]

