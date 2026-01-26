FROM python:3.12.1-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app

# COPY ./requirements.txt /app
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# RUN pip install -r requirements.txt
RUN uv sync --locked

COPY ingest_data.py .

ENTRYPOINT [ "uv", "run", "python", "ingest_data.py" ] 