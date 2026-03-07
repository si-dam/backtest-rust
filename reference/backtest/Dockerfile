FROM node:20-bookworm-slim AS frontend-build

WORKDIR /frontend/dash

COPY frontend/dash/package.json ./
COPY frontend/dash/package-lock.json ./
RUN npm ci
COPY frontend/dash/ ./
RUN npm run build

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /app/
COPY app /app/app
COPY scripts /app/scripts
COPY frontend/dash /app/frontend/dash
COPY --from=frontend-build /frontend/dash/dist /app/app/static/dash

RUN pip install --no-cache-dir .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
