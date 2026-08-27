FROM python:3.13.15-slim-trixie@sha256:7e3a6aca9d74f93cca21a91d86a8dad8c34749afd5b4a98ee481c9c47b9f5ed4

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN addgroup --system mccx && adduser --system --ingroup mccx mccx

WORKDIR /app
COPY requirements.lock .
RUN python -m pip install --require-hashes -r requirements.lock

COPY --chown=mccx:mccx . .
RUN mkdir -p /app/runtime && chown mccx:mccx /app/runtime
USER mccx

HEALTHCHECK --interval=60s --timeout=5s --start-period=90s --retries=3 \
    CMD ["python", "tools/container_healthcheck.py"]

CMD ["python", "bot.py"]
