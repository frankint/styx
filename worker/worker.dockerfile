FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VENV_PATH=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r styx && useradd -l -rm -d /usr/local/styx -g styx styx

ENV PYTHONPATH="/usr/local/styx"
ENV UV_LINK_MODE=copy
WORKDIR /usr/local/styx

USER styx

COPY --chown=styx:styx worker/pyproject.toml worker/uv.lock ./
COPY --chown=styx:styx styx-package /usr/local/styx-package/

# Use a cache mount to improve performance across builds
RUN --mount=type=cache,target=/home/styx/.cache/uv,uid=1000,gid=1000 \
    uv sync --frozen

COPY --chown=styx:styx worker worker
COPY --chown=styx:styx worker/start-worker.sh /usr/local/bin/

RUN uv run python worker/setup.py build_ext --inplace

RUN chmod a+x /usr/local/bin/start-worker.sh

ARG epoch_size=100
ARG worker_threads=1
ARG enable_compression=true
ARG use_composite_keys=true
ARG use_fallback_cache=true

ENV SEQUENCE_MAX_SIZE=$epoch_size \
    WORKER_THREADS=$worker_threads \
    ENABLE_COMPRESSION=$enable_compression \
    USE_COMPOSITE_KEYS=$use_composite_keys \
    USE_FALLBACK_CACHE=$use_fallback_cache

CMD ["/usr/local/bin/start-worker.sh"]
