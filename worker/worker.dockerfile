FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

# 1. Install dependencies, create user, and clean up in one step
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r styx && useradd -l -rm -d /usr/local/styx -g styx styx

ENV PYTHONPATH="/usr/local/styx"
ENV UV_LINK_MODE=copy
WORKDIR /usr/local/styx

# Switch to non-root user as early as possible for better security
USER styx

COPY --chown=styx:styx worker/pyproject.toml worker/uv.lock ./
COPY --chown=styx:styx styx-package /usr/local/styx-package/

# Use a cache mount to improve performance across builds
RUN --mount=type=cache,target=/home/styx/.cache/uv,uid=1000,gid=1000 \
    uv sync --frozen

COPY --chown=styx:styx worker worker
COPY --chown=styx:styx worker/start-worker.sh /usr/local/bin/

RUN chmod a+x /usr/local/bin/start-worker.sh

ARG epoch_size=100
ENV SEQUENCE_MAX_SIZE=${epoch_size}
ARG worker_threads=1
ENV WORKER_THREADS=${worker_threads}
ARG enable_compression=true
ENV ENABLE_COMPRESSION=${enable_compression}
ARG use_composite_keys=true
ENV USE_COMPOSITE_KEYS=${use_composite_keys}
ARG use_fallback_cache=true
ENV USE_FALLBACK_CACHE=${use_fallback_cache}

CMD ["/usr/local/bin/start-worker.sh"]
