FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r styx && useradd -l -rm -d /usr/local/styx -g styx styx

ENV PYTHONPATH="/usr/local/styx"
ENV UV_LINK_MODE=copy
WORKDIR /usr/local/styx

USER styx

COPY --chown=styx:styx coordinator/pyproject.toml coordinator/uv.lock ./
COPY --chown=styx:styx styx-package /usr/local/styx-package/

# Use a cache mount to improve performance across builds
RUN --mount=type=cache,target=/home/styx/.cache/uv,uid=1000,gid=1000 \
    uv sync --frozen

COPY --chown=styx:styx coordinator coordinator
COPY --chown=styx:styx coordinator/start-coordinator.sh /usr/local/bin/

RUN chmod a+x /usr/local/bin/start-coordinator.sh

EXPOSE 8888

ARG max_operator_parallelism=10
ENV MAX_OPERATOR_PARALLELISM=${max_operator_parallelism}
ARG enable_compression=true
ENV ENABLE_COMPRESSION=${enable_compression}
ARG use_composite_keys=true
ENV USE_COMPOSITE_KEYS=${use_composite_keys}
ARG use_fallback_cache=true
ENV USE_FALLBACK_CACHE=${use_fallback_cache}

CMD ["/usr/local/bin/start-coordinator.sh"]
