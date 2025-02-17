FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS base

RUN useradd -ms /bin/sh -u 1001 app
USER app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1
# Disable cache
ENV UV_NO_CACHE=1

# Copy only the lockfile and project settings (for better caching)
COPY --chown=app:app uv.lock pyproject.toml /app/

WORKDIR /app

# Install the project's dependencies using the lockfile and settings
RUN uv sync --frozen --no-install-project --no-dev

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"


FROM base AS build

# Create volume directories and chown them before they get mounted
ARG VOLUME_DIRNAMES
USER root
RUN <<EOT
    #!/usr/bin/env bash
    for mnt_dir in $(echo $VOLUME_DIRNAMES | tr ";" "\n")
    do
        mkdir "$mnt_dir"
        chown app:app "$mnt_dir"
    done
EOT

# Copy the rest of the project files and install it in editable mode
USER app
WORKDIR /app
RUN mkdir -p /app/src
COPY --chown=app:app ./src/granicus_archiver/ /app/src/granicus_archiver/
RUN uv sync --frozen --no-dev

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []


FROM build AS webrunner


COPY --chmod=755 <<EOT /entrypoint.sh
#!/usr/bin/env bash
set -e
python -m granicus_archiver --load-config-env web collect-static \${AIOHTTP_STATIC_ROOT} &&
python -m granicus_archiver --load-config-env web -h \${AIOHTTP_HOST} -p \${AIOHTTP_PORT} --no-serve-static --use-s3 --s3-data-dir \${S3_TMP_DIR} serve --no-launch-browser
EOT

ENTRYPOINT [ "/entrypoint.sh" ]
