FROM python:3.12.5-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    build-essential \
    libssl-dev \
    libffi-dev \
    git \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Install Rye non-interactively
RUN curl -sSf https://rye.astral.sh/get | RYE_INSTALL_OPTION="--yes" bash

# Set the PATH
ENV PATH="/root/.rye/bin:/root/.rye/shims:${PATH}"

# Copy project files
COPY . /app
# Use Rye to set up the environment
RUN rye sync


RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
