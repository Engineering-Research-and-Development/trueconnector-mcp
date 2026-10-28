FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /bin/uv
WORKDIR /app
# Dependencies first: this layer is cached until uv.lock changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev && useradd --system app
USER app
ENV PATH=/app/.venv/bin:$PATH
EXPOSE 8000
CMD ["tc-mcp", "--transport", "streamable-http", "--host", "0.0.0.0", "--port", "8000"]
