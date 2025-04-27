# Use a base image with uv and Python
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set working directory
WORKDIR /app

# Enable bytecode compilation (optional, for performance)
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume (optional, for hot reload/dev)
ENV UV_LINK_MODE=copy

# Copy only the dependency files first for better cache on dependency changes
COPY pyproject.toml uv.lock /app/

# Install dependencies (excluding dev dependencies and without installing project code)
RUN uv sync --frozen --no-install-project --no-dev

# Now copy the rest of the application code
COPY . /app

# Now install the project itself (no dev dependencies)
RUN uv sync --frozen --no-dev

# Set the PATH to include the virtual environment created by uv
ENV PATH="/app/.venv/bin:$PATH"

# Expose FastAPI's default port
EXPOSE 8000

# Start the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]