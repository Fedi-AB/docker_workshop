# Base Docker image that we will build on
FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set the working directory in the container
WORKDIR /app

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"


# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# Copy the current directory contents into the container at /app
COPY ingest_data.py .

# Define the command to run the pipeline script when the container starts
ENTRYPOINT [ "python", "ingest_data.py"]
