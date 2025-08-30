# Multi-stage build for Disheap Python SDK
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependency files
COPY pyproject.toml ./
COPY README.md ./

# Copy source code needed for installation
COPY disheap/ ./disheap/
COPY scripts/ ./scripts/

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir poetry
RUN poetry config virtualenvs.create false
RUN poetry install --only=main

# Production stage
FROM python:3.11-slim as production

# Set working directory
WORKDIR /app

# Install runtime system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy source code
COPY disheap/ ./disheap/
COPY examples/ ./examples/

# Create non-root user
RUN useradd --create-home --shell /bin/bash disheap
RUN chown -R disheap:disheap /app
USER disheap

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-c", "import disheap; print(f'Disheap Python SDK v{disheap.__version__} ready')"]

# Development stage
FROM production as development

# Switch back to root for development tools
USER root

# Install development dependencies
RUN pip install --no-cache-dir \
    pytest \
    pytest-asyncio \
    pytest-cov \
    black \
    flake8 \
    mypy \
    ipython \
    jupyter

# Copy test files
COPY tests/ ./tests/
COPY pytest.ini ./

# Switch back to disheap user
USER disheap

# Default command for development
CMD ["bash"]
