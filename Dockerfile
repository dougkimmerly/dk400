FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/

# Create non-root user
RUN useradd -m -u 1000 dk400
USER dk400

# Default command (overridden by compose)
CMD ["celery", "-A", "src.dk400.celery_app", "worker", "--loglevel=info"]
