# dk400 - AS/400-inspired job queue system
#
# Platform image. Deployments extend this with their own programs
# and additional dependencies.

FROM python:3.11-slim

WORKDIR /app

# Install minimal system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy platform code
COPY dk400/ ./dk400/

# Copy user programs directory
COPY programs/ ./programs/

# Copy supervisor config
COPY supervisord.conf .

# Create non-root user
RUN useradd -m -u 1000 dk400 && chown -R dk400:dk400 /app

USER dk400

# Default command runs supervisor (robot + api + web)
CMD ["supervisord", "-c", "supervisord.conf"]
