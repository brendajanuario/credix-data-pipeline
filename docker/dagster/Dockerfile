FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy Dagster project
COPY . .

# Set environment variables
ENV DAGSTER_HOME=/app/dagster_home
ENV PYTHONPATH=/app

# Create dagster home directory
RUN mkdir -p $DAGSTER_HOME

# Expose Dagster webserver port
EXPOSE 3000

# Default command to run Dagster webserver
CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000", "--module-name", "credix_pipeline"]
