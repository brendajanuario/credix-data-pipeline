FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file first for better Docker layer caching
COPY docker/data-loader/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the data loading script
COPY parquet_to_postgres.py .

# Create data directory
RUN mkdir -p /app/data

# Default command to run the data loader
CMD ["python", "parquet_to_postgres.py"]
