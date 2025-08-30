#!/usr/bin/env python3
"""
Parquet to PostgreSQL Data Loader

This script loads parquet files into PostgreSQL tables.
It's designed to run in a Docker container as part of the data pipeline setup.
"""

import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
from pathlib import Path
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def wait_for_postgres(host, port, user, password, database, max_retries=30):
    """Wait for PostgreSQL to be ready."""
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
            conn.close()
            logger.info("PostgreSQL is ready!")
            return True
        except psycopg2.OperationalError:
            logger.info(f"Waiting for PostgreSQL... ({i+1}/{max_retries})")
            time.sleep(2)
    
    logger.error("Failed to connect to PostgreSQL after maximum retries")
    return False

def get_db_connection():
    """Create database connection using environment variables."""
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'credix_transactions')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres123') # TODO: Use a more secure method to handle passwords

    # Wait for PostgreSQL to be ready
    if not wait_for_postgres(host, port, user, password, database):
        raise Exception("Could not connect to PostgreSQL")
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    
    return engine

def load_parquet_to_postgres(parquet_file_path, table_name, schema='oltp'):
    """Load a parquet file into a PostgreSQL table."""
    try:
        logger.info(f"Loading {parquet_file_path} into {schema}.{table_name}")
        
        # Read parquet file
        df = pd.read_parquet(parquet_file_path)
        logger.info(f"Read {len(df)} rows from {parquet_file_path}")
        
        # Get database engine
        engine = get_db_connection()
        
        # Clean column names (remove special characters, lowercase)
        df.columns = df.columns.str.lower().str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
        
        # Load data to PostgreSQL
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',  # Use append to add to existing table
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Successfully loaded {len(df)} rows into {schema}.{table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error loading {parquet_file_path}: {str(e)}")
        return False

def main():
    """Main function to load parquet data into PostgreSQL."""
    logger.info("Starting parquet to PostgreSQL data loading...")
    
    data_dir = Path('/app/data')
    
    # Check if data directory exists and has parquet files
    parquet_files = list(data_dir.glob('*.parquet'))
    
    if not parquet_files:
        logger.warning("No parquet files found in data directory")
        success = False
    else:
        logger.info(f"Found {len(parquet_files)} parquet files")
        
        success_count = 0
        for parquet_file in parquet_files:
            # Extract table name from filename
            table_name = parquet_file.stem.lower()
            
            if load_parquet_to_postgres(parquet_file, table_name):
                success_count += 1
        
        success = success_count == len(parquet_files)
        logger.info(f"Loaded {success_count}/{len(parquet_files)} files successfully")
    
    if success:
        logger.info("Data loading completed successfully!")
    else:
        logger.error("Data loading completed with errors")
        exit(1)

if __name__ == "__main__":
    main()
