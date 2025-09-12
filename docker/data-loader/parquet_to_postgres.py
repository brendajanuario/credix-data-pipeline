import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import logging
from pathlib import Path
import time


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
                dbname=database
            )
            conn.close()
            logger.info("✅ PostgreSQL is ready!")
            return True
        except psycopg2.OperationalError as e:
            logger.warning(f"Waiting for PostgreSQL... ({i+1}/{max_retries}) - {e}")
            time.sleep(2)

    logger.error("❌ Failed to connect to PostgreSQL after maximum retries")
    return False

def get_db_connection():
    """Create database connection using environment variables."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "credix_transactions")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres123")

    if not wait_for_postgres(host, port, user, password, database):
        raise Exception("Could not connect to PostgreSQL")

    logger.info(f"Connecting to PostgreSQL at {host}:{port}, DB={database}, USER={user}")
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password
    )
    return conn

# Pandas -> PostgreSQL dtype mapping
dtype_mapping = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "float64": "DOUBLE PRECISION",
    "float32": "REAL",
    "object": "TEXT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "category": "TEXT",
}

def load_parquet_to_postgres(parquet_file_path, table_name, schema="oltp", batch_size=20000):
    """Load a parquet file into a PostgreSQL table using psycopg2 in batches."""
    try:
        logger.info(f"📂 Loading {parquet_file_path} into {schema}.{table_name}")

        df = pd.read_parquet(parquet_file_path)
        logger.info(f"📊 Read {len(df)} rows and {len(df.columns)} columns from {parquet_file_path}")

        df.columns = df.columns.str.lower().str.replace(r"[^a-zA-Z0-9_]", "_", regex=True)

        conn = get_db_connection()
        cur = conn.cursor()

        columns = []
        for col, dtype in df.dtypes.items():
            pg_type = dtype_mapping.get(str(dtype), "TEXT")
            columns.append(f'"{col}" {pg_type}')

        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        logger.info(f"✅ Ensured schema exists: {schema}")

        create_stmt = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({", ".join(columns)});'
        logger.info(f"🛠️ Running CREATE TABLE: {create_stmt}")
        cur.execute(create_stmt)

        cols = ",".join([f'"{c}"' for c in df.columns])
        insert_stmt = f'INSERT INTO "{schema}"."{table_name}" ({cols}) VALUES %s'
        logger.info(f"📝 Insert statement template: {insert_stmt}")

        data = [tuple(x) for x in df.to_numpy()]
        total_rows = len(data)

        for i in range(0, total_rows, batch_size):
            batch = data[i:i+batch_size]
            try:
                extras.execute_values(cur, insert_stmt, batch, page_size=batch_size)
                logger.info(f"➡️ Inserted {min(i+batch_size, total_rows)}/{total_rows} rows")
            except Exception as e:
                logger.error(f"❌ Error inserting batch {i//batch_size+1}: {e}")
                logger.debug(f"Batch data: {batch[:5]} ...")

        conn.commit()
        logger.info(f"✅ Finished inserting {total_rows} rows into {schema}.{table_name}")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"🔥 Error loading {parquet_file_path}: {str(e)}")
        return False

def main():
    """Main function to load parquet data into PostgreSQL."""
    logger.info("🚀 Starting parquet to PostgreSQL data loading...")

    data_dir = Path("/app/data")
    parquet_files = list(data_dir.glob("*.parquet"))

    if not parquet_files:
        logger.warning("⚠️ No parquet files found in data directory")
        success = False
    else:
        logger.info(f"📂 Found {len(parquet_files)} parquet files to process")

        success_count = 0
        for parquet_file in parquet_files:
            table_name = parquet_file.stem.lower()
            if load_parquet_to_postgres(parquet_file, table_name):
                success_count += 1

        success = success_count == len(parquet_files)
        logger.info(f"📊 Loaded {success_count}/{len(parquet_files)} files successfully")

    if success:
        logger.info("🎉 Data loading completed successfully!")
    else:
        logger.error("⚠️ Data loading completed with errors")
        exit(1)

if __name__ == "__main__":
    main()
