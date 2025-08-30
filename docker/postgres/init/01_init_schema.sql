-- Initialize database schema for Credix data pipeline
-- This script runs when PostgreSQL container starts for the first time

\echo 'Creating Credix database schema...'

-- Create main schema for raw data ingestion
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw_data TO postgres;

\echo 'Credix database schema created successfully!'
