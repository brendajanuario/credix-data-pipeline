from dagster import ConfigurableResource
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

class PostgresResource(ConfigurableResource):
    """Resource for PostgreSQL database connections."""
    
    host: str = "localhost"
    port: int = 5432
    database: str = "credix_transactions"
    user: str = "postgres"
    password: str = "postgres123"
    
    def get_connection(self):
        """Get a psycopg2 connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
    
    def get_engine(self):
        """Get SQLAlchemy engine."""
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(connection_string)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        engine = self.get_engine()
        return pd.read_sql(query, engine)
