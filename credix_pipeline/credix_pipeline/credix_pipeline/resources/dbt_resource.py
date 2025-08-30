from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource
import os

class DBTResource(ConfigurableResource):
    """Resource for dbt CLI operations."""
    
    project_dir: str = "/app/credix_analytics"
    profiles_dir: str = "/app/profiles"
    
    def get_dbt_cli_resource(self) -> DbtCliResource:
        """Get configured dbt CLI resource."""
        return DbtCliResource(
            project_dir=self.project_dir,
            profiles_dir=self.profiles_dir
        )
