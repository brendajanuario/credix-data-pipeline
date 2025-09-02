
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtProject, dbt_assets, DbtCliResource

DBT_PROJECT_DIR = "/Users/jemzin/Github/credix-data-pipeline/dbt/business_case"
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)

@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="payment_analytics_detailed", 
    name="payment_analytics_detailed_gold_layer"
)
def dbt_gold_payment_analytics_detailed(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt gold layer for payment analytics detailed."""
    yield from dbt.cli(["build", "--select", "payment_analytics_detailed"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="company_payment_summary", 
    name="company_payment_summary_gold_layer"
)
def dbt_gold_company_payment_summary(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt gold layer for company payment summary."""
    yield from dbt.cli(["build", "--select", "company_payment_summary"], context=context).stream()