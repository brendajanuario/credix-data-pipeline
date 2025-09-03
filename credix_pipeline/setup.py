from setuptools import find_packages, setup

setup(
    name="credix_pipeline",
    packages=find_packages(exclude=["credix_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "dagster-postgres", 
        "dagster-gcp",
        "pandas",
        "pyarrow",
        "sqlalchemy",
        "psycopg2-binary",
        "python-dotenv",
        "google-cloud-storage",
        "google-cloud-bigquery"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
