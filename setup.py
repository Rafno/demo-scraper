import glob

from setuptools import find_packages, setup

setup(
    name="dagster_etl",
    packages=find_packages(exclude=["dagster_etl_tests"]),
    # package data paths are relative to the package key
    package_data={
        "dagster_etl": ["../" + path for path in glob.glob("dbt_project/**", recursive=True)]
    },
    install_requires=[
        "dagster==1.6.*",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-duckdb-pandas",
        "dagster-dbt",
        "azure-storage-blob",
        "azure-mgmt-storage",
        "dbt-duckdb",
        "geopandas",
        "kaleido",
        "pandas",
        "plotly",
        "shapely",
        "smart_open[s3]",
        "s3fs",
        "smart_open",
        "boto3",
        "pyarrow",
        "jsonpath-ng",
        "websocket-client",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
