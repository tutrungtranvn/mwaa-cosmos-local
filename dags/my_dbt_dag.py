"""
## My dbt dag

We use this DAG to transform idms data for reporting. DAG is scheduled to run hourly
"""
import os

from airflow import DAG
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles.postgres.user_pass import PostgresUserPasswordProfileMapping
from datetime import datetime

airflow_home = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
dbt_path = f"{airflow_home}/dags/dbt/jaffle_shop"

with DAG(
    dag_id="my_dbt_dag",
    start_date=datetime(2023, 9, 23),
    schedule=None,
    doc_md=__doc__,
    catchup=False,
    tags=["dbt", "postgres"],
):

    DbtTaskGroup(
        project_config=ProjectConfig(dbt_project_path=dbt_path),
        profile_config=ProfileConfig(
            profile_name="airflow_db",
            target_name="cosmos_target",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_custome", profile_args={"dbname": "airflow", "schema": "jaffle_shop"}
            ),
        ),
    )