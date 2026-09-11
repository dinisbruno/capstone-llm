import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# The Docker image built in Task 2.1 (must exist on the host Docker daemon).
IMAGE = "capstone-llm"
TAG = "python-polars"

default_args = {
    "owner": "airflow",
    "description": "Run the capstone clean job in a Docker container",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "capstone_clean",
    default_args=default_args,
    schedule=None,      # trigger manually; set a cron string to run on a schedule
    catchup=False,
) as dag:
    clean = DockerOperator(
        task_id="clean",
        image=IMAGE,
        command=f"python3 -m capstonellm.tasks.clean --env airflow --tag {TAG}",
        # Forward AWS credentials from the Airflow worker's environment into the
        # task container so the job can reach S3. Never hard-code secrets here.
        environment={
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"),
        },
        docker_url="unix://var/run/docker.sock",   # host Docker, via the mounted socket
        network_mode="bridge",
        auto_remove="force",                        # clean up the container when done
        api_version="auto",
        mount_tmp_dir=False,
    )
