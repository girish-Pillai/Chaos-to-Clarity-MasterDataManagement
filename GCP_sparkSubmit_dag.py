import os
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
   DataprocCreateClusterOperator,
   DataprocDeleteClusterOperator,
   DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.google.cloud.hooks.secrets_manager import SecretsManagerHook


DAG_ID = "dataproc_chaos_to_clarity"
PROJECT_ID = "chaos-to-clarity"
CLUSTER_NAME =  "chaos-to-clarity-airflow-cluster"
REGION = "us-east1"
STORAGE_BUCKET = "mdm_data_bucket"
PIP_INSTALL_PATH = f"gs://goog-dataproc-initialization-actions-{REGION}/python/pip-install.sh"

FUNCTION_PATH = f"gs://{STORAGE_BUCKET}/Code/function.py"
MAIN_PATH = f"gs://{STORAGE_BUCKET}/Code/Main.py"

SECRET_MANAGER_HOOK = SecretsManagerHook(project_id=PROJECT_ID)
SECRET_ID = "composer_key"
SERVICE_ACCOUNT_KEY = SECRET_MANAGER_HOOK.get_secret_value(secret_id=SECRET_ID)

YESTERDAY = datetime.now() - timedelta(days=1)

default_dag_args = {
    'depends_on_past': False,
    'start_date': YESTERDAY,
}

# Use this if there is no initialization action
"""
Cluster definition

CLUSTER_CONFIG = {
   "master_config": {
       "num_instances": 1,
       "machine_type_uri": "n2-standard-2",
       "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
   },
   "worker_config": {
       "num_instances": 2,
       "machine_type_uri": "n2-standard-2",
       "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
   },
}
"""

# Cluster Config for Dataproc Cluster with initialization action

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    storage_bucket=STORAGE_BUCKET,

    #Master Node Details
    num_masters=1,
    master_machine_type="n1-standard-2",
    master_disk_type="pd-standard",
    master_disk_size=50,

    #Worker Node Details
    num_workers=2,
    worker_machine_type="n1-standard-2",
    worker_disk_type="pd-standard",
    worker_disk_size=50,

    properties={},
    image_version="2.1-ubuntu20",
    autoscaling_policy=None,
    idle_delete_ttl=1800,
    metadata={"PIP_PACKAGES": 'apache-airflow apache-airflow-providers-google google-api-python-client fuzzywuzzy pandas'},
    init_actions_uris=[
                    PIP_INSTALL_PATH
                ],
).make()

CLUSTER_CONFIG["service_account_secrets"] = {
    "service_account": SERVICE_ACCOUNT_KEY
}


# Pyspark Job submit definition
PYSPARK_JOB = {
   "reference": {"project_id": PROJECT_ID},
   "placement": {"cluster_name": CLUSTER_NAME},
   "pyspark_job": {"main_python_file_uri": MAIN_PATH},
   }


# DAG to create the Cluster
with DAG(
    DAG_ID,
    schedule="@once",
    default_args=default_dag_args,
    description='DAG to create a Dataproc workflow',
   ) as dag:

   # [START cloud_dataproc_create_cluster_operator]
   create_cluster = DataprocCreateClusterOperator(
       task_id="create_cluster",
       project_id=PROJECT_ID,
       cluster_config=CLUSTER_CONFIG,
       region=REGION,
       cluster_name=CLUSTER_NAME,

   )

   # [Submit a job to the cluster]
   pyspark_task = DataprocSubmitJobOperator(
       task_id="pyspark_task", 
       job=PYSPARK_JOB, 
       region=REGION, 
       project_id=PROJECT_ID
   )

   
   # [Delete the cluster after Job Ends]
   delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

   create_cluster >>  pyspark_task >> delete_cluster