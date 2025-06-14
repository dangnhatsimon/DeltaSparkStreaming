# Databricks notebook source
# dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
# dbutils.widgets.text("Host", "", "Databricks Workspace URL")
# dbutils.widgets.text("AccessToken", "", "Secure Access Token")

# COMMAND ----------

# env = dbutils.widgets.get("Environment")
# host = dbutils.widgets.get("Host")
# token = dbutils.widgets.get("AccessToken")

# COMMAND ----------

# MAGIC %run ./02-setup

# COMMAND ----------
from setup import SetupHelper
import requests
import json
from bronze import Bronze
from silver import Silver
from gold import Gold
from history import HistoryLoader
from producer import Producer
from pyspark.sql.connect.session import SparkSession


# COMMAND ----------

job_payload = {
    "name": "stream-test",
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "stream-test-task",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "/Repos/gym_workout/run",
                "source": "WORKSPACE"
            },
            "job_cluster_key": "Job_cluster",
            "timeout_seconds": 0,
            "email_notifications": {}
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "Job_cluster",
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode"
                },
                "azure_attributes": {
                    "first_on_demand": 1,
                    "availability": "ON_DEMAND_AZURE",
                    "spot_bid_max_price": -1
                },
                "node_type_id": "Standard_DS4_v2",
                "driver_node_type_id": "Standard_DS4_v2",
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                },
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD",
                "num_workers": 0
            }
        }
    ],
    "format": "MULTI_TASK"
}

# COMMAND ----------

# Create a streaming job

# create_response = requests.post(host + '/api/2.1/jobs/create', data=json.dumps(job_payload), auth=("token", token))
# print(f"Response: {create_response}")
# job_id = json.loads(create_response.content.decode('utf-8'))["job_id"]
# print(f"Created Job {job_id}")

# COMMAND ----------

# Trigger the streaming job
# run_payload = {"job_id": job_id, "notebook_params": {"Environment":env, "RunType": "stream", "ProcessingTime": "1 seconds"}}
# run_response = requests.post(host + '/api/2.1/jobs/run-now', data=json.dumps(run_payload), auth=("token", token))
# run_id = json.loads(run_response.content.decode('utf-8'))["run_id"]
# print(f"Started Job run {run_id}")

# COMMAND ----------

# Wait until job starts
# status_payload = {"run_id": run_id}
# job_status="PENDING"
# while job_status == "PENDING":
#     status_job_response = requests.get(host + '/api/2.1/jobs/runs/get', data=json.dumps(status_payload), auth=("token", token))
#     job_status = json.loads(status_job_response.content.decode('utf-8'))["tasks"][0]["state"]["life_cycle_state"]

# COMMAND ----------

#Terminate the streaming Job
# cancel_payload = {"run_id": run_id}
# cancel_response = requests.post(host + '/api/2.1/jobs/runs/cancel', data=json.dumps(cancel_payload), auth=("token", token))
# print(f"Canceled Job run {run_id}. Status {cancel_response}")

# COMMAND ----------

#Delete the Job
# delete_job_payload = {"job_id": job_id}
# delete_job_response = requests.post(host + '/api/2.1/jobs/delete', data=json.dumps(delete_job_payload), auth=("token", token))
# print(f"Canceled Job run {run_id}. Status {delete_job_response}")

# COMMAND ----------

# dbutils.notebook.exit("SUCCESS")

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("GymWorkoutStream")
        .enableHiveSupport()
        .getOrCreate()
    )
    sc = spark.sparkContext
    spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    env = "dev"
    once = True
    processing_time = "5 seconds"

    sh = SetupHelper(spark, env)
    hl = HistoryLoader(spark, env)
    pr = Producer(spark)

    pr.produce(1)

    bronze = Bronze(spark, env)
    silver = Silver(spark, env)
    gold = Gold(spark, env)

    pr.produce(2)