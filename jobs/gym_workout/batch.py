# # Databricks notebook source
# dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
# env = dbutils.widgets.get("Environment")
#
# # COMMAND ----------
#
# # MAGIC %run ./02-setup
#
# # COMMAND ----------
#
# SH = SetupHelper(env)
# SH.cleanup()
#
# # COMMAND ----------
#
# dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})
#
# # COMMAND ----------
#
# # MAGIC %run ./03-history-loader
#
# # COMMAND ----------
#
# HL = HistoryLoader(env)
# SH.validate()
# HL.validate()
#
# # COMMAND ----------
#
# # MAGIC %run ./10-producer
#
# # COMMAND ----------
#
# PR =Producer()
# PR.produce(1)
# PR.validate(1)
# dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})
#
# # COMMAND ----------
#
# # MAGIC %run ./04-bronze
#
# # COMMAND ----------
#
# # MAGIC %run ./05-silver
#
# # COMMAND ----------
#
# # MAGIC %run ./06-gold
#
# # COMMAND ----------
#
# BZ = Bronze(env)
# SL = Silver(env)
# GL = Gold(env)
# BZ.validate(1)
# SL.validate(1)
# GL.validate(1)
#
# # COMMAND ----------
#
# PR.produce(2)
# PR.validate(2)
# dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})
#
# # COMMAND ----------
#
# BZ.validate(2)
# SL.validate(2)
# GL.validate(2)
# SH.cleanup()

from pyspark.sql.connect.session import SparkSession
from setup import SetupHelper
from history import HistoryLoader
from bronze import Bronze
from silver import Silver
from gold import Gold


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("GymWorkoutBatch")
        .enableHiveSupport()
        .getOrCreate()
    )
    env = "dev"
    once = True
    processing_time = "5 seconds"

    sh = SetupHelper(spark, env)
    hl = HistoryLoader(spark, env)