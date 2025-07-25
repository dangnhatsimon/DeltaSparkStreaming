# Databricks notebook source
# MAGIC %run ./01-config
from pyspark.sql.connect.session import SparkSession
from config import Config
# COMMAND ----------

class HistoryLoader:
    def __init__(self, spark: SparkSession, env: str):
        self.spark = spark
        self.conf = Config(self.spark)
        self.landing_zone = self.conf.base_dir_data + "/raw"
        self.test_data_dir = self.conf.base_dir_data + "/test_data"
        self.catalog = env
        self.db_name = self.conf.db_name

    def load_date_lookup(self):
        self.spark.sql(f"""
            INSERT OVERWRITE TABLE {self.catalog}.{self.db_name}.date_look
            SELECT date, week, year, month, dayofweek, dayofmonth,dayofyear, week_part
            FROM json.`{self.test_data_dir}/6-date-lookup.json`
            )
        """)

    def load_history(self):
        self.load_date_lookup()

