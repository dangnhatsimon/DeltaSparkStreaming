from dotenv import load_dotenv
import os
from pyspark.sql.functions import explode, split, trim, lower, expr
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark import SparkContext
import logging
from os.path import abspath
from pathlib import Path
import shutil
from pathlib import Path
from typing import Optional, Union, List, Tuple, Any


load_dotenv(dotenv_path="/opt/spark/spark-config/.env")


class Bronze:
    def __init__(self, path: str):
        self.BOOTSTRAP_SERVERS = os.getenv("bootstrap.servers")
        self.SECURITY_PROTOCOL = os.getenv("security.protocol")
        self.SASL_MECHANISM = os.getenv("sasl.mechanism")
        self.SASL_USERNAME = os.getenv("sasl.username")
        self.SASL_PASSWORD = os.getenv("sasl.password")
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.SASL_JAAS = f"""{self.JAAS_MODULE} required username='{self.SASL_USERNAME}' password='{self.SASL_PASSWORD}';"""
        self.path = path
        self.spark = (
            SparkSession.builder
            .appName("InvoicesKafka")
            .enableHiveSupport()
            .getOrCreate()
        )

    def ingest(self, topic: str, max_offsets_per_trigger: int, starting_time: int):
        return (self.spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS)
        .option("kafka.security.protocol", self.SECURITY_PROTOCOL)
        .option("kafka.sasl.mechanism", self.SASL_MECHANISM)
        .option("kafka.sasl.jaas.config", self.SASL_JAAS)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .option("subscribe", topic)
        .option("startingTimestamp", starting_time)
        .load())

    def get_invoices(self, df: DataFrame):
        return df.select(df.key.cast("string").alias())