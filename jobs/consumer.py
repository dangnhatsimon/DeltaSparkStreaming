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

BOOTSTRAP_SERVERS = os.getenv("bootstrap.servers")
SECURITY_PROTOCOL = os.getenv("security.protocol")
SASL_MECHANISM = os.getenv("sasl.mechanism")
SASL_USERNAME = os.getenv("sasl.username")
SASL_PASSWORD = os.getenv("sasl.password")
JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
SASL_JAAS = f"""{JAAS_MODULE} required username='{SASL_USERNAME}' password='{SASL_PASSWORD}';"""

spark = (
    SparkSession.builder
    .appName("InvoicesKafka")
    .enableHiveSupport()
    .getOrCreate()
)

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.security.protocol", SECURITY_PROTOCOL)
    .option("kafka.sasl.mechanism", SASL_MECHANISM)
    .option("kafka.sasl.jaas.config", SASL_JAAS)
    .option("subscribe", "invoices")
    .load()
)