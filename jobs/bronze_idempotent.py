from dotenv import load_dotenv
import os
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode, split, trim, lower, expr
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark import SparkContext
import logging
from os.path import abspath
from pathlib import Path
import shutil
from pathlib import Path
from typing import Optional, Union, List, Tuple, Any, Literal
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

load_dotenv(dotenv_path="/opt/spark/spark-config/.env")


class InvoiceBronze:
    def __init__(self, spark: SparkSession):
        self.BOOTSTRAP_SERVERS = os.getenv("bootstrap.servers")
        self.SECURITY_PROTOCOL = os.getenv("security.protocol")
        self.SASL_MECHANISM = os.getenv("sasl.mechanism")
        self.SASL_USERNAME = os.getenv("sasl.username")
        self.SASL_PASSWORD = os.getenv("sasl.password")
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.SASL_JAAS = f"""{self.JAAS_MODULE} required username='{self.SASL_USERNAME}' password='{self.SASL_PASSWORD}';"""
        self.spark = spark

    def read_stream_df(self, topic: str, max_offsets_per_trigger: int, starting_time: int) -> DataFrame:
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

    def get_invoices(self, df: DataFrame, schema) -> DataFrame:
        return df.select(df.key.cast("string").alias("key"),
                         from_json(df.value.cast("string"), schema).alias("value"),
                         "topic", "timestamp")

    def upsert(self, df: DataFrame, batch_id):
        df.createOrReplaceTempView("df_temp_view")
        merge_statment = """
            MERGE INTO invoice_bronze AS s
            USING df_temp_view AS t
            ON s.value == t.value AND s.timestamp == t.timestamp
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
        df._jdf.sparkSession().sql(merge_statment)

    def write_stream_query(self, df: DataFrame, query_name: str, checkpoint_location: str, output_mode: Literal["update", "complete", "append"], table: str) -> StreamingQuery:
        return (df.writeStream
                .queryName(query_name)
                .foreachBatch(self.upsert)
                .option("checkpointLocation", checkpoint_location)
                .outputMode(output_mode)
                .start())


if __name__ == "__main__":
    schema = """
        InvoiceNumber string,
        CreatedTime bigint,
        StoreID string,
        PosID string,
        CashierID string,
        CustomerType string,
        CustomerCardNo string,
        TotalAmount double,
        NumberOfItems bigint,
        PaymentMethod string,
        TaxableAmount double,
        CGST double,
        SGST double,
        CESS double,
        DeliveryType string,
        DeliveryAddress struct<
            AddressLine string,
            City string,
            ContactNumber string,
            PinCode string,
            State string
        >,
        InvoiceLineItems array<
            struct<
                ItemCode string,
                ItemDescription string,
                ItemPrice double,
                ItemQty bigint,
                TotalValue double
            >
        >
    """

    spark = (
        SparkSession.builder
        .appName("InvoicesKafka")
        .enableHiveSupport()
        .getOrCreate()
    )
    bronze = InvoiceBronze(spark=spark)
    df = bronze.read_stream_df(
        topic="invoices",
        max_offsets_per_trigger=10,
        starting_time=1
    )
    invoice_df = bronze.get_invoices(
        df=df,
        schema=schema
    )
    squery = bronze.write_stream_query(
        df=invoice_df,
        query_name="bronze-ingestion",
        checkpoint_location="/opt/spark/spark-checkpoint",
        output_mode="append",
        table="invoice_bronze"
    )
    