from dotenv import load_dotenv
import os
from pyspark.sql.functions import explode, split, trim, lower, expr, from_json, input_file_name, sum
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
        self.spark = spark

    def read_invoices(
        self,
        format: str,
        path: Union[str, Path],
        schema: Union[str, Any]
    ) -> DataFrame:
        return (self.spark.readStream
                .format(format)
                .schema(schema)
                .load(path)
                .withColumn("InputFile", input_file_name()))

    def write_invoices(
        self,
        df: DataFrame,
        query_name: str,
        format: str,
        checkpoint_location: str,
        output_mode: Literal["update", "complete", "append"],
        table: str
    ) -> StreamingQuery:
        return (
            df.writeStream
            .queryName(query_name)
            .format(format)
            .option("checkpointLocation", checkpoint_location)
            .outputMode(output_mode)
            .toTable(table)
        )


class InvoiceGold:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_stream_table(self, table: str):
        return self.spark.readStream.table(table)

    def aggregate(self, df: DataFrame):
        return (df.groupBy("CustomerCardNo")
                .agg(sum("TotalAmount").alias("TotalAmount"),
                     sum(expr("TotalAmount * 0.2")).alias("TotalPoints")))

    def aggregate_upsert(self, df: DataFrame, batch_id):
        df_agg = self.aggregate(df)
        df_agg.createOrReplaceTempView("customer_rewards_df_temp_view")
        merge_statment = """
            MERGE INTO customer_rewards AS s
            USING customer_rewards_df_temp_view AS t
            ON s.CustomerCardNo == t.CustomerCardNo
            WHEN MATCHED THEN
                UPDATE SET
                    s.TotalAmount = s.TotalAmount + t.TotalAmount,
                    s.TotalPoints = s.TotalPoints + t.TotalPoints
            WHEN NOT MATCHED THEN
                INSERT *
        """
        df_agg._jdf.sparkSession().sql(merge_statment)

    def write_aggregation(
        self,
        df: DataFrame,
        query_name: str,
        format: str,
        checkpoint_location: str,
        output_mode: Literal["update", "complete", "append"]
    ) -> StreamingQuery:
        return (
            df.writeStream
            .queryName(query_name)
            .format(format)
            .option("checkpointLocation", checkpoint_location)
            .outputMode(output_mode)
            .foreachBatch(self.aggregate_upsert)
            .start()
        )


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
        .appName("Aggregate")
        .enableHiveSupport()
        .getOrCreate()
    )
    invoice_bronze = InvoiceBronze(
        spark=spark
    )
    invoice_df = invoice_bronze.read_invoices(
        format="json",
        schema=schema,
        path="/opt/spark/datasets/invoices/*.json"
    )
    squery_bronze = invoice_bronze.write_invoices(
        df=invoice_df,
        query_name="bronze-ingestion",
        checkpoint_location="/opt/spark/spark-checkpoint/invoice-bronze",
        output_mode="append",
        table="invoice_bronze"
    )

    invoice_gold = InvoiceGold(
        spark=spark
    )
    gold_df = invoice_gold.read_stream_table(
        table="invoice_bronze"
    )
    agg_df = invoice_gold.aggregate(
        df=gold_df
    )
    squery_gold = invoice_gold.write_aggregation(
        df=agg_df,
        query_name="gold-update",
        checkpoint_location="/opt/spark/spark-checkpoint/invoice-gold",
        output_mode="update"
    )
