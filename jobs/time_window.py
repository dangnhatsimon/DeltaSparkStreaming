from dotenv import load_dotenv
import os
from pyspark.sql.functions import explode, split, trim, lower, expr, from_json, input_file_name, sum, window
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
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


class TradeSummary:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_table(self, table: str):
        return self.spark.readStream.table(table)

    def get_trade(self, df: DataFrame, schema: StructType) -> DataFrame:
        return (
            df.select(from_json(df.value, schema).alias("value"))
            .select("value.*")
            .withColumn("CreatedTime", expr("to_timestamp(CreatedTime, 'yyyy-MM-dd HH:mm:ss')"))
            .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end"))
            .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))
        )

    def aggregate(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy(window("CreatedTime", "15 minutes"))
            .agg(sum("Buy").alias("TotalBuy"), sum("Sell").alias("TotalSell"))
            .select("window.start", "window.end", "TotalBuy", "TotalSell")
        )

    def write_aggregation(
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


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Time_Window")
        .enableHiveSupport()
        .getOrCreate()
    )
    schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Type", StringType()),
        StructField("Amount", DoubleType()),
        StructField("BrokerCode", StringType()),
    ])

    trade = TradeSummary(spark)
    df = trade.read_table("invoice_bronze")
    trade_df = trade.get_trade(df, schema)
    agg_df = trade.aggregate(trade_df)
    squery = trade.write_aggregation(
        df=agg_df,
        checkpoint_location="/opt/spark/spark-checkpoint/trade-summary",
        output_mode="complete",
        table="trade_summary"
    )