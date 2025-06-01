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


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


class InvoiceStreamBronze:
    def __init__(
        self,
        spark: SparkSession
    ):
        self.spark = spark

    def read_invoices(
        self,
        format: str,
        path: Union[str, Path],
        schema: Union[str, Any],
        clean_source: Literal["delete", "archive", "off"],
        archive_dir: Optional[str],
    ) -> DataFrame:
        if isinstance(path, str):
            path = Path(path).as_posix()
        if clean_source == "archive":
            return (
                self.spark.readStream
                .format(format)
                .schema(schema)
                .option("cleanSource", "archive")
                .option("sourceArchiveDir", archive_dir)
                .load(path)
            )
        return (
            self.spark.readStream
            .format(format)
            .schema(schema)
            .option("cleanSource", clean_source)
            .load(path)
        )

    def write_invoices(
        self,
        df: DataFrame,
        format: str,
        checkpoint_location: str,
        output_mode: Literal["update", "complete", "append"],
        table: str,
        query_name: str
    ):
        return (
            df.writeStream
            .queryName(query_name)
            .format(format)
            .option("checkpointLocation", checkpoint_location)
            .outputMode(output_mode)
            .toTable(table)
        )


class InvoiceStreamSilver:
    def __init__(
        self,
        spark: SparkSession
    ):
        self.spark = spark

    def read_invoices(self, table_name: str) -> DataFrame:
        return (
            self.spark.readStream
            .table(table_name)
        )

    def explode_invoices(self, df: DataFrame) -> DataFrame:
        return (
            df.selectExpr(
                "InvoiceNumber",
                "CreatedTime",
                "StoreID",
                "PosID",
                "CustomerType",
                "PaymentMethod",
                "DeliveryType",
                "DeliveryAddress.City",
                "DeliveryAddress.PinCode",
                "DeliveryAddress.State",
                "explode(InvoiceLineItems) AS LineItem"
            )
        )

    def flatten_invoices(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("ItemCode", expr("LineItem.ItemCode"))
            .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
            .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
            .withColumn("ItemQty", expr("LineItem.ItemQty"))
            .withColumn("TotalValue", expr("LineItem.TotalValue"))
            .drop("LineItem")
        )

    def write_invoices(
        self,
        df: DataFrame,
        format: str,
        checkpoint_location: str,
        output_mode: Literal["update", "complete", "append"],
        table: str,
        query_name: str,
    ):
        return (
            df.writeStream
            .queryName(query_name)
            .format(format)
            .option("checkpointLocation", checkpoint_location)
            .outputMode(output_mode)
            .toTable(table)
        )


if __name__ == "__main__":
    table_name = "invoice_line_items"
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
        .appName("InvoicesStreamMedallion")
        .enableHiveSupport()
        .getOrCreate()
    )
    invoices_bronze = InvoiceStreamBronze(spark)
    invoices_df = invoices_bronze.read_invoices(
        format="json",
        path="/opt/spark/datasets/invoices/*.json",
        schema=schema,
        clean_source="archive",
        archive_dir="/opt/spark/spark-archive/invoices"
    )
    squery_bronze = invoices_bronze.write_invoices(
        df=invoices_df,
        format="delta",
        checkpoint_location="/opt/spark/spark-checkpoint/invoices/bronze",
        output_mode="append",
        table="invoices_bronze",
        query_name="ingestion_bronze"
    )

    invoices_silver = InvoiceStreamSilver(spark)
    exploded_df = invoices_silver.explode_invoices(invoices_df)
    flatten_df = invoices_silver.flatten_invoices(exploded_df)
    squery_silver = invoices_silver.write_invoices(
        df=flatten_df,
        format="delta",
        checkpoint_location="/opt/spark/spark-checkpoint/invoices/silver",
        output_mode="append",
        table="invoices_silver",
        query_name="ingestion_silver",
    )

