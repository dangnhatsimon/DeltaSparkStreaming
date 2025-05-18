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


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


class InvoiceStream:
    def __init__(
        self,
        spark: SparkSession
    ):
        self.spark = spark

    def read_invoices(
        self,
        format: str,
        path: Union[str, Path],
        schema: Union[str, Any]
    ) -> DataFrame:
        if isinstance(path, str):
            path = Path(path).as_posix()
        return (self.spark.readStream
                .format(format)
                .schema(schema)
                .load(path))

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
        output_mode: str,
        table: str
    ):
        return (
            df.writeStream
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
        .appName("InvoicesStream")
        .enableHiveSupport()
        .getOrCreate()
    )
    invoices_stream = InvoiceStream(spark)
    invoices_df = invoices_stream.read_invoices(
        format="json",
        path="/opt/spark/datasets/invoices/*.json",
        schema=schema
    )
    exploded_df = invoices_stream.explode_invoices(invoices_df)
    flatten_df = invoices_stream.flatten_invoices(exploded_df)
    squery = invoices_stream.write_invoices(
        df=flatten_df,
        format="delta",
        checkpoint_location="/opt/spark/datasets/checkpoint/invoices",
        output_mode="append",
        table=table_name
    )

