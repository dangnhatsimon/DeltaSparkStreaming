from pyspark.sql.functions import explode, split, trim, lower
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark import SparkContext
import logging
from os.path import abspath
from pathlib import Path
import shutil

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


class InvoiceStream():
    def __init__(
        self,
        spark: SparkSession
    ):
        self.spark = spark

    def read_text(
        self,
        dir: str
    ) -> DataFrame:
        lines = (
            self.spark.read
            .format("text")
            .option("lineSep", ".")
            .load(dir)
        )
        raw_sdf = lines.select(explode(split(lines.value, " ")).alias("word"))
        return raw_sdf

    def process_text(
        self,
        raw_sdf: DataFrame
    ) -> DataFrame:
        processed_sdf = (
            raw_sdf.select(lower(trim(raw_sdf.word)).alias("word"))
            .where("word is not null")
            .where("word rlike '[a-z]'")
        )
        return processed_sdf

    def count_words(
        self,
        processed_sdf: DataFrame
    ) -> DataFrame:
        sdf = processed_sdf.groupBy("word").count()
        return sdf

    def write_table(
        self,
        sdf: DataFrame,
        format: str,
        mode: str,
        table_name: str
    ):
        (
            sdf.write
            .format(format)
            .mode(mode)
            .saveAsTable(table_name)
        )