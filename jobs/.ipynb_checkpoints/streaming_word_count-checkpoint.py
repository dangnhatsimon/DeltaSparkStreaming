from pyspark.sql.functions import explode, split, trim, lower
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark import SparkContext
import logging
from os.path import abspath
from pathlib import Path
import shutil
from delta import *

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


class BatchWordCount:
    def __init__(
        self,
        spark: SparkSession
    ):
        self.spark = spark

    def read_text(
        self,
        path: str,
        format: str = "text",
        line_sep: str = "."
    ) -> DataFrame:
        lines = (
            self.spark.read
            .format(format)
            .option("lineSep", line_sep)
            .load(path)
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


class StreamWordCount:
    def __init__(
        self,
        spark: SparkSession
    ):
        self.spark = spark

    def read_text(
        self,
        path: str,
        format: str = "text",
        line_sep: str = "."
    ):
        lines = (
            self.spark.readStream
            .format(format)
            .option("lineSep", line_sep)
            .load(path)
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
        output_mode: str,
        table_name: str,
        checkpoint_location: str
    ):
        squery = (
            sdf.writeStream
            .format(format)
            .option("truncate", value=False)
            .option("checkpointLocation", checkpoint_location)
            .outputMode(output_mode)
            .toTable(table_name)
            .start()
            .awaitTermination()
        )
        return squery


if __name__ == "__main__":
    table_name = "word_count_table"
    spark = (
        SparkSession.builder
        .appName("streaming_word_count")
        .enableHiveSupport()
        .getOrCreate()
    )
    # if Path(warehouse_location).exists() and Path(warehouse_location).is_dir():
    #     shutil.rmtree(Path(warehouse_location))
    batch = BatchWordCount(spark)
    raw_sdf = batch.read_text(path="/opt/spark/datasets/text/*.txt")
    processed_sdf = batch.process_text(raw_sdf)
    sdf = batch.count_words(processed_sdf)
    batch.write_table(
        sdf,
        format="delta",
        mode="overwrite",
        table_name=table_name
    )
    spark.read.table(table_name).show()

    stream = StreamWordCount(spark)
    raw_sdf = stream.read_text(path="/opt/spark/datasets/text/*.txt")
    processed_sdf = stream.process_text(raw_sdf)
    sdf = stream.count_words(processed_sdf)
    squery = stream.write_table(
        sdf,
        format="delta",
        output_mode="complete",
        table_name=table_name,
        checkpoint_location="/opt/spark/checkpoints/word_count"
    )
    spark.read.table(table_name).show()

# docker exec -it delta-streaming-spark-master-1 spark-submit --master spark://172.19.0.2:7077 --packages io.delta:delta-spark_2.13:3.3.0,org.apache.spark:spark-sql_2.12:3.5.3 --deploy-mode client /opt/spark/jobs/streaming_word_count.py
