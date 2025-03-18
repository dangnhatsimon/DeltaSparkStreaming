from pyspark.sql.functions import explode, split, trim, lower
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark import SparkContext
import logging
from os.path import abspath
from pathlib import Path
import shutil
from config.config import configuration
from delta import *

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


class BatchWordCount():
    def __init__(
        self,
        spark: SparkSession
    ):
        self.spark = spark
        logging.info("Initiated SparkSession.")

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
        logging.info(f"Read Text Files as DataFrame, {raw_sdf.count()} rows, {len(raw_sdf.columns)} columns, schema: {raw_sdf.schema}")
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
        logging.info(f"Processed DataFrame, {processed_sdf.count()} rows, {len(processed_sdf.columns)} columns, schema: {processed_sdf.schema}")
        return processed_sdf

    def count_words(
        self,
        processed_sdf: DataFrame
    ) -> DataFrame:
        sdf = processed_sdf.groupBy("word").count()
        logging.info(f"Grouped DataFrame by word, {sdf.count()} rows, {len(sdf.columns)} columns, schema: {sdf.schema}")
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
        logging.info(f"Wrote {sdf.count()} rows, {len(sdf.columns)} columns, schema: {sdf.schema}  to table: {table_name}, format: {format}, mode: {mode}")


class StreamWordCount():
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
        logging.info(f"Read Text Files Streaming as DataFrame, {raw_sdf.count()} rows, {len(raw_sdf.columns)} columns, schema: {raw_sdf.schema}")
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
        logging.info(f"Processed Streaming DataFrame, {processed_sdf.count()} rows, {len(processed_sdf.columns)} columns, schema: {processed_sdf.schema}")
        return processed_sdf

    def count_words(
        self,
        processed_sdf: DataFrame
    ) -> DataFrame:
        sdf = processed_sdf.groupBy("word").count()
        logging.info(f"Grouped Streaming DataFrame by word, {sdf.count()} rows, {len(sdf.columns)} columns, schema: {sdf.schema}")
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
            # .toTable(table_name)
            .start()
            .awaitTermination()
        )
        logging.info(f"Wrote streaming {sdf.count()} rows, {len(sdf.columns)} columns, schema: {sdf.schema}  to table: {table_name}, format: {format}, outputMode: {output_mode}")
        return squery


if __name__ == "__main__":
    table_name = "word_count_table"
    warehouse_location = abspath("spark-warehouse")
    logging.info(f"spark-warehouse: {warehouse_location}")
    # access_key = configuration.get("AWS_ACCESS_KEY")
    # secret_key = configuration.get("AWS_SECRET_KEY")
    # conf = SparkConf()
    # conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # conf.set("spark.hadoop.fs.s3a.access.key", access_key)
    # conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
    # conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    spark = (
        SparkSession.builder
        .appName("streaming_word_count")
        .config("spark.sql.warehouse.dir", warehouse_location)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.3.0,org.apache.spark:spark-sql_2.12:3.5.3")
        .config("spark.driver.cores", 2)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1g")
        .config("spark.submit.deployMode", "client")
        .config("spark.log.level", "ALL")
        # .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )
    # if Path(warehouse_location).exists() and Path(warehouse_location).is_dir():
    #     shutil.rmtree(Path(warehouse_location))
    batch = BatchWordCount(spark)
    logging.info("\tInitializing Batch Word Count...")
    raw_sdf = batch.read_text(path="/opt/bitnami/spark/datasets/text/*.txt")
    processed_sdf = batch.process_text(raw_sdf)
    sdf = batch.count_words(processed_sdf)
    batch.write_table(
        sdf,
        format="delta",
        mode="overwrite",
        table_name=table_name
    )
    logging.info("Successfully written batch word count table.")
    spark.read.table(table_name).show()

    # stream = StreamWordCount(spark)
    # logging.info("\tInitializing Streaming Word Count...")
    # raw_sdf = stream.read_text(path="/opt/bitnami/spark/datasets/text/*.txt")
    # processed_sdf = stream.process_text(raw_sdf)
    # sdf = stream.count_words(processed_sdf)
    # squery = stream.write_table(
    #     sdf,
    #     format="delta",
    #     output_mode="complete",
    #     # table_name=table_name,
    #     checkpoint_location="/opt/bitnami/spark/checkpoints/word_count"
    # )
    # logging.info(f"squery: {squery}")
    # spark.read.table(table_name).show()

# docker exec -it deltasparkstreaming-spark-master-1 spark-submit --master spark://172.19.0.2:7077 --packages io.delta:delta-spark_2.13:3.3.0,org.apache.spark:spark-sql_2.12:3.5.3 --deploy-mode client /opt/bitnami/spark/jobs/streaming_word_count.py
