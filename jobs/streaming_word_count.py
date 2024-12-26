from pyspark.sql.functions import explode, split, trim, lower
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
import logging
from os.path import abspath

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


class BatchWordCount():
    def __init__(
        self,
        spark
    ):
        self.spark = spark

    def read_text(
        self,
        dir: str
    ):
        lines = (
            self.spark.read
            .format("text")
            .option("lineSep", ".")
            .load(dir)
        )
        raw_sdf = lines.select(explode(split(lines.value, " ")).alias("word"))
        return raw_sdf

    def process_text(self, raw_sdf):
        processed_sdf = (
            raw_sdf.select(lower(trim(raw_sdf.word)).alias("word"))
            .where("word is not null")
            .where("word rlike '[a-z]'")
        )
        return processed_sdf

    def count_words(self, processed_sdf):
        sdf = processed_sdf.groupBy("word").count()
        return sdf

    def write_table(
        self,
        sdf,
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


class StreamWordCount():
    def __init__(
        self,
        spark
    ):
        self.spark = spark

    def read_text(
        self,
        dir: str
    ):
        lines = (
            self.spark.readStream
            .format("text")
            .option("lineSep", ".")
            .load(dir)
        )
        raw_sdf = lines.select(explode(split(lines.value, " ")).alias("word"))
        return raw_sdf

    def process_text(self, raw_sdf):
        processed_sdf = (
            raw_sdf.select(lower(trim(raw_sdf.word)).alias("word"))
            .where("word is not null")
            .where("word rlike '[a-z]'")
        )
        return processed_sdf

    def count_words(self, processed_sdf):
        sdf = processed_sdf.groupBy("word").count()
        return sdf

    def write_table(
        self,
        sdf,
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
        return squery


if __name__ == "__main__":
    table_name = "word_count_table"
    warehouse_location = abspath('spark-warehouse')
    spark = (
        SparkSession.builder
        .appName("streaming_word_count")
        .config("spark.sql.warehouse.dir", warehouse_location)
        .enableHiveSupport()
        .getOrCreate()
    )

    # batch = BatchWordCount(spark)
    # logging.info(f"\tInitializing Batch Word Count...")
    # raw_sdf = batch.read_text(dir="/opt/bitnami/spark/datasets/text/*.txt")
    # processed_sdf = batch.process_text(raw_sdf)
    # sdf = batch.count_words(processed_sdf)
    # batch.write_table(
    #     sdf,
    #     format="parquet",
    #     mode="overwrite",
    #     table_name=table_name
    # )
    # logging.info("Successfully written batch word count table.")
    # spark.read.table(table_name).show()

    stream = StreamWordCount(spark)
    logging.info(f"\tInitializing Streaming Word Count...")
    raw_sdf = stream.read_text(dir="/opt/bitnami/spark/datasets/text/*.txt")
    processed_sdf = stream.process_text(raw_sdf)
    sdf = stream.count_words(processed_sdf)
    squery = stream.write_table(
        sdf,
        format="console",
        output_mode="complete",
        table_name=table_name,
        checkpoint_location="/opt/bitnami/spark/checkpoints/word_count"
    )
    logging.info(f"squery: {squery}")
    # spark.read.table(table_name).show()

# docker exec 684117bf7216 spark-submit --master spark://172.21.0.2:7077 --deploy-mode client /opt/bitnami/spark/jobs/streaming_word_count.py
