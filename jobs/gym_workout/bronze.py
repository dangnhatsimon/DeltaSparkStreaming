import time

from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, broadcast, to_date, col
from config import Config


class Bronze:
    def __init__(self, spark: SparkSession, env: str):
        self.spark = spark
        self.conf = Config(self.spark)
        self.landing_zone = self.conf.base_dir_data + "/raw"
        self.checkpoint_base = self.conf.base_dir_checkpoint + "/checkpoint"
        self.catalog = env
        self.db_name = self.conf.db_name
        self.spark.sql(f"USE {self.catalog}.{self.db_name}")

    def consume_user_registration(
            self,
            once: bool = True,
            processing_time: str = "5 seconds",
    ):
        schema = "user_id long, device_id long, mac_address string, registration_timestamp double"
        df = (
            self.spark.readStream
            .format("cloudFiles")
            .schema(schema)
            .option("maxFilePerTrigger", 1)
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(self.landing_zone + "/registered_users_bz")
            .withColumn("load_time", current_timestamp())
            .withColumn("source_file", input_file_name())
        )
        writer = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", self.checkpoint_base + "/registered_users_bz")
            .outputMode("append")
            .queryName("registered_users_bz_ingestion_stream")
        )

        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")

        if once:
            return writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.registered_users_bz")
        else:
            return writer.trigger(processingTime=processing_time).toTable(
                f"{self.catalog}.{self.db_name}.registered_users_bz")

    def consume_gym_logins(
            self,
            once: bool = True,
            processing_time: str = "5 seconds",
    ):
        schema = "mac_address string, gym bigint, login double, logout double"
        df = (
            self.spark.readStream
            .format("cloudFiles")
            .schema(schema)
            .option("maxFilePerTrigger", 1)
            .option("cloudFiles.format", "csv")
            .option("header", )
            .load(self.landing_zone + "/gym_logins_bz")
            .withColumn("load_time", current_timestamp())
            .withColumn("source_file", input_file_name())
        )
        writer = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", self.checkpoint_base + "/gym_logins_bz")
            .outputMode("append")
            .queryName("gym_logins_bz_ingestion_stream")
        )

        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")

        if once:
            return writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
        else:
            return writer.trigger(processingTime=processing_time).toTable(
                f"{self.catalog}.{self.db_name}.gym_logins_bz")

    def consume_kafka_multiplex(
        self,
        once: bool = True,
        processing_time: str = "5 seconds",
    ):
        schema = "key string, value string, topic string, partition bigint, offset bigint, timestamp bigint"
        df_date_lookup = self.spark.table(f"{self.catalog}.{self.db_name}.date_lookup").select("date", "week_part")

        df = (
            self.spark.readStream
            .format("cloudFiles")
            .schema(schema)
            .option("maxFilePerTrigger", 1)
            .option("cloudFiles.format", "json")
            .load(self.landing_zone + "/kafka_multiplex_bz")
            .withColumn("load_time", current_timestamp())
            .withColumn("source_file", input_file_name())
            .join(
                broadcast(df_date_lookup),
                on=[to_date(col("timestamp") / 1000).cast("timestamp") == col("date")],
                how="left"
            )
        )
        writer = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", self.checkpoint_base + "/kafka_multiplex_bz")
            .outputMode("append")
            .queryName("kafka_multiplex_bz_ingestion_stream")
        )
        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p1")

        if once:
            return writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
        else:
            return writer.trigger(processingTime=processing_time).toTable(
                f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")

    def consume(
        self,
        once: bool = True,
        processing_time: str = "5 seconds",
    ):
        self.consume_user_registration(once, processing_time)
        self.consume_gym_logins(once, processing_time)
        self.consume_kafka_multiplex(once, processing_time)
        if once:
            for stream in self.spark.streams.active:
                stream.awaitTermination()
