import time
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, broadcast, to_date, col, min, max, avg, mean, count
from config import Config


class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name

    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)


class Gold:
    def __init__(self, spark: SparkSession, env: str):
        self.spark = spark
        self.Conf = Config(self.spark)
        self.test_data_dir = self.Conf.base_dir_data + "/test_data"
        self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        self.db_name = self.Conf.db_name
        self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
        self.spark.sql(f"USE {self.catalog}.{self.db_name}")

    def upsert_workout_bpm_summary(self, once: bool = True, processing_time: str = "15 seconds",
                                   startingVersion: int = 0):
        # Idempotent - Once a workout session is complete, It doesn't change. So insert only the new records
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.workout_bpm_summary a
            USING workout_bpm_summary_delta b
            ON a.user_id = b.user_id AND a.workout_id = b.workout_id AND a.session_id = b.session_id
            WHEN NOT MATCHED THEN INSERT *
        """

        data_upserter = Upserter(query, "workout_bpm_summary_delta")

        df_users = self.spark.read.table(f"{self.catalog}.{self.db_name}.user_bins")

        df_delta = (
            self.spark.readStream
            .option("startingVersion", startingVersion)
            # .option("ignoreDeletes", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.workout_bpm")
            .withWatermark("end_time", "30 seconds")
            .groupBy("user_id", "workout_id", "session_id", "end_time")
            .agg(
                min("heartrate").alias("min_bpm"), mean("heartrate").alias("avg_bpm"),
                max("heartrate").alias("max_bpm"), count("heartrate").alias("num_recordings")
            )
            .join(df_users, ["user_id"])
            .select("workout_id", "session_id", "user_id", "age", "gender", "city", "state", "min_bpm", "avg_bpm",
                    "max_bpm", "num_recordings")
        )

        stream_writer = (
            df_delta.writeStream
            .foreachBatch(data_upserter.upsert)
            .outputMode("append")
            .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm_summary")
            .queryName("workout_bpm_summary_upsert_stream")
        )

        self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", "gold_p1")

        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def upsert(
        self,
        once: bool = True,
        processing_time: str = "5 seconds"
    ):
        self.upsert_workout_bpm_summary(once, processing_time)
        if once:
            for stream in self.spark.streams.active:
                stream.awaitTermination()