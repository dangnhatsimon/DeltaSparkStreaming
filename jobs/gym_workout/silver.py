import time
from pyspark.sql import DataFrame
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, broadcast, to_date, col, rank, from_json, expr, when, floor, months_between, current_date
from pyspark.sql.window import Window


class Upserter:
    def __init__(self, merge_query, temp_view):
        self.merge_query = merge_query
        self.temp_view = temp_view

    def upsert(
        self,
        df_micro_batch: DataFrame,
        batch_id: str,
    ):
        df_micro_batch.createOrReplaceTempView(self.temp_view)
        df_micro_batch._jdk.sparkSession().sql(self.merge_query)


class CDCUpserter:
    def __init__(self, merge_query, temp_view, id_column, sort_by):
        self.merge_query = merge_query
        self.temp_view = temp_view
        self.id_column = id_column
        self.sort_by = sort_by

    def upsert(
            self,
            df_micro_batch: DataFrame,
            batch_id: str,
    ):
        window = Window.partitionBy(self.id_column).orderBy(col(self.sort_by).desc())
        (
            df_micro_batch
            .filter(col("update_type").isin(["new", "update"]))
            .withColumn("rank", rank().over(window)).filter("rank == 1")
            .drop("rank")
            .createOrReplaceTempView(self.temp_view)
        )
        df_micro_batch._jdk.sparkSession().sql(self.merge_query)


class Silver:
    def __init__(self, spark: SparkSession, env: str):
        self.spark = spark
        self.conf = Config(spark)
        self.checkpoint_base = self.conf.base_dir_checkpoint + "/checkpoint"
        self.catalog = env
        self.db_name = self.conf.db_name
        self.spark.sql(f"USE {self.catalog}.{self.db_name}")

    def upsert_users(self, once: bool = True, processing_time: str = "15 seconds", startingVersion: int = 0):
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.users a
            USING users_delta b
            ON a.user_id = b.user_id
            WHEN NOT MATCHED THEN INSERT *
        """
        upserter = Upserter(query, "users_delta")

        # Spark Structured Streaming accepts append only sources.
        #      - This is not a problem for silver layer streams because bronze layer is insert only
        #      - However, you may want to allow bronze layer deletes due to regulatory compliance
        # Spark Structured Streaming throws an exception if any modifications occur on the table being used as a source
        #      - This is a problem for silver layer streaming jobs.
        #      - ignoreDeletes allows to delete records on partition column in the bronze layer without exception on silver layer streams
        # Starting version is to allow you to restart your stream from a given version just in case you need it
        #      - startingVersion is only applied for an empty checkpoint
        # Limiting your input stream size is critical for running on limited capacity
        #      - maxFilesPerTrigger/maxBytesPerTrigger can be used with the readStream
        #      - Default value is 1000 for maxFilesPerTrigger and maxBytesPerTrigger has no default value
        #      - The recomended maxFilesPerTrigger is equal to #executors assuming auto optimize file size to 128 MB

        df = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.registered_users_bz")
            .selectExpr("user_id", "device_id", "mac_address", "cast(registration_timestamp as timestamp)")
            .withWatermark("registration_timestamp", "30 seconds")
            .dropDuplicates(["user_id", "device_id"])
        )

        # We read new records in bronze layer and insert them to silver. So, the silver layer is insert only in a typical case.
        # However, we want to ignoreDeletes, remove duplicates and also merge with an update statement depending upon the scenarios
        # Hence, it is recomended to se the update mode
        stream_writer = (
            df.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{self.checkpoint_base}/users")
            .queryName("users_upsert_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")

        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def upsert_gym_logs(self, once: bool = True, processing_time: str = "15 seconds", startingVersion: int = 0):
        # Idempotent - Insert new login records
        #           - Update logout time when
        #                   1. It is greater than login time
        #                   2. It is greater than earlier logout
        #                   3. It is not NULL (This is also satisfied by above conditions)
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.gym_logs a
            USING gym_logs_delta b
            ON a.mac_address = b.mac_address AND a.gym = b.gym AND a.login = b.login
            WHEN MATCHED AND b.logout > a.login AND b.logout > a.logout
              THEN UPDATE SET logout = b.logout
            WHEN NOT MATCHED THEN INSERT *
            """

        data_upserter = Upserter(query, "gym_logs_delta")

        df_delta = (spark.readStream
                    .option("startingVersion", startingVersion)
                    .option("ignoreDeletes", True)
                    # .option("withEventTimeOrder", "true")
                    # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                    .table(f"{self.catalog}.{self.db_name}.gym_logins_bz")
                    .selectExpr("mac_address", "gym", "cast(login as timestamp)", "cast(logout as timestamp)")
                    .withWatermark("login", "30 seconds")
                    .dropDuplicates(["mac_address", "gym", "login"])
                    )

        stream_writer = (
            df_delta.writeStream
            .foreachBatch(data_upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{self.checkpoint_base}/gym_logs")
            .queryName("gym_logs_upsert_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")

        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def upsert_user_profile(self, once: bool = False, processing_time: str = "15 seconds", startingVersion: int = 0):

        # Idempotent - Insert new record
        #           - Ignore deletes
        #           - Update user details when
        #               1. update_type in ("new", "append")
        #               2. current update is newer than the earlier
        schema = """
            user_id bigint, update_type STRING, timestamp FLOAT, 
            dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, 
            address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>
        """

        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.user_profile a
            USING user_profile_cdc b
            ON a.user_id = b.user_id
            WHEN MATCHED AND a.updated < b.updated
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
        """

        data_upserter = CDCUpserter(query, "user_profile_cdc", "user_id", "updated")

        df_cdc = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
            .filter("topic = 'user_info'")
            .select(from_json(col("value").cast("string"), schema).alias("v"))
            .select("v.*")
            .select(
                "user_id", to_date('dob', 'MM/dd/yyyy').alias('dob'),
                "sex", "gender", "first_name", "last_name", "address.*",
                col("timestamp").cast("timestamp").alias("updated"),
                "update_type"
            )
            .withWatermark("updated", "30 seconds")
            .dropDuplicates(["user_id", "updated"])
        )

        stream_writer = (
            df_cdc.writeStream
            .foreachBatch(data_upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{self.checkpoint_base}/user_profile")
            .queryName("user_profile_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")

        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def upsert_workouts(self, once: bool = False, processing_time: str = "10 seconds", startingVersion: int = 0):
        schema = "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT"

        # Idempotent - User cannot have two workout sessions at the same time. So ignore the duplicates and insert the new records
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.workouts a
            USING workouts_delta b
            ON a.user_id = b.user_id AND a.time = b.time
            WHEN NOT MATCHED THEN INSERT *
        """

        data_upserter = Upserter(query, "workouts_delta")

        df_delta = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
            .filter("topic = 'workout'")
            .select(from_json(col("value").cast("string"), schema).alias("v"))
            .select("v.*")
            .select(
                "user_id", "workout_id",
                col("timestamp").cast("timestamp").alias("time"),
                "action", "session_id"
            )
            .withWatermark("time", "30 seconds")
            .dropDuplicates(["user_id", "time"])
        )

        stream_writer = (
            df_delta.writeStream
            .foreachBatch(data_upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{self.checkpoint_base}/workouts")
            .queryName("workouts_upsert_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")

        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def upsert_heart_rate(self, once: bool = False, processing_time: str = "10 seconds", startingVersion: int = 0):
        schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

        # Idempotent - Only one BPM signal is allowed at a timestamp. So ignore the duplicates and insert the new records
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.heart_rate a
            USING heart_rate_delta b
            ON a.device_id = b.device_id AND a.time = b.time
            WHEN NOT MATCHED THEN INSERT *
        """

        data_upserter = Upserter(query, "heart_rate_delta")

        df_delta = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
            .filter("topic = 'bpm'")
            .select(from_json(col("value").cast("string"), schema).alias("v"))
            .select("v.*", when(col("v.heartrate") <= 0, False).otherwise(True).alias("valid"))
            .withWatermark("time", "30 seconds")
            .dropDuplicates(["device_id", "time"])
        )

        stream_writer = (
            df_delta.writeStream
            .foreachBatch(data_upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{self.checkpoint_base}/heart_rate")
            .queryName("heart_rate_upsert_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")

        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def age_bins(self, dob_col):
        age_col = floor(months_between(current_date(), dob_col) / 12).alias("age")
        return (when((age_col < 18), "under 18")
                .when((age_col >= 18) & (age_col < 25), "18-25")
                .when((age_col >= 25) & (age_col < 35), "25-35")
                .when((age_col >= 35) & (age_col < 45), "35-45")
                .when((age_col >= 45) & (age_col < 55), "45-55")
                .when((age_col >= 55) & (age_col < 65), "55-65")
                .when((age_col >= 65) & (age_col < 75), "65-75")
                .when((age_col >= 75) & (age_col < 85), "75-85")
                .when((age_col >= 85) & (age_col < 95), "85-95")
                .when((age_col >= 95), "95+")
                .otherwise("invalid age").alias("age"))

    def upsert_user_bins(self, once: bool = True, processing_time: str = "15 seconds", startingVersion: int = 0):
        # Idempotent - This table is maintained as SCD Type 1 dimension
        #            - Insert new user_id records
        #            - Update old records using the user_id

        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.user_bins a
            USING user_bins_delta b
            ON a.user_id=b.user_id
            WHEN MATCHED 
              THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """

        data_upserter = Upserter(query, "user_bins_delta")

        df_user = spark.table(f"{self.catalog}.{self.db_name}.users").select("user_id")

        # Running stream on silver table requires ignoreChanges
        # No watermark required - Stream to staic join is stateless
        df_delta = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreChanges", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.user_profile")
            .join(df_user, ["user_id"], "left")
            .select("user_id", self.age_bins(col("dob")), "gender", "city", "state")
        )

        stream_writer = (
            df_delta.writeStream
            .foreachBatch(data_upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{self.checkpoint_base}/user_bins")
            .queryName("user_bins_upsert_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")

        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def upsert_completed_workouts(self, once: bool = True, processing_time: str = "15 seconds",
                                  startingVersion: int = 0):

        # Idempotent - Only one user workout session completes. So ignore the duplicates and insert the new records
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.completed_workouts a
            USING completed_workouts_delta b
            ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id
            WHEN NOT MATCHED THEN INSERT *
        """

        data_upserter = Upserter(query, "completed_workouts_delta")

        df_start = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.workouts")
            .filter("action = 'start'")
            .selectExpr("user_id", "workout_id", "session_id", "time as start_time")
            .withWatermark("start_time", "30 seconds")
            # .dropDuplicates(["user_id", "workout_id", "session_id", "start_time"])
        )

        df_stop = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.workouts")
            .filter("action = 'stop'")
            .selectExpr("user_id", "workout_id", "session_id", "time as end_time")
            .withWatermark("end_time", "30 seconds")
            # .dropDuplicates(["user_id", "workout_id", "session_id", "end_time"])
        )

        # State cleanup - Define a condition to clean the state
        #               - stop must occur within 3 hours of start
        #               - stop < start + 3 hours
        join_condition = [
            df_start.user_id == df_stop.user_id,
            df_start.workout_id == df_stop.workout_id,
            df_start.session_id == df_stop.session_id,
            df_stop.end_time < df_start.start_time + expr('interval 3 hour')
        ]

        df_delta = (
            df_start.join(df_stop, join_condition)
            .select(df_start.user_id, df_start.workout_id, df_start.session_id, df_start.start_time, df_stop.end_time)
        )

        stream_writer = (
            df_delta.writeStream
            .foreachBatch(data_upserter.upsert)
            .outputMode("append")
            .option("checkpointLocation", f"{self.checkpoint_base}/completed_workouts")
            .queryName("completed_workouts_upsert_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p1")

        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def upsert_workout_bpm(self, once: bool = True, processing_time: str = "15 seconds", startingVersion: int = 0):

        # Idempotent - Only one user workout session completes. So ignore the duplicates and insert the new records
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.workout_bpm a
            USING workout_bpm_delta b
            ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id AND a.time=b.time
            WHEN NOT MATCHED THEN INSERT *
        """

        data_upserter = Upserter(query, "workout_bpm_delta")

        df_users = spark.read.table("users")

        df_completed_workouts = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.completed_workouts")
            .join(df_users, "user_id")
            .selectExpr("user_id", "device_id", "workout_id", "session_id", "start_time", "end_time")
            .withWatermark("end_time", "30 seconds")
        )

        df_bpm = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            # .option("withEventTimeOrder", "true")
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(f"{self.catalog}.{self.db_name}.heart_rate")
            .filter("valid = True")
            .selectExpr("device_id", "time", "heartrate")
            .withWatermark("time", "30 seconds")
        )

        # State cleanup - Define a condition to clean the state
        #               - Workout could be a maximum of three hours
        #               - workout must end within 3 hours of bpm
        #               - workout.end < bpm.time + 3 hours
        join_condition = [
            df_completed_workouts.device_id == df_bpm.device_id,
            df_bpm.time > df_completed_workouts.start_time, df_bpm.time <= df_completed_workouts.end_time,
            df_completed_workouts.end_time < df_bpm.time + expr('interval 3 hour')
        ]

        df_delta = (df_bpm.join(df_completed_workouts, join_condition)
                    .select("user_id", "workout_id", "session_id", "start_time", "end_time", "time", "heartrate")
                    )

        stream_writer = (
            df_delta.writeStream
            .foreachBatch(data_upserter.upsert)
            .outputMode("append")
            .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm")
            .queryName("workout_bpm_upsert_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")

        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()

    def await_queries(self):
        for stream in self.spark.streams.active:
            stream.awaitTermination()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("GymWorkoutSilver")
        .enableHiveSupport()
        .getOrCreate()
    )

    silver = Silver(spark, "dev")
    silver.upsert_users(
        once=True,
        processing_time="5 seconds"
    )
    silver.upsert_gym_logs(
        once=True,
        processing_time="5 seconds"
    )
    silver.upsert_user_profile(
        once=True,
        processing_time="5 seconds"
    )
    silver.upsert_workouts(
        once=True,
        processing_time="5 seconds"
    )
    silver.upsert_heart_rate(
        once=True,
        processing_time="5 seconds"
    )
    silver.await_queries()

    silver.upsert_user_bins(
        once=True,
        processing_time="5 seconds"
    )
    silver.upsert_completed_workouts(
        once=True,
        processing_time="5 seconds"
    )
    silver.await_queries()

    silver.upsert_workout_bpm(
        once=True,
        processing_time="5 seconds"
    )
    silver.await_queries()

