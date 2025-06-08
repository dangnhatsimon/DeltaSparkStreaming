from pyspark.sql import SparkSession


class Config:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.base_dir_data = self.spark.sql("describe external location `data_zone`").select("url").collect()[0][0]
        self.base_dir_checkpoint = self.spark.sql("describe external location `checkpoint`").select("url").collect()[0][0]
        self.db_name = "gym_workout_db"
        self.maxFilesPerTrigger = 1000