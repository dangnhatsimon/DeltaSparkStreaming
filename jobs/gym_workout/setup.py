import time

from pyspark.sql.connect.session import SparkSession
# %run ./config.ipynb


class SetupHelper:
    def __init__(self, spark: SparkSession, env: str):
        self.spark = spark
        self.conf = Config(spark)
        self.landing_zone = self.conf.base_dir_data + "/raw"
        self.checkpoint_base = self.conf.base_dir_checkpoint + "/checkpoint"
        self.catalog = env
        self.db_name = self.conf.db_name
        self.initialized = False

    def create_db(self):
        self.spark.catalog.clearCache()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")
        self.spark.sql(f"USE {self.catalog}.{self.db_name}")
        self.initialized = True

    def create_registered_users_bronze(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.registered_users_bz(
                    user_id long,
                    device_id long,
                    mac_address string,
                    registration_timestamp double,
                    load_time timestamp,
                    source_file string
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_gym_logins_bz(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logins_bz(
                    mac_address string,
                    gym bigint,
                    login double,
                    logout double,
                    load_time timestamp,
                    source_file string
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_kafka_multiplex_bz(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.kafka_multiplex_bz(
                    key string,
                    value string,
                    topic string,
                    partition bigint,
                    offset bigint,
                    timestamp bigint,
                    date date,
                    week_part string,
                    load_time timestamp,
                    source_file string
                ) PARTITIONED BY (topic, week_part)
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_users(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
                    user_id long,
                    device_id long,
                    mac_address string,
                    registration_timestamp double
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_gym_logs(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.users(
                    mac_address string,
                    gym bigint,
                    login timestamp,
                    logout timestamp
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_user_profile(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
                    user_id bigint,
                    dob date,
                    sex string,
                    gender string,
                    first_name string,
                    last_name string,
                    street_address string,
                    city string,
                    state string,
                    zip string,
                    update timestamp
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_heart_rate(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
                    device_id bigint,
                    time timestamp,
                    heartrate double,
                    valid boolean
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_user_bins(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
                    user_id bigint,
                    age string,
                    gender string,
                    city string,
                    state string
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_workouts(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workouts(
                    user_id int,
                    workout_id int,
                    time timestamp,
                    action string,
                    session_id int
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_completed_workouts(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.completed_workouts(
                    user_id int,
                    workout_id int,
                    session_id int,
                    start_time timestamp,
                    end_time timestamp
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_workout_bpm(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm(
                    user_id int,
                    workout_id int,
                    session_id int,
                    start_time timestamp,
                    end_time timestamp,
                    time timestamp,
                    heartrate double
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_date_lookup(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.date_lookup(
                    date date,
                    week int,
                    year int,
                    month int,
                    dayofweek int,
                    dayofmonth int,
                    dayofmyear int,
                    week_part string
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_workout_bpm_summary(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
                    user_id int,
                    workout_id int,
                    session_id int,
                    age string,
                    gender string,
                    city string,
                    state string,
                    min_bpm double,
                    avg_bpm double,
                    max_bpm double,
                    num_recordings bigint
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_gym_summary(self):
        if self.initialized:
            self.spark.sql(f"""
                CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS(
                    SELECT
                        to_date(login::timestamp) date,
                        gym,
                        l.mac_address,
                        workout_id,
                        session_id,
                        round((logout::long - login::long)/60, 2) AS minutes_in_gym,
                        round((end_time::long - start_time::long)/60, 2) AS minutes_exercising
                    FROM gym_logs AS l
                    JOIN (
                        SELECT mac_address, workout_id, session_id, start_time, end_time
                        FROM completed_workouts AS w
                        INNER JOIN users AS u
                        ON w.user_id = u.user_id
                    ) AS w
                    ON
                        l.mac_address = w.mac_address AND
                        w.start_time BETWEEN l.login AND l.logout
                    ORDER BY date, gym, l.mac_address, session_id
                )
            """)
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def setup(self):
        self.create_db()
        self.create_registered_users_bronze()
        self.create_gym_logins_bz()
        self.create_kafka_multiplex_bz()
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_date_lookup()
        self.create_workout_bpm_summary()
        self.create_gym_summary()