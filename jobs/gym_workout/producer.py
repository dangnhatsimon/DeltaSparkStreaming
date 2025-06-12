# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Producer:
    def __init__(self):
        self.conf = Config()
        self.landing_zone = self.conf.base_dir_data + "/raw"
        self.test_data_dir = self.conf.base_dir_data + "/test_data"
               
    def user_registration(self, set_num):
        source = f"{self.test_data_dir}/1-registered_users_{set_num}.csv"
        target = f"{self.landing_zone}/registered_users_bz/1-registered_users_{set_num}.csv" 
        dbutils.fs.cp(source, target)
        
    def profile_cdc(self, set_num):
        source = f"{self.test_data_dir}/2-user_info_{set_num}.json"
        target = f"{self.landing_zone}/kafka_multiplex_bz/2-user_info_{set_num}.json"
        dbutils.fs.cp(source, target)

    def workout(self, set_num):
        source = f"{self.test_data_dir}/4-workout_{set_num}.json"
        target = f"{self.landing_zone}/kafka_multiplex_bz/4-workout_{set_num}.json"
        dbutils.fs.cp(source, target)

    def bpm(self, set_num):
        source = f"{self.test_data_dir}/3-bpm_{set_num}.json"
        target = f"{self.landing_zone}/kafka_multiplex_bz/3-bpm_{set_num}.json"
        dbutils.fs.cp(source, target)

    def gym_logins(self, set_num):
        source = f"{self.test_data_dir}/5-gym_logins_{set_num}.csv"
        target = f"{self.landing_zone}/gym_logins_bz/5-gym_logins_{set_num}.csv" 
        dbutils.fs.cp(source, target)

    def produce(self, set_num):
        if set_num <=2:
            self.user_registration(set_num)
            self.profile_cdc(set_num)        
            self.workout(set_num)
            self.gym_logins(set_num)
        if set_num <=10:
            self.bpm(set_num)
