# Databricks notebook source
import json
import pandas as pd
import uuid
import random
from random import randrange
from datetime import datetime, timedelta 



# COMMAND ----------

def get_params():
  params = json.loads(_params)
  project_directory = params['project_directory']
  database_name = params['database_name']
   
  data_gen_path = "/dbfs{}/raw/attribution_data.csv".format(project_directory)
  raw_data_path = "dbfs:{}/raw".format(project_directory)
  bronze_tbl_path = "dbfs:{}/bronze".format(project_directory)
  gold_user_journey_tbl_path = "dbfs:{}/gold_user_journey".format(project_directory)
  gold_attribution_tbl_path = "dbfs:{}/gold_attribution".format(project_directory)
  gold_ad_spend_tbl_path = "dbfs:{}/gold_ad_spend".format(project_directory)

  params = {"project_directory": project_directory,
            "database_name": database_name,
            "data_gen_path": data_gen_path,
            "raw_data_path": raw_data_path,
            "bronze_tbl_path": bronze_tbl_path,
            "gold_user_journey_tbl_path": gold_user_journey_tbl_path,
            "gold_attribution_tbl_path": gold_attribution_tbl_path,
            "gold_ad_spend_tbl_path": gold_ad_spend_tbl_path,
           }
  
  return params

# COMMAND ----------

def reset_workspace(reset_flag="False"):
  params = get_params()
  project_directory = params['project_directory']
  database_name = params['database_name']
  raw_data_path = params['raw_data_path']
  
  if reset_flag == "True":
    # Replace existing project directory with new one
    dbutils.fs.rm(project_directory, recurse=True)
    #dbutils.fs.mkdirs(project_directory)
    dbutils.fs.mkdirs(raw_data_path)
    
    # Replace existing database with new one
    spark.sql("DROP DATABASE IF EXISTS " +database_name+ " CASCADE")
    spark.sql("CREATE DATABASE " + database_name)
  return None

# COMMAND ----------


def _get_random_datetime(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def data_gen(data_gen_path):
  """
  This function will generate and write synthetic data for multi-touch 
  attribution modeling
  """
  
  # Open file and write out header row
  my_file = open(data_gen_path, "w")
  my_file.write("uid,time,interaction,channel,conversion\n")
  
  # Specify the number of unique user IDs that data will be generated for
  unique_id_count = 500000
  
  # Specify the desired conversion rate for the full data set
  total_conversion_rate_for_campaign = .14
  
  # Specify the desired channels and channel-level conversion rates
  base_conversion_rate_per_channel = {'Social Network':.3, 
                                      'Search Engine Marketing':.2, 
                                      'Google Display Network':.1, 
                                      'Affiliates':.39, 
                                      'Email':0.01}
  
  
  channel_list = list(base_conversion_rate_per_channel.keys())
  
  base_conversion_weight = tuple(base_conversion_rate_per_channel.values())
  intermediate_channel_weight = (20, 30, 15, 30, 5)
  channel_probability_weights = (20, 20, 20, 20, 20)
  
  # Generate list of random user IDs
  uid_list = []
  
  for _ in range(unique_id_count):
    uid_list.append(str(uuid.uuid4()).replace('-',''))
  
  # Generate data / user journey for each unique user ID
  for uid in uid_list:
      user_journey_end = random.choices(['impression', 'conversion'], 
                                        (1-total_conversion_rate_for_campaign, total_conversion_rate_for_campaign), k=1)[0]
      
      steps_in_customer_journey = random.choice(range(1,10))
      
      d1 = datetime.strptime('5/17/2020 1:30 PM', '%m/%d/%Y %I:%M %p')
      d2 = datetime.strptime('6/10/2020 4:50 AM', '%m/%d/%Y %I:%M %p')

      final_channel = random.choices(channel_list, base_conversion_weight , k=1)[0]
      
      for i in range(steps_in_customer_journey):
        next_step_in_user_journey = random.choices(channel_list, weights=intermediate_channel_weight, k=1)[0] 
        time = str(_get_random_datetime(d1, d2))
        d1 = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
        
        if user_journey_end == 'conversion' and i == (steps_in_customer_journey-1):
          my_file.write(uid+','+time+',conversion,'+final_channel+',1\n')
        else:
          my_file.write(uid+','+time+',impression,'+next_step_in_user_journey+',0\n')
  
  # Close file
  my_file.close() 

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *
import time

def build_bronze(raw_data_path, bronze_tbl_path):
  
  schema = spark.read.csv(raw_data_path, header=True).schema
  raw_data_df = spark.readStream.format("cloudFiles") \
              .option("cloudFiles.validateOptions", "false") \
              .option("cloudFiles.format", "csv") \
              .option("header", "true") \
              .option("cloudFiles.region", "us-west-2") \
              .option("cloudFiles.includeExistingFiles", "true") \
              .schema(schema) \
              .load(params['raw_data_path']) 
  
  raw_data_df = raw_data_df.withColumn("time", to_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss"))\
                .withColumn("conversion", col("conversion").cast("int"))

  wait_df = raw_data_df.writeStream.format("delta") \
    .trigger(once=True) \
    .option("checkpointLocation", bronze_tbl_path+"/checkpoint") \
    .start(bronze_tbl_path)
  return wait_df

# COMMAND ----------

def register_metastore(database_name, tbl_path, reset_db=False):
  if reset_db == True:
    spark.sql('DROP DATABASE IF EXISTS {} CASCADE'.format(database_name))
    spark.sql('CREATE DATABASE {}'.format(database_name))
    
  spark.sql('''
    CREATE TABLE IF NOT EXISTS  `{}`.bronze 
    USING DELTA 
    LOCATION '{}'
    '''.format(database_name, tbl_path))
  
  return None

# COMMAND ----------


