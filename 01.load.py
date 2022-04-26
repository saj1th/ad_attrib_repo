# Databricks notebook source
# MAGIC %md Include config / util scripts

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./utils 

# COMMAND ----------

# DBTITLE 1,Get job params
params = get_params()

# COMMAND ----------

# DBTITLE 1,Generate synthetic data
data_gen(params['data_gen_path'])

# COMMAND ----------

display(spark.read.format('csv').option('header','true').load(params['raw_data_path']))

# COMMAND ----------

# DBTITLE 1,Write data to delta tables
wait_df = build_bronze(params['raw_data_path'],  params['bronze_tbl_path'])

# COMMAND ----------

wait_df.awaitTermination()
register_metastore(params['database_name'],  params['bronze_tbl_path'])

# COMMAND ----------

bronze_tbl = spark.table("{}.bronze".format(database_name))
display(bronze_tbl)

# COMMAND ----------


