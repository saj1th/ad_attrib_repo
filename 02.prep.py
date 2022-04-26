# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./utils 

# COMMAND ----------

params = get_params()

# COMMAND ----------

spark.sql("use {}".format(params['database_name']))

# COMMAND ----------

spark.sql('''
CREATE OR REPLACE TEMP VIEW user_journey_view AS
SELECT
  sub2.uid AS uid,CASE
    WHEN sub2.conversion == 1 then concat('Start > ', sub2.path, ' > Conversion')
    ELSE concat('Start > ', sub2.path, ' > Null')
  END AS path,
  sub2.first_interaction AS first_interaction,
  sub2.last_interaction AS last_interaction,
  sub2.conversion AS conversion,
  sub2.visiting_order AS visiting_order
FROM
  (
    SELECT
      sub.uid AS uid,
      concat_ws(' > ', collect_list(sub.channel)) AS path,
      element_at(collect_list(sub.channel), 1) AS first_interaction,
      element_at(collect_list(sub.channel), -1) AS last_interaction,
      element_at(collect_list(sub.conversion), -1) AS conversion,
      collect_list(sub.visit_order) AS visiting_order
    FROM
      (
        SELECT
          uid,
          channel,
          time,
          conversion,
          dense_rank() OVER (
            PARTITION BY uid
            ORDER BY
              time asc
          ) as visit_order
        FROM
          bronze
      ) AS sub
    GROUP BY
      sub.uid
  ) AS sub2;  ''')
  


# COMMAND ----------

# MAGIC %sql SELECT * FROM user_journey_view

# COMMAND ----------

spark.sql('''
  CREATE TABLE IF NOT EXISTS `{}`.gold_user_journey
  USING DELTA 
  LOCATION '{}'
  AS SELECT * from user_journey_view
  '''.format(params['database_name'], params['gold_user_journey_tbl_path']))

# COMMAND ----------

spark.sql('''OPTIMIZE gold_user_journey ZORDER BY uid''')

# COMMAND ----------

spark.sql('''
CREATE OR REPLACE TEMP VIEW attribution_view AS
SELECT
  'first_touch' AS attribution_model,
  first_interaction AS channel,
  round(count(*) / (
     SELECT COUNT(*)
     FROM gold_user_journey
     WHERE conversion = 1),2) AS attribution_percent
FROM gold_user_journey
WHERE conversion = 1
GROUP BY first_interaction
UNION
SELECT
  'last_touch' AS attribution_model,
  last_interaction AS channel,
  round(count(*) /(
      SELECT COUNT(*)
      FROM gold_user_journey
      WHERE conversion = 1),2) AS attribution_percent
FROM gold_user_journey
WHERE conversion = 1
GROUP BY last_interaction
''')

# COMMAND ----------

spark.sql('''
CREATE TABLE IF NOT EXISTS gold_attribution
USING DELTA
LOCATION '{}'
AS
SELECT * FROM attribution_view'''.format(params['gold_attribution_tbl_path']))

# COMMAND ----------

# MAGIC %sql SELECT * FROM gold_attribution

# COMMAND ----------


