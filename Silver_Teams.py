# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://bronze@sportsproject.dfs.core.windows.net/teams")

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView("teams")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION sports_catalog.bronze.win_pct_func(p_wins INT, p_losses INT)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN round(p_wins / (p_wins + p_losses) * 100, 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_id,sports_catalog.bronze.win_pct_func(wins, losses) AS win_percentage
# MAGIC FROM teams
# MAGIC ORDER BY win_percentage DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://silver@sportsproject.dfs.core.windows.net/teams")\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sports_catalog.silver.teams_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@sportsproject.dfs.core.windows.net/teams'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sports_catalog.silver.teams_silver

# COMMAND ----------

