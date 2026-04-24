# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.table('sports_catalog.bronze.regions')

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save('abfss://silver@sportsproject.dfs.core.windows.net/regions')

# COMMAND ----------

df = spark.read.format("delta")\
    .load('abfss://silver@sportsproject.dfs.core.windows.net/regions')
display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sports_catalog.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@sportsproject.dfs.core.windows.net/regions'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sports_catalog.silver.regions_silver

# COMMAND ----------

