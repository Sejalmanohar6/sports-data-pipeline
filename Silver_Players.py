# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
      .load("abfss://bronze@sportsproject.dfs.core.windows.net/players")

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.groupBy("nationality").agg(count("player_id").alias("players_count"))\
        .sort("players_count",ascending=True)\
        .limit(10).display()

# COMMAND ----------

df.groupBy("college").agg(count("player_id").alias("players_count"))\
        .sort("players_count",ascending=True)\
        .limit(10).display()

# COMMAND ----------

    df = df.withColumn("player_position", when(col("position") == "PG","Point Guard")
                    .when(col("position") == "SG","Shooting Guard")
                    .when(col("position") == "SF","Small Forward")
                    .when(col("position") == "PF","Power Forward")
                    .when(col("position") == "C","Center")               
    )

# COMMAND ----------

df.groupBy("player_position").agg(count("player_id").alias("players_per_position"))\
        .sort("player_position", ascending=True)\
        .display()

# COMMAND ----------

df = df.withColumn("player_status", when(col("is_active") == 0, "Retired")
                   .when(col("is_active") >= 1, "Active")
)

# COMMAND ----------

df.limit(15).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .save("abfss://silver@sportsproject.dfs.core.windows.net/players")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sports_catalog.silver.players_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@sportsproject.dfs.core.windows.net/players'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sports_catalog.silver.players_silver