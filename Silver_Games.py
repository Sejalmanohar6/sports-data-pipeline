# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("delta")\
      .load("abfss://bronze@sportsproject.dfs.core.windows.net/games")

# COMMAND ----------

df = df.drop("_rescued_data")
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Transformation**

# COMMAND ----------

df = df.withColumn("game_date", to_timestamp(col("game_date")))
df = df.withColumn("game_year", year(col("game_date")))

# COMMAND ----------

df = df.withColumn("game_result", when(col("home_score") > col("away_score"), "Home Win")
    .when(col("home_score") < col("away_score"), "Away Win")
    .otherwise("Draw")
)

df = df.withColumn("total_goals", col("home_score") + col("away_score"))

# COMMAND ----------

df = df.withColumn("rank_flag", rank().over(Window.partitionBy("game_year").orderBy(desc("attendance"))))\
       .withColumn("row_flag", row_number().over(Window.partitionBy("game_year").orderBy(desc("attendance"))))
df.limit(10).display()

# COMMAND ----------

class windows:
  def rank(self, df):
    df_rank = df.withColumn("rank_flag", rank().over(Window.partitionBy("game_year").orderBy(desc("attendance"))))
    return df_rank

  def row_number(self, df):
    df_row_number = df.withColumn("row_flag", row_number().over(Window.partitionBy("game_year").orderBy(desc("attendance"))))
    return df_row_number

# COMMAND ----------

df_new = df

# COMMAND ----------

obj = windows()
obj.rank(df_new).limit(2).display()
obj.row_number(df_new).limit(2).display()

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .save("abfss://silver@sportsproject.dfs.core.windows.net/games")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sports_catalog.silver.games_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@sportsproject.dfs.core.windows.net/games'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sports_catalog.silver.games_silver

# COMMAND ----------

