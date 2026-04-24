# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("init_load_flag", "0")

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.sql("select * from sports_catalog.silver.players_silver")

# COMMAND ----------

df = df.withColumn("age",year(current_date()) - year(col("birth_date")))

# COMMAND ----------

df = df.withColumn("age_group", when (col("age") < 25, "Young")
                  .when(col("age").between(25, 40), "Middle")
                  .otherwise("Seniors")      
)

# COMMAND ----------

df = df.withColumn(
    "player_salary",
    when(col("salary_usd") >= 5000000, "High")
    .when(col("salary_usd").between(2000000, 4999999), "Mid")
    .otherwise("Low")
)

# COMMAND ----------

df_teams = spark.sql("select team_id,season,wins from sports_catalog.silver.teams_silver")

df = df.join(df_teams, on="team_id", how="left")

# COMMAND ----------

df_regions = spark.sql("select region_id,region_name from sports_catalog.silver.regions_silver")

df = df.join(df_regions, "region_id", "left")

# COMMAND ----------

df = df.withColumn(
    "BMI_BodyMassIndex",
    col("weight_kg") / ((col("height_cm")/100) ** 2)
)

# COMMAND ----------

df_active_players = df.filter(col("is_active")==True)

# COMMAND ----------

df = df.dropDuplicates(subset=['player_id'])

# COMMAND ----------

df_final = df

# COMMAND ----------

df_final.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing with Slowly Changing Dimension Type - 1

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("sports_catalog.gold.Dim_Players"):

    delta_obj = DeltaTable.forPath(
        spark,
        "abfss://gold@sportsproject.dfs.core.windows.net/Dim_Players"
    )

    delta_obj.alias("target").merge(
        df_final.alias("src"),
        "target.player_id = src.player_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    df_final.write.mode("overwrite") \
    .format("delta") \
    .option("path", "abfss://gold@sportsproject.dfs.core.windows.net/Dim_Players") \
    .saveAsTable("sports_catalog.gold.Dim_Players")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sports_catalog.gold.dim_players

# COMMAND ----------

df_gold = spark.read.format("delta") \
    .load("abfss://gold@sportsproject.dfs.core.windows.net/Dim_Players")

# COMMAND ----------

df_gold.write.format("parquet") \
    .mode("overwrite") \
    .save("abfss://gold@sportsproject.dfs.core.windows.net/Dim_Players_Parquet")

# COMMAND ----------

dbutils.fs.ls("abfss://gold@sportsproject.dfs.core.windows.net/Dim_Players_Parquet")

# COMMAND ----------

