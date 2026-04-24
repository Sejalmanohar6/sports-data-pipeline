# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELTA Pipeline

# COMMAND ----------

@dlt.table
def Dim_Games_Stage():
    df = spark.readStream.table("sports_catalog.silver.games_silver")

    return df

# COMMAND ----------

@dlt.table
def Dim_Teams_Stage():
    df = spark.readStream.table("sports_catalog.silver.teams_silver")

    return df

# COMMAND ----------

@dlt.table
def Dim_Regions_Stage():
    df = spark.readStream.table("sports_catalog.silver.regions_silver")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming View

# COMMAND ----------

@dlt.view

def Dim_Games_view():

    df = spark.readStream.table("Live.Dim_Games_Stage")
    return df

# COMMAND ----------

@dlt.view

def Dim_Teams_view():

    df = spark.readStream.table("Live.Dim_Teams_Stage")
    return df

# COMMAND ----------

@dlt.view

def Dim_Regions_view():

    df = spark.readStream.table("Live.Dim_Regions_Stage")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_Games

# COMMAND ----------

dlt.create_streaming_table("Dim_Games")

# COMMAND ----------

dlt.apply_changes(
    target = "Dim_Games",
    source = "Live._Dim_Games_view",
    keys = ["game_id"],
    sequence_by = "update_date",
    stored_as_scd_type = 2
)

# COMMAND ----------

df = spark.sql("select * from sports_catalog.silver.games_silver")

# COMMAND ----------

df_final = df

# COMMAND ----------

if spark.catalog.tableExists("sports_catalog.gold.Dim_Games"):

    df_final.createOrReplaceTempView("src_games")

    cols = df_final.columns
    update_clause = ", ".join([f"target.{c} = src.{c}" for c in cols])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"src.{c}" for c in cols])

    spark.sql(f"""
        MERGE INTO sports_catalog.gold.Dim_Games AS target
        USING src_games AS src
        ON target.game_id = src.game_id
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

else:
    df_final.write.mode("overwrite") \
    .format("delta") \
    .option("path", "abfss://gold@sportsproject.dfs.core.windows.net/Dim_Games") \
    .saveAsTable("sports_catalog.gold.Dim_Games")

# COMMAND ----------

dlt.create_streaming_table("Dim_Teams")

# COMMAND ----------

dlt.apply_changes(
    target = "Dim_Teams",
    source = "Live._Dim_Teams_view",
    keys = ["team_id"],
    sequence_by = "update_date",
    stored_as_scd_type = 2
)

# COMMAND ----------

dlt.create_streaming_table("Dim_Regions")

# COMMAND ----------

dlt.apply_changes(
    target = "Dim_Regions",
    source = "Live._Dim_Regions_view",
    keys = ["region_id"],
    sequence_by = "update_date",
    stored_as_scd_type = 2
)