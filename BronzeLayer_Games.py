# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

df_games = spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load("abfss://source@sportsproject.dfs.core.windows.net/games")
df_games.display()

# COMMAND ----------

df_players = spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load("abfss://source@sportsproject.dfs.core.windows.net/players")
df_players.display()

# COMMAND ----------

df_teams = spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load("abfss://source@sportsproject.dfs.core.windows.net/teams")
df_teams.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dynamic Access**

# COMMAND ----------

dbutils.widgets.text("file_name","")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

p_file_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .option("cloudFiles.schemaLocation", f"abfss://bronze@sportsproject.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .load(f"abfss://source@sportsproject.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"abfss://bronze@sportsproject.dfs.core.windows.net/checkpoint_{p_file_name}") \
    .option("path", f"abfss://bronze@sportsproject.dfs.core.windows.net/{p_file_name}") \
    .trigger(once=True) \
    .start()

# COMMAND ----------

df = spark.read.format("delta") \
    .load("abfss://bronze@sportsproject.dfs.core.windows.net/teams")

df.display()

# COMMAND ----------

