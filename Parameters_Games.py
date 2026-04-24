# Databricks notebook source
dataset = [
    {
        "file_name" : "games"
    },
    {
        "file_name" : "players"
    },
    {
        "file_name" : "teams"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_dataset", dataset)