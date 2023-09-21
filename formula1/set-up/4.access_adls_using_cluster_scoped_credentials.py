# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. set the spark config fs.azure.key
# MAGIC 1. list files from demo container
# MAGIC 1. Read data from circuits.csv file
# MAGIC
# MAGIC ##### added access at cluster instead of going through notebook.  Removed afer test, then added role blob conttribuer in storage acount.

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlsrv.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlsrv.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

