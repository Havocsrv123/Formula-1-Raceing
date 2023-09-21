# Databricks notebook source
# MAGIC %md
# MAGIC ##### Using the temp view created in another notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results; -- Have to add global_temp to access global views

# COMMAND ----------

