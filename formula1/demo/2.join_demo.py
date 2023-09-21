# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
    .filter('circuit_id < 70') \
    .withColumnRenamed('name', 'cicruit_name')

# COMMAND ----------

# Filtering to one year to make smaller and esier to work with in this sample code
races_df = spark.read.parquet(f'{processed_folder_path}/races').filter('race_year = 2019') \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner Join
# MAGIC ##### inner is default if not join is specified

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'inner') \
    .select(circuits_df.cicruit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer Join

# COMMAND ----------

# Left Outer
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'left') \
    .select(circuits_df.cicruit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Right Outer
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'right') \
    .select(circuits_df.cicruit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Full Outer
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'full') \
    .select(circuits_df.cicruit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Joins

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'semi') \
    .select(circuits_df.cicruit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti-Join
# MAGIC ##### opposite of semi

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'anti')

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Join

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.count()