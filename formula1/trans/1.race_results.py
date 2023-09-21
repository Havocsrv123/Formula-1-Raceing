# Databricks notebook source
dbutils.widgets.text('p_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read all the data as required

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f'{processed_folder_path}/drivers') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f'{processed_folder_path}/constructors') \
    .withColumnRenamed('name', 'team')

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f'{processed_folder_path}/circuits') \
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

races_df = spark.read.format("delta").load(f'{processed_folder_path}/races') \
    .withColumnRenamed('name', 'race_name') \
    .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

results_df = spark.read.format("delta").load(f'{processed_folder_path}/results') \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed('time', 'race_time') \
    .withColumnRenamed('race_id', 'result_race_id') \
    .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join circuits to races

# COMMAND ----------

# Two styles of selecting columns, both work

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join results to all other df

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

# Old version
#final_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

#New Version to wite file and create table

# final_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_presentation.race_results')

# COMMAND ----------

# %sql
# -- Run this is getting casting error in next step
# drop table f1_presentation.race_results

# COMMAND ----------

# Version 3- Use the function

#overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

