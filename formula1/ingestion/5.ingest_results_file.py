# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

dbutils.widgets.text('p_data_source', "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# To verify variable value is correct
v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1- Read JSON file using spark df reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

#define schema for read of JSAON
results_schema = StructType(fields= [StructField('resultId', IntegerType(), False),
                                     StructField('raceId', IntegerType(), True),
                                     StructField('driverId', IntegerType(), True),
                                     StructField('constructorId', IntegerType(), True),
                                     StructField('number', IntegerType()),
                                     StructField('grid', IntegerType(), True),
                                     StructField('position', IntegerType(), True),
                                     StructField('positionText', StringType(), True),
                                     StructField('positionOrder', IntegerType(), True),
                                     StructField('points', FloatType(), True),
                                     StructField('laps', IntegerType(), True),
                                     StructField('time', StringType(), True),
                                     StructField('milliseconds', IntegerType(), True),
                                     StructField('fastestLap', IntegerType(), True),
                                     StructField('rank', IntegerType(), True),
                                     StructField('fastestLapTime', StringType(), True),
                                     StructField('fastestLapSpeed', FloatType(), True),
                                     StructField('statusId', IntegerType(), True)])

# COMMAND ----------



# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Rename columns and add new columns
# MAGIC 1. resultId renamed to result_id
# MAGIC 2. raceId renamed to race_id
# MAGIC 3. driverId renamed to driver_id
# MAGIC 4. constructorId renamed to constructor_id
# MAGIC 5. positionText renamed to position_text
# MAGIC 6. positionOrder renamed to position_order
# MAGIC 7. fastestLap renamed to fastest_lap
# MAGIC 8. fastestLapTime renamed to fastest_lap_time
# MAGIC 9. fastestLapSpeed renamed to fastest_lap_speed
# MAGIC 10. ingestion date added

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed('resultId', 'result_id') \
                                    .withColumnRenamed('raceId', 'race_id') \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('constructorId', 'constructor_id') \
                                    .withColumnRenamed('positionText', 'position_text') \
                                    .withColumnRenamed('positionOrder', 'position_order') \
                                    .withColumnRenamed('fastestLap', 'fastest_lap') \
                                    .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                                    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
                                    .withColumn('data_source', lit(v_data_source)) \
                                    .withColumn('file_date', lit(v_file_date))
                                    

# COMMAND ----------

# Using function to add timestamp

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

display(results_with_ingestion_date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- Drop unwanted columns
# MAGIC 1. Drop statusId

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Remove duplicates from df

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4- Write output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select('race_id').distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#         spark.sql(f"ALTER TABLE  f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should be 0 if all the dupicates where removed correctly
# MAGIC
# MAGIC SELECT  race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING count(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

