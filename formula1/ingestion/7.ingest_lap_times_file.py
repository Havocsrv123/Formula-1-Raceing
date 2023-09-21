# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1- Read the CSV file using the spaek dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields= [StructField('raceId', IntegerType(), False),
                                       StructField('driverId', IntegerType(), True),
                                       StructField('lap', IntegerType(), True),
                                       StructField('position', IntegerType(), True),
                                       StructField('time', StringType(), True),
                                       StructField('millisecond', IntegerType(), True)

])

# COMMAND ----------

# Read entire folder to pick up all the files at once 
lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Rename columns and add new column
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion date with current timestamp

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed('driverId', 'driver_id') \
                        .withColumnRenamed('raceId', 'race_id') \
                        .withColumn('ingestion_date', current_timestamp()) \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- Write output to processed container in parquet format

# COMMAND ----------

# Method 1

# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

# Method 2

# overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# %sql
# SELECT  race_id, COUNT(1)
# FROM f1_processed.lap_times
# GROUP BY race_id
# ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

