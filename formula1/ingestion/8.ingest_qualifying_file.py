# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1- Read the JSON file using the spaek dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields= [StructField('qualifyId', IntegerType(), False),
                                       StructField('raceId', IntegerType(), True),
                                       StructField('driverId', IntegerType(), True),
                                       StructField('constructorId', IntegerType(), True),
                                       StructField('number', IntegerType(), True),
                                       StructField('position', IntegerType(), True),
                                       StructField('q1', StringType(), True),
                                       StructField('q2', StringType(), True),
                                       StructField('q3', StringType(), True)

])

# COMMAND ----------

# Read entire folder to pick up all the files at once, but alos option for multiline JSON
qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option('multiLine', True) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Rename columns and add new column
# MAGIC 1. Rename qualifyId, driverId, constructorId, and raceId
# MAGIC 2. Add ingestion date with current timestamp

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = qualifying_with_ingestion_date_df.withColumnRenamed('qualifyId', 'qualify_id') \
                        .withColumnRenamed('driverId', 'driver_id') \
                        .withColumnRenamed('constructorId', 'constructor_id') \
                        .withColumnRenamed('raceId', 'race_id') \
                        .withColumn('ingestion_date', current_timestamp()) \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- Write output to processed container in parquet format

# COMMAND ----------

# Write using function.  See notebooks 5-7 for alternative method

# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# %sql
# SELECT  race_id, COUNT(1)
# FROM f1_processed.qualifying
# GROUP BY race_id
# ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit('Success')