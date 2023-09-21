# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text('p_data_source', "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1- Read the CSV using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField('raceID', IntegerType(), False),
                                  StructField('year', IntegerType(), True),
                                  StructField('round', IntegerType(), True),
                                  StructField('circuitId', IntegerType(), True),
                                  StructField('name', StringType(), True),
                                  StructField('date', DateType(), True),
                                  StructField('time', StringType(), True),
                                  StructField('url', StringType(), True),
                                  ])

# COMMAND ----------

races_df = spark.read \
    .option('header', True) \
    .schema(races_schema) \
    .csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Add ingestion data and race_timestamp to df

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn('ingestion_date', current_timestamp()) \
    .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- Select only the columns required and rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceID').alias('race_id'), col('year').alias('race_year'),
                                                   col('round'), col('circuitid').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'),
                                                   col('data_source'), col('file_date')
                                                   )

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the output to processed container in parquet format

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlsrv/processed/races

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit('Success')