# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

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
# MAGIC #### Step 1- Read the JSON file using the spark df reader

# COMMAND ----------

# create variable for schema whem we read in the file
constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

# read the file and include teh schema we defined in previous cell
constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Drop unwanted columns from df

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- Rename columns and add ingestion date

# COMMAND ----------

constructor_dropped_df.columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed('constructorId', 'constructor_id')\
                                            .withColumnRenamed('constructorRef', 'constructor_ref') \
                                            .withColumn('ingestion_date', current_timestamp()) \
                                            .withColumn('data_source', lit(v_data_source)) \
                                            .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4- Write output to parquet file
# MAGIC ##### Edit: Switching to Delta

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit('Success')