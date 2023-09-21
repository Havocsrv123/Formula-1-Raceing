# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_data_source', "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using magic command to run child notebook and make variables created in itavailbe in this notenook
# MAGIC ###### Each one needs to be in it's own seperate cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1- Read the CSV file using the spark dataframe reader

# COMMAND ----------

# Use util to find path of the mounts
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlsrv/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
                                     ])

# COMMAND ----------

#read the csv with the schema we created in the circuits_schema variable
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only the required columns
# MAGIC #### Multiple methods.  Extra methods commented out so they can stay here for reference

# COMMAND ----------

circuits_selected_df = circuits_df.select('circuitId','circuitRef', 'name','location', 'country', 'lat', 'lng', 'alt')

# COMMAND ----------

#another method of selecting the lines
circuits_selected_df = circuits_df.select(circuits_df['circuitId'],circuits_df['circuitRef'], 
                                          circuits_df['name'],circuits_df['location'], circuits_df['country'], circuits_df['lat'], circuits_df['lng'], circuits_df['alt'])

# COMMAND ----------

#third method of selecting the lines
from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'),col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3- Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('circuitRef', 'circuit_ref') \
    .withColumnRenamed('lat', 'latitude') \
    .withColumnRenamed('lng', 'longitude') \
    .withColumnRenamed('alt', 'altitude') \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))



# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4- Add ingetsion date to the dataframe
# MAGIC ###### Using function created in child notebook

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5- Write data to datalake as parquet file

# COMMAND ----------

# includine 'overwrite' as mode so that the notebook can be ran more than once.  Without this, it will error out with a warning that the path already exists
## Making change to existing doe so that it creates a table in the db we set up in another notebook

## old version: circuits_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/circuits')

circuits_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit('Success')