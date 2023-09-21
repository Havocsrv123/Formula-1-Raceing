# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation functions demo

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Built-in Aggregate functions

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter('race_year == 2020')

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count('*')).show()

# COMMAND ----------

demo_df.select(count('race_name')).show()

# COMMAND ----------

demo_df.select(countDistinct('race_name')).show()

# COMMAND ----------

demo_df.select(sum('points')).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points')).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points'), countDistinct('race_name')).show()


# COMMAND ----------

# Renaming the columns

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points'), countDistinct('race_name')) \
    .withColumnRenamed('sum(points)', 'total_points') \
    .withColumnRenamed('count(DISTINCT race_name)', 'number_of_races') \
    .show()

# COMMAND ----------

# Can only apply one aggregate method in this method

demo_df.groupBy('driver_name') \
    .sum('points') \
    .show()

# COMMAND ----------

# Can do multiple aggregations using the .agg() method, like in Pandas.  Also includes new method to alias columns

demo_df.groupBy('driver_name') \
    .agg(sum('points').alias("total_points"), countDistinct('race_name').alias('number_of_races')) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

demo_grouped_df = demo_df \
    .groupBy('race_year','driver_name') \
    .agg(sum('points').alias("total_points"), countDistinct('race_name').alias('number_of_races')) 

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRankSpec = Window.partitionBy('race_year').orderBy(desc('total_points'))

# COMMAND ----------

demo_grouped_df.withColumn('rank', rank().over(driverRankSpec)).show(100)