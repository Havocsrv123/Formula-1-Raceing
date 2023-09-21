-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL DOcumentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE demo;

-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create managed tables using Pythin
-- MAGIC 1. Create managed tables using SQL
-- MAGIC 1. Effect of dropping a managed table
-- MAGIC 1. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format('parquet').saveAsTable('demo.race_results_python', mode = "overwrite", ifNotExists = True)

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python

-- COMMAND ----------

SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format('parquet').option('path', f'{presentation_folder_path}/race_results_ext_py').saveAsTable('demo.race_results_ext_py', mode= "overwrite", ifNotExists = True)

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formula1dlsrv/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT count(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create Temp view
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Pemranent View

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW   v_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW   gv_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

CREATE OR REPLACE VIEW   pv_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

SELECT * FROM demo.pv_race_results

-- COMMAND ----------

show tables;