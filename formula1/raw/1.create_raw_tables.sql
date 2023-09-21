-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create tables for CSV files

-- COMMAND ----------

CREATE DATABASE f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;

CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  cicruitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path'/mnt/formula1dlsrv/raw/circuits.csv', header = True);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.racess;

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path'/mnt/formula1dlsrv/raw/races.csv', header = True);

-- COMMAND ----------

select * from f1_raw.races limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructor table
-- MAGIC - Single line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING JSON
OPTIONS(path'/mnt/formula1dlsrv/raw/constructors.json')

-- COMMAND ----------

select * from f1_raw.constructors limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC - Single line JSON
-- MAGIC - Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT <forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path '/mnt/formula1dlsrv/raw/drivers.json')

-- COMMAND ----------

select * from f1_raw.drivers limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create results table
-- MAGIC - Single line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
 raceId INT,
 driverId INT,
 constructorId INT,
 number INT,
 grid INT,
 position INT,
 positionText STRING,
 positionOrder INT,
 points FLOAT,
 laps INT,
 time STRING,
 milliseconds INT,
 fastestLap INT,
 rank INT,
 fastestLapTime STRING,
 fastestLapSpeed FLOAT,
 statusId STRING
)
USING JSON
OPTIONS (PATH "/mnt/formula1dlsrv/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC - Multi line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING JSON
options (path '/mnt/formula1dlsrv/raw/pit_stops.json', multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Tables from Listg of Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap TIme Table
-- MAGIC - CSV file
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time INT,
  milliseconds INT
)
USING CSV
OPTIONS ( path '/mnt/formula1dlsrv/raw/lap_times')

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Quailifying Table
-- MAGIC - JSON file
-- MAGIC - Multiline JSON
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
)
USING JSON
OPTIONS (path '/mnt/formula1dlsrv/raw/qualifying', multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

