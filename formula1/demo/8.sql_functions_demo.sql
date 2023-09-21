-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT *, concat(driver_ref, '-', code) AS new_driver_ref
FROM drivers;

-- COMMAND ----------

SELECT *, split(name, ' ')
FROM drivers;

-- COMMAND ----------

SELECT *, split(name, ' ')[0] forename, split(name, ' ')[1] surname
FROM drivers;

-- COMMAND ----------

SELECT *, current_timestamp()
FROM drivers

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM_yyyy')
FROM drivers

-- COMMAND ----------

SELECT *, date_add(dob, 1)
FROM drivers

-- COMMAND ----------

select count(*)
from drivers

-- COMMAND ----------

select max(dob)
from drivers

-- COMMAND ----------

select * from drivers where dob = '2000-05-11'

-- COMMAND ----------

select nationality, count(*)
from drivers
group by nationality
order by nationality

-- COMMAND ----------

select nationality, count(*)
from drivers
group by nationality
having count(*) > 100
order by nationality

-- COMMAND ----------

select nationality, name, dob,
rank() over(partition by nationality order by dob desc) AS age_rank
from drivers
order by nationality, age_rank