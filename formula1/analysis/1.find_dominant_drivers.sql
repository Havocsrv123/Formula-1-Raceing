-- Databricks notebook source
SELECT  driver_name,
        count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY total_points DESC

-- COMMAND ----------

SELECT  driver_name,
        count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY total_points DESC

-- COMMAND ----------

