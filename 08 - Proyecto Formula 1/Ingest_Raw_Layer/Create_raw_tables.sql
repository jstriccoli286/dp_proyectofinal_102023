-- Databricks notebook source
-- MAGIC %md ###Creamos la base de datos "f1_raw", si no existe

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md ###Creamos las tablas CSV

-- COMMAND ----------

-- MAGIC %md ####Cargamos la tabla Circuits

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
    circuitId       INT,
    circuitRef      STRING,
    name            STRING,
    location        STRING,
    country         STRING,
    lat             DOUBLE,
    lng             DOUBLE,
    alt             INT,
    url             STRING
)
USING  csv
OPTIONS (path "/mnt/saproyecto102023/raw/circuits.csv", header true);

-- COMMAND ----------

SELECT  * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md ####Cargamos la tabla Races

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
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
OPTIONS (path "/mnt/saproyecto102023/raw/races.csv", header true, delimiter ',');

-- COMMAND ----------

SELECT  * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md ###Creamos las tablas para los archivos JSON

-- COMMAND ----------

-- MAGIC %md ####Cargamos la tabla Constructors

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/saproyecto102023/raw/constructors.json");

-- COMMAND ----------

SELECT  * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md ####Cargamos la tabla Drivers

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/saproyecto102023/raw/drivers.json");

-- COMMAND ----------

SELECT  * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md ###Creamos las tablas de Resultados

-- COMMAND ----------

-- MAGIC %md ####Cargamos la tabla Pit Stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
 qualifyId INT,
 raceId INT,
 driverId INT,
 constructorId INT,
 number INT,
 position INT,
 q1 STRING,
 q2 STRING,
 q3 STRING
)
USING json
OPTIONS (path "/mnt/saproyecto102023/raw/qualifying");

-- COMMAND ----------

SELECT  * FROM f1_raw.qualifying;

-- COMMAND ----------

SELECT  * FROM f1_raw.qualifying;

-- COMMAND ----------

-- MAGIC %md ####Cargamos la tabla Pit Stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lapTimes;
CREATE TABLE IF NOT EXISTS f1_raw.lapTimes(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/saproyecto102023/raw/lap_times");

-- COMMAND ----------

SELECT  * FROM f1_raw.lapTimes;

-- COMMAND ----------

-- MAGIC %md ###Creamos la base de datos "f1_processed", si no existe

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/saproyecto102023/processed";

-- COMMAND ----------

--DROP DATABASE f1_processed CASCADE

-- COMMAND ----------

-- MAGIC %md ###Creamos la base de datos "f1_presentation", si no existe

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/saproyecto102023/presentation";

-- COMMAND ----------

--DROP DATABASE f1_presentation CASCADE