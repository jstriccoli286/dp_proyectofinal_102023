# Databricks notebook source
# MAGIC %md ###Ingestando el archivo results.json

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %md **Paso 1 -Leer el archivo JSON utilizando Spark DataFrame reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

{"resultId":1,"raceId":18,"driverId":1,"constructorId":1,"number":22,"grid":1,"position":1,"positionText":1,"positionOrder":1,"points":10,"laps":58,"time":"1:34:50.616","milliseconds":5690616,"fastestLap":39,"rank":2,"fastestLapTime":"1:27.452","fastestLapSpeed":218.3,"statusId":1}

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", IntegerType(), True),
                                    StructField("positionOrder", IntegerType(), False),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read \
  .schema(results_schema) \
  .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC **Paso 2 - Renombrar las columnas y agregar nuevas columnas**
# MAGIC 1. Renombrar la columna driverId a driver_id
# MAGIC 1. Renombrar la columna driverRef a driver_ref
# MAGIC 1. Agregar concatenación de las columnas forename y surname

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **Paso 3 - Borrar las columnas no deseadas**
# MAGIC 1. number
# MAGIC 2. grid
# MAGIC 3. positionText

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("number"), col("grid"), col("positionText"))

# COMMAND ----------

# MAGIC %md **Paso 4 - Escribir el resultado como archivo parquet**

# COMMAND ----------

results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results