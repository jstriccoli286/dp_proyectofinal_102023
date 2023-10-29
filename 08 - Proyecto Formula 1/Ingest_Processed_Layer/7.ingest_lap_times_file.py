# Databricks notebook source
# MAGIC %md ####Ingestar los archivos lap_times

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_schema = StructType(fields=[
                                        StructField("raceId",IntegerType(), False),
                                        StructField("driverId",IntegerType(), True),
                                        StructField("lap",IntegerType(), True),
                                        StructField("position",IntegerType(), True),
                                        StructField("time",StringType(), True),
                                        StructField("milliseconds",IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %md **Paso 2 - Renombrar y agregar nuevas columnas**
# MAGIC 1. Renombrar driverId y raceId
# MAGIC 1. Agregar el campo ingestion_date y current timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId","driver_id") \
                                      .withColumnRenamed("raceId", "race_id") \
                                      .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md **Paso 3 - Escribir el resultado en un archivo Parquet**

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lapTimes")