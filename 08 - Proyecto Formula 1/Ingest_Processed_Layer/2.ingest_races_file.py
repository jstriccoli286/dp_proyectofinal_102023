# Databricks notebook source
# MAGIC %md ####Ingestando el archivo races.csv

# COMMAND ----------

# MAGIC %run "./../Includes/configuration"

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer el archivo csv usando el Reader de Spark Dataframe API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import to_timestamp,concat,lit,col

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                ])

# COMMAND ----------

races_df = spark.read \
  .option("header",True) \
  .schema(races_schema) \
  .csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md **Paso 2 - Agregar el campo data_source al dataframe**

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn(
                                                "race_timestamp",
                                                to_timestamp(
                                                concat(
                                                    col('date'),
                                                    lit(' '),
                                                    col('time')
                                                ),
                                                'yyyy-MM-dd HH:mm:ss'
                                                )
                                            ) \
                                            .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md **Paso 3 - Selecciona solo las columnas requeridas**

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'),
                                                   col('year').alias('race_year'),
                                                   col('round'),
                                                   col('circuitId').alias('circuit_id'),
                                                   col('name'),
                                                   col('race_timestamp'),
                                                   col('data_source')
                                                   )

# COMMAND ----------

# MAGIC %md Paso 4 - Escribir el DataFrame al Data Lake

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL f1_processed.races

# COMMAND ----------

display(dbutils.fs.ls("/mnt/saproyecto102023/processed/races"))