# Databricks notebook source
# MAGIC %md Paso 1 - Leer el archivo CSV usando el Reader de Spark

# COMMAND ----------

# MAGIC %run "./../Includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
from pyspark.sql.functions import col, lit

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

circuits_df = spark.read\
              .option("header",True)\
              .schema(circuits_schema)\
              .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# MAGIC %md Paso 2 - Seleccionar la columnas necesarias

# COMMAND ----------

circuits_selected_df = circuits_df.select(
                            col("circuitId"),
                            col("circuitRef"),
                            col("name"),
                            col("location"),
                            col("country").alias("race_country"),
                            col("lat"),
                            col("lng"),
                            col("alt")
                        )

# COMMAND ----------

# MAGIC %md Paso 3 - Renombrar columnar segun se necesite

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
                      .withColumnRenamed("circuitRef","circuit_ref") \
                      .withColumnRenamed("lat","latitude") \
                      .withColumnRenamed("lng","longitude") \
                      .withColumnRenamed("alt","altitude") \
                      .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md Paso 4 - Escribir el DataFrame como resultante al Data Lake como parquet

# COMMAND ----------

circuits_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits LIMIT 100;