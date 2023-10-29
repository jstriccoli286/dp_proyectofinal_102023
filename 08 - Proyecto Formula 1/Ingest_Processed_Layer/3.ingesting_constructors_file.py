# Databricks notebook source
# MAGIC %md ###Ingestando el archivo constructors.json file

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer el archivo JSON usando Spark DataFrame Reader**

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema) \
                            .json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md **Paso 2 - Borrar columnas no deseadas del DataFrame**

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md **Paso 3 - Renombrar columnas**
# MAGIC

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorRef", "constructor_ref") \
                                            .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md 
# MAGIC **Paso 4 - Escribir resultado a un archivo parquet**

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")