# Databricks notebook source
# MAGIC %md ###Ingesting Race Results - Presentation Layer

# COMMAND ----------

# MAGIC %md **Importación de Librerías**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "./../Includes/configuration"

# COMMAND ----------

# MAGIC %md **Leer Datasets**

# COMMAND ----------

#Races

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date") \
    .select("circuit_id","race_year","race_name", "race_date", "race_id")

#Circuits

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")

#Drivers

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")

#Constructors

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

#Results

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md **Join Circuit - Races**

# COMMAND ----------

race_circuits_df = races_df.join(
    circuits_df,(races_df.circuit_id == circuits_df.circuit_id),"inner"
).select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md **Join Results - All Other DataFrames**

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "fastest_lap", "race_time", "points", "position")  \
                            .withColumn("created_date", current_timestamp())

# COMMAND ----------

# MAGIC %md **Aloco final_df en la capa de presentación**

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results