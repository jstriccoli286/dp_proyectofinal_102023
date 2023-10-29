# Databricks notebook source
# MAGIC %md ###Drivers / Constructors Race Results - Presentation Layer

# COMMAND ----------

# MAGIC %md **Importación de Librerías**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "./../Includes/configuration"

# COMMAND ----------

# MAGIC %md **Leer Datasets**

# COMMAND ----------

#Results

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date") \
    .select("driver_id","constructor_id","points","position","result_race_id")

#Drivers

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality") \
    .select("driver_id","driver_name")

#Constructors

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team") \
    .select("team","constructor_id")

#Races

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date") \
    .select("circuit_id","race_year","race_name", "race_date", "race_id")

# COMMAND ----------

# MAGIC %md **Join Results - All Other DataFrames**

# COMMAND ----------

race_results_df2 = results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                              .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
                              .join(races_df, results_df.result_race_id == races_df.race_id) \
                              .select(drivers_df.driver_id, constructors_df.constructor_id, results_df.points, results_df.position, drivers_df.driver_name, constructors_df.team,races_df.race_year)

# COMMAND ----------

results_df['result_race_id']

# COMMAND ----------

final_df2 = race_results_df2.withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df2)

# COMMAND ----------

# MAGIC %md **Aloco final_df2 en la capa de presentación**

# COMMAND ----------

final_df2.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.drivers_constructors_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT      driver_name,team,sum(CASE WHEN position = 1 THEN 1 ELSE 0 END) as Wins,sum(points) as Points
# MAGIC FROM        f1_presentation.race_results
# MAGIC WHERE       race_year = (SELECT MAX(race_year) FROM f1_presentation.race_results)
# MAGIC GROUP BY    driver_name,team
# MAGIC ORDER BY    Wins desc, Points desc;