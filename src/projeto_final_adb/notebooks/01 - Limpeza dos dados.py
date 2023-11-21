# Databricks notebook source
# MAGIC %md
# MAGIC #Definição do workspace

# COMMAND ----------

repository_path = "file:/Workspace/Repos/rctbatista@gmail.com/projeto-final-adb/"
data_path = repository_path + "src/projeto_final_adb/data/"

# COMMAND ----------

# MAGIC %md
# MAGIC #Inicialização dos imports e dataframe

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd

# COMMAND ----------

df = spark.read.table("hive_metastore.default.spotify_2023")
display(df)

# COMMAND ----------

df.write.parquet(data_path+"bronze/spotify", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze
# MAGIC ####O que deveria ser feito na etapa Bronze-Silver:
# MAGIC - Limpeza e correção de tipos
# MAGIC - Enriquecimento com possíveis novas variáveis
# MAGIC - Remoção de valores faltantes
# MAGIC - Analise exploratória

# COMMAND ----------

bronze_df = spark.read.parquet(data_path+"bronze/spotify")
display(bronze_df)

# COMMAND ----------

#bronze_df = bronze_df.withColumn("in_deezer_playlists", regexp_replace($"in_deezer_playlists", ",", ""))
bronze_df = bronze_df.withColumn('streams', F.col('streams').cast('long'))
bronze_df = bronze_df.withColumn('in_deezer_playlists', F.col('in_deezer_playlists').cast('long'))
bronze_df = bronze_df.withColumn('in_shazam_charts', F.col('in_shazam_charts').cast('long'))

# 01 = 1500000001+
# 02 = 1000000001 a 1500000000
# 03 = 500000001 a 1000000000
# 04 = 250000001 a 500000000
# 05 = 100000001 a 250000000
# 06 = 50000001 a 100000000
# 07 = 10000001 a 50000000
# 08 = 5000001 a 10000000
# 09 = 5000000-
bronze_df = bronze_df.withColumn('categoria_mais_ouvidas', 
    F.when(F.col('streams') < 5000000, 9)
    .when(F.col('streams').between(5000000, 10000000), 8)
    .when(F.col('streams').between(10000001, 50000000), 7)
    .when(F.col('streams').between(50000001, 100000000), 6)
    .when(F.col('streams').between(100000001, 250000000), 5)
    .when(F.col('streams').between(250000001, 500000000), 4)
    .when(F.col('streams').between(500000001, 1000000000), 3)
    .when(F.col('streams').between(1000000001, 1500000000), 2)
    .otherwise(1))
display(bronze_df)

# COMMAND ----------

bronze_df = bronze_df.na.drop()
bronze_df = bronze_df.orderBy("streams", ascending=False)
display(bronze_df)

# COMMAND ----------

bronze_df.write.parquet(data_path+"silver/spotify", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver

# COMMAND ----------

silver_df = spark.read.parquet(data_path+"silver/spotify")
display(silver_df)

# COMMAND ----------

silver_df.write.parquet(data_path+"gold/spotify", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold

# COMMAND ----------

gold_df = spark.read.parquet(data_path+"gold/spotify")
display(gold_df)
