# Databricks notebook source
import pyspark.sql.functions as F
import pandas as pnd

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`spotify_2023`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   min(`bpm`) as minimo_bpm,
# MAGIC   max(`bpm`) as maximo_bpm,
# MAGIC   min(`danceability_%`) as minimo_dancabilidade,
# MAGIC   max(`danceability_%`) as maximo_dancabilidade
# MAGIC FROM `hive_metastore`.`default`.`spotify_2023`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM `hive_metastore`.`default`.`spotify_2023`
# MAGIC ORDER BY `bpm` DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM `hive_metastore`.`default`.`spotify_2023`
# MAGIC ORDER BY `key` ASC, `streams` DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DISTINCT `key`
# MAGIC FROM `hive_metastore`.`default`.`spotify_2023`
# MAGIC ORDER BY `key` ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM `hive_metastore`.`default`.`spotify_2023`
# MAGIC WHERE `key` IN ("A", "A#", "B", "C#", "D", "D#", "E", "F", "F#", "G", "G#");
