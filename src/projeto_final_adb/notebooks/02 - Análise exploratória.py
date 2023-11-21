# Databricks notebook source
# MAGIC %md
# MAGIC #Definição do workspace

# COMMAND ----------

repository_path = "file:/Workspace/Repos/rctbatista@gmail.com/Mastercloud-Trilha-03-Onda-2/"
data_path = repository_path + "src/projeto_final_adb/data/"

# COMMAND ----------

# MAGIC %md
# MAGIC #Inicialização dos imports e dataframe

# COMMAND ----------

import  pyspark.sql.functions as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# MAGIC %md
# MAGIC #Leitura dos dados que foram previamente extraidos e tratados

# COMMAND ----------

df_spark = spark.read.parquet(data_path + "silver/spotify")
df = df_spark.toPandas()
display(df)

# COMMAND ----------

print(df.info())

# COMMAND ----------

print(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #Análise exploratória

# COMMAND ----------

# Histograma para quantidade de artistas
df['artist_count'].hist()
plt.title('Distribuição da quantidade de artistas por música')
plt.xlabel('Qtd Artistas')
plt.ylabel('Frequência')
plt.show()

# Histograma para ano de lancamento
df['released_year'].hist(bins=100)
plt.title('Distribuição do lançamento de músicas por ano')
plt.xlabel('Ano Lançamento')
plt.ylabel('Quantidade')
plt.show()

# COMMAND ----------

df.hist(bins=50, figsize=(20,15))
plt.show()

# COMMAND ----------

# Removendo músicas com ano de lançamento anterior a 2010
df_filtrado = df[(df['released_year'] >= 2010)]
display(df_filtrado)
df_filtrado.hist(bins=50, figsize=(20,15))
plt.show()

# COMMAND ----------

df_filtrado[(df_filtrado['categoria_mais_ouvidas'] == 1)]

# COMMAND ----------

df_filtrado.groupby('artist_count')['categoria_mais_ouvidas'].mean()

# COMMAND ----------

df_filtrado.groupby('released_year')['categoria_mais_ouvidas'].mean()
