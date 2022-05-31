# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC **Definindo Caminho dos Dados**

# COMMAND ----------

# TODO
username = 'thaisnakazone'

# COMMAND ----------

northwind = f"/letscode/{username}/northwind/"

# COMMAND ----------

# MAGIC %md
# MAGIC **Configurando Banco de Dados**

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS letscode_{username}")
spark.sql(f"USE letscode_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Importanto Funções**

# COMMAND ----------

# MAGIC %run ./utilities