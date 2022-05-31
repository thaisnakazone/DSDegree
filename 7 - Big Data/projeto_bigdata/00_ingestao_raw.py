# Databricks notebook source
# MAGIC %md
# MAGIC # Obter Dados para Camada Raw
# MAGIC 
# MAGIC **Objective:** Neste notebook faremos ingestão de dados de uma fonte remota para a primera camada do Data Lake, o diretório `raw`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza

# COMMAND ----------

dbutils.fs.rm(northwind, recurse=True)

spark.sql(f"""
DROP TABLE IF EXISTS northwind_trusted
""")

spark.sql(f"""
DROP TABLE IF EXISTS northwind_aggregate_region
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtendo dados
# MAGIC 
# MAGIC Utilizaremos a função `retrieve_data` para obter os dados da ingestão.
# MAGIC A função recebe os argumentos:
# MAGIC 
# MAGIC - `year: int`
# MAGIC - `month: int`
# MAGIC - `rawPath: str`
# MAGIC - `is_late: bool` (optional)

# COMMAND ----------

_generate_file_handles("categories", northwind + "raw/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arquivo esperado
# MAGIC 
# MAGIC O arquivo esperado tem o nome a seguir:

# COMMAND ----------

file_categories = "northwind_categories.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrar os arquivos no diretório Raw

# COMMAND ----------

display(dbutils.fs.ls(northwind + "raw/"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercício:** Utilize a função assert para verificar a ingestão dos arquivos
# MAGIC 
# MAGIC Nota: a função print geralmente não é incluida nos códigos de produção, apenas para testar este notebook.

# COMMAND ----------

# Validar ingestão
assert "northwind_categories.csv" in [item.name for item in dbutils.fs.ls(northwind + "raw")], "File not present in Raw Path"
print("Assertion passed.")