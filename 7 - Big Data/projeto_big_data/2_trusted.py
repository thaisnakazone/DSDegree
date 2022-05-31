# Databricks notebook source
# MAGIC %md
# MAGIC # Criando um Data Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tratamento dos dados na camada "trusted"
# MAGIC Os dados que estão na camada "raw" deverão ser tratados antes de serem salvos no formato "parquet".

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Configuração

# COMMAND ----------

username = 'thais'

northwind = f'/letscode/{username}/northwind/'

spark.sql(f'CREATE DATABASE IF NOT EXISTS letscode_{username}_trusted')
spark.sql(f'USE letscode_{username}_trusted')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Carregando os dados
# MAGIC Criando um dicionário para armazenar um DataFrame do Spark para cada tabela CSV carregada.

# COMMAND ----------

dict_df_northwind = dict()
path_raw = northwind + 'raw/'
for folder in dbutils.fs.ls(path_raw):
    for file in dbutils.fs.ls(path_raw + folder.name):
        file_without_extension = f'{file.name}'.rsplit('.', 1)[0]
        dict_df_northwind[file_without_extension] = spark.read.csv(path_raw + folder.name + file.name, header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Exploração dos dados
# MAGIC Como o objetivo do projeto não é analisar os dados, farei uma verificação simples em cada tabela, apenas para observar se o tipo dos dados está correto, se há dados nulos ou duplicados.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.1 Tabela "categories"

# COMMAND ----------

dict_df_northwind['categories'].toPandas()

# COMMAND ----------

dict_df_northwind['categories'].toPandas().info()

# COMMAND ----------

dict_df_northwind['categories'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.2 Tabela "customers"

# COMMAND ----------

dict_df_northwind['customers'].toPandas().head()

# COMMAND ----------

# Percebe-se acima que existem dados nulos em "region", mas, nas informações abaixo não há registro de dados faltantes.
# Como esse fato não impactaria muito no objetivo do projeto de Big Data, não farei o tratamento dos dados nulos.

dict_df_northwind['customers'].toPandas().info()

# COMMAND ----------

dict_df_northwind['customers'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.3 Tabela "employee_territories"

# COMMAND ----------

dict_df_northwind['employee_territories'].toPandas().head()

# COMMAND ----------

dict_df_northwind['employee_territories'].toPandas().info()

# COMMAND ----------

dict_df_northwind['employee_territories'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.4 Tabela "employees"

# COMMAND ----------

dict_df_northwind['employees'].toPandas().head()

# COMMAND ----------

dict_df_northwind['employees'].toPandas().info()

# COMMAND ----------

dict_df_northwind['employees'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.5 Tabela "order_details"

# COMMAND ----------

dict_df_northwind['order_details'].toPandas().head()

# COMMAND ----------

dict_df_northwind['order_details'].toPandas().info()

# COMMAND ----------

dict_df_northwind['order_details'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.6 Tabela "orders"

# COMMAND ----------

dict_df_northwind['orders'].toPandas().head()

# COMMAND ----------

dict_df_northwind['orders'].toPandas().info()

# COMMAND ----------

dict_df_northwind['orders'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.7 Tabela "products"

# COMMAND ----------

dict_df_northwind['products'].toPandas().head()

# COMMAND ----------

dict_df_northwind['products'].toPandas().info()

# COMMAND ----------

dict_df_northwind['products'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.8 Tabela "region"

# COMMAND ----------

dict_df_northwind['region'].toPandas().head()

# COMMAND ----------

dict_df_northwind['region'].toPandas().info()

# COMMAND ----------

dict_df_northwind['region'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.9 Tabela "shippers"

# COMMAND ----------

dict_df_northwind['shippers'].toPandas().head()

# COMMAND ----------

dict_df_northwind['shippers'].toPandas().info()

# COMMAND ----------

dict_df_northwind['shippers'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.10 Tabela "suppliers"

# COMMAND ----------

dict_df_northwind['suppliers'].toPandas().head()

# COMMAND ----------

dict_df_northwind['suppliers'].toPandas().info()

# COMMAND ----------

dict_df_northwind['suppliers'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.11 Tabela "territories"

# COMMAND ----------

dict_df_northwind['territories'].toPandas().head()

# COMMAND ----------

dict_df_northwind['territories'].toPandas().info()

# COMMAND ----------

dict_df_northwind['territories'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3.12 Tabela "us_states"

# COMMAND ----------

dict_df_northwind['us_states'].toPandas().head()

# COMMAND ----------

dict_df_northwind['us_states'].toPandas().info()

# COMMAND ----------

dict_df_northwind['us_states'].toPandas().duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Transformando os dados

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.4.1 Adicionando as colunas "order_year" e "order_month" no DF da tabela "orders"  
# MAGIC Essas colunas foram criadas com o intuito de usá-las para gerar tabelas agregadas no Data Lakehouse.

# COMMAND ----------

from pyspark.sql.functions import *

df_orders_processed = dict_df_northwind['orders'].select(
    'order_id',
    'customer_id',
    'employee_id',
    'order_date',
    year(dict_df_northwind['orders'].order_date).alias('order_year'),
    month(dict_df_northwind['orders'].order_date).alias('order_month'),
    'required_date',
    'shipped_date',
    'ship_via',
    'freight',
    'ship_name',
    'ship_address',
    'ship_city',
    'ship_region',
    'ship_postal_code',
    'ship_country')

# COMMAND ----------

display(df_orders_processed.toPandas().head())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Substituindo o DataFrame
# MAGIC Substituindo "dict_df_northwind['orders']" por "df_orders_processed" no dicionário de DataFrames.   
# MAGIC Ou seja, substituindo o DF da tabela "orders" pelo DF processado com as novas colunas ("order_year" e "order_month").

# COMMAND ----------

dict_df_northwind.update({'orders': df_orders_processed})

# COMMAND ----------

# Visualizando o DF da tabela "orders" com as duas novas colunas ("order_year" e "order_month")
dict_df_northwind['orders'].toPandas().head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Limpeza
# MAGIC Removendo o diretório "trusted", caso este notebook seja executado mais de uma vez.

# COMMAND ----------

dbutils.fs.rm(northwind + 'trusted', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 Gravando os arquivos na camada "trusted"

# COMMAND ----------

for key, value in dict_df_northwind.items():
    value.write.mode("overwrite").format("parquet").save(northwind + f'trusted/{key}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.8 Registrando as tabelas no Metastore

# COMMAND ----------

path_trusted = northwind + 'trusted/'

for folder in dbutils.fs.ls(path_trusted):
    folder_name = f'{folder.name}'.rsplit('/', 1)[0]
    spark.sql(
        f"""
        DROP TABLE IF EXISTS {folder_name}
        """
    )
    spark.sql(
        f"""
        CREATE TABLE {folder_name}
        USING PARQUET
        LOCATION "{northwind}trusted/{folder.name}"
        """
    )
    spark.sql(
        f"""
        REFRESH TABLE {folder_name}
        """
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.9 Verificando o número de registros na primeira tabela ("categories")

# COMMAND ----------

tb_trusted_categories = spark.read.table('categories')
tb_trusted_categories.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.10 Exibindo os atributos da tabela "categories", criada na camada "trusted"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED categories;