# Databricks notebook source
# MAGIC %md
# MAGIC # Análises com Apache Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuração

# COMMAND ----------

username = 'thais'

northwind = f'/letscode/{username}/northwind/'

spark.sql(f'USE letscode_{username}_refined')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Análises

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Quais são os três produtos MENOS vendidos?

# COMMAND ----------

from pyspark.sql.functions import col, count, sum

df_ft_orders = spark.read.table('ft_orders')

prod_menos_vend = (
  df_ft_orders
  .groupby('sk_products')
  .agg((count(col('ft_orders_id'))).alias('qtde_vend'))
  .sort(('qtde_vend'))
)

display(prod_menos_vend)

# COMMAND ----------

df_dm_products = spark.read.table('dm_products')
df_dm_products.toPandas()[(df_dm_products.toPandas()['sk_products'] == 37) |
                         (df_dm_products.toPandas()['sk_products'] == 66) |
                         (df_dm_products.toPandas()['sk_products'] == 9) |
                         (df_dm_products.toPandas()['sk_products'] == 48)]

# COMMAND ----------

# MAGIC %md
# MAGIC __Resposta:__
# MAGIC Os três (ou quatro) produtos menos vendidos foram: "Gravad lax", "Louisiana Hot Spiced Okra", "Mishi Kobe Niku" e "Chocolade".

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 - Quais são os cinco clientes que mais compras fizeram?

# COMMAND ----------

client_mais_compr = (
  df_ft_orders
  .groupby('sk_customers')
  .agg((count(col('ft_orders_id'))).alias('qtde_vend'))
  .sort(['qtde_vend'], ascending=False)
)

display(client_mais_compr)

# COMMAND ----------

df_dm_customers = spark.read.table('dm_customers')
df_dm_customers.toPandas()[(df_dm_customers.toPandas()['sk_customers'] == 71) |
                         (df_dm_customers.toPandas()['sk_customers'] == 20) |
                         (df_dm_customers.toPandas()['sk_customers'] == 63) |
                         (df_dm_customers.toPandas()['sk_customers'] == 65) |
                          (df_dm_customers.toPandas()['sk_customers'] == 5)]

# COMMAND ----------

# MAGIC %md
# MAGIC __Resposta:__
# MAGIC Os cinco clientes que mais compras fizeram foram: Jose Pavarotti, Roland Mendel, Horst Kloss, Paula Wilson e Christina Berglund.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 - Quais são os cinco clientes com maior total de vendas?

# COMMAND ----------

client_maior_total_vend = (
  df_ft_orders
  .groupby('sk_customers')
  .agg((sum(col('unit_price') * col('quantity') - col('discount'))).alias('total_vend'))
  .sort(['total_vend'], ascending=False)
)

display(client_maior_total_vend)

# COMMAND ----------

df_dm_customers = spark.read.table('dm_customers')
df_dm_customers.toPandas()[(df_dm_customers.toPandas()['sk_customers'] == 63) |
                         (df_dm_customers.toPandas()['sk_customers'] == 71) |
                         (df_dm_customers.toPandas()['sk_customers'] == 20) |
                         (df_dm_customers.toPandas()['sk_customers'] == 37) |
                          (df_dm_customers.toPandas()['sk_customers'] == 34)]

# COMMAND ----------

# MAGIC %md
# MAGIC __Resposta:__
# MAGIC Os cinco clientes com o maior valor total de vendas foram: Horst Kloss, Jose Pavarotti, Roland Mendel, Patricia McKenna e Mario Pontes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 - Qual o melhor funcionário do último mês registrado? (total de vendas)

# COMMAND ----------

func_maior_total_vend = (
  df_ft_orders
  .groupby('sk_employees', 'order_year', 'order_month')
  .agg((sum(col('unit_price') * col('quantity') - col('discount'))).alias('total_vend'))
  .sort(['order_year', 'order_month', 'total_vend'], ascending=False)
)

display(func_maior_total_vend)

# COMMAND ----------

df_dm_employees = spark.read.table('dm_employees')
df_dm_employees.toPandas()[df_dm_employees.toPandas()['sk_employees'] == 1]

# COMMAND ----------

# MAGIC %md
# MAGIC __Resposta:__
# MAGIC A melhor funcionária do último mês registrado foi: Nancy Davolio.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 - Quais as regiões com menos clientes cadastrados?

# COMMAND ----------

reg_menos_client = (
  df_dm_customers
  .groupby('region')
  .agg((count(col('sk_customers'))).alias('qtde_client'))
  .sort(['qtde_client'])
)

display(reg_menos_client)

# COMMAND ----------

# MAGIC %md
# MAGIC __Resposta:__
# MAGIC As regiões com menos clientes cadastrados são: MT, NM, Québec, WY, Táchira, DF, Lara, AK, Co. Cork, ID, Isle of Wight, CA e Nueva Esparta.